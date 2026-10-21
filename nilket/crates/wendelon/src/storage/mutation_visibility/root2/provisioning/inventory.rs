//! Owner inventory events and their pure fold step; the VISOEV02 codec and fold are in `v2`.
//! Parsed inventory data only: nothing here reads files, proves provenance, or yields a root,
//! authority state or trust token.
use crate::storage::mutation_visibility::redacted_debug;
use crate::storage::mutation_visibility::root2::admin::manifest::{
	AnyAdminManifest2, ManifestProfile2, any_manifest_digest2,
};
use crate::storage::mutation_visibility::root2::admin::record::{
	BootstrapObservation2, BootstrapRecord2,
};
use crate::storage::mutation_visibility::root2::codec::hash;
use crate::storage::mutation_visibility::root2::types::Digest2;
use std::collections::BTreeSet;

pub(in crate::storage::mutation_visibility::root2) const ORDINARY_EVENTS2: u64 = 65_000;
pub(in crate::storage::mutation_visibility::root2) const TOTAL_EVENTS2: u64 = 65_536;
pub(in crate::storage::mutation_visibility::root2) const MAX_ROOTS2: usize = 4096;

const RESERVE: u8 = 1;
const APPROVE: u8 = 2;
const CONFLICT: u8 = 3;
const RETIRE: u8 = 4;
const REPAIR: u8 = 5;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) struct RootIdentity2 {
	pub generation: [u8; 16],
	pub cluster: [u8; 16],
	pub pin: Digest2,
	pub admin_key: [u8; 32],
}
redacted_debug!(RootIdentity2, "RootIdentity2");
#[derive(Clone, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) enum EventBody2 {
	Reserve {
		manifest: AnyAdminManifest2,
	},
	Approve {
		record: BootstrapRecord2,
	},
	Conflict {
		observation: BootstrapObservation2,
	},
	RetireTerminal {
		terminal: super::terminal::TerminalRecord3,
	},
	Repair {
		tail_sha256: Digest2,
		original_len: u64,
		boundary: u64,
		tail_len: u32,
	},
}
#[derive(Clone, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) struct Event2 {
	pub seq: u64,
	pub op_id: [u8; 16],
	pub identity: Option<RootIdentity2>,
	pub body: EventBody2,
}
redacted_debug!(Event2, "Event2");
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) enum RootState2 {
	Prepared,
	Bound,
	Conflicted,
	Retired,
}
#[derive(Clone)]
pub(in crate::storage::mutation_visibility::root2) struct RootEntry2 {
	pub identity: RootIdentity2,
	pub manifest: AnyAdminManifest2,
	pub state: RootState2,
	pub last_seq: u64,
	pub record: Option<BootstrapRecord2>,
	pub terminal: Option<super::terminal::TerminalRecord3>,
}
redacted_debug!(RootEntry2, "RootEntry2");
#[derive(Clone)]
pub(in crate::storage::mutation_visibility::root2) struct LogState2 {
	pub roots: Vec<RootEntry2>,
	pub last_seq: u64,
}
redacted_debug!(LogState2, "LogState2");
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) enum CorruptReason2 {
	Checksum,
	Magic,
	Version,
	UnknownOp,
	Reserved,
	ZeroOpId,
	Header,
	Payload,
	Sequence,
	DuplicateOpId,
	IdentityTaken,
	NoReservation,
	IdentityMismatch,
	Transition,
	RootCap,
	OrdinarySlot,
	RepairBounds,
	Binding,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) enum LogError2 {
	TooLong,
	Corrupt {
		seq: u64,
		reason: CorruptReason2,
	},
	Ambiguous {
		boundary: u64,
		tail_len: u32,
		tail_sha256: Digest2,
	},
}

fn corrupt(seq: u64, reason: CorruptReason2) -> LogError2 {
	LogError2::Corrupt { seq, reason }
}
fn array<const N: usize>(b: &[u8], at: usize) -> [u8; N] {
	b[at..at + N].try_into().expect("fixed event offset")
}
fn u64_at(b: &[u8], at: usize) -> u64 {
	u64::from_le_bytes(array(b, at))
}
fn zero(b: &[u8]) -> bool {
	b.iter().all(|&x| x == 0)
}

/// The pin binds the whole manifest (governance included); it is never the Admission digest.
fn identity_of(m: &AnyAdminManifest2) -> RootIdentity2 {
	let bytes = m.encode().expect("decoded manifest re-encodes");
	let admission = m.admission();
	RootIdentity2 {
		generation: admission.generation,
		cluster: admission.cluster,
		pin: any_manifest_digest2(&bytes),
		admin_key: admission.admin_key,
	}
}
fn shares_component(a: &RootIdentity2, b: &RootIdentity2) -> bool {
	a.generation == b.generation
		|| a.cluster == b.cluster
		|| a.pin == b.pin
		|| a.admin_key == b.admin_key
}

// Semantic step for one locally valid event at 1-based position `at`.
fn apply(
	state: &mut LogState2,
	ops: &mut BTreeSet<[u8; 16]>,
	e: Event2,
	at: u64,
) -> Result<(), LogError2> {
	let bad = |r| corrupt(at, r);
	if e.seq != at {
		return Err(bad(CorruptReason2::Sequence));
	}
	let ordinary = !matches!(e.body, EventBody2::Repair { .. });
	if (ordinary && e.seq > ORDINARY_EVENTS2) || e.seq > TOTAL_EVENTS2 {
		return Err(bad(CorruptReason2::OrdinarySlot));
	}
	if !ops.insert(e.op_id) {
		return Err(bad(CorruptReason2::DuplicateOpId));
	}
	let Some(identity) = e.identity else {
		state.last_seq = e.seq;
		return Ok(());
	};
	if let EventBody2::Reserve { manifest } = &e.body {
		if identity_of(manifest) != identity {
			return Err(bad(CorruptReason2::Binding));
		}
		if state
			.roots
			.iter()
			.any(|r| shares_component(&r.identity, &identity))
		{
			return Err(bad(CorruptReason2::IdentityTaken));
		}
		if state.roots.len() >= MAX_ROOTS2 {
			return Err(bad(CorruptReason2::RootCap));
		}
		let at_index = state.roots.partition_point(|r| {
			(r.identity.generation, r.identity.cluster) < (identity.generation, identity.cluster)
		});
		state.roots.insert(
			at_index,
			RootEntry2 {
				identity,
				manifest: manifest.clone(),
				state: RootState2::Prepared,
				last_seq: e.seq,
				record: None,
				terminal: None,
			},
		);
		state.last_seq = e.seq;
		return Ok(());
	}
	let Some(index) = state.roots.iter().position(|r| r.identity == identity) else {
		let same_key = state.roots.iter().any(|r| {
			(r.identity.generation, r.identity.cluster) == (identity.generation, identity.cluster)
		});
		return Err(bad(if same_key {
			CorruptReason2::IdentityMismatch
		} else {
			CorruptReason2::NoReservation
		}));
	};
	let root = &mut state.roots[index];
	match &e.body {
		EventBody2::Approve { record } => {
			if root.state != RootState2::Prepared {
				return Err(bad(CorruptReason2::Transition));
			}
			let r = &record.identity;
			if (r.generation, r.cluster, r.pin)
				!= (identity.generation, identity.cluster, identity.pin)
				|| r.initial_epoch != root.manifest.admission().initial_epoch
				|| record.inventory_seq != e.seq
			{
				return Err(bad(CorruptReason2::Binding));
			}
			root.state = RootState2::Bound;
			root.record = Some(*record);
		}
		EventBody2::Conflict { observation } => {
			if root.state != RootState2::Prepared {
				return Err(bad(CorruptReason2::Transition));
			}
			let o = &observation.identity;
			if (o.generation, o.cluster, o.pin, o.initial_epoch)
				== (
					identity.generation,
					identity.cluster,
					identity.pin,
					root.manifest.admission().initial_epoch,
				) {
				return Err(bad(CorruptReason2::Binding));
			}
			root.state = RootState2::Conflicted;
		}
		EventBody2::RetireTerminal { terminal } => {
			use super::terminal::{TerminalObservation3, encode_terminal3, observation_digest3};
			if root.manifest.profile() != ManifestProfile2::Three
				|| root.state != RootState2::Bound
				|| terminal.inventory_seq != e.seq
				|| encode_terminal3(terminal).is_err()
			{
				return Err(bad(CorruptReason2::Transition));
			}
			let record = root.record.ok_or_else(|| bad(CorruptReason2::Binding))?;
			let observation = TerminalObservation3 {
				identity: record.identity,
				original_admin_key: root.identity.admin_key,
				at: terminal.at,
				fingerprint: terminal.fingerprint,
				command_digest: terminal.command_digest,
				state_digest: terminal.state_digest,
			};
			if observation_digest3(&observation).map_err(|_| bad(CorruptReason2::Binding))?
				!= terminal.observation_digest
			{
				return Err(bad(CorruptReason2::Binding));
			}
			root.state = RootState2::Retired;
			root.record = None;
			root.terminal = Some(*terminal);
		}
		EventBody2::Reserve { .. } | EventBody2::Repair { .. } => unreachable!("handled above"),
	}
	root.last_seq = e.seq;
	state.last_seq = e.seq;
	Ok(())
}

pub(super) mod v2;
