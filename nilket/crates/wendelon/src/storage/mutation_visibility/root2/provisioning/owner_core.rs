//! Pure owner-core decisions and the repair-evidence codec (directory v2, in `v2`). Nothing here reads files, takes a lock,
//! syncs, or yields a root, authority state or trust token. A decision is not a durability claim,
//! and evidence checksums are integrity bindings, not authentication.
use super::inventory::{
	Event2, EventBody2, LogError2, LogState2, MAX_ROOTS2, ORDINARY_EVENTS2, RootEntry2,
	RootIdentity2, RootState2, TOTAL_EVENTS2,
};
use crate::storage::mutation_visibility::redacted_debug;
use crate::storage::mutation_visibility::root2::admin::manifest::{
	AnyAdminManifest2, ManifestBytes2, any_manifest_digest2,
};
use crate::storage::mutation_visibility::root2::admin::record::{
	BootstrapIdentity2, BootstrapObservation2, BootstrapRecord2,
};
use crate::storage::mutation_visibility::root2::codec::hash;
use crate::storage::mutation_visibility::root2::types::Digest2;

// The VISREP04 evidence frame (`v2`): fixed header and trailer around the torn tail.
const HEADER: usize = 124;
const TRAILER: usize = 32;
pub(in crate::storage::mutation_visibility::root2) const MAX_REPAIR_ENTRIES2: usize = 1024;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) struct OpId2([u8; 16]);
redacted_debug!(OpId2, "OpId2");
#[derive(Clone)]
pub(in crate::storage::mutation_visibility::root2) enum OwnerRequest2 {
	Reserve {
		manifest: AnyAdminManifest2,
	},
	Approve {
		identity: RootIdentity2,
		observation: BootstrapObservation2,
		confirmed: RootIdentity2,
	},
	/// Terminal retirement only: the committed retirement's observation is required.
	Retire {
		identity: RootIdentity2,
		terminal: super::terminal::TerminalObservation3,
	},
}
redacted_debug!(OwnerRequest2, "OwnerRequest2");
#[derive(Clone, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) enum RequestKey2 {
	Reserve {
		manifest: ManifestBytes2,
	},
	Approve {
		identity: RootIdentity2,
		observation: BootstrapIdentity2,
	},
	Retire {
		identity: RootIdentity2,
	},
}
redacted_debug!(RequestKey2, "RequestKey2");
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) enum EventOp2 {
	Reserve,
	Approve,
	Conflict,
	Retire,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) struct EventRef2 {
	pub op: EventOp2,
	pub seq: u64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) enum OwnerError2 {
	ProfileUnsupported,
	RetirementMismatch,
	RetirementField(super::super::admin::record::BootstrapField2),
	NotBound,
	Log(LogError2),
	Codec,
	BadConfirmation,
	OpIdReused { seq: u64 },
	AlreadyReserved,
	IdentityTaken,
	IdentityMismatch,
	NoSuchRoot,
	NotPrepared,
	BoundConflict,
	RootCap,
	LogFull,
	Inconsistent,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) enum Decision2 {
	Append(Event2),
	NoOp(RootState2),
	Retry {
		original: EventRef2,
		current: RootState2,
	},
	Refuse(OwnerError2),
}
#[derive(Clone, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) struct RepairEvidence2 {
	pub prefix_sha256: Digest2,
	pub boundary: u64,
	pub original_len: u64,
	pub tail_sha256: Digest2,
	pub repair_op_id: [u8; 16],
	pub repair_seq: u64,
	pub tail: Vec<u8>,
}
redacted_debug!(RepairEvidence2, "RepairEvidence2");
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) enum EvidenceError2 {
	Name,
	Length,
	Magic,
	Version,
	Reserved,
	TailLength,
	Trailer,
	TailHash,
	Bounds,
	Sequence,
	Context,
	OpId,
	Event,
	NotAmbiguous,
	OpIdCollision,
	Format,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) enum RepairStage2 {
	NotTruncated,
	PartialRepair,
	Truncated,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) enum Classification2 {
	Clean {
		last_seq: u64,
	},
	Pending {
		context: Digest2,
		stage: RepairStage2,
	},
	Unclaimed {
		boundary: u64,
		tail_len: u32,
		tail_sha256: Digest2,
		prefix_sha256: Digest2,
	},
	Conflicting {
		count: u32,
	},
	Log(LogError2),
	EvidenceInvalid {
		index: u32,
		error: EvidenceError2,
	},
	RepairDirFull,
	RepairDirForeign {
		index: u32,
	},
	RepairOpIdCollision {
		context: Digest2,
	},
}

/// None for the all-zero id, which the event codec refuses.
pub(in crate::storage::mutation_visibility::root2) fn op_id2(bytes: [u8; 16]) -> Option<OpId2> {
	(bytes != [0; 16]).then_some(OpId2(bytes))
}

fn nonzero(i: &RootIdentity2) -> bool {
	i.generation != [0; 16] && i.cluster != [0; 16] && i.pin != [0; 32] && i.admin_key != [0; 32]
}
fn shares_component(a: &RootIdentity2, b: &RootIdentity2) -> bool {
	a.generation == b.generation
		|| a.cluster == b.cluster
		|| a.pin == b.pin
		|| a.admin_key == b.admin_key
}

/// Local, state-independent validation: a bad Approve confirmation first, then every typed
/// field through its accepted encoder. The key is what a retry must reproduce.
pub(in crate::storage::mutation_visibility::root2) fn request_key2(
	req: &OwnerRequest2,
) -> Result<RequestKey2, OwnerError2> {
	match req {
		OwnerRequest2::Reserve { manifest } => Ok(RequestKey2::Reserve {
			manifest: manifest.encode().map_err(|_| OwnerError2::Codec)?,
		}),
		OwnerRequest2::Approve {
			identity,
			observation,
			confirmed,
		} => {
			if confirmed != identity {
				return Err(OwnerError2::BadConfirmation);
			}
			if !nonzero(identity) || observation.encode().is_err() {
				return Err(OwnerError2::Codec);
			}
			Ok(RequestKey2::Approve {
				identity: *identity,
				observation: observation.identity,
			})
		}
		OwnerRequest2::Retire { identity, .. } => {
			if !nonzero(identity) {
				return Err(OwnerError2::Codec);
			}
			Ok(RequestKey2::Retire {
				identity: *identity,
			})
		}
	}
}

/// The request an event came from, recovered from the event alone. An Approve request may have
/// produced either an Approve or a Conflict event. Repair events come from no request.
pub(in crate::storage::mutation_visibility::root2) fn event_request_key2(
	event: &Event2,
) -> Option<(EventOp2, RequestKey2)> {
	let identity = event.identity?;
	match &event.body {
		EventBody2::Reserve { manifest } => Some((
			EventOp2::Reserve,
			RequestKey2::Reserve {
				manifest: manifest.encode().ok()?,
			},
		)),
		EventBody2::Approve { record } => Some((
			EventOp2::Approve,
			RequestKey2::Approve {
				identity,
				observation: record.identity,
			},
		)),
		EventBody2::Conflict { observation } => Some((
			EventOp2::Conflict,
			RequestKey2::Approve {
				identity,
				observation: observation.identity,
			},
		)),
		EventBody2::RetireTerminal { .. } => {
			Some((EventOp2::Retire, RequestKey2::Retire { identity }))
		}
		EventBody2::Repair { .. } => None,
	}
}

pub(super) mod v2;
