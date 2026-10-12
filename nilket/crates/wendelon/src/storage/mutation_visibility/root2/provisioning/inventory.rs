//! Owner inventory event codec and pure fold. Parsed inventory data only: nothing here reads
//! files, proves provenance, or yields a root, authority state or trust token.
use crate::storage::mutation_visibility::redacted_debug;
use crate::storage::mutation_visibility::root2::admin::errors::AdminManifestError2;
use crate::storage::mutation_visibility::root2::admin::manifest::{
	ADMIN_MANIFEST_BYTES2, ADMIN_MANIFEST_P2_BYTES2, AdminManifest2, AdminManifestTwo2,
	AnyAdminManifest2, ManifestProfile2, any_manifest_digest2, manifest_profile2,
};
use crate::storage::mutation_visibility::root2::admin::record::{
	BOOTSTRAP_RECORD_BYTES2, BootstrapObservation2, BootstrapRecord2,
};
use crate::storage::mutation_visibility::root2::codec::hash;
use crate::storage::mutation_visibility::root2::types::Digest2;
use std::collections::BTreeSet;

pub(in crate::storage::mutation_visibility::root2) const EVENT_BYTES2: usize = 384;
pub(in crate::storage::mutation_visibility::root2) const ORDINARY_EVENTS2: u64 = 65_000;
pub(in crate::storage::mutation_visibility::root2) const TOTAL_EVENTS2: u64 = 65_536;
pub(in crate::storage::mutation_visibility::root2) const MAX_ROOTS2: usize = 4096;
pub(in crate::storage::mutation_visibility::root2) const MAX_TAIL2: usize = 2 * EVENT_BYTES2 - 1;
pub(in crate::storage::mutation_visibility::root2) const MAX_LOG_BYTES2: usize =
	TOTAL_EVENTS2 as usize * EVENT_BYTES2 + EVENT_BYTES2 - 1;

const MAGIC: &[u8; 8] = b"VISIEV02";
const VERSION: u16 = 1;
/// Written only for a profile-2 Reserve; every other event stays version 1.
const VERSION_TWO: u16 = 2;
const RESERVE: u8 = 1;
const APPROVE: u8 = 2;
const CONFLICT: u8 = 3;
const RETIRE: u8 = 4;
const REPAIR: u8 = 5;
const PAYLOAD: usize = 132;
const CHECKED: usize = 352;

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
	Retire,
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

pub(in crate::storage::mutation_visibility::root2) fn encode_event2(
	event: &Event2,
) -> Result<[u8; EVENT_BYTES2], LogError2> {
	let bad = |r| corrupt(event.seq, r);
	let mut b = [0u8; EVENT_BYTES2];
	b[0..8].copy_from_slice(MAGIC);
	let version = match &event.body {
		EventBody2::Reserve { manifest } if manifest.profile() == ManifestProfile2::Two => {
			VERSION_TWO
		}
		_ => VERSION,
	};
	b[8..10].copy_from_slice(&version.to_le_bytes());
	let op = match event.body {
		EventBody2::Reserve { .. } => RESERVE,
		EventBody2::Approve { .. } => APPROVE,
		EventBody2::Conflict { .. } => CONFLICT,
		EventBody2::Retire => RETIRE,
		EventBody2::Repair { .. } => REPAIR,
	};
	b[10] = op;
	b[12..20].copy_from_slice(&event.seq.to_le_bytes());
	b[20..36].copy_from_slice(&event.op_id);
	if let Some(i) = &event.identity {
		b[36..52].copy_from_slice(&i.generation);
		b[52..68].copy_from_slice(&i.cluster);
		b[68..100].copy_from_slice(&i.pin);
		b[100..132].copy_from_slice(&i.admin_key);
	}
	let p = &mut b[PAYLOAD..PAYLOAD + BOOTSTRAP_RECORD_BYTES2];
	match &event.body {
		EventBody2::Reserve { manifest } => {
			let bytes = manifest
				.encode()
				.map_err(|_| bad(CorruptReason2::Payload))?;
			p[..bytes.as_slice().len()].copy_from_slice(bytes.as_slice());
		}
		EventBody2::Approve { record } => {
			p.copy_from_slice(&record.encode().map_err(|_| bad(CorruptReason2::Payload))?)
		}
		EventBody2::Conflict { observation } => p.copy_from_slice(
			&observation
				.encode()
				.map_err(|_| bad(CorruptReason2::Payload))?,
		),
		EventBody2::Retire => {}
		EventBody2::Repair {
			tail_sha256,
			original_len,
			boundary,
			tail_len,
		} => {
			p[0..32].copy_from_slice(tail_sha256);
			p[32..40].copy_from_slice(&original_len.to_le_bytes());
			p[40..48].copy_from_slice(&boundary.to_le_bytes());
			p[48..52].copy_from_slice(&tail_len.to_le_bytes());
		}
	}
	let sum = hash(b"", &b[..CHECKED]);
	b[CHECKED..].copy_from_slice(&sum);
	// The encoder emits only what the decoder accepts.
	let decoded = decode_event2(&b)?;
	if decoded != *event {
		return Err(bad(CorruptReason2::Payload));
	}
	Ok(b)
}

/// Local checks only, in the fixed order: checksum, magic, version (1 or 2), op, version 2 only
/// for Reserve, reserved bytes, nonzero op id, header zero iff Repair, typed payload and zero
/// fill, Repair bounds. A profile-2 payload in a version-1 event stays `Payload`.
pub(in crate::storage::mutation_visibility::root2) fn decode_event2(
	bytes: &[u8],
) -> Result<Event2, LogError2> {
	if bytes.len() != EVENT_BYTES2 {
		return Err(corrupt(0, CorruptReason2::Payload));
	}
	let seq = u64_at(bytes, 12);
	let bad = |r| corrupt(seq, r);
	if hash(b"", &bytes[..CHECKED]) != array::<32>(bytes, CHECKED) {
		return Err(bad(CorruptReason2::Checksum));
	}
	if &bytes[0..8] != MAGIC {
		return Err(bad(CorruptReason2::Magic));
	}
	let version = u16::from_le_bytes(array(bytes, 8));
	if version != VERSION && version != VERSION_TWO {
		return Err(bad(CorruptReason2::Version));
	}
	let op = bytes[10];
	if !(RESERVE..=REPAIR).contains(&op) {
		return Err(bad(CorruptReason2::UnknownOp));
	}
	if version == VERSION_TWO && op != RESERVE {
		return Err(bad(CorruptReason2::Version));
	}
	if bytes[11] != 0 || !zero(&bytes[340..CHECKED]) {
		return Err(bad(CorruptReason2::Reserved));
	}
	let op_id: [u8; 16] = array(bytes, 20);
	if op_id == [0; 16] {
		return Err(bad(CorruptReason2::ZeroOpId));
	}
	let header = &bytes[36..PAYLOAD];
	let identity = if op == REPAIR {
		if !zero(header) {
			return Err(bad(CorruptReason2::Header));
		}
		None
	} else {
		let i = RootIdentity2 {
			generation: array(bytes, 36),
			cluster: array(bytes, 52),
			pin: array(bytes, 68),
			admin_key: array(bytes, 100),
		};
		if i.generation == [0; 16]
			|| i.cluster == [0; 16]
			|| i.pin == [0; 32]
			|| i.admin_key == [0; 32]
		{
			return Err(bad(CorruptReason2::Header));
		}
		Some(i)
	};
	let p = &bytes[PAYLOAD..PAYLOAD + BOOTSTRAP_RECORD_BYTES2];
	let body = match op {
		RESERVE if version == VERSION => {
			if !zero(&p[ADMIN_MANIFEST_BYTES2..]) {
				return Err(bad(CorruptReason2::Payload));
			}
			let manifest = AdminManifest2::decode(&p[..ADMIN_MANIFEST_BYTES2])
				.map_err(|_| bad(CorruptReason2::Payload))?;
			EventBody2::Reserve {
				manifest: AnyAdminManifest2::One(manifest),
			}
		}
		RESERVE => {
			let m = &p[..ADMIN_MANIFEST_P2_BYTES2];
			if !zero(&p[ADMIN_MANIFEST_P2_BYTES2..]) {
				return Err(bad(CorruptReason2::Payload));
			}
			match manifest_profile2(m) {
				Ok(ManifestProfile2::Two) => {}
				Ok(ManifestProfile2::One) | Err(AdminManifestError2::ProfileLength) => {
					return Err(bad(CorruptReason2::Version));
				}
				Err(_) => return Err(bad(CorruptReason2::Payload)),
			}
			let manifest =
				AdminManifestTwo2::decode(m).map_err(|_| bad(CorruptReason2::Payload))?;
			EventBody2::Reserve {
				manifest: AnyAdminManifest2::Two(manifest),
			}
		}
		APPROVE => EventBody2::Approve {
			record: BootstrapRecord2::decode(p).map_err(|_| bad(CorruptReason2::Payload))?,
		},
		CONFLICT => EventBody2::Conflict {
			observation: BootstrapObservation2::decode(p)
				.map_err(|_| bad(CorruptReason2::Payload))?,
		},
		RETIRE => {
			if !zero(p) {
				return Err(bad(CorruptReason2::Payload));
			}
			EventBody2::Retire
		}
		_ => {
			if !zero(&p[52..]) {
				return Err(bad(CorruptReason2::Payload));
			}
			let tail_sha256: Digest2 = array(p, 0);
			let original_len = u64_at(p, 32);
			let boundary = u64_at(p, 40);
			let tail_len = u32::from_le_bytes(array(p, 48));
			let expected_boundary = seq
				.checked_sub(1)
				.and_then(|n| n.checked_mul(EVENT_BYTES2 as u64));
			let expected_len = boundary.checked_add(u64::from(tail_len));
			if expected_boundary != Some(boundary)
				|| expected_len != Some(original_len)
				|| tail_len == 0
				|| tail_len as usize > MAX_TAIL2
			{
				return Err(bad(CorruptReason2::RepairBounds));
			}
			EventBody2::Repair {
				tail_sha256,
				original_len,
				boundary,
				tail_len,
			}
		}
	};
	Ok(Event2 {
		seq,
		op_id,
		identity,
		body,
	})
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
		EventBody2::Retire => {
			if root.state == RootState2::Retired {
				return Err(bad(CorruptReason2::Transition));
			}
			root.state = RootState2::Retired;
			root.record = None;
		}
		EventBody2::Reserve { .. } | EventBody2::Repair { .. } => unreachable!("handled above"),
	}
	root.last_seq = e.seq;
	state.last_seq = e.seq;
	Ok(())
}

/// Folds a complete log image with deterministic error precedence: every whole event is
/// validated in order; only a checksum-invalid final whole event or trailing partial bytes
/// after a fully valid prefix are Ambiguous. Evidence for a Repair is not checked here.
pub(in crate::storage::mutation_visibility::root2) fn fold_events2(
	log: &[u8],
) -> Result<LogState2, LogError2> {
	if log.len() > MAX_LOG_BYTES2 {
		return Err(LogError2::TooLong);
	}
	let whole = log.len() / EVENT_BYTES2;
	let mut state = LogState2 {
		roots: Vec::new(),
		last_seq: 0,
	};
	let mut ops = BTreeSet::new();
	let ambiguous = |boundary: usize| LogError2::Ambiguous {
		boundary: boundary as u64,
		tail_len: (log.len() - boundary) as u32,
		tail_sha256: hash(b"", &log[boundary..]),
	};
	for i in 0..whole {
		let at = i as u64 + 1;
		let bytes = &log[i * EVENT_BYTES2..(i + 1) * EVENT_BYTES2];
		let event = match decode_event2(bytes) {
			Ok(e) => e,
			Err(LogError2::Corrupt {
				reason: CorruptReason2::Checksum,
				..
			}) if i + 1 == whole => {
				return Err(ambiguous(i * EVENT_BYTES2));
			}
			Err(LogError2::Corrupt { reason, .. }) => return Err(corrupt(at, reason)),
			Err(other) => return Err(other),
		};
		apply(&mut state, &mut ops, event, at)?;
	}
	if log.len() % EVENT_BYTES2 != 0 {
		return Err(ambiguous(whole * EVENT_BYTES2));
	}
	Ok(state)
}
