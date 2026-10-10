//! Replica inventory snapshot codec. Codec validity is not provenance (the file pin, delivered
//! out of band, is) and not readiness.
use super::inventory::{LogState2, MAX_ROOTS2, RootIdentity2, RootState2};
use crate::storage::mutation_visibility::redacted_debug;
use crate::storage::mutation_visibility::root2::admin::key::check_enrollable_admin_key2;
use crate::storage::mutation_visibility::root2::admin::record::{
	BOOTSTRAP_RECORD_BYTES2, BootstrapRecord2,
};
use crate::storage::mutation_visibility::root2::codec::hash;

const MAGIC: &[u8; 8] = b"VISINV02";
const VERSION: u16 = 1;
const HEADER: usize = 22;
const ENTRY: usize = 320;
const TRAILER: usize = 32;

#[derive(Clone, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) struct SnapshotEntry2 {
	pub identity: RootIdentity2,
	pub state: RootState2,
	pub last_seq: u64,
	pub record: Option<BootstrapRecord2>,
}
redacted_debug!(SnapshotEntry2, "SnapshotEntry2");
#[derive(Clone, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) struct InventorySnapshot2 {
	pub export_seq: u64,
	pub entries: Vec<SnapshotEntry2>,
}
redacted_debug!(InventorySnapshot2, "InventorySnapshot2");
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) enum SnapshotError2 {
	Length,
	Magic,
	Version,
	Count,
	Order,
	Duplicate,
	ZeroIdentity,
	Key,
	Reserved,
	State,
	Sequence,
	Record,
	Binding,
	Trailer,
	NotCanonical,
}

fn state_byte(s: RootState2) -> u8 {
	match s {
		RootState2::Prepared => 1,
		RootState2::Bound => 2,
		RootState2::Conflicted => 3,
		RootState2::Retired => 4,
	}
}
fn array<const N: usize>(b: &[u8], at: usize) -> [u8; N] {
	b[at..at + N].try_into().expect("fixed snapshot offset")
}

// Every invariant a snapshot must satisfy, checked identically for encode and decode.
fn validate(s: &InventorySnapshot2) -> Result<(), SnapshotError2> {
	if s.entries.len() > MAX_ROOTS2 {
		return Err(SnapshotError2::Count);
	}
	if s.export_seq == 0 && !s.entries.is_empty() {
		return Err(SnapshotError2::Sequence);
	}
	for (i, e) in s.entries.iter().enumerate() {
		let id = &e.identity;
		if id.generation == [0; 16]
			|| id.cluster == [0; 16]
			|| id.pin == [0; 32]
			|| id.admin_key == [0; 32]
		{
			return Err(SnapshotError2::ZeroIdentity);
		}
		check_enrollable_admin_key2(&id.admin_key).map_err(|_| SnapshotError2::Key)?;
		if e.last_seq == 0 || e.last_seq > s.export_seq {
			return Err(SnapshotError2::Sequence);
		}
		match (e.state, &e.record) {
			(RootState2::Bound, Some(r)) => {
				let b = &r.identity;
				if (b.generation, b.cluster, b.pin) != (id.generation, id.cluster, id.pin)
					|| r.inventory_seq != e.last_seq
				{
					return Err(SnapshotError2::Binding);
				}
			}
			(RootState2::Bound, None) | (_, Some(_)) => return Err(SnapshotError2::Record),
			(_, None) => {}
		}
		if i > 0 {
			let p = &s.entries[i - 1].identity;
			if (p.generation, p.cluster) >= (id.generation, id.cluster) {
				return Err(SnapshotError2::Order);
			}
		}
	}
	for (i, a) in s.entries.iter().enumerate() {
		for b in &s.entries[i + 1..] {
			let (x, y) = (&a.identity, &b.identity);
			if x.generation == y.generation
				|| x.cluster == y.cluster
				|| x.pin == y.pin
				|| x.admin_key == y.admin_key
			{
				return Err(SnapshotError2::Duplicate);
			}
		}
	}
	Ok(())
}
fn encode(s: &InventorySnapshot2) -> Result<Vec<u8>, SnapshotError2> {
	validate(s)?;
	let len = HEADER + s.entries.len() * ENTRY + TRAILER;
	let mut b = Vec::with_capacity(len);
	b.extend(MAGIC);
	b.extend(VERSION.to_le_bytes());
	b.extend((s.entries.len() as u32).to_le_bytes());
	b.extend(s.export_seq.to_le_bytes());
	for e in &s.entries {
		b.extend(e.identity.generation);
		b.extend(e.identity.cluster);
		b.extend(e.identity.pin);
		b.extend(e.identity.admin_key);
		b.push(state_byte(e.state));
		b.extend([0u8; 7]);
		b.extend(e.last_seq.to_le_bytes());
		match &e.record {
			Some(r) => b.extend(r.encode().map_err(|_| SnapshotError2::Record)?),
			None => b.extend([0u8; BOOTSTRAP_RECORD_BYTES2]),
		}
	}
	let sum = hash(b"", &b);
	b.extend(sum);
	Ok(b)
}
/// The snapshot of a folded owner log, exported at its last sequence.
pub(in crate::storage::mutation_visibility::root2) fn encode_snapshot2(
	state: &LogState2,
) -> Result<Vec<u8>, SnapshotError2> {
	// The cap is settled before any entry is copied.
	if state.roots.len() > MAX_ROOTS2 {
		return Err(SnapshotError2::Count);
	}
	encode(&InventorySnapshot2 {
		export_seq: state.last_seq,
		entries: state
			.roots
			.iter()
			.map(|r| SnapshotEntry2 {
				identity: r.identity,
				state: r.state,
				last_seq: r.last_seq,
				record: r.record,
			})
			.collect(),
	})
}
pub(in crate::storage::mutation_visibility::root2) fn decode_snapshot2(
	bytes: &[u8],
) -> Result<InventorySnapshot2, SnapshotError2> {
	// Lengths are settled before anything is allocated.
	if bytes.len() < HEADER + TRAILER {
		return Err(SnapshotError2::Length);
	}
	if &bytes[0..8] != MAGIC {
		return Err(SnapshotError2::Magic);
	}
	if u16::from_le_bytes(array(bytes, 8)) != VERSION {
		return Err(SnapshotError2::Version);
	}
	let count = u32::from_le_bytes(array(bytes, 10)) as usize;
	if count > MAX_ROOTS2 {
		return Err(SnapshotError2::Count);
	}
	if bytes.len() != HEADER + count * ENTRY + TRAILER {
		return Err(SnapshotError2::Length);
	}
	let body = bytes.len() - TRAILER;
	if hash(b"", &bytes[..body]) != array::<32>(bytes, body) {
		return Err(SnapshotError2::Trailer);
	}
	let export_seq = u64::from_le_bytes(array(bytes, 14));
	let mut entries = Vec::with_capacity(count);
	for i in 0..count {
		let e = &bytes[HEADER + i * ENTRY..HEADER + (i + 1) * ENTRY];
		let identity = RootIdentity2 {
			generation: array(e, 0),
			cluster: array(e, 16),
			pin: array(e, 32),
			admin_key: array(e, 64),
		};
		if e[97..104].iter().any(|&x| x != 0) {
			return Err(SnapshotError2::Reserved);
		}
		let state = match e[96] {
			1 => RootState2::Prepared,
			2 => RootState2::Bound,
			3 => RootState2::Conflicted,
			4 => RootState2::Retired,
			_ => return Err(SnapshotError2::State),
		};
		let last_seq = u64::from_le_bytes(array(e, 104));
		let raw = &e[112..112 + BOOTSTRAP_RECORD_BYTES2];
		let record = if raw.iter().all(|&x| x == 0) {
			None
		} else {
			Some(BootstrapRecord2::decode(raw).map_err(|_| SnapshotError2::Record)?)
		};
		entries.push(SnapshotEntry2 {
			identity,
			state,
			last_seq,
			record,
		});
	}
	let s = InventorySnapshot2 {
		export_seq,
		entries,
	};
	validate(&s)?;
	if encode(&s)? != bytes {
		return Err(SnapshotError2::NotCanonical);
	}
	Ok(s)
}
