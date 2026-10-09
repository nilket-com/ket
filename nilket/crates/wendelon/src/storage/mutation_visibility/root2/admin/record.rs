use super::super::super::redacted_debug;
use super::super::types::{Digest2, Log2};
use super::errors::{BootstrapRecordError2, BootstrapResult2};

pub(in crate::storage::mutation_visibility::root2) const BOOTSTRAP_RECORD_BYTES2: usize = 208;
const RECORD_MAGIC: &[u8; 8] = b"VISADR2\0";
const OBSERVATION_MAGIC: &[u8; 8] = b"VISAOB2\0";

// Identity of the original committed Bootstrap as observed by a node and approved by the owner.
// Equality here is byte math only; it authenticates no history and grants no readiness.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) struct BootstrapIdentity2 {
	pub generation: [u8; 16],
	pub cluster: [u8; 16],
	pub pin: Digest2,
	pub initial_epoch: u64,
	pub bootstrap: Log2,
	pub bootstrap_fingerprint: Digest2,
	pub bootstrap_command: Digest2,
	pub genesis_fingerprint: Digest2,
}
redacted_debug!(BootstrapIdentity2, "BootstrapIdentity2");
/// Owner-approved record: inventory sequence is nonzero.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) struct BootstrapRecord2 {
	pub identity: BootstrapIdentity2,
	pub inventory_seq: u64,
}
redacted_debug!(BootstrapRecord2, "BootstrapRecord2");
/// Node observation: same identity, inventory sequence always zero.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) struct BootstrapObservation2 {
	pub identity: BootstrapIdentity2,
}
redacted_debug!(BootstrapObservation2, "BootstrapObservation2");
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) enum BootstrapField2 {
	Generation,
	Cluster,
	Pin,
	InitialEpoch,
	Bootstrap,
	BootstrapFingerprint,
	BootstrapCommand,
	GenesisFingerprint,
}

fn array<const N: usize>(b: &[u8], at: usize) -> [u8; N] {
	b[at..at + N].try_into().expect("fixed record offset")
}
fn u64_at(b: &[u8], at: usize) -> u64 {
	u64::from_le_bytes(array(b, at))
}
fn check_identity(i: &BootstrapIdentity2) -> BootstrapResult2<()> {
	if i.generation == [0; 16] || i.cluster == [0; 16] || i.initial_epoch == 0 {
		return Err(BootstrapRecordError2::ZeroField);
	}
	let l = i.bootstrap;
	if l.term == 0 || l.node == 0 || l.index == 0 {
		return Err(BootstrapRecordError2::InvalidLog);
	}
	Ok(())
}
fn decode(input: &[u8], magic: &[u8; 8]) -> BootstrapResult2<(BootstrapIdentity2, u64)> {
	if input.len() != BOOTSTRAP_RECORD_BYTES2 {
		return Err(BootstrapRecordError2::Length);
	}
	if &input[0..8] != magic {
		return Err(BootstrapRecordError2::Magic);
	}
	if u16::from_le_bytes(array(input, 8)) != 1 {
		return Err(BootstrapRecordError2::UnsupportedFormat);
	}
	if u16::from_le_bytes(array(input, 10)) != 1 {
		return Err(BootstrapRecordError2::UnsupportedProfile);
	}
	let identity = BootstrapIdentity2 {
		generation: array(input, 12),
		cluster: array(input, 28),
		pin: array(input, 44),
		initial_epoch: u64_at(input, 76),
		bootstrap: Log2 {
			term: u64_at(input, 84),
			node: u32::from_le_bytes(array(input, 92)),
			index: u64_at(input, 96),
		},
		bootstrap_fingerprint: array(input, 104),
		bootstrap_command: array(input, 136),
		genesis_fingerprint: array(input, 168),
	};
	check_identity(&identity)?;
	Ok((identity, u64_at(input, 200)))
}
fn encode(
	magic: &[u8; 8],
	i: &BootstrapIdentity2,
	seq: u64,
) -> BootstrapResult2<[u8; BOOTSTRAP_RECORD_BYTES2]> {
	check_identity(i)?;
	let mut b = [0u8; BOOTSTRAP_RECORD_BYTES2];
	b[0..8].copy_from_slice(magic);
	b[8..10].copy_from_slice(&1u16.to_le_bytes());
	b[10..12].copy_from_slice(&1u16.to_le_bytes());
	b[12..28].copy_from_slice(&i.generation);
	b[28..44].copy_from_slice(&i.cluster);
	b[44..76].copy_from_slice(&i.pin);
	b[76..84].copy_from_slice(&i.initial_epoch.to_le_bytes());
	b[84..92].copy_from_slice(&i.bootstrap.term.to_le_bytes());
	b[92..96].copy_from_slice(&i.bootstrap.node.to_le_bytes());
	b[96..104].copy_from_slice(&i.bootstrap.index.to_le_bytes());
	b[104..136].copy_from_slice(&i.bootstrap_fingerprint);
	b[136..168].copy_from_slice(&i.bootstrap_command);
	b[168..200].copy_from_slice(&i.genesis_fingerprint);
	b[200..208].copy_from_slice(&seq.to_le_bytes());
	Ok(b)
}
fn canonical(input: &[u8], encoded: [u8; BOOTSTRAP_RECORD_BYTES2]) -> BootstrapResult2<()> {
	if encoded != *input {
		return Err(BootstrapRecordError2::NotCanonical);
	}
	Ok(())
}
impl BootstrapRecord2 {
	pub(in crate::storage::mutation_visibility::root2) fn decode(
		input: &[u8],
	) -> BootstrapResult2<Self> {
		let (identity, inventory_seq) = decode(input, RECORD_MAGIC)?;
		let record = Self {
			identity,
			inventory_seq,
		};
		canonical(input, record.encode()?)?;
		Ok(record)
	}
	pub(in crate::storage::mutation_visibility::root2) fn encode(
		&self,
	) -> BootstrapResult2<[u8; BOOTSTRAP_RECORD_BYTES2]> {
		if self.inventory_seq == 0 {
			return Err(BootstrapRecordError2::Sequence);
		}
		encode(RECORD_MAGIC, &self.identity, self.inventory_seq)
	}
}
impl BootstrapObservation2 {
	pub(in crate::storage::mutation_visibility::root2) fn decode(
		input: &[u8],
	) -> BootstrapResult2<Self> {
		let (identity, seq) = decode(input, OBSERVATION_MAGIC)?;
		if seq != 0 {
			return Err(BootstrapRecordError2::Sequence);
		}
		let observation = Self { identity };
		canonical(input, observation.encode()?)?;
		Ok(observation)
	}
	pub(in crate::storage::mutation_visibility::root2) fn encode(
		&self,
	) -> BootstrapResult2<[u8; BOOTSTRAP_RECORD_BYTES2]> {
		encode(OBSERVATION_MAGIC, &self.identity, 0)
	}
}
impl BootstrapIdentity2 {
	/// First differing field in a fixed order; `None` when every field is equal.
	pub(in crate::storage::mutation_visibility::root2) fn first_mismatch(
		&self,
		other: &Self,
	) -> Option<BootstrapField2> {
		use BootstrapField2::*;
		[
			(self.generation != other.generation, Generation),
			(self.cluster != other.cluster, Cluster),
			(self.pin != other.pin, Pin),
			(self.initial_epoch != other.initial_epoch, InitialEpoch),
			(self.bootstrap != other.bootstrap, Bootstrap),
			(
				self.bootstrap_fingerprint != other.bootstrap_fingerprint,
				BootstrapFingerprint,
			),
			(
				self.bootstrap_command != other.bootstrap_command,
				BootstrapCommand,
			),
			(
				self.genesis_fingerprint != other.genesis_fingerprint,
				GenesisFingerprint,
			),
		]
		.into_iter()
		.find_map(|(differs, field)| differs.then_some(field))
	}
}
