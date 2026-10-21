//! Plain retirement observations and terminal codecs. Integrity is not authority.
use super::inventory::RootIdentity2;
use crate::storage::mutation_visibility::redacted_debug;
use crate::storage::mutation_visibility::root2::{
	admin::manifest::{AdminManifestThree3, manifest_digest3},
	admin::record::{BootstrapIdentity2, BootstrapObservation2},
	codec::{hash, valid_log},
	types::{Digest2, Log2},
};

pub(in crate::storage::mutation_visibility::root2) const TERMINAL_BYTES3: usize = 156;
pub(in crate::storage::mutation_visibility::root2) const TERMINAL_ARTIFACT_BYTES3: usize = 336;
#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) struct TerminalObservation3 {
	pub identity: BootstrapIdentity2,
	pub original_admin_key: Digest2,
	pub at: Log2,
	pub fingerprint: Digest2,
	pub command_digest: Digest2,
	pub state_digest: Digest2,
}
redacted_debug!(TerminalObservation3, "TerminalObservation3");
#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) struct TerminalRecord3 {
	pub at: Log2,
	pub fingerprint: Digest2,
	pub command_digest: Digest2,
	pub state_digest: Digest2,
	pub observation_digest: Digest2,
	pub inventory_seq: u64,
}
redacted_debug!(TerminalRecord3, "TerminalRecord3");
#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) struct TerminalArtifact3 {
	pub identity: RootIdentity2,
	pub terminal: TerminalRecord3,
	pub inventory_pin: Digest2,
	pub export_seq: u64,
}
redacted_debug!(TerminalArtifact3, "TerminalArtifact3");
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) enum TerminalError3 {
	Length,
	Magic,
	Version,
	Profile,
	Identity,
	Log,
	Digest,
	Sequence,
	Binding,
	Trailer,
}
fn array<const N: usize>(b: &[u8], at: usize) -> [u8; N] {
	b[at..at + N].try_into().expect("fixed terminal offset")
}
fn checked_record(r: &TerminalRecord3) -> Result<(), TerminalError3> {
	valid_log(r.at).map_err(|_| TerminalError3::Log)?;
	if [
		r.fingerprint,
		r.command_digest,
		r.state_digest,
		r.observation_digest,
	]
	.contains(&[0; 32])
	{
		return Err(TerminalError3::Digest);
	}
	if r.inventory_seq == 0 {
		return Err(TerminalError3::Sequence);
	}
	Ok(())
}
/// All observation fields reach the digest, including the original Bootstrap identity.
pub(in crate::storage::mutation_visibility::root2) fn encode_terminal_observation3(
	o: &TerminalObservation3,
) -> Result<Vec<u8>, TerminalError3> {
	let mut bytes = BootstrapObservation2 {
		identity: o.identity,
	}
	.encode()
	.map_err(|_| TerminalError3::Identity)?
	.to_vec();
	valid_log(o.at).map_err(|_| TerminalError3::Log)?;
	if [
		o.original_admin_key,
		o.identity.pin,
		o.identity.bootstrap_fingerprint,
		o.identity.bootstrap_command,
		o.identity.genesis_fingerprint,
		o.fingerprint,
		o.command_digest,
		o.state_digest,
	]
	.contains(&[0; 32])
	{
		return Err(TerminalError3::Digest);
	}
	bytes.extend(o.original_admin_key);
	bytes.extend(o.at.term.to_le_bytes());
	bytes.extend(o.at.node.to_le_bytes());
	bytes.extend(o.at.index.to_le_bytes());
	bytes.extend(o.fingerprint);
	bytes.extend(o.command_digest);
	bytes.extend(o.state_digest);
	Ok(bytes)
}
pub(in crate::storage::mutation_visibility::root2) fn observation_digest3(
	o: &TerminalObservation3,
) -> Result<Digest2, TerminalError3> {
	Ok(hash(
		b"VISROOT2-RETIREMENT-OBSERVATION3\0",
		&encode_terminal_observation3(o)?,
	))
}
pub(in crate::storage::mutation_visibility::root2) fn terminal_record3(
	o: &TerminalObservation3,
	inventory_seq: u64,
) -> Result<TerminalRecord3, TerminalError3> {
	let r = TerminalRecord3 {
		at: o.at,
		fingerprint: o.fingerprint,
		command_digest: o.command_digest,
		state_digest: o.state_digest,
		observation_digest: observation_digest3(o)?,
		inventory_seq,
	};
	checked_record(&r)?;
	Ok(r)
}
pub(in crate::storage::mutation_visibility::root2) fn encode_terminal3(
	r: &TerminalRecord3,
) -> Result<[u8; TERMINAL_BYTES3], TerminalError3> {
	checked_record(r)?;
	let mut b = [0; TERMINAL_BYTES3];
	b[..8].copy_from_slice(&r.at.term.to_le_bytes());
	b[8..12].copy_from_slice(&r.at.node.to_le_bytes());
	b[12..20].copy_from_slice(&r.at.index.to_le_bytes());
	b[20..52].copy_from_slice(&r.fingerprint);
	b[52..84].copy_from_slice(&r.command_digest);
	b[84..116].copy_from_slice(&r.state_digest);
	b[116..148].copy_from_slice(&r.observation_digest);
	b[148..156].copy_from_slice(&r.inventory_seq.to_le_bytes());
	Ok(b)
}
pub(in crate::storage::mutation_visibility::root2) fn decode_terminal3(
	b: &[u8],
) -> Result<TerminalRecord3, TerminalError3> {
	if b.len() != TERMINAL_BYTES3 {
		return Err(TerminalError3::Length);
	}
	let r = TerminalRecord3 {
		at: Log2 {
			term: u64::from_le_bytes(array(b, 0)),
			node: u32::from_le_bytes(array(b, 8)),
			index: u64::from_le_bytes(array(b, 12)),
		},
		fingerprint: array(b, 20),
		command_digest: array(b, 52),
		state_digest: array(b, 84),
		observation_digest: array(b, 116),
		inventory_seq: u64::from_le_bytes(array(b, 148)),
	};
	checked_record(&r)?;
	Ok(r)
}
pub(in crate::storage::mutation_visibility::root2) fn encode_terminal_artifact3(
	a: &TerminalArtifact3,
) -> Result<[u8; TERMINAL_ARTIFACT_BYTES3], TerminalError3> {
	let i = &a.identity;
	if i.generation == [0; 16] || i.cluster == [0; 16] || i.pin == [0; 32] || i.admin_key == [0; 32]
	{
		return Err(TerminalError3::Identity);
	}
	if a.export_seq < a.terminal.inventory_seq || a.inventory_pin == [0; 32] {
		return Err(TerminalError3::Sequence);
	}
	let mut b = [0; TERMINAL_ARTIFACT_BYTES3];
	b[..8].copy_from_slice(b"VISTRM03");
	b[8..10].copy_from_slice(&1u16.to_le_bytes());
	b[10..12].copy_from_slice(&3u16.to_le_bytes());
	b[12..28].copy_from_slice(&i.generation);
	b[28..44].copy_from_slice(&i.cluster);
	b[44..76].copy_from_slice(&i.pin);
	b[76..108].copy_from_slice(&i.admin_key);
	b[108..264].copy_from_slice(&encode_terminal3(&a.terminal)?);
	b[264..272].copy_from_slice(&a.export_seq.to_le_bytes());
	b[272..304].copy_from_slice(&a.inventory_pin);
	let sum = hash(b"", &b[..304]);
	b[304..].copy_from_slice(&sum);
	Ok(b)
}
/// The inventory pin is supplied out of band; parsed bytes confer no readiness.
pub(in crate::storage::mutation_visibility::root2) fn read_terminal3(
	b: &[u8],
	manifest: &AdminManifestThree3,
	inventory_pin: Digest2,
) -> Result<TerminalArtifact3, TerminalError3> {
	if b.len() != TERMINAL_ARTIFACT_BYTES3 {
		return Err(TerminalError3::Length);
	}
	if &b[..8] != b"VISTRM03" {
		return Err(TerminalError3::Magic);
	}
	if u16::from_le_bytes(array(b, 8)) != 1 {
		return Err(TerminalError3::Version);
	}
	if u16::from_le_bytes(array(b, 10)) != 3 {
		return Err(TerminalError3::Profile);
	}
	if hash(b"", &b[..304]) != array::<32>(b, 304) {
		return Err(TerminalError3::Trailer);
	}
	let identity = RootIdentity2 {
		generation: array(b, 12),
		cluster: array(b, 28),
		pin: array(b, 44),
		admin_key: array(b, 76),
	};
	let a = TerminalArtifact3 {
		identity,
		terminal: decode_terminal3(&b[108..264])?,
		export_seq: u64::from_le_bytes(array(b, 264)),
		inventory_pin: array(b, 272),
	};
	let m = manifest.base();
	let pin = manifest_digest3(&manifest.encode().map_err(|_| TerminalError3::Identity)?);
	if (
		identity.generation,
		identity.cluster,
		identity.pin,
		identity.admin_key,
	) != (m.generation, m.cluster, pin, m.admin_key)
		|| a.inventory_pin != inventory_pin
	{
		return Err(TerminalError3::Binding);
	}
	if encode_terminal_artifact3(&a)?.as_slice() != b {
		return Err(TerminalError3::Binding);
	}
	Ok(a)
}
