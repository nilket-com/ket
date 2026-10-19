//! Binds the operator's out-of-band profile-3 manifest to the root's install entry in the log.
//!
//! The manifest is the only trust input. The install entry is located in the retained log, its
//! command must commit to exactly this manifest (deployment, pin, length, initial epoch), and the
//! replica tuple the shared verifiers take is built from that entry's own coordinates through the
//! ordinary tuple validator, over a transient in-memory inventory snapshot that is never stored.
//! Nothing here is owner approval or provisioning state: during the fold the production Bootstrap
//! check re-verifies every bound field against the real entry, and `install_admin2` (or the
//! terminal confirmation) re-binds the whole administrator context at the cut. There is no partial
//! binding: any mismatch refuses.
use crate::consensus::{WendelonRequest, WendelonTypeConfig};
use crate::storage::mutation_visibility::root2::admin::manifest::{
	AnyAdminManifest2, any_manifest_digest2,
};
use crate::storage::mutation_visibility::root2::admin::record::{
	BootstrapIdentity2, BootstrapRecord2,
};
use crate::storage::mutation_visibility::root2::codec::hash;
use crate::storage::mutation_visibility::root2::command::Command2;
use crate::storage::mutation_visibility::root2::entry_identity::{
	EntryPosition2, blank_entry_identity2, membership_entry_identity2, normal_entry_identity2,
};
use crate::storage::mutation_visibility::root2::provisioning::inventory::{
	LogState2, RootEntry2, RootIdentity2, RootState2,
};
use crate::storage::mutation_visibility::root2::provisioning::replica_anchor::{
	PinnedBoundTuple2, ReplicaInputs2, validate_replica_tuple2,
};
use crate::storage::mutation_visibility::root2::provisioning::snapshot::encode_snapshot2;
use crate::storage::mutation_visibility::root2::recovery::Deployment2;
use crate::storage::mutation_visibility::root2::types::{Digest2, Log2};
use openraft::{Entry, EntryPayload};
use std::collections::BTreeMap;

/// A decoded profile-3 manifest and its pin. Its source is the operator, never the log.
pub(super) struct TrustedManifest2 {
	bytes: Vec<u8>,
	any: AnyAdminManifest2,
	pin: Digest2,
}
/// Why the manifest is not bound to this log's root.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BindingError2 {
	/// No committed Root2 install entry precedes the stop.
	InstallMissing,
	/// The install entry names another generation or cluster.
	Deployment,
	/// The install entry commits to another manifest digest.
	Pin,
	/// The install entry commits to another manifest length.
	Length,
	/// The install entry's initial epoch is not the manifest's.
	Epoch,
	/// Entry identity math refused an entry.
	Identity,
	/// The ordinary tuple validator refused the binding.
	Tuple,
}

/// The trusted manifest, or why it is refused as a trust input: only an exact profile-3 manifest.
pub(super) fn trusted_manifest2(bytes: &[u8]) -> Result<TrustedManifest2, &'static str> {
	let any = AnyAdminManifest2::decode(bytes)
		.map_err(|_| "the trusted manifest does not decode as an administrator manifest")?;
	if !matches!(any, AnyAdminManifest2::Three(_)) {
		return Err("the trusted manifest is not a profile-3 manifest");
	}
	let pin = any_manifest_digest2(&any.encode().map_err(|_| "manifest re-encode")?);
	Ok(TrustedManifest2 {
		bytes: bytes.to_vec(),
		any,
		pin,
	})
}
fn position(entry: &Entry<WendelonTypeConfig>) -> EntryPosition2 {
	EntryPosition2 {
		term: entry.log_id.leader_id.term,
		node: entry.log_id.leader_id.node_id,
		index: entry.log_id.index,
	}
}
fn identity(entry: &Entry<WendelonTypeConfig>) -> Result<Digest2, BindingError2> {
	let at = position(entry);
	match &entry.payload {
		EntryPayload::Blank => Ok(blank_entry_identity2(at)),
		EntryPayload::Membership(m) => {
			membership_entry_identity2(at, m).map_err(|_| BindingError2::Identity)
		}
		EntryPayload::Normal(r) => {
			normal_entry_identity2(at, r).map_err(|_| BindingError2::Identity)
		}
	}
}
/// The first well-formed Bootstrap carrier at or below `stop`, with its envelope context.
fn install_entry(
	entries: &BTreeMap<u64, Entry<WendelonTypeConfig>>,
	stop: u64,
) -> Option<(&Entry<WendelonTypeConfig>, [u8; 16], [u8; 16], Command2)> {
	entries.range(..=stop).find_map(|(_, entry)| {
		let EntryPayload::Normal(WendelonRequest::Root2Command(carrier)) = &entry.payload else {
			return None;
		};
		match Command2::decode(carrier.command()) {
			Ok((g, c, command @ Command2::Bootstrap { .. })) => Some((entry, g, c, command)),
			_ => None,
		}
	})
}
/// The replica tuple and deployment for the root this manifest installs in `entries`.
pub(super) fn bind2(
	manifest: &TrustedManifest2,
	entries: &BTreeMap<u64, Entry<WendelonTypeConfig>>,
	stop: u64,
) -> Result<(PinnedBoundTuple2, Deployment2), BindingError2> {
	use BindingError2 as E;
	let a = manifest.any.admission();
	let (entry, g, c, command) = install_entry(entries, stop).ok_or(E::InstallMissing)?;
	let Command2::Bootstrap {
		epoch,
		control,
		length,
	} = command
	else {
		return Err(E::InstallMissing);
	};
	if (g, c) != (a.generation, a.cluster) {
		return Err(E::Deployment);
	}
	if control != manifest.pin {
		return Err(E::Pin);
	}
	if usize::try_from(length).ok() != Some(manifest.bytes.len()) {
		return Err(E::Length);
	}
	if epoch != a.initial_epoch {
		return Err(E::Epoch);
	}
	let genesis = entries.get(&0).ok_or(E::InstallMissing)?;
	let at = position(entry);
	let record = BootstrapRecord2 {
		identity: BootstrapIdentity2 {
			generation: g,
			cluster: c,
			pin: manifest.pin,
			initial_epoch: epoch,
			bootstrap: Log2 {
				term: at.term,
				node: at.node,
				index: at.index,
			},
			bootstrap_fingerprint: identity(entry)?,
			bootstrap_command: command.digest(g, c).map_err(|_| E::Identity)?,
			genesis_fingerprint: identity(genesis)?,
		},
		inventory_seq: 2,
	};
	let snapshot = encode_snapshot2(&LogState2 {
		roots: vec![RootEntry2 {
			identity: RootIdentity2 {
				generation: g,
				cluster: c,
				pin: manifest.pin,
				admin_key: a.admin_key,
			},
			manifest: manifest.any.clone(),
			state: RootState2::Bound,
			last_seq: 2,
			record: Some(record),
			terminal: None,
		}],
		last_seq: 2,
	})
	.map_err(|_| E::Tuple)?;
	let tuple = validate_replica_tuple2(&ReplicaInputs2 {
		manifest: Some(&manifest.bytes),
		manifest_pin: Some(&manifest.pin),
		snapshot: Some(&snapshot),
		inventory_pin: Some(&hash(b"", &snapshot)),
	})
	.map_err(|_| E::Tuple)?;
	Ok((
		tuple,
		Deployment2 {
			generation: g,
			cluster: c,
		},
	))
}
