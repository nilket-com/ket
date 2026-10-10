//! Dormant derived administrator view. It joins a rooted origin state with a pinned replica tuple
//! and checks that they describe the same original Bootstrap. The view is not persisted and is not
//! new authority created from a parsed tuple: any authority comes only from the origin state having
//! been built through `bootstrap2` from a committed entry, whose production producer is held.
//! A G1d consumer must bind the whole `AdminContext2`; an administrator key or id alone never
//! authorizes anything in another root.
use super::super::redacted_debug;
use super::admin::errors::BootstrapRecordError2;
use super::admin::manifest::{ADMIN_MANIFEST_BYTES2, AdminNamespace2};
use super::admin::record::{BootstrapIdentity2, BootstrapObservation2};
use super::origin::{MutationAuthorityState2, root_and_origin2};
use super::provisioning::replica_anchor::{
	PinnedBoundTuple2, tuple_manifest_pin2, tuple_manifest2, tuple_record2,
};
use super::types::{Digest2, Log2};

#[derive(Clone, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) struct InstalledAdmin2 {
	context: AdminContext2,
	admin_id: [u8; 16],
	admin_key: [u8; 32],
	namespace: AdminNamespace2,
}
redacted_debug!(InstalledAdmin2, "InstalledAdmin2");
/// The exact root, original Bootstrap and genesis an administrator is bound to.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) struct AdminContext2 {
	pub generation: [u8; 16],
	pub cluster: [u8; 16],
	pub installed_at: Log2,
	pub bootstrap_fingerprint: Digest2,
	pub bootstrap_command: Digest2,
	pub genesis: Digest2,
	pub initial_epoch: u64,
	pub manifest_pin: Digest2,
}
redacted_debug!(AdminContext2, "AdminContext2");
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) enum CommitmentField2 {
	ControlDigest,
	ControlLength,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) enum InstallField2 {
	Generation,
	Cluster,
	Bootstrap,
	BootstrapFingerprint,
	BootstrapCommand,
	Genesis,
	InitialEpoch,
	Pin,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) enum ObserveError2 {
	PrerequisiteUnavailable,
	ControlLength,
	Codec(BootstrapRecordError2),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) enum InstallError2 {
	PrerequisiteUnavailable,
	Commitment(CommitmentField2),
	Record(InstallField2),
}

/// Root required; then the commitment rule; then the record against the immutable root fields and
/// the origin-preserved Bootstrap pair and genesis. The covered tip is never compared.
pub(in crate::storage::mutation_visibility::root2) fn install_admin2(
	state: &MutationAuthorityState2,
	tuple: &PinnedBoundTuple2,
) -> Result<InstalledAdmin2, InstallError2> {
	let Some((root, origin)) = root_and_origin2(state) else {
		return Err(InstallError2::PrerequisiteUnavailable);
	};
	let s = &root.state;
	let pin = tuple_manifest_pin2(tuple);
	if s.control_digest != pin {
		return Err(InstallError2::Commitment(CommitmentField2::ControlDigest));
	}
	if s.control_len != ADMIN_MANIFEST_BYTES2 as u64 {
		return Err(InstallError2::Commitment(CommitmentField2::ControlLength));
	}
	let record = tuple_record2(tuple);
	let r = &record.identity;
	// Pin is already bound by the tuple and the commitment rule; it is re-checked, not reachable.
	for (field, equal) in [
		(InstallField2::Generation, r.generation == s.generation),
		(InstallField2::Cluster, r.cluster == s.cluster),
		(InstallField2::Bootstrap, r.bootstrap == s.bootstrap),
		(
			InstallField2::BootstrapFingerprint,
			r.bootstrap_fingerprint == origin.bootstrap_fingerprint,
		),
		(
			InstallField2::BootstrapCommand,
			r.bootstrap_command == origin.bootstrap_command,
		),
		(
			InstallField2::Genesis,
			r.genesis_fingerprint == origin.genesis,
		),
		(
			InstallField2::InitialEpoch,
			r.initial_epoch == s.initial_epoch,
		),
		(InstallField2::Pin, r.pin == pin),
	] {
		if !equal {
			return Err(InstallError2::Record(field));
		}
	}
	let manifest = tuple_manifest2(tuple);
	Ok(InstalledAdmin2 {
		context: AdminContext2 {
			generation: s.generation,
			cluster: s.cluster,
			installed_at: s.bootstrap,
			bootstrap_fingerprint: origin.bootstrap_fingerprint,
			bootstrap_command: origin.bootstrap_command,
			genesis: origin.genesis,
			initial_epoch: s.initial_epoch,
			manifest_pin: pin,
		},
		admin_id: manifest.admin_id,
		admin_key: manifest.admin_key,
		namespace: manifest.namespace,
	})
}
pub(in crate::storage::mutation_visibility::root2) fn admin_context2(
	a: &InstalledAdmin2,
) -> AdminContext2 {
	a.context
}
pub(in crate::storage::mutation_visibility::root2) fn admin_id2(a: &InstalledAdmin2) -> [u8; 16] {
	a.admin_id
}
pub(in crate::storage::mutation_visibility::root2) fn admin_key2(a: &InstalledAdmin2) -> [u8; 32] {
	a.admin_key
}
pub(in crate::storage::mutation_visibility::root2) fn admin_namespace2(
	a: &InstalledAdmin2,
) -> AdminNamespace2 {
	a.namespace
}
pub(in crate::storage::mutation_visibility::root2) fn admin_installed_at2(
	a: &InstalledAdmin2,
) -> Log2 {
	a.context.installed_at
}

/// The observation a node offers its owner for approval: the original Bootstrap identity of a
/// rooted state, with the control digest as the candidate manifest pin. Derived data only: the
/// digest is not thereby a valid manifest, and owner approval, Part A and install still refuse any
/// inconsistency. The covered tip is never read.
pub(in crate::storage::mutation_visibility::root2) fn observe_bootstrap2(
	state: &MutationAuthorityState2,
) -> Result<BootstrapObservation2, ObserveError2> {
	let Some((root, origin)) = root_and_origin2(state) else {
		return Err(ObserveError2::PrerequisiteUnavailable);
	};
	let s = &root.state;
	if s.control_len != ADMIN_MANIFEST_BYTES2 as u64 {
		return Err(ObserveError2::ControlLength);
	}
	let observation = BootstrapObservation2 {
		identity: BootstrapIdentity2 {
			generation: s.generation,
			cluster: s.cluster,
			pin: s.control_digest,
			initial_epoch: s.initial_epoch,
			bootstrap: s.bootstrap,
			bootstrap_fingerprint: origin.bootstrap_fingerprint,
			bootstrap_command: origin.bootstrap_command,
			genesis_fingerprint: origin.genesis,
		},
	};
	observation.encode().map_err(ObserveError2::Codec)?;
	Ok(observation)
}
