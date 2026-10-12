//! Pure validation of a replica's pinned provisioning tuple: the administrator manifest and the
//! owner inventory snapshot, each checked against a separately supplied pin, then bound into one
//! coherent Bound entry and its owner-approved record. Digests of the supplied bytes are computed
//! only to compare them; the expected pins are caller-provided data whose deployment provenance is
//! an unimplemented assumption. The result is a parsed tuple, not proof of a committed Bootstrap
//! or history, and not production eligibility. An older coherent pinned snapshot is accepted: no
//! rollback, freshness or current-revocation claim is made here.
use super::inventory::{MAX_ROOTS2, RootState2};
use super::snapshot::{SnapshotError2, decode_snapshot2};
use crate::storage::mutation_visibility::redacted_debug;
use crate::storage::mutation_visibility::root2::admin::errors::AdminManifestError2;
use crate::storage::mutation_visibility::root2::admin::manifest::{
	ADMIN_MANIFEST_BYTES2, ADMIN_MANIFEST_P2_BYTES2, AnyAdminManifest2, admin_manifest_digest2,
	admin_manifest_two_digest2,
};
use crate::storage::mutation_visibility::root2::admin::record::BootstrapRecord2;
use crate::storage::mutation_visibility::root2::codec::hash;
use crate::storage::mutation_visibility::root2::types::Digest2;

// The replica snapshot layout: 22-byte header, 320-byte entries, 32-byte trailer.
pub(in crate::storage::mutation_visibility::root2) const MAX_SNAPSHOT_BYTES2: usize =
	22 + MAX_ROOTS2 * 320 + 32;

#[derive(Clone, Copy)]
pub(in crate::storage::mutation_visibility::root2) struct ReplicaInputs2<'a> {
	pub manifest: Option<&'a [u8]>,
	pub manifest_pin: Option<&'a Digest2>,
	pub snapshot: Option<&'a [u8]>,
	pub inventory_pin: Option<&'a Digest2>,
}
redacted_debug!(ReplicaInputs2<'_>, "ReplicaInputs2");
#[derive(Clone, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) struct PinnedBoundTuple2 {
	manifest: AnyAdminManifest2,
	manifest_pin: Digest2,
	inventory_pin: Digest2,
	export_seq: u64,
	entry_last_seq: u64,
	record: BootstrapRecord2,
}
redacted_debug!(PinnedBoundTuple2, "PinnedBoundTuple2");
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) enum ReplicaInput2 {
	Manifest,
	ManifestPin,
	Snapshot,
	InventoryPin,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) enum TupleField2 {
	Generation,
	Cluster,
	Pin,
	AdminKey,
	InitialEpoch,
	Profile,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) enum ReplicaAnchorError2 {
	PrerequisiteUnavailable(ReplicaInput2),
	Length(ReplicaInput2),
	ManifestPinMismatch,
	InventoryPinMismatch,
	ManifestCodec(AdminManifestError2),
	SnapshotCodec(SnapshotError2),
	NotInInventory,
	EntryMismatch(TupleField2),
	NotBound(RootState2),
	RecordBinding(TupleField2),
}

/// Order: presence, lengths, manifest digest, inventory digest, accepted codecs, tuple bindings.
pub(in crate::storage::mutation_visibility::root2) fn validate_replica_tuple2(
	inputs: &ReplicaInputs2,
) -> Result<PinnedBoundTuple2, ReplicaAnchorError2> {
	use ReplicaAnchorError2 as E;
	let missing = E::PrerequisiteUnavailable;
	let manifest_bytes = inputs.manifest.ok_or(missing(ReplicaInput2::Manifest))?;
	let manifest_pin = *inputs
		.manifest_pin
		.ok_or(missing(ReplicaInput2::ManifestPin))?;
	let snapshot_bytes = inputs.snapshot.ok_or(missing(ReplicaInput2::Snapshot))?;
	let inventory_pin = *inputs
		.inventory_pin
		.ok_or(missing(ReplicaInput2::InventoryPin))?;
	// Exactly one canonical manifest length, settled before any digest; the digest is over the
	// exact supplied bytes.
	let manifest_digest = if let Ok(one) = <&[u8; ADMIN_MANIFEST_BYTES2]>::try_from(manifest_bytes)
	{
		admin_manifest_digest2(one)
	} else if let Ok(two) = <&[u8; ADMIN_MANIFEST_P2_BYTES2]>::try_from(manifest_bytes) {
		admin_manifest_two_digest2(two)
	} else {
		return Err(E::Length(ReplicaInput2::Manifest));
	};
	if snapshot_bytes.len() > MAX_SNAPSHOT_BYTES2 {
		return Err(E::Length(ReplicaInput2::Snapshot));
	}
	// Integrity before parsing: a wrong pin is never reported as malformed bytes.
	if manifest_digest != manifest_pin {
		return Err(E::ManifestPinMismatch);
	}
	if hash(b"", snapshot_bytes) != inventory_pin {
		return Err(E::InventoryPinMismatch);
	}
	let any = AnyAdminManifest2::decode(manifest_bytes).map_err(E::ManifestCodec)?;
	let manifest = any.admission();
	let snapshot = decode_snapshot2(snapshot_bytes).map_err(E::SnapshotCodec)?;
	let Some(entry) = snapshot.entries.iter().find(|e| {
		(e.identity.generation, e.identity.cluster) == (manifest.generation, manifest.cluster)
	}) else {
		return Err(E::NotInInventory);
	};
	if entry.identity.pin != manifest_pin {
		return Err(E::EntryMismatch(TupleField2::Pin));
	}
	if entry.identity.admin_key != manifest.admin_key {
		return Err(E::EntryMismatch(TupleField2::AdminKey));
	}
	// The selected manifest's tag only; unseen entries are not authenticated here.
	if entry.profile != any.profile() {
		return Err(E::EntryMismatch(TupleField2::Profile));
	}
	if entry.state != RootState2::Bound {
		return Err(E::NotBound(entry.state));
	}
	// The decoder refuses a Bound entry without a record; never panic if that invariant moves.
	let Some(record) = entry.record else {
		return Err(E::NotBound(RootState2::Bound));
	};
	// Generation, cluster and pin are already bound by snapshot decoding plus the entry checks
	// above, so these three refusals are unreachable today; the epoch is the one new binding.
	let r = &record.identity;
	for (field, equal) in [
		(TupleField2::Generation, r.generation == manifest.generation),
		(TupleField2::Cluster, r.cluster == manifest.cluster),
		(TupleField2::Pin, r.pin == manifest_pin),
		(
			TupleField2::InitialEpoch,
			r.initial_epoch == manifest.initial_epoch,
		),
	] {
		if !equal {
			return Err(E::RecordBinding(field));
		}
	}
	Ok(PinnedBoundTuple2 {
		manifest: any,
		manifest_pin,
		inventory_pin,
		export_seq: snapshot.export_seq,
		entry_last_seq: entry.last_seq,
		record,
	})
}
pub(in crate::storage::mutation_visibility::root2) fn tuple_manifest2(
	t: &PinnedBoundTuple2,
) -> &AnyAdminManifest2 {
	&t.manifest
}
/// The canonical length of the validated manifest variant (137 or 189); the only length source
/// for install and C's Bootstrap check.
pub(in crate::storage::mutation_visibility::root2) fn tuple_manifest_len2(
	t: &PinnedBoundTuple2,
) -> u64 {
	match t.manifest {
		AnyAdminManifest2::One(_) => ADMIN_MANIFEST_BYTES2 as u64,
		AnyAdminManifest2::Two(_) => ADMIN_MANIFEST_P2_BYTES2 as u64,
	}
}
pub(in crate::storage::mutation_visibility::root2) fn tuple_manifest_pin2(
	t: &PinnedBoundTuple2,
) -> Digest2 {
	t.manifest_pin
}
pub(in crate::storage::mutation_visibility::root2) fn tuple_inventory_pin2(
	t: &PinnedBoundTuple2,
) -> Digest2 {
	t.inventory_pin
}
pub(in crate::storage::mutation_visibility::root2) fn tuple_export_seq2(
	t: &PinnedBoundTuple2,
) -> u64 {
	t.export_seq
}
pub(in crate::storage::mutation_visibility::root2) fn tuple_entry_last_seq2(
	t: &PinnedBoundTuple2,
) -> u64 {
	t.entry_last_seq
}
pub(in crate::storage::mutation_visibility::root2) fn tuple_record2(
	t: &PinnedBoundTuple2,
) -> BootstrapRecord2 {
	t.record
}
