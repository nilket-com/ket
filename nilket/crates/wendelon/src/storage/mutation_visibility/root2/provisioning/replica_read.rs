//! Reads a replica's pinned manifest and inventory snapshot beneath one accepted anchor, with the
//! accepted bounded reader, and validates them as a Part A tuple. Both pins are required before
//! any file is opened. Only a missing file is a missing prerequisite; every other reader refusal is
//! reported as itself. No fallback path, staging or unbounded read; the pins are caller data whose
//! delivery provenance is not established here.
use super::anchored::{AnchorDir2, AnchoredError2, AnchoredFile2};
use super::replica_anchor::{
	MAX_SNAPSHOT_BYTES2, PinnedBoundTuple2, ReplicaAnchorError2, ReplicaInput2, ReplicaInputs2,
	validate_replica_tuple2,
};
use crate::storage::mutation_visibility::redacted_debug;
use crate::storage::mutation_visibility::root2::admin::manifest::ADMIN_MANIFEST_BYTES2;
use crate::storage::mutation_visibility::root2::types::Digest2;
use std::ffi::CStr;

#[derive(Clone, Copy)]
pub(in crate::storage::mutation_visibility::root2) struct ReplicaFiles2<'a> {
	pub anchor: &'a AnchorDir2,
	pub manifest: &'a CStr,
	pub manifest_pin: Option<&'a Digest2>,
	pub snapshot: &'a CStr,
	pub inventory_pin: Option<&'a Digest2>,
}
redacted_debug!(ReplicaFiles2<'_>, "ReplicaFiles2");
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) enum ReplicaReadError2 {
	Read {
		input: ReplicaInput2,
		error: AnchoredError2,
	},
	Tuple(ReplicaAnchorError2),
}

// The admitted limit is passed as is: the reader itself probes one byte past it.
fn read(
	anchor: &AnchorDir2,
	path: &CStr,
	input: ReplicaInput2,
	max: usize,
) -> Result<Vec<u8>, ReplicaReadError2> {
	let file = match AnchoredFile2::open(anchor, path) {
		Ok(file) => file,
		Err(AnchoredError2::NotFound) => {
			return Err(ReplicaReadError2::Tuple(
				ReplicaAnchorError2::PrerequisiteUnavailable(input),
			));
		}
		Err(error) => return Err(ReplicaReadError2::Read { input, error }),
	};
	match file.read_bounded(max) {
		Ok(bytes) => Ok(bytes),
		Err(AnchoredError2::TooLarge) => {
			Err(ReplicaReadError2::Tuple(ReplicaAnchorError2::Length(input)))
		}
		Err(error) => Err(ReplicaReadError2::Read { input, error }),
	}
}

/// Order: both pins, the manifest file, the snapshot file, then Part A validation.
pub(in crate::storage::mutation_visibility::root2) fn read_replica_tuple2(
	files: &ReplicaFiles2,
) -> Result<PinnedBoundTuple2, ReplicaReadError2> {
	let missing =
		|input| ReplicaReadError2::Tuple(ReplicaAnchorError2::PrerequisiteUnavailable(input));
	let manifest_pin = files
		.manifest_pin
		.ok_or(missing(ReplicaInput2::ManifestPin))?;
	let inventory_pin = files
		.inventory_pin
		.ok_or(missing(ReplicaInput2::InventoryPin))?;
	let manifest = read(
		files.anchor,
		files.manifest,
		ReplicaInput2::Manifest,
		ADMIN_MANIFEST_BYTES2,
	)?;
	let snapshot = read(
		files.anchor,
		files.snapshot,
		ReplicaInput2::Snapshot,
		MAX_SNAPSHOT_BYTES2,
	)?;
	validate_replica_tuple2(&ReplicaInputs2 {
		manifest: Some(&manifest),
		manifest_pin: Some(manifest_pin),
		snapshot: Some(&snapshot),
		inventory_pin: Some(inventory_pin),
	})
	.map_err(ReplicaReadError2::Tuple)
}
