//! P-S2 (contract r1 §E test 5): every §C delivery shape has one concrete outcome, over
//! owner-produced artifacts, with every preceding prerequisite satisfied unless the case is a
//! stated precedence pin. Disclosed fixture paths:
//! - non-Bound roots: `export_tuple` refuses them, so their snapshot comes from the owner's
//!   `export` after legitimate owner operations, and their manifest is the exact reserved
//!   manifest written by the test (Prepared, Conflicted) or the root's earlier owner export
//!   (Retired);
//! - oversize, bad-length and replaced files are deliberate post-export mutations of owner output
//!   (negative fixtures), not owner export semantics;
//! - the replacement cases are deterministic delivery states, not a concurrent read race.
//!
//! No freshness, owner authentication, transport or concurrency guarantee is claimed: a coherent
//! older tuple stays accepted under its original pins.
//!
//! The deliveries are profile 3 (`M3_*`) in marked directories; a Retired root is reached by the
//! committed terminal Retire.
use super::owner_tuple_delivery::{
	INVENTORY, MANIFEST, Scratch, delivered_three, identity_of, read_at, read_delivered, reserved,
};
use super::profile_three_fixture::{M3_OTHER, M3_PRIMARY, rooted};
use super::*;
use crate::storage::mutation_visibility::root2::admin::manifest::AnyAdminManifest2;
use crate::storage::mutation_visibility::root2::admin_install::observe_bootstrap2;
use crate::storage::mutation_visibility::root2::provisioning::anchored::AnchoredError2;
use crate::storage::mutation_visibility::root2::provisioning::inventory::RootState2;
use crate::storage::mutation_visibility::root2::provisioning::owner_core::{
	EventOp2, OwnerRequest2, op_id2,
};
use crate::storage::mutation_visibility::root2::provisioning::owner_log::OwnerIoError2;
use crate::storage::mutation_visibility::root2::provisioning::replica_anchor::*;
use crate::storage::mutation_visibility::root2::provisioning::replica_read::*;
use crate::storage::mutation_visibility::root2::provisioning::terminal::TerminalObservation3;
use std::fs;
use std::os::unix::fs::symlink;

use ReplicaAnchorError2 as E;
use ReplicaInput2 as I;

fn tuple_err(e: ReplicaAnchorError2) -> Option<ReplicaReadError2> {
	Some(ReplicaReadError2::Tuple(e))
}
fn read_err(input: ReplicaInput2, error: AnchoredError2) -> Option<ReplicaReadError2> {
	Some(ReplicaReadError2::Read { input, error })
}

/// An absent pin refuses before any file is opened: the names given would otherwise be a missing
/// file, an escape and a symlink. The manifest pin is checked first.
#[test]
fn absent_pins_refuse_before_any_file_access() {
	let d = delivered_three(&M3_PRIMARY);
	symlink(d.out.at(MANIFEST), d.out.at("link")).unwrap();
	let (m, i) = (&d.export.manifest_pin, &d.export.inventory_pin);
	for (manifest, snapshot) in [
		(MANIFEST, INVENTORY),
		("absent", "../escape"),
		("link", "absent"),
	] {
		let at = |mp, ip| read_at(&d.out.0, manifest, mp, snapshot, ip).err();
		assert_eq!(
			at(None, Some(i)),
			tuple_err(E::PrerequisiteUnavailable(I::ManifestPin))
		);
		assert_eq!(
			at(Some(m), None),
			tuple_err(E::PrerequisiteUnavailable(I::InventoryPin))
		);
		assert_eq!(
			at(None, None),
			tuple_err(E::PrerequisiteUnavailable(I::ManifestPin))
		);
	}
}

/// Both pins present: a missing file is the one missing prerequisite (manifest first).
#[test]
fn an_absent_file_is_its_missing_prerequisite() {
	let d = delivered_three(&M3_PRIMARY);
	let (m, i) = (Some(&d.export.manifest_pin), Some(&d.export.inventory_pin));
	assert_eq!(
		read_at(&d.out.0, "absent", m, INVENTORY, i).err(),
		tuple_err(E::PrerequisiteUnavailable(I::Manifest))
	);
	assert_eq!(
		read_at(&d.out.0, MANIFEST, m, "absent", i).err(),
		tuple_err(E::PrerequisiteUnavailable(I::Snapshot))
	);
	assert_eq!(
		read_at(&d.out.0, "absent", m, "absent", i).err(),
		tuple_err(E::PrerequisiteUnavailable(I::Manifest))
	);
}

/// A wrong manifest pin and a wrong inventory pin each give their own mismatch; with both wrong
/// the manifest's is reported. The wrong pins are real pins of other owner output.
#[test]
fn a_wrong_pin_is_its_own_mismatch() {
	for (name, manifest, other) in [
		("primary", M3_PRIMARY, M3_OTHER),
		("other", M3_OTHER, M3_PRIMARY),
	] {
		let d = delivered_three(&manifest);
		let o = delivered_three(&other);
		let (m, i) = (&d.export.manifest_pin, &d.export.inventory_pin);
		let (om, oi) = (&o.export.manifest_pin, &o.export.inventory_pin);
		let at = |mp, ip| read_at(&d.out.0, MANIFEST, Some(mp), INVENTORY, Some(ip)).err();
		assert_eq!(at(om, i), tuple_err(E::ManifestPinMismatch), "{name}");
		assert_eq!(at(m, oi), tuple_err(E::InventoryPinMismatch), "{name}");
		assert_eq!(at(om, oi), tuple_err(E::ManifestPinMismatch), "{name}");
		// The two pin rules are not interchangeable.
		assert_eq!(at(i, m), tuple_err(E::ManifestPinMismatch), "{name}");
		assert_eq!(at(m, m), tuple_err(E::InventoryPinMismatch), "{name}");
	}
}

/// Swapped files: the snapshot in the manifest slot is too long for any manifest, whatever the
/// pins; the manifest in the snapshot slot fails the inventory pin.
#[test]
fn swapped_files_refuse_on_length_or_pin() {
	let d = delivered_three(&M3_PRIMARY);
	let (m, i) = (&d.export.manifest_pin, &d.export.inventory_pin);
	for (mp, ip) in [(m, i), (i, m)] {
		assert_eq!(
			read_at(&d.out.0, INVENTORY, Some(mp), MANIFEST, Some(ip)).err(),
			tuple_err(E::Length(I::Manifest))
		);
	}
	assert_eq!(
		read_at(&d.out.0, MANIFEST, Some(m), MANIFEST, Some(i)).err(),
		tuple_err(E::InventoryPinMismatch)
	);
	assert_eq!(
		read_at(&d.out.0, INVENTORY, Some(m), INVENTORY, Some(i)).err(),
		tuple_err(E::Length(I::Manifest))
	);
}

/// A digest-valid mixed delivery: root A's owner-exported manifest (copied byte-for-byte beside
/// root B's export) with B's snapshot, each under its own owner-returned pin. B's snapshot has no
/// entry for A.
#[test]
fn a_mixed_generation_tuple_is_not_in_inventory() {
	let (a, b) = (delivered_three(&M3_OTHER), delivered_three(&M3_PRIMARY));
	fs::copy(a.out.at(MANIFEST), b.out.at("a.manifest")).unwrap();
	assert_eq!(
		read_at(
			&b.out.0,
			"a.manifest",
			Some(&a.export.manifest_pin),
			INVENTORY,
			Some(&b.export.inventory_pin)
		)
		.err(),
		tuple_err(E::NotInInventory)
	);
}

/// Prepared, Conflicted and Retired roots from legitimate owner operations: `export_tuple`
/// refuses each, so the snapshot is the owner's `export` and the manifest is the reserved exact
/// bytes (Retired: the root's own earlier owner export, after its terminal Retire).
#[test]
fn a_root_that_is_not_bound_is_refused_with_its_state() {
	let bytes = M3_PRIMARY.to_vec();
	let id = identity_of(&bytes);
	let not_bound = |dir: &Scratch, pin: &Digest2| {
		read_at(&dir.0, MANIFEST, Some(&id.pin), INVENTORY, Some(pin)).err()
	};
	// Prepared.
	let (_dir, mut owner) = reserved(&bytes);
	let out = Scratch::new();
	assert_eq!(
		owner
			.export_tuple(&out.0, MANIFEST, INVENTORY, id.generation, id.cluster)
			.err(),
		Some(OwnerIoError2::ExportNotBound(RootState2::Prepared))
	);
	let pin = owner.export(&out.0, INVENTORY).unwrap();
	fs::write(out.at(MANIFEST), &bytes).unwrap();
	assert_eq!(
		not_bound(&out, &pin),
		tuple_err(E::NotBound(RootState2::Prepared))
	);
	// Conflicted: approved from the observation of another root's committed Bootstrap.
	let (_dir, mut owner) = reserved(&bytes);
	let other = rooted(&M3_OTHER, 241).3;
	let conflicted = owner
		.apply(
			&op_id2([2; 16]).unwrap(),
			&OwnerRequest2::Approve {
				identity: id,
				observation: observe_bootstrap2(&other).unwrap(),
				confirmed: id,
			},
		)
		.unwrap();
	assert_eq!(
		(conflicted.original.map(|e| e.op), conflicted.current),
		(Some(EventOp2::Conflict), RootState2::Conflicted)
	);
	let out = Scratch::new();
	assert_eq!(
		owner
			.export_tuple(&out.0, MANIFEST, INVENTORY, id.generation, id.cluster)
			.err(),
		Some(OwnerIoError2::ExportNotBound(RootState2::Conflicted))
	);
	let pin = owner.export(&out.0, INVENTORY).unwrap();
	fs::write(out.at(MANIFEST), &bytes).unwrap();
	assert_eq!(
		not_bound(&out, &pin),
		tuple_err(E::NotBound(RootState2::Conflicted))
	);
	// Retired after a Bound export: the earlier owner manifest with the Retired snapshot.
	let mut d = delivered_three(&M3_PRIMARY);
	retire(&mut d);
	let retired = Scratch::new();
	assert_eq!(
		d.owner
			.export_tuple(&retired.0, MANIFEST, INVENTORY, id.generation, id.cluster)
			.err(),
		Some(OwnerIoError2::ExportNotBound(RootState2::Retired))
	);
	assert!(
		fs::read_dir(&retired.0).unwrap().next().is_none(),
		"nothing published"
	);
	let pin = d.owner.export(&retired.0, INVENTORY).unwrap();
	fs::copy(d.out.at(MANIFEST), retired.at(MANIFEST)).unwrap();
	assert_eq!(
		not_bound(&retired, &pin),
		tuple_err(E::NotBound(RootState2::Retired))
	);
}
/// The committed terminal Retire of a delivered root, observed from its own committed Bootstrap.
fn retire(d: &mut super::owner_tuple_delivery::Delivered) {
	let retired = d
		.owner
		.apply(
			&op_id2([3; 16]).unwrap(),
			&OwnerRequest2::Retire {
				terminal: TerminalObservation3 {
					identity: observe_bootstrap2(&d.root).unwrap().identity,
					original_admin_key: d.identity.admin_key,
					at: l(9),
					fingerprint: [0x19; 32],
					command_digest: [0x1a; 32],
					state_digest: [0x1b; 32],
				},
				identity: d.identity,
			},
		)
		.unwrap();
	assert_eq!(
		(retired.original.map(|e| e.op), retired.current),
		(Some(EventOp2::Retire), RootState2::Retired)
	);
}

/// Prepared and Conflicted profile-3 roots from legitimate owner operations in a marked directory:
/// `export_tuple` refuses each, so the snapshot is the owner's `export` and the manifest is the
/// reserved exact bytes. (The Retired case is in `a_root_that_is_not_bound_is_refused_with_its_state`.)
#[test]
fn a_profile_three_root_that_is_not_bound_is_refused_with_its_state() {
	let bytes = M3_PRIMARY.to_vec();
	let id = identity_of(&bytes);
	let not_bound = |dir: &Scratch, pin: &Digest2| {
		read_at(&dir.0, MANIFEST, Some(&id.pin), INVENTORY, Some(pin)).err()
	};
	// Prepared.
	let (_dir, mut owner) = reserved(&bytes);
	let out = Scratch::new();
	assert_eq!(
		owner
			.export_tuple(&out.0, MANIFEST, INVENTORY, id.generation, id.cluster)
			.err(),
		Some(OwnerIoError2::ExportNotBound(RootState2::Prepared))
	);
	let pin = owner.export(&out.0, INVENTORY).unwrap();
	fs::write(out.at(MANIFEST), &bytes).unwrap();
	assert_eq!(
		not_bound(&out, &pin),
		tuple_err(E::NotBound(RootState2::Prepared))
	);
	// Conflicted: approved from the observation of another root's committed Bootstrap.
	let (_dir, mut owner) = reserved(&bytes);
	let other = rooted(&M3_OTHER, 241).3;
	let conflicted = owner
		.apply(
			&op_id2([2; 16]).unwrap(),
			&OwnerRequest2::Approve {
				identity: id,
				observation: observe_bootstrap2(&other).unwrap(),
				confirmed: id,
			},
		)
		.unwrap();
	assert_eq!(
		(conflicted.original.map(|e| e.op), conflicted.current),
		(Some(EventOp2::Conflict), RootState2::Conflicted)
	);
	let out = Scratch::new();
	assert_eq!(
		owner
			.export_tuple(&out.0, MANIFEST, INVENTORY, id.generation, id.cluster)
			.err(),
		Some(OwnerIoError2::ExportNotBound(RootState2::Conflicted))
	);
	let pin = owner.export(&out.0, INVENTORY).unwrap();
	fs::write(out.at(MANIFEST), &bytes).unwrap();
	assert_eq!(
		not_bound(&out, &pin),
		tuple_err(E::NotBound(RootState2::Conflicted))
	);
}
/// Deliberate post-export mutations of owner output: one byte past the largest manifest is an
/// oversize read; a short manifest is a non-canonical length; a snapshot past the largest
/// inventory is an oversize read. Length precedes the pin checks.
#[test]
fn mutated_lengths_refuse_before_the_pins() {
	let d = delivered_three(&M3_PRIMARY);
	let (m, i) = (&d.export.manifest_pin, &d.export.inventory_pin);
	let manifest = fs::read(d.out.at(MANIFEST)).unwrap();
	let snapshot = fs::read(d.out.at(INVENTORY)).unwrap();
	let long = [&manifest[..], &[0]].concat();
	fs::write(d.out.at("long.manifest"), &long).unwrap();
	fs::write(d.out.at("short.manifest"), &manifest[..240]).unwrap();
	let mut big = snapshot.clone();
	big.resize(MAX_SNAPSHOT_BYTES2 + 1, 0);
	fs::write(d.out.at("big.inventory"), &big).unwrap();
	for name in ["long.manifest", "short.manifest"] {
		for mp in [m, i] {
			assert_eq!(
				read_at(&d.out.0, name, Some(mp), INVENTORY, Some(i)).err(),
				tuple_err(E::Length(I::Manifest)),
				"{name}"
			);
		}
	}
	for ip in [i, m] {
		assert_eq!(
			read_at(&d.out.0, MANIFEST, Some(m), "big.inventory", Some(ip)).err(),
			tuple_err(E::Length(I::Snapshot))
		);
	}
	// The untouched files still read.
	assert!(read_delivered(&d).is_ok());
}

/// The anchored refusals: a symlink, an escape from the anchor and a non-regular file, for either
/// input; the manifest is read first.
#[test]
fn anchored_refusals_are_reported_per_input() {
	let d = delivered_three(&M3_PRIMARY);
	let (m, i) = (Some(&d.export.manifest_pin), Some(&d.export.inventory_pin));
	symlink(d.out.at(MANIFEST), d.out.at("m.link")).unwrap();
	symlink(d.out.at(INVENTORY), d.out.at("i.link")).unwrap();
	fs::create_dir(d.out.at("dir")).unwrap();
	for (bad, error) in [
		("m.link", AnchoredError2::Symlink),
		("../escape", AnchoredError2::Escape),
		("dir", AnchoredError2::NotRegular),
	] {
		assert_eq!(
			read_at(&d.out.0, bad, m, INVENTORY, i).err(),
			read_err(I::Manifest, error),
			"{bad}"
		);
		assert_eq!(
			read_at(&d.out.0, MANIFEST, m, bad, i).err(),
			read_err(I::Snapshot, error),
			"{bad}"
		);
		assert_eq!(
			read_at(&d.out.0, bad, m, bad, i).err(),
			read_err(I::Manifest, error),
			"{bad}"
		);
	}
	assert_eq!(
		read_at(&d.out.0, MANIFEST, m, "i.link", i).err(),
		read_err(I::Snapshot, AnchoredError2::Symlink)
	);
}

/// Two deterministic replacement states (not a concurrent read): the inventory file replaced in
/// place by a later coherent owner snapshot is refused under the old pin and accepted under the
/// operator's newly supplied pin.
#[test]
fn a_replaced_file_needs_the_newly_supplied_pin() {
	let mut d = delivered_three(&M3_PRIMARY);
	let old = read_delivered(&d).unwrap();
	// A later owner state: a second root reserved, the delivered root still Bound.
	let other = M3_OTHER.to_vec();
	let added = d
		.owner
		.apply(
			&op_id2([4; 16]).unwrap(),
			&OwnerRequest2::Reserve {
				manifest: AnyAdminManifest2::decode(&other).unwrap(),
			},
		)
		.unwrap();
	assert_eq!(added.current, RootState2::Prepared);
	let later = Scratch::new();
	let new_pin = d.owner.export(&later.0, INVENTORY).unwrap();
	assert_ne!(new_pin, d.export.inventory_pin);
	fs::copy(later.at(INVENTORY), d.out.at(INVENTORY)).unwrap();
	let (m, i) = (&d.export.manifest_pin, &d.export.inventory_pin);
	assert_eq!(
		read_at(&d.out.0, MANIFEST, Some(m), INVENTORY, Some(i)).err(),
		tuple_err(E::InventoryPinMismatch)
	);
	let new = read_at(&d.out.0, MANIFEST, Some(m), INVENTORY, Some(&new_pin)).unwrap();
	assert_eq!(tuple_inventory_pin2(&new), new_pin);
	assert!(tuple_export_seq2(&new) > tuple_export_seq2(&old));
	assert!(tuple_record2(&new) == tuple_record2(&old));
}

/// The stated limitation: a tuple captured from the owner before a later Retire stays accepted
/// under its original pins; the owner itself now refuses to export it.
#[test]
fn a_coherent_older_tuple_is_still_accepted() {
	let mut d = delivered_three(&M3_PRIMARY);
	let before = read_delivered(&d).unwrap();
	retire(&mut d);
	let now = Scratch::new();
	assert_eq!(
		d.owner
			.export_tuple(
				&now.0,
				MANIFEST,
				INVENTORY,
				d.identity.generation,
				d.identity.cluster
			)
			.err(),
		Some(OwnerIoError2::ExportNotBound(RootState2::Retired))
	);
	let after = read_delivered(&d).expect("the older coherent tuple is accepted");
	assert!(after == before);
}
