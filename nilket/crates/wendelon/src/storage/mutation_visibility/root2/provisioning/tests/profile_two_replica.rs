//! M3 replica tuple: exact canonical lengths before any digest, digest over the exact supplied
//! bytes, pairing refused by the codec, and the selected manifest's snapshot tag cross-checked
//! (M3 r2 §3). Every fixture snapshot is locally valid and asserted to decode before the tuple is
//! checked; the trailer and the external inventory pin are recomputed after any edit. Manifests are
//! the independent M2 vectors.
use super::profile_two_inventory::p2_manifest;
use super::*;
use crate::storage::mutation_visibility::root2::admin::errors::AdminManifestError2;
use crate::storage::mutation_visibility::root2::admin::manifest::{
	AdminManifest2, AnyAdminManifest2, ManifestProfile2, admin_manifest_digest2,
	admin_manifest_two_digest2,
};
use crate::storage::mutation_visibility::root2::admin::record::{
	BootstrapIdentity2, BootstrapRecord2,
};
use crate::storage::mutation_visibility::root2::codec::hash;
use crate::storage::mutation_visibility::root2::provisioning::anchored::AnchorDir2;
use crate::storage::mutation_visibility::root2::provisioning::inventory::*;
use crate::storage::mutation_visibility::root2::provisioning::replica_anchor::*;
use crate::storage::mutation_visibility::root2::provisioning::replica_read::*;
use crate::storage::mutation_visibility::root2::provisioning::snapshot::*;
use crate::storage::mutation_visibility::root2::types::{Digest2, Log2};
use ReplicaAnchorError2 as E;
use std::ffi::CString;

fn any(name: &str) -> AnyAdminManifest2 {
	AnyAdminManifest2::decode(&p2_manifest(name).0).unwrap()
}
fn identity_of(name: &str) -> RootIdentity2 {
	let (b, pin) = p2_manifest(name);
	let a = any(name);
	let a = a.admission();
	assert_eq!(
		b.len() == 189,
		matches!(any(name), AnyAdminManifest2::Two(_))
	);
	RootIdentity2 {
		generation: a.generation,
		cluster: a.cluster,
		pin,
		admin_key: a.admin_key,
	}
}
fn record(id: &RootIdentity2, epoch: u64, seq: u64) -> BootstrapRecord2 {
	BootstrapRecord2 {
		identity: BootstrapIdentity2 {
			generation: id.generation,
			cluster: id.cluster,
			pin: id.pin,
			initial_epoch: epoch,
			bootstrap: Log2 {
				term: 1,
				node: 1,
				index: 2,
			},
			bootstrap_fingerprint: [0xA0; 32],
			bootstrap_command: [0xB0; 32],
			genesis_fingerprint: [0xC0; 32],
		},
		inventory_seq: seq,
	}
}
/// A root entry for manifest `name`, tagged by `tag_as` (its own manifest, or a profile-1 view of
/// the same Admission fields to forge a tag), optionally Bound with a coherent record.
fn entry(name: &str, tag_as: AnyAdminManifest2, bound: bool, seq: u64) -> RootEntry2 {
	let id = identity_of(name);
	let epoch = any(name).admission().initial_epoch;
	RootEntry2 {
		identity: id,
		manifest: tag_as,
		state: if bound {
			RootState2::Bound
		} else {
			RootState2::Prepared
		},
		last_seq: seq,
		record: bound.then(|| record(&id, epoch, seq)),
	}
}
fn one_view(name: &str) -> AnyAdminManifest2 {
	AnyAdminManifest2::One(any(name).admission().clone())
}
/// Encodes, asserts it decodes, returns the bytes.
fn snapshot(roots: Vec<RootEntry2>) -> Vec<u8> {
	let last_seq = roots.iter().map(|r| r.last_seq).max().unwrap_or(0);
	let b = encode_snapshot2(&LogState2 { roots, last_seq }).unwrap();
	decode_snapshot2(&b).unwrap();
	b
}
fn retag(mut b: Vec<u8>, entry: usize, tag: u8) -> Vec<u8> {
	b[22 + entry * 320 + 97] = tag;
	let b = reseal_snapshot(b);
	decode_snapshot2(&b).expect("the retagged fixture stays locally valid");
	b
}
fn tuple(
	manifest: &[u8],
	pin: &Digest2,
	snapshot: &[u8],
) -> Result<PinnedBoundTuple2, ReplicaAnchorError2> {
	validate_replica_tuple2(&ReplicaInputs2 {
		manifest: Some(manifest),
		manifest_pin: Some(pin),
		snapshot: Some(snapshot),
		inventory_pin: Some(&hash(b"", snapshot)),
	})
}
fn select(name: &str, snapshot: &[u8]) -> Result<PinnedBoundTuple2, ReplicaAnchorError2> {
	let (b, pin) = p2_manifest(name);
	tuple(&b, &pin, snapshot)
}

#[test]
fn correct_tags_form_tuples_for_both_profiles() {
	// A selected Two.
	let s = snapshot(vec![entry("b", any("b"), true, 2)]);
	let t = select("b", &s).unwrap();
	assert!(*tuple_manifest2(&t) == any("b"));
	assert_eq!(tuple_manifest_len2(&t), 189);
	// A selected One inside a mixed version-2 snapshot.
	let s = snapshot(vec![
		entry("a", any("a"), true, 2),
		entry("b", any("b"), false, 1),
	]);
	assert_eq!(u16::from_le_bytes([s[8], s[9]]), 2);
	let t = select("a", &s).unwrap();
	assert_eq!(tuple_manifest_len2(&t), 137);
	assert!(*tuple_manifest2(&t) == any("a"));
	// A selected One in a version-1 snapshot.
	let s = snapshot(vec![entry("a", any("a"), true, 2)]);
	assert_eq!(u16::from_le_bytes([s[8], s[9]]), 1);
	assert!(select("a", &s).is_ok());
}

#[test]
fn the_selected_manifest_tag_is_cross_checked() {
	// Mixed v2: b (selected, Bound) and c (still Two); b's tag flipped 2 -> 1 stays canonical.
	let s = snapshot(vec![
		entry("b", any("b"), true, 2),
		entry("c", any("c"), false, 1),
	]);
	let flipped = retag(s, 0, 1);
	assert_eq!(
		decode_snapshot2(&flipped).unwrap().entries[0].profile,
		ManifestProfile2::One
	);
	assert_eq!(
		select("b", &flipped).err(),
		Some(E::EntryMismatch(TupleField2::Profile))
	);
	// Forged version 1: every tag byte zero, records locally valid, the selected pin is b's.
	let forged = snapshot(vec![entry("b", one_view("b"), true, 2)]);
	assert_eq!(u16::from_le_bytes([forged[8], forged[9]]), 1);
	assert_eq!(
		select("b", &forged).err(),
		Some(E::EntryMismatch(TupleField2::Profile))
	);
	// A selected One whose entry is tagged Two (mixed with another Two).
	let s = snapshot(vec![
		entry("a", any("a"), true, 2),
		entry("b", any("b"), false, 1),
	]);
	assert_eq!(
		select("a", &retag(s, 0, 2)).err(),
		Some(E::EntryMismatch(TupleField2::Profile))
	);
	// An unseen entry's tag is not authenticated here: c's flipped tag does not affect b.
	let s = snapshot(vec![
		entry("b", any("b"), true, 2),
		entry("c", any("c"), false, 1),
		entry("d", any("d"), false, 1),
	]);
	assert!(select("b", &retag(s, 1, 1)).is_ok());
}

#[test]
fn tuple_precedence_pin_admin_key_profile_not_bound() {
	let other_key = identity_of("a").admin_key;
	// Each fault keeps the snapshot locally valid (record pin follows the entry pin).
	let faulted = |pin: bool, key: bool, tag: bool, bound: bool| {
		let mut e = entry("b", if tag { one_view("b") } else { any("b") }, bound, 2);
		if pin {
			e.identity.pin = [0x77; 32];
			if let Some(r) = e.record.as_mut() {
				r.identity.pin = [0x77; 32];
			}
		}
		if key {
			e.identity.admin_key = other_key;
		}
		snapshot(vec![e])
	};
	for (pin, key, tag, bound, want) in [
		(true, true, true, true, E::EntryMismatch(TupleField2::Pin)),
		(
			false,
			true,
			true,
			true,
			E::EntryMismatch(TupleField2::AdminKey),
		),
		(
			false,
			false,
			true,
			false,
			E::EntryMismatch(TupleField2::Profile),
		),
		(
			false,
			false,
			true,
			true,
			E::EntryMismatch(TupleField2::Profile),
		),
		(
			false,
			false,
			false,
			false,
			E::NotBound(RootState2::Prepared),
		),
		(
			true,
			false,
			false,
			false,
			E::EntryMismatch(TupleField2::Pin),
		),
	] {
		assert_eq!(
			select("b", &faulted(pin, key, tag, bound)).err(),
			Some(want),
			"{pin} {key} {tag} {bound}"
		);
	}
	assert!(select("b", &faulted(false, false, false, true)).is_ok());
}

#[test]
fn invalid_pairings_reach_the_codec_with_exact_digests() {
	let s = snapshot(vec![entry("b", any("b"), true, 2)]);
	let (b, b_pin) = p2_manifest("b");
	// Profile 2 at 137 bytes, pinned over exactly those bytes.
	let short: [u8; 137] = b[..137].try_into().unwrap();
	assert_eq!(
		tuple(&short, &admin_manifest_digest2(&short), &s).err(),
		Some(E::ManifestCodec(AdminManifestError2::ProfileLength))
	);
	// Profile 1 at 189 bytes, pinned over exactly those bytes.
	let mut long: [u8; 189] = b.clone().try_into().unwrap();
	long[10..12].copy_from_slice(&1u16.to_le_bytes());
	assert_eq!(
		tuple(&long, &admin_manifest_two_digest2(&long), &s).err(),
		Some(E::ManifestCodec(AdminManifestError2::ProfileLength))
	);
	// Stale-pin control: the same bytes under b's real pin are a pin mismatch, before parsing.
	assert_eq!(tuple(&long, &b_pin, &s).err(), Some(E::ManifestPinMismatch));
	// The profile-1 decoder is unchanged: it still refuses 189 bytes by length.
	assert_eq!(
		AdminManifest2::decode(&b).err(),
		Some(AdminManifestError2::Length)
	);
}

#[test]
fn manifest_lengths_are_settled_before_any_digest() {
	let s = snapshot(vec![entry("b", any("b"), true, 2)]);
	let (b, _) = p2_manifest("b");
	let mut longer = b.clone();
	longer.extend_from_slice(&[0, 0]);
	for bytes in [
		&b[..136],
		&b[..138],
		&b[..188],
		&longer[..190],
		&longer[..191],
	] {
		// A deliberately wrong pin: the length refusal comes first.
		assert_eq!(
			tuple(bytes, &[0; 32], &s).err(),
			Some(E::Length(ReplicaInput2::Manifest)),
			"{}",
			bytes.len()
		);
	}
}

/// A scratch directory removed on drop.
struct Scratch(std::path::PathBuf);
impl Drop for Scratch {
	fn drop(&mut self) {
		let _ = std::fs::remove_dir_all(&self.0);
	}
}

#[test]
fn the_replica_read_bound_is_189() {
	let dir = Scratch(std::env::temp_dir().join(format!(
		"wendelon-profile-two-replica-{}-{}",
		std::process::id(),
		uuid::Uuid::new_v4()
	)));
	std::fs::create_dir_all(&dir.0).unwrap();
	let (b, pin) = p2_manifest("b");
	let s = snapshot(vec![entry("b", any("b"), true, 2)]);
	let sp = hash(b"", &s);
	std::fs::write(dir.0.join("inv"), &s).unwrap();
	let mut over = b.clone();
	over.push(0);
	std::fs::write(dir.0.join("over.adm"), &over).unwrap();
	std::fs::write(dir.0.join("p2.adm"), &b).unwrap();
	let anchor = AnchorDir2::open(&dir.0).unwrap();
	let read = |m: &str| {
		let (mf, sf) = (CString::new(m).unwrap(), CString::new("inv").unwrap());
		read_replica_tuple2(&ReplicaFiles2 {
			anchor: &anchor,
			manifest: &mf,
			manifest_pin: Some(&pin),
			snapshot: &sf,
			inventory_pin: Some(&sp),
		})
	};
	assert_eq!(
		read("over.adm").err(),
		Some(ReplicaReadError2::Tuple(E::Length(ReplicaInput2::Manifest)))
	);
	let t = read("p2.adm").unwrap();
	assert_eq!(tuple_manifest_len2(&t), 189);
}

/// The Rust-built fixtures above are byte-identical to the independent reference
/// (`profile_two_replica_reference.py`), and the pairing pins are its exact-byte digests.
#[test]
fn fixtures_match_the_independent_reference() {
	let v: serde_json::Value =
		serde_json::from_str(include_str!("profile_two_replica_vectors.json")).unwrap();
	let want = |k: &str| unhex(v["snapshots"][k].as_str().unwrap());
	let mixed_b_c = snapshot(vec![
		entry("b", any("b"), true, 2),
		entry("c", any("c"), false, 1),
	]);
	let mixed_a_b = snapshot(vec![
		entry("a", any("a"), true, 2),
		entry("b", any("b"), false, 1),
	]);
	for (k, got) in [
		(
			"selected_two",
			snapshot(vec![entry("b", any("b"), true, 2)]),
		),
		("mixed_a_b", mixed_a_b.clone()),
		(
			"selected_one_v1",
			snapshot(vec![entry("a", any("a"), true, 2)]),
		),
		("mixed_b_c", mixed_b_c.clone()),
		("mixed_b_c_selected_flipped", retag(mixed_b_c, 0, 1)),
		(
			"forged_v1_selected_two",
			snapshot(vec![entry("b", one_view("b"), true, 2)]),
		),
		("mixed_a_b_selected_one_tagged_two", retag(mixed_a_b, 0, 2)),
	] {
		assert_eq!(got, want(k), "{k}");
	}
	for (k, len) in [("p2_at_137", 137), ("p1_at_189", 189)] {
		let bytes = unhex(v["pairings"][k]["bytes"].as_str().unwrap());
		let pin: Digest2 = unhex(v["pairings"][k]["exact_pin"].as_str().unwrap())
			.try_into()
			.unwrap();
		assert_eq!(bytes.len(), len);
		let s = snapshot(vec![entry("b", any("b"), true, 2)]);
		assert_eq!(
			tuple(&bytes, &pin, &s).err(),
			Some(E::ManifestCodec(AdminManifestError2::ProfileLength)),
			"{k}"
		);
	}
}
