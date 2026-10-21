//! Profile-3 ports of the `replica_anchor.rs` rows: the pinned tuple refusals with the same typed
//! errors at the same steps for 241-byte manifests (240/242 for the wrong lengths) and tag-3 v3/v4
//! snapshots, from the independent `p3_` anchor vectors. The empty snapshot stays the kept v1 one.
use super::*;
use crate::storage::mutation_visibility::root2::admin::manifest::{
	AdminManifestThree3, AnyAdminManifest2, any_manifest_digest2,
};
use crate::storage::mutation_visibility::root2::admin::record::BootstrapRecord2;
use crate::storage::mutation_visibility::root2::provisioning::inventory::*;
use crate::storage::mutation_visibility::root2::provisioning::replica_anchor::*;
use crate::storage::mutation_visibility::root2::provisioning::snapshot::{
	SnapshotError2, encode_snapshot2,
};
use ReplicaAnchorError2 as E;

fn sha(b: &[u8]) -> [u8; 32] {
	crate::storage::mutation_visibility::root2::codec::hash(b"", b)
}
fn anchor_vectors() -> serde_json::Value {
	serde_json::from_str(include_str!("anchor_vectors.json")).unwrap()
}
fn pinned(v: &serde_json::Value) -> (Vec<u8>, [u8; 32]) {
	let bytes = unhex(v["bytes"].as_str().unwrap());
	let pin = unhex(v["pin"].as_str().unwrap());
	(bytes, pin.try_into().unwrap())
}
fn manifest(name: &str) -> (Vec<u8>, [u8; 32]) {
	pinned(&anchor_vectors()["p3_manifests"][name])
}
fn snapshot(name: &str) -> (Vec<u8>, [u8; 32]) {
	let v = anchor_vectors();
	if name == "empty" {
		return pinned(&v["snapshots"]["empty"]);
	}
	pinned(&v["p3_snapshots"][name])
}
fn check(
	m: &[u8],
	mp: &[u8; 32],
	s: &[u8],
	sp: &[u8; 32],
) -> Result<PinnedBoundTuple2, ReplicaAnchorError2> {
	validate_replica_tuple2(&ReplicaInputs2 {
		manifest: Some(m),
		manifest_pin: Some(mp),
		snapshot: Some(s),
		inventory_pin: Some(sp),
	})
}
// A snapshot holding exactly one tag-3 root, encoded by the accepted codec, with its file pin.
fn one_root(
	identity: RootIdentity2,
	state: RootState2,
	record: Option<BootstrapRecord2>,
) -> (Vec<u8>, [u8; 32]) {
	let EventBody2::Reserve { manifest } = p3_event("reserve").body else {
		unreachable!()
	};
	let last_seq = record.map_or(1, |r| r.inventory_seq);
	let bytes = encode_snapshot2(&LogState2 {
		roots: vec![RootEntry2 {
			terminal: None,
			identity,
			manifest,
			state,
			last_seq,
			record,
		}],
		last_seq,
	})
	.unwrap();
	let pin = sha(&bytes);
	(bytes, pin)
}

#[test]
fn the_golden_bound_tuple_matches_the_independent_vectors() {
	let v = anchor_vectors();
	let x = &v["p3_bound_m1"];
	let (m, mp) = manifest("m1");
	assert_eq!(m, unhex(x["manifest"].as_str().unwrap()));
	let (s, sp) = snapshot("bound");
	let t = check(&m, &mp, &s, &sp).unwrap();
	assert_eq!(tuple_manifest_len2(&t), 241);
	assert_eq!(tuple_manifest2(&t).encode().unwrap().as_slice(), m);
	let manifest = tuple_manifest2(&t).admission();
	let f = &x["fields"];
	assert_eq!(
		manifest.generation.to_vec(),
		unhex(f["generation"].as_str().unwrap())
	);
	assert_eq!(
		manifest.cluster.to_vec(),
		unhex(f["cluster"].as_str().unwrap())
	);
	assert_eq!(manifest.initial_epoch, f["initial_epoch"].as_u64().unwrap());
	assert_eq!(
		manifest.admin_id.to_vec(),
		unhex(f["admin_id"].as_str().unwrap())
	);
	assert_eq!(
		manifest.admin_key.to_vec(),
		unhex(f["admin_key"].as_str().unwrap())
	);
	assert_eq!(manifest.namespace.tenant, f["tenant"].as_u64().unwrap());
	assert_eq!(
		manifest.namespace.scope as u64,
		f["scope"].as_u64().unwrap()
	);
	assert_eq!(
		manifest.namespace.table.to_vec(),
		unhex(f["table"].as_str().unwrap())
	);
	assert_eq!(manifest.namespace.schema, f["schema"].as_u64().unwrap());
	assert_eq!(
		tuple_manifest_pin2(&t).to_vec(),
		unhex(x["manifest_pin"].as_str().unwrap())
	);
	assert_eq!(
		tuple_inventory_pin2(&t).to_vec(),
		unhex(x["inventory_pin"].as_str().unwrap())
	);
	assert_eq!(tuple_export_seq2(&t), x["export_seq"].as_u64().unwrap());
	assert_eq!(
		tuple_entry_last_seq2(&t),
		x["entry_last_seq"].as_u64().unwrap()
	);
	assert_eq!(
		tuple_record2(&t).encode().unwrap().to_vec(),
		unhex(x["record"].as_str().unwrap())
	);
	assert_eq!(format!("{t:?}"), "PinnedBoundTuple2 { redacted }");
}
#[test]
fn absence_is_a_missing_prerequisite_in_input_order() {
	let (m, mp) = manifest("m1");
	let (s, sp) = snapshot("bound");
	let short = &m[..10];
	for mask in 0u8..16 {
		let has = |i: u8| mask & (1 << i) != 0;
		let inputs = ReplicaInputs2 {
			// A malformed manifest never outranks an absent later input.
			manifest: has(0).then_some(short),
			manifest_pin: has(1).then_some(&mp),
			snapshot: has(2).then_some(s.as_slice()),
			inventory_pin: has(3).then_some(&sp),
		};
		let first_missing = (0..4).find(|i| !has(*i));
		let expected = match first_missing {
			Some(0) => E::PrerequisiteUnavailable(ReplicaInput2::Manifest),
			Some(1) => E::PrerequisiteUnavailable(ReplicaInput2::ManifestPin),
			Some(2) => E::PrerequisiteUnavailable(ReplicaInput2::Snapshot),
			Some(3) => E::PrerequisiteUnavailable(ReplicaInput2::InventoryPin),
			_ => E::Length(ReplicaInput2::Manifest),
		};
		assert_eq!(
			validate_replica_tuple2(&inputs).err(),
			Some(expected),
			"mask {mask}"
		);
	}
}
#[test]
fn lengths_then_digests_then_codecs() {
	let (m, mp) = manifest("m1");
	let (s, sp) = snapshot("bound");
	assert!(check(&m, &mp, &s, &sp).is_ok());
	for bad in [&m[..240], &[m.as_slice(), &[0]].concat()[..]] {
		assert_eq!(
			check(bad, &mp, &s, &sp).err(),
			Some(E::Length(ReplicaInput2::Manifest))
		);
	}
	let huge = vec![0u8; 22 + MAX_ROOTS2 * 320 + 32 + 1];
	assert_eq!(
		check(&m, &mp, &huge, &sha(&huge)).err(),
		Some(E::Length(ReplicaInput2::Snapshot))
	);
	// Each pin is bound to the exact bytes; a mismatch outranks malformed bytes.
	let mut wrong = mp;
	wrong[0] ^= 1;
	assert_eq!(
		check(&m, &wrong, &s, &sp).err(),
		Some(E::ManifestPinMismatch)
	);
	let mut wrong = sp;
	wrong[31] ^= 1;
	assert_eq!(
		check(&m, &mp, &s, &wrong).err(),
		Some(E::InventoryPinMismatch)
	);
	let garbage = [0x5a; 241];
	assert_eq!(
		check(&garbage, &mp, &s, &sp).err(),
		Some(E::ManifestPinMismatch)
	);
	assert_eq!(
		check(&m, &mp, &[1; 40], &sp).err(),
		Some(E::InventoryPinMismatch)
	);
	let (other, _) = snapshot("prepared");
	assert_eq!(
		check(&m, &mp, &other, &sp).err(),
		Some(E::InventoryPinMismatch)
	);
	// Malformed bytes whose own pins match reach the accepted codecs.
	let mut zero_epoch = m.clone();
	zero_epoch[44..52].copy_from_slice(&0u64.to_le_bytes());
	let zp = crate::storage::mutation_visibility::root2::codec::hash(
		b"VISROOT2-ADMIN-MANIFEST\0",
		&zero_epoch,
	);
	assert!(matches!(
		check(&zero_epoch, &zp, &s, &sp).err(),
		Some(E::ManifestCodec(_))
	));
	for short in [&[][..], &[0u8; 10][..]] {
		assert_eq!(
			check(&m, &mp, short, &sha(short)).err(),
			Some(E::SnapshotCodec(SnapshotError2::Length))
		);
	}
	let mut bad_state = s.clone();
	bad_state[22 + 96] = 9;
	let bad_state = reseal_snapshot(bad_state);
	assert_eq!(
		check(&m, &mp, &bad_state, &sha(&bad_state)).err(),
		Some(E::SnapshotCodec(SnapshotError2::State))
	);
	let (full, _) = snapshot("full");
	let mut swapped = full.clone();
	let (a, b) = (full[22..342].to_vec(), full[342..662].to_vec());
	swapped[22..342].copy_from_slice(&b);
	swapped[342..662].copy_from_slice(&a);
	let swapped = reseal_snapshot(swapped);
	assert_eq!(
		check(&m, &mp, &swapped, &sha(&swapped)).err(),
		Some(E::SnapshotCodec(SnapshotError2::Order))
	);
}
#[test]
fn only_a_coherent_bound_entry_forms_a_tuple() {
	let (m1, p1) = manifest("m1");
	let (m2, p2) = manifest("m2");
	for (m, p, snap, expected) in [
		(&m2, &p2, "bound", E::NotInInventory),
		(&m1, &p1, "empty", E::NotInInventory),
		(&m1, &p1, "prepared", E::NotBound(RootState2::Prepared)),
		(&m1, &p1, "full", E::NotBound(RootState2::Retired)),
		(&m2, &p2, "full", E::NotBound(RootState2::Conflicted)),
	] {
		let (s, sp) = snapshot(snap);
		assert_eq!(check(m, p, &s, &sp).err(), Some(expected), "{snap}");
	}
	// Same generation and cluster, but a different pin or administrator key in the entry.
	let id = p3_event("reserve").identity.unwrap();
	let mut other_pin = id;
	other_pin.pin[0] ^= 1;
	let (s, sp) = one_root(other_pin, RootState2::Prepared, None);
	assert_eq!(
		check(&m1, &p1, &s, &sp).err(),
		Some(E::EntryMismatch(TupleField2::Pin))
	);
	let mut other_key = id;
	other_key.admin_key = ed25519_dalek::SigningKey::from_bytes(&[0x61; 32])
		.verifying_key()
		.to_bytes();
	let (s, sp) = one_root(other_key, RootState2::Prepared, None);
	assert_eq!(
		check(&m1, &p1, &s, &sp).err(),
		Some(E::EntryMismatch(TupleField2::AdminKey))
	);
	// The epoch is the one binding the snapshot cannot make: rebind and re-pin both the entry and
	// its record to a manifest whose epoch differs from the approved record's.
	let AnyAdminManifest2::Three(three) = AnyAdminManifest2::decode(&m1).unwrap() else {
		unreachable!()
	};
	let mut base = three.base().clone();
	base.initial_epoch += 1;
	let changed = AdminManifestThree3::new(base, three.governance(), three.recovery()).unwrap();
	let me = changed.encode().unwrap().to_vec();
	let pe = any_manifest_digest2(&AnyAdminManifest2::Three(changed).encode().unwrap());
	let EventBody2::Approve { mut record } = p3_event("approve").body else {
		unreachable!()
	};
	record.identity.pin = pe;
	let mut entry = id;
	entry.pin = pe;
	let (s, sp) = one_root(entry, RootState2::Bound, Some(record));
	assert_eq!(
		check(&me, &pe, &s, &sp).err(),
		Some(E::RecordBinding(TupleField2::InitialEpoch))
	);
	// Generation, cluster and pin record bindings are unreachable here: snapshot decoding binds
	// the record to the entry, and the entry checks above bind the entry to the manifest.
}
#[test]
fn an_older_coherent_pinned_snapshot_is_still_accepted() {
	// The "bound" snapshot predates the "full" one in which m1 is Retired. Both are internally
	// coherent and pinned; this check does not detect rollback or prove current revocation.
	let (m, mp) = manifest("m1");
	let (old, op) = snapshot("bound");
	let (new, np) = snapshot("full");
	assert!(check(&m, &mp, &old, &op).is_ok());
	assert_eq!(
		check(&m, &mp, &new, &np).err(),
		Some(E::NotBound(RootState2::Retired))
	);
}
