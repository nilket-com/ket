//! M2: profile-2 manifests through owner request -> event (version 2) -> fold -> snapshot
//! (version 2). Every byte comes from the independent `profile_two_inventory_reference.py`.
use super::*;
use crate::storage::mutation_visibility::root2::admin::errors::AdminManifestError2;
use crate::storage::mutation_visibility::root2::admin::manifest::{
	AnyAdminManifest2, ManifestProfile2, admin_manifest_digest2, any_manifest_digest2,
};
use crate::storage::mutation_visibility::root2::admin::record::BootstrapObservation2;
use crate::storage::mutation_visibility::root2::provisioning::inventory::*;
use crate::storage::mutation_visibility::root2::provisioning::owner_core::*;
use crate::storage::mutation_visibility::root2::provisioning::snapshot::*;

pub(super) fn p2() -> serde_json::Value {
	serde_json::from_str(include_str!("profile_two_inventory_vectors.json")).unwrap()
}
pub(super) fn p2_manifest(name: &str) -> (Vec<u8>, [u8; 32]) {
	let v = &p2()["manifests"][name];
	(
		unhex(v["bytes"].as_str().unwrap()),
		unhex(v["pin"].as_str().unwrap()).try_into().unwrap(),
	)
}
pub(super) fn p2_bytes(field: &str) -> Vec<u8> {
	unhex(p2()[field].as_str().unwrap())
}
pub(super) fn p2_events() -> Vec<Vec<u8>> {
	p2()["mixed_log"]
		.as_array()
		.unwrap()
		.iter()
		.map(|e| unhex(e.as_str().unwrap()))
		.collect()
}
pub(super) fn p2_errors(kind: &str) -> Vec<(String, Vec<u8>, Option<String>)> {
	p2()[kind]
		.as_array()
		.unwrap()
		.iter()
		.map(|e| {
			let want = e["reason"]
				.as_str()
				.or(e["error"].as_str())
				.map(str::to_owned);
			(
				e["name"].as_str().unwrap().to_owned(),
				unhex(e["bytes"].as_str().unwrap()),
				want,
			)
		})
		.collect()
}
fn reason(name: &str) -> CorruptReason2 {
	use CorruptReason2::*;
	match name {
		"Checksum" => Checksum,
		"Magic" => Magic,
		"Version" => Version,
		"UnknownOp" => UnknownOp,
		"Reserved" => Reserved,
		"ZeroOpId" => ZeroOpId,
		"Header" => Header,
		"Payload" => Payload,
		other => panic!("unknown reason {other}"),
	}
}
fn snapshot_error(name: &str) -> SnapshotError2 {
	use SnapshotError2::*;
	match name {
		"Length" => Length,
		"Version" => Version,
		"Reserved" => Reserved,
		"State" => State,
		"Trailer" => Trailer,
		"NotCanonical" => NotCanonical,
		"ProfileTag" => ProfileTag,
		other => panic!("unknown error {other}"),
	}
}
fn any(name: &str) -> AnyAdminManifest2 {
	AnyAdminManifest2::decode(&p2_manifest(name).0).unwrap()
}
// The header a Reserve must carry, from the generator's whole-manifest pin.
fn identity(name: &str) -> RootIdentity2 {
	let (b, pin) = p2_manifest(name);
	RootIdentity2 {
		generation: b[12..28].try_into().unwrap(),
		cluster: b[28..44].try_into().unwrap(),
		pin,
		admin_key: b[68..100].try_into().unwrap(),
	}
}
fn reserve_event(seq: u64, op: u8, name: &str, identity: RootIdentity2) -> Vec<u8> {
	encode_event2(&Event2 {
		seq,
		op_id: [op; 16],
		identity: Some(identity),
		body: EventBody2::Reserve {
			manifest: any(name),
		},
	})
	.unwrap()
	.to_vec()
}
fn corrupt_at(log: &[u8]) -> Option<(u64, CorruptReason2)> {
	match fold_events2(log) {
		Err(LogError2::Corrupt { seq, reason }) => Some((seq, reason)),
		_ => None,
	}
}

#[test]
fn v2_event_round_trip_and_exact_decode_order() {
	let events = p2_events();
	assert_eq!(events.len(), 8);
	for (i, b) in events.iter().enumerate() {
		let e = decode_event2(b).unwrap();
		assert_eq!(encode_event2(&e).unwrap().to_vec(), *b, "event {i}");
		let two = matches!(&e.body, EventBody2::Reserve { manifest } if manifest.profile() == ManifestProfile2::Two);
		assert_eq!(u16::from_le_bytes([b[8], b[9]]), if two { 2 } else { 1 });
	}
	// The decoded profile-2 Reserve carries the whole manifest, byte for byte.
	let EventBody2::Reserve { manifest } = decode_event2(&events[1]).unwrap().body else {
		unreachable!()
	};
	assert_eq!(
		manifest.encode().unwrap().as_slice(),
		&p2_manifest("b").0[..]
	);
	let errors = p2_errors("event_errors");
	assert_eq!(errors.len(), 24);
	for (name, b, want) in errors {
		let want = reason(&want.unwrap());
		assert!(
			matches!(decode_event2(&b), Err(LogError2::Corrupt { reason, .. }) if reason == want),
			"{name}: {:?}",
			decode_event2(&b).err()
		);
	}
}

#[test]
fn v1_events_and_goldens_are_unchanged() {
	for name in [
		"reserve",
		"approve",
		"reserve_second",
		"conflict",
		"retire",
		"repair",
	] {
		let b = event_bytes(name);
		let e = decode_event2(&b).unwrap();
		assert_eq!(encode_event2(&e).unwrap().to_vec(), b, "{name}");
		assert_eq!(u16::from_le_bytes([b[8], b[9]]), 1);
		if let EventBody2::Reserve { manifest } = &e.body {
			assert_eq!(manifest.profile(), ManifestProfile2::One);
			// A profile-1 pin is still the profile-1 digest of the same 137 bytes.
			let AnyAdminManifest2::One(m) = manifest else {
				unreachable!()
			};
			assert_eq!(
				e.identity.unwrap().pin,
				admin_manifest_digest2(&m.encode().unwrap())
			);
			assert_eq!(
				any_manifest_digest2(&manifest.encode().unwrap()),
				e.identity.unwrap().pin
			);
		}
	}
	let log = log_bytes();
	for (name, events) in [("empty", 0), ("prepared", 1), ("bound", 2), ("full", 6)] {
		let state = fold_events2(&log[..events * 384]).unwrap();
		assert_eq!(encode_snapshot2(&state).unwrap(), snapshot_bytes(name));
		let s = decode_snapshot2(&snapshot_bytes(name)).unwrap();
		assert!(s.entries.iter().all(|e| e.profile == ManifestProfile2::One));
		assert_eq!(
			u16::from_le_bytes([snapshot_bytes(name)[8], snapshot_bytes(name)[9]]),
			1
		);
	}
	// The M1-pinned outcome stays: a profile-2 payload in a version-1 Reserve is Payload.
	let b = p2_errors("event_errors")
		.into_iter()
		.find(|e| e.0 == "v1_profile_two_payload")
		.unwrap()
		.1;
	assert!(matches!(
		decode_event2(&b),
		Err(LogError2::Corrupt {
			reason: CorruptReason2::Payload,
			..
		})
	));
}

#[test]
fn snapshot_v2_tags_and_canonical_version() {
	use ManifestProfile2::{One, Two};
	for (field, version, tags) in [
		("mixed_snapshot", 2, vec![One, Two, Two]),
		("two_snapshot", 2, vec![Two]),
		("one_snapshot", 1, vec![One]),
	] {
		let b = p2_bytes(field);
		assert_eq!(u16::from_le_bytes([b[8], b[9]]), version, "{field}");
		let s = decode_snapshot2(&b).unwrap();
		let got: Vec<_> = s.entries.iter().map(|e| e.profile).collect();
		assert_eq!(got, tags, "{field}");
	}
	// Version 2 writes both tags explicitly; nothing defaults to profile 1.
	let mixed = p2_bytes("mixed_snapshot");
	assert_eq!(
		[mixed[22 + 97], mixed[22 + 320 + 97], mixed[22 + 640 + 97]],
		[1, 2, 2]
	);
	let errors = p2_errors("snapshot_errors");
	assert_eq!(errors.len(), 19);
	for (name, b, want) in errors {
		match want {
			Some(want) => assert_eq!(
				decode_snapshot2(&b).err(),
				Some(snapshot_error(&want)),
				"{name}"
			),
			// Decode sees no manifest: a flipped valid tag is locally well formed.
			None => {
				let s = decode_snapshot2(&b).unwrap();
				assert_eq!(s.entries[1].profile, One, "{name}");
			}
		}
	}
}

#[test]
fn fold_and_export_mixed_log() {
	let events = p2_events();
	let log = events.concat();
	// Every prefix is a valid generated transition sequence.
	for n in 0..=events.len() {
		let state = fold_events2(&log[..n * 384]).unwrap();
		assert_eq!(state.last_seq, n as u64);
	}
	let state = fold_events2(&log).unwrap();
	let roots: Vec<_> = state
		.roots
		.iter()
		.map(|r| (r.manifest.profile(), r.state, r.last_seq))
		.collect();
	assert_eq!(
		roots,
		[
			(ManifestProfile2::One, RootState2::Retired, 7),
			(ManifestProfile2::Two, RootState2::Bound, 5),
			(ManifestProfile2::Two, RootState2::Conflicted, 6)
		]
	);
	for (root, name) in state.roots.iter().zip(["a", "b", "c"]) {
		assert!(root.manifest == any(name));
		assert_eq!(root.identity, identity(name));
	}
	// The pin binds the whole manifest, never the Admission subview's digest.
	for root in &state.roots[1..] {
		let admission = admin_manifest_digest2(&root.manifest.admission().encode().unwrap());
		assert_ne!(root.identity.pin, admission);
	}
	assert_eq!(
		encode_snapshot2(&state).unwrap(),
		p2_bytes("mixed_snapshot")
	);
	let two = fold_events2(&p2_bytes("two_log")).unwrap();
	assert_eq!(encode_snapshot2(&two).unwrap(), p2_bytes("two_snapshot"));
	let one = fold_events2(&p2_bytes("one_log")).unwrap();
	assert_eq!(encode_snapshot2(&one).unwrap(), p2_bytes("one_snapshot"));
	// The encoder emits only what the decoder accepts, and every entry's tag is its manifest's.
	let s = decode_snapshot2(&p2_bytes("mixed_snapshot")).unwrap();
	for (e, r) in s.entries.iter().zip(&state.roots) {
		assert!(e.profile == r.manifest.profile() && e.identity == r.identity);
	}
}

/// Profile-2 manifests sharing the Admission fields (and so the Admission key) but not the
/// governance id or key are distinct pins and distinct roots; the shared-component policy refuses
/// the second, and no Admission-only digest stands in for the whole-manifest pin.
#[test]
fn pin_binds_governance_not_admin_key() {
	let b = identity("b");
	let variants = [
		"b_governance_id",
		"b_governance_key",
		"b_admission_as_profile_one",
	];
	let mut pins = vec![b.pin];
	for name in variants {
		let id = identity(name);
		assert_eq!(
			(id.generation, id.cluster, id.admin_key),
			(b.generation, b.cluster, b.admin_key)
		);
		pins.push(id.pin);
		// Folded after B: the second root shares components.
		let log = [reserve_event(1, 1, "b", b), reserve_event(2, 2, name, id)].concat();
		assert_eq!(
			corrupt_at(&log),
			Some((2, CorruptReason2::IdentityTaken)),
			"{name}"
		);
		// Its own header claiming B's pin is not its pin.
		let lie = [reserve_event(1, 1, "b", b), reserve_event(2, 2, name, b)].concat();
		assert_eq!(
			corrupt_at(&lie),
			Some((2, CorruptReason2::Binding)),
			"{name}"
		);
		// Owner decisions: the same refusals, before any append.
		let first = reserve_event(1, 1, "b", b);
		assert_eq!(
			decide2(
				&first,
				&op_id2([9; 16]).unwrap(),
				&OwnerRequest2::Reserve {
					manifest: any(name)
				}
			),
			Decision2::Refuse(OwnerError2::IdentityTaken)
		);
		// Same generation and cluster, other pin: a mismatch, not a substitute.
		let observation = BootstrapObservation2::decode(&p2_events()[5][132..340]).unwrap();
		assert_eq!(
			decide2(
				&first,
				&op_id2([9; 16]).unwrap(),
				&OwnerRequest2::Approve {
					identity: id,
					observation,
					confirmed: id
				}
			),
			Decision2::Refuse(OwnerError2::IdentityMismatch)
		);
	}
	pins.sort();
	pins.dedup();
	assert_eq!(pins.len(), 4, "every whole manifest has its own pin");
	// The Admission subview's digest is not B's pin and cannot reserve B.
	let admission = AnyAdminManifest2::One(any("b").admission().clone());
	let admission_pin = admin_manifest_digest2(&any("b").admission().encode().unwrap());
	assert_ne!(admission_pin, b.pin);
	let mut header = b;
	header.pin = admission_pin;
	assert_eq!(
		corrupt_at(&reserve_event(1, 1, "b", header)),
		Some((1, CorruptReason2::Binding))
	);
	assert!(admission == any("b_admission_as_profile_one"));
	// The same request again is AlreadyReserved; a retry by its op id is recognised by its exact
	// bytes.
	let first = reserve_event(1, 1, "b", b);
	let again = OwnerRequest2::Reserve { manifest: any("b") };
	assert_eq!(
		decide2(&first, &op_id2([9; 16]).unwrap(), &again),
		Decision2::Refuse(OwnerError2::AlreadyReserved)
	);
	assert!(matches!(
		decide2(&first, &op_id2([1; 16]).unwrap(), &again),
		Decision2::Retry {
			original: EventRef2 {
				op: EventOp2::Reserve,
				seq: 1
			},
			..
		}
	));
	assert_eq!(
		decide2(
			&first,
			&op_id2([1; 16]).unwrap(),
			&OwnerRequest2::Reserve {
				manifest: any("b_governance_key")
			}
		),
		Decision2::Refuse(OwnerError2::OpIdReused { seq: 1 })
	);
	// A different mask is not a second manifest: it does not decode.
	let mut mask = p2_manifest("b").0;
	mask[185..189].copy_from_slice(&0xFFu32.to_le_bytes());
	assert_eq!(
		AnyAdminManifest2::decode(&mask).err(),
		Some(AdminManifestError2::UnsupportedAction)
	);
}
