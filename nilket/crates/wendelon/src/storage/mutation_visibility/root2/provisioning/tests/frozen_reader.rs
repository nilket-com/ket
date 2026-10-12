//! The old reader, executed: the frozen d6ce07b8 decoders and fold (see `frozen_d6ce07b8`) refuse
//! every M2 version-2 artifact at their version check and accept every version-1-only artifact
//! byte-identically. A frozen decoder and fold proof plus the separate owner source trace; it is
//! not execution of the old `OwnerDir2`.
use super::frozen_d6ce07b8::inventory as old;
use super::frozen_d6ce07b8::snapshot as old_snapshot;
use super::profile_two_inventory::{p2_bytes, p2_errors, p2_events};
use super::*;
use crate::storage::mutation_visibility::root2::codec::hash;
use crate::storage::mutation_visibility::root2::provisioning::inventory as live;
use crate::storage::mutation_visibility::root2::provisioning::snapshot as live_snapshot;

/// SHA-256 of `git show d6ce07b8:<path> | tail -n +3`, verified when the source package was
/// prepared (never by running git here).
const INVENTORY_EXCERPT: &str = "bcf8617987117b594f2fe03291cb2c4d1034f5821205cb8167c8653b19e42f0c";
const SNAPSHOT_EXCERPT: &str = "fdb231104b9b9cda8a66fca524d48a4fcdff657905ea11a064ea1294bdfbf54a";

fn hex(d: [u8; 32]) -> String {
	d.iter().map(|b| format!("{b:02x}")).collect()
}
fn old_version(b: &[u8]) -> bool {
	matches!(
		old::decode_event2(b),
		Err(old::LogError2::Corrupt {
			reason: old::CorruptReason2::Version,
			..
		})
	)
}

#[test]
fn frozen_excerpt_matches_published_source() {
	let inventory = include_bytes!("frozen_d6ce07b8/inventory.in");
	let snapshot = include_bytes!("frozen_d6ce07b8/snapshot.in");
	assert_eq!(hex(hash(b"", inventory)), INVENTORY_EXCERPT);
	assert_eq!(hex(hash(b"", snapshot)), SNAPSHOT_EXCERPT);
	assert_eq!(inventory.iter().filter(|&&b| b == b'\n').count(), 473);
	assert_eq!(snapshot.iter().filter(|&&b| b == b'\n').count(), 228);
}

#[test]
fn frozen_reader_refuses_every_v2_artifact_and_accepts_v1() {
	// Events: every version-2 event refuses with Version; every version-1 event decodes and
	// re-encodes to the same bytes through the frozen codec.
	let mut v1 = p2_events();
	v1.push(p2_bytes("one_log"));
	for name in [
		"reserve",
		"approve",
		"reserve_second",
		"conflict",
		"retire",
		"repair",
	] {
		v1.push(event_bytes(name));
	}
	let v2 = p2_bytes("two_log")[..384].to_vec();
	let (v2s, v1s): (Vec<_>, Vec<_>) = v1
		.into_iter()
		.chain([v2])
		.partition(|b| u16::from_le_bytes([b[8], b[9]]) == 2);
	assert_eq!((v2s.len(), v1s.len()), (3, 13));
	for b in &v2s {
		assert!(old_version(b));
		assert!(live::decode_event2(b).is_ok());
	}
	for b in &v1s {
		let e = old::decode_event2(b).unwrap();
		assert_eq!(old::encode_event2(&e).unwrap().to_vec(), *b);
	}
	// Every version-2 refusal vector the frozen reader reaches past checksum and magic is a
	// Version refusal there too.
	for (name, b, _) in p2_errors("event_errors") {
		let version = u16::from_le_bytes([b[8], b[9]]);
		let sealed = hash(b"", &b[..352])[..] == b[352..] && &b[..8] == b"VISIEV02";
		if version != 1 && sealed {
			assert!(old_version(&b), "{name}");
		}
	}
	// Folds: the mixed log is Corrupt at its first version-2 event (seq 2), never Ambiguous, so
	// the old classify path reports a log error and offers no repair; a version-2 final event is
	// checksum-valid and still Corrupt.
	let mixed = p2_events().concat();
	assert!(matches!(
		old::fold_events2(&mixed),
		Err(old::LogError2::Corrupt {
			seq: 2,
			reason: old::CorruptReason2::Version
		})
	));
	// The generated version-2 Reserve of B is seq 2, so it follows the profile-1 log directly.
	let last = [p2_bytes("one_log"), p2_events()[1].clone()].concat();
	assert!(matches!(
		old::fold_events2(&last),
		Err(old::LogError2::Corrupt {
			seq: 2,
			reason: old::CorruptReason2::Version
		})
	));
	assert!(live::fold_events2(&last).is_ok());
	// Version-1-only logs: the frozen fold and export equal the live ones byte for byte.
	let golden = log_bytes();
	for log in [
		&golden[..0],
		&golden[..384],
		&golden[..768],
		&golden[..],
		&p2_bytes("one_log")[..],
		&mixed[..0],
	] {
		let old_state = old::fold_events2(log).unwrap();
		let new_state = live::fold_events2(log).unwrap();
		let old_bytes = old_snapshot::encode_snapshot2(&old_state).unwrap();
		assert_eq!(
			old_bytes,
			live_snapshot::encode_snapshot2(&new_state).unwrap()
		);
		assert!(old_snapshot::decode_snapshot2(&old_bytes).is_ok());
	}
	// Snapshots: every version-2 snapshot refuses with Version; version 1 is accepted unchanged.
	for field in ["mixed_snapshot", "two_snapshot"] {
		assert_eq!(
			old_snapshot::decode_snapshot2(&p2_bytes(field)).err(),
			Some(old_snapshot::SnapshotError2::Version)
		);
	}
	for (name, b, _) in p2_errors("snapshot_errors") {
		if b.len() >= 54 && &b[..8] == b"VISINV02" && u16::from_le_bytes([b[8], b[9]]) != 1 {
			assert_eq!(
				old_snapshot::decode_snapshot2(&b).err(),
				Some(old_snapshot::SnapshotError2::Version),
				"{name}"
			);
		}
	}
	for b in [
		p2_bytes("one_snapshot"),
		snapshot_bytes("empty"),
		snapshot_bytes("prepared"),
		snapshot_bytes("bound"),
		snapshot_bytes("full"),
	] {
		assert!(old_snapshot::decode_snapshot2(&b).is_ok());
		assert!(live_snapshot::decode_snapshot2(&b).is_ok());
	}
}
