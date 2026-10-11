//! M1 dormancy: every unchanged provisioning consumer refuses profile-2 bytes through its EXISTING
//! code, reached with coherent checksums and pins. No consumer was edited for this.
use super::*;
use crate::storage::mutation_visibility::root2::admin::manifest::{
	ADMIN_MANIFEST_P2_BYTES2, admin_manifest_two_digest2,
};
use crate::storage::mutation_visibility::root2::provisioning::anchored::AnchorDir2;
use crate::storage::mutation_visibility::root2::provisioning::inventory::*;
use crate::storage::mutation_visibility::root2::provisioning::replica_anchor::*;
use crate::storage::mutation_visibility::root2::provisioning::replica_read::*;
use crate::storage::mutation_visibility::root2::provisioning::snapshot::*;
use std::ffi::CString;

fn profile_two() -> Vec<u8> {
	let v: serde_json::Value =
		serde_json::from_str(include_str!("../../admin/tests/profile_two_vectors.json")).unwrap();
	unhex(v["valid"][0]["bytes"].as_str().unwrap())
}
fn pin_two(b: &[u8]) -> [u8; 32] {
	let fixed: [u8; ADMIN_MANIFEST_P2_BYTES2] = b.try_into().unwrap();
	admin_manifest_two_digest2(&fixed)
}

#[test]
fn a_reserve_event_carrying_profile_two_bytes_is_payload_corrupt() {
	// The golden Reserve event with its payload replaced by profile-2 bytes, resealed so the
	// checksum, magic, version, op and reserved checks all pass.
	let mut b = event_bytes("reserve");
	b[132..132 + 189].copy_from_slice(&profile_two());
	let b = reseal_event(b);
	assert!(matches!(
		decode_event2(&b),
		Err(LogError2::Corrupt {
			reason: CorruptReason2::Payload,
			..
		})
	));
	assert!(fold_events2(&b).is_err(), "no fold of a profile-2 Reserve");
}

#[test]
fn a_profile_tag_in_a_v1_snapshot_is_a_reserved_byte() {
	// HEADER (22) + entry byte 97: the reserved byte a later v2 would use for the profile tag.
	let mut b = snapshot_bytes("bound");
	b[22 + 97] = 2;
	assert_eq!(
		decode_snapshot2(&reseal_snapshot(b)).err(),
		Some(SnapshotError2::Reserved)
	);
}

#[test]
fn the_replica_tuple_refuses_profile_two_by_length_before_any_digest() {
	let m = profile_two();
	let pin = pin_two(&m);
	let s = snapshot_bytes("bound");
	let sp = crate::storage::mutation_visibility::root2::codec::hash(b"", &s);
	assert_eq!(
		validate_replica_tuple2(&ReplicaInputs2 {
			manifest: Some(&m),
			manifest_pin: Some(&pin),
			snapshot: Some(&s),
			inventory_pin: Some(&sp),
		})
		.err(),
		Some(ReplicaAnchorError2::Length(ReplicaInput2::Manifest))
	);
}

/// A scratch directory removed on drop (no /tmp leak).
struct Scratch(std::path::PathBuf);
impl Drop for Scratch {
	fn drop(&mut self) {
		let _ = std::fs::remove_dir_all(&self.0);
	}
}

#[test]
fn the_replica_read_keeps_its_137_byte_bound() {
	let dir = Scratch(std::env::temp_dir().join(format!(
		"wendelon-profile-two-read-{}-{}",
		std::process::id(),
		uuid::Uuid::new_v4()
	)));
	std::fs::create_dir_all(&dir.0).unwrap();
	let m = profile_two();
	std::fs::write(dir.0.join("m.adm"), &m).unwrap();
	let anchor = AnchorDir2::open(&dir.0).unwrap();
	let (mf, sf) = (CString::new("m.adm").unwrap(), CString::new("inv").unwrap());
	let (mp, sp) = (pin_two(&m), [1u8; 32]);
	let out = read_replica_tuple2(&ReplicaFiles2 {
		anchor: &anchor,
		manifest: &mf,
		manifest_pin: Some(&mp),
		snapshot: &sf,
		inventory_pin: Some(&sp),
	});
	assert!(matches!(
		out,
		Err(ReplicaReadError2::Tuple(ReplicaAnchorError2::Length(
			ReplicaInput2::Manifest
		)))
	));
}
