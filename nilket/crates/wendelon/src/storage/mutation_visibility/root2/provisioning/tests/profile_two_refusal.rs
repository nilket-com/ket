//! Profile-2 refusals that still hold after M3: a version-1 Reserve carrying profile-2 bytes and a
//! tag byte in a version-1 snapshot. The M1 replica-tuple length and 137-byte read-bound refusals
//! are superseded by M3 and retained as refusals in `profile_two_replica` (a 190/191-byte manifest,
//! the 189-byte read bound, invalid pairings).
use super::*;
use crate::storage::mutation_visibility::root2::provisioning::inventory::*;
use crate::storage::mutation_visibility::root2::provisioning::snapshot::*;

fn profile_two() -> Vec<u8> {
	let v: serde_json::Value =
		serde_json::from_str(include_str!("../../admin/tests/profile_two_vectors.json")).unwrap();
	unhex(v["valid"][0]["bytes"].as_str().unwrap())
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
