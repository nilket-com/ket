//! The profile-3-only `p3_` keys of profile_three_vectors.json: the profile-3 root's events as
//! seq 1-2, its v3 snapshot and a VISREP04 repair over them. Profile-3 twins of the mixed-profile
//! fold, repair-context, framing and tail-bound rows above.
use super::*;

fn p3_log() -> Vec<u8> {
	vectors()["p3_events"]
		.as_array()
		.unwrap()
		.iter()
		.flat_map(b)
		.collect()
}
fn seal(mut x: Vec<u8>) -> Vec<u8> {
	let sum = hash(b"", &x[..480]);
	x[480..].copy_from_slice(&sum);
	x
}
#[test]
fn p3_events_fold_to_the_tag_three_snapshot() {
	let v = vectors();
	let mut prefix = Vec::new();
	for bytes in v["p3_events"].as_array().unwrap() {
		let bytes = b(bytes);
		let e = inventory::v2::decode_event2(&bytes).unwrap();
		assert_eq!(inventory::v2::encode_event2(&e).unwrap().as_slice(), bytes);
		prefix.extend(bytes);
	}
	let state = inventory::v2::fold_events2(&prefix).unwrap();
	assert_eq!(state.roots.len(), 1);
	assert_eq!(
		snapshot::encode_snapshot2(&state).unwrap(),
		b(&v["p3_three"])
	);
	let s = snapshot::decode_snapshot2(&b(&v["p3_three"])).unwrap();
	assert_eq!(
		s.entries[0].profile,
		crate::storage::mutation_visibility::root2::admin::manifest::ManifestProfile2::Three
	);
}
#[test]
fn p3_repair_context_reopen_and_cross_evidence_refusal() {
	let v = vectors();
	let prefix = p3_log();
	let tail = b(&v["p3_repair"]["tail"]);
	let torn = [prefix.clone(), tail.clone()].concat();
	let (name, evidence) = owner_core::v2::encode_evidence2(&torn).unwrap();
	assert_eq!(name, v["p3_repair"]["name"].as_str().unwrap());
	assert_eq!(evidence, b(&v["p3_repair"]["bytes"]));
	assert_eq!(evidence.len(), 124 + 5 + 32);
	let s = Scratch::new();
	drop(OwnerDir2::create_v2(&s.0).unwrap());
	fs::write(s.at("inventory.log"), &torn).unwrap();
	let mut o = OwnerDir2::open(&s.0).unwrap();
	o.repair_fresh(&hash(b"", &tail)).unwrap();
	drop(o);
	let mut reopened = OwnerDir2::open(&s.0).unwrap();
	assert_eq!(
		reopened.repair_check().unwrap(),
		crate::storage::mutation_visibility::root2::provisioning::owner_log::RepairOutcome2::NothingToRepair
	);
	let repaired = fs::read(s.at("inventory.log")).unwrap();
	assert_eq!(&repaired[..prefix.len()], prefix);
	assert_eq!(repaired.len(), 3 * 512);
	let mut bad_format = evidence;
	bad_format[10..12].copy_from_slice(&1u16.to_le_bytes());
	assert_eq!(
		owner_core::v2::decode_evidence2(&name, &bad_format).err(),
		Some(owner_core::EvidenceError2::Format)
	);
}
#[test]
fn p3_framing_payload_zero_fill_and_repair_bounds_are_strict() {
	let reserve = b(&vectors()["p3_events"][0]);
	assert!(inventory::v2::decode_event2(&reserve).is_ok());
	let mut bytes = reserve.clone();
	bytes[132..134].copy_from_slice(&240u16.to_le_bytes());
	assert!(matches!(
		inventory::v2::decode_event2(&seal(bytes)),
		Err(inventory::LogError2::Corrupt {
			reason: inventory::CorruptReason2::Payload,
			..
		})
	));
	for offset in [11, 134, 144 + 241] {
		let mut x = reserve.clone();
		x[offset] = 1;
		let reason = if offset == 144 + 241 {
			inventory::CorruptReason2::Payload
		} else {
			inventory::CorruptReason2::Reserved
		};
		assert!(
			matches!(inventory::v2::decode_event2(&seal(x)), Err(inventory::LogError2::Corrupt { reason: r, .. }) if r == reason),
			"offset {offset}"
		);
	}
	let e = inventory::Event2 {
		seq: 3,
		op_id: [7; 16],
		identity: None,
		body: inventory::EventBody2::Repair {
			tail_sha256: [1; 32],
			original_len: 2 * 512 + 1024,
			boundary: 2 * 512,
			tail_len: 1024,
		},
	};
	assert!(matches!(
		inventory::v2::encode_event2(&e),
		Err(inventory::LogError2::Corrupt {
			reason: inventory::CorruptReason2::RepairBounds,
			..
		})
	));
}
#[test]
fn p3_max_tail_evidence_bounds_are_exact() {
	let torn = [p3_log(), vec![0; 1023]].concat();
	let (name, bytes) = owner_core::v2::encode_evidence2(&torn).unwrap();
	assert_eq!(bytes.len(), 1179);
	assert_eq!(
		owner_core::v2::decode_evidence2(&name, &bytes)
			.unwrap()
			.tail
			.len(),
		1023
	);
	// The log read bound (moved from the deleted mixed-profile row): one byte past the largest log.
	let too_big = vec![0; 33554944];
	assert!(matches!(
		inventory::v2::fold_events2(&too_big),
		Err(inventory::LogError2::TooLong)
	));
	let mut evidence = bytes;
	evidence.push(0);
	assert_eq!(
		owner_core::v2::decode_evidence2(&name, &evidence).err(),
		Some(owner_core::EvidenceError2::Length)
	);
}
