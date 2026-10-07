use super::*;
fn goldens() -> Vec<(String, Vec<u8>)> {
	let j: serde_json::Value = serde_json::from_str(include_str!("goldens.json")).unwrap();
	j["fixtures"]
		.as_array()
		.unwrap()
		.iter()
		.map(|f| {
			let s = f["hex"].as_str().unwrap();
			(
				f["name"].as_str().unwrap().to_owned(),
				(0..s.len())
					.step_by(2)
					.map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
					.collect(),
			)
		})
		.collect()
}
pub(super) fn fixture(name: &str) -> Vec<u8> {
	goldens().into_iter().find(|(n, _)| n == name).unwrap().1
}
fn roundtrip(b: &[u8]) -> Result2<Vec<u8>> {
	match b.get(..8) {
		Some(b"VISBND02") => UnverifiedBundle2::decode(b)?.encode(),
		Some(b"VISDAT02") => UnverifiedData2::decode(b)?.encode(),
		Some(b"VISIDX02") => UnverifiedIndex2::decode(b)?.encode(),
		Some(b"VISRCP02") => UnverifiedReceipt2::decode(b)?.encode(),
		Some(b"VISWIT02") => UnverifiedWitness2::decode(b)?.encode(),
		Some(b"VISJRN02") => UnverifiedJournal2::decode(b)?.encode(),
		Some(b"VISOUT02") => UnverifiedOutcome2::decode(b)?.encode(),
		Some(b"VISGRP02") => UnverifiedGrantArchive2::decode(b)?.encode(),
		_ => Err(RootError2::Codec),
	}
}
fn repaired(mut b: Vec<u8>) -> Vec<u8> {
	let n = b.len();
	let sum = sha(&b[..n - 32]);
	b[n - 32..].copy_from_slice(&sum);
	b
}
#[test]
fn all_fifteen_independent_documentation_goldens_are_byte_identical() {
	let g = goldens();
	assert_eq!(g.len(), 15);
	for (n, b) in g {
		assert_eq!(
			roundtrip(&b).unwrap_or_else(|e| panic!("{n}: {e:?}")),
			b,
			"{n}"
		);
	}
}
#[test]
fn every_family_refuses_corrupt_prefix_version_flags_lengths_and_trailing() {
	for (n, b) in goldens() {
		for index in [0, 8, 10, 11, 12, b.len() - 1] {
			let mut bad = b.clone();
			bad[index] ^= 1;
			if index < b.len() - 32 {
				bad = repaired(bad);
			}
			assert!(roundtrip(&bad).is_err(), "{n} {index}");
		}
		let mut bad = b.clone();
		bad.push(0);
		assert!(roundtrip(&bad).is_err());
		for end in [0, 7, 15, 16, b.len() - 33, b.len() - 1] {
			assert!(roundtrip(&b[..end]).is_err(), "{n} {end}");
		}
	}
}
#[test]
fn public_debug_is_redacted() {
	let bytes = fixture("data_opaque_companion");
	let d = UnverifiedData2::decode(&bytes).unwrap();
	let text = format!("{d:?}");
	assert!(!text.contains("NOT-IPC"));
	assert!(text.contains("redacted"));
}
#[test]
fn unknown_or_wrong_placement_and_boolean_refuse_even_with_repaired_checksum() {
	let b = fixture("index_one_row");
	for (offset, value) in [(16 + 245, 2), (16 + 246, 2), (16 + 246, 1)] {
		let mut bad = b.clone();
		bad[offset] = value;
		assert!(UnverifiedIndex2::decode(&repaired(bad)).is_err());
	}
}
#[test]
fn index_key_and_extent_must_match_and_no_offset_overflow() {
	let mut i = UnverifiedIndex2::decode(&fixture("index_one_row")).unwrap();
	let key = i.key().unwrap();
	i.check_locator(&key, i.length as u64).unwrap();
	assert!(i.check_locator(&key[..108], u64::MAX).is_err());
	assert!(i.check_locator(&key, (i.length - 1) as u64).is_err());
	i.offset = u64::MAX;
	assert!(i.encode().is_err());
}
#[test]
fn no_mandatory_zero_logs_or_foreign_receipt_generation() {
	let mut r = UnverifiedReceipt2::decode(&fixture("receipt_holder")).unwrap();
	r.reservation.index = 0;
	assert!(r.encode().is_err());
	r.reservation.index = 5;
	r.generation = [7; 16];
	assert!(r.encode().is_err());
}
#[test]
fn witness_refuses_duplicate_foreign_partial_plan_and_binding() {
	let w = UnverifiedWitness2::decode(&fixture("witness_two_receipts")).unwrap();
	let mut bad = w.clone();
	bad.receipts.push(bad.receipts[0].clone());
	assert!(bad.encode().is_err());
	let mut bad = w.clone();
	bad.receipts.pop();
	assert!(bad.encode().is_err());
	let mut bad = w.clone();
	bad.receipts[1].node.incarnation = [9; 16];
	assert!(bad.encode().is_err());
	let mut bad = w.clone();
	bad.receipts[0].binding[0] ^= 1;
	assert!(bad.encode().is_err());
	let mut bad = w;
	bad.reservation.index += 1;
	assert!(bad.encode().is_err());
}
#[test]
fn no_nonterminal_or_foreign_result_outcome() {
	let mut o = UnverifiedOutcome2::decode(&fixture("outcome_accepted_unverified")).unwrap();
	o.result = ApplyResult2::PendingEvidence(1)
		.encode(o.generation, o.cluster, o.decision)
		.unwrap();
	assert!(o.encode().is_err());
	let mut o = UnverifiedOutcome2::decode(&fixture("outcome_accepted_unverified")).unwrap();
	o.identity.sequence += 1;
	assert!(o.encode().is_err());
}
#[test]
fn archive_refuses_non_grant_and_inconsistent_original() {
	let mut g = UnverifiedGrantArchive2::decode(&fixture("grant_original_unverified")).unwrap();
	g.original_digest[0] ^= 1;
	assert!(g.encode().is_err());
	g.command = Command2::Bootstrap {
		epoch: 1,
		control: [1; 32],
		length: 1,
	};
	assert!(g.encode().is_err());
}
fn budget() -> JournalLoadBudget2 {
	JournalLoadBudget2 {
		chains: 8,
		open_handles: 8,
		records_per_chain: 32,
		total_bytes_per_chain: 2 * 1024 * 1024,
	}
}
fn chain() -> (String, Vec<u8>) {
	let first = UnverifiedJournal2::decode(&fixture("journal_step_1")).unwrap();
	(
		first.owner.locator().unwrap(),
		(1..=7)
			.flat_map(|n| fixture(&format!("journal_step_{n}")))
			.collect(),
	)
}
#[test]
fn journal_goldens_form_one_exact_complete_per_parent_chain() {
	let obj = chain();
	let r = inspect_journal_objects2(Some(&[obj]), budget()).unwrap();
	assert_eq!(r[0].validated_records, 7);
	assert!(!r[0].unavailable);
}
#[test]
fn journal_sequence_gap_suffix_corruption_and_missing_prefix_are_unavailable() {
	let (locator, bytes) = chain();
	let mut bad = bytes.clone();
	bad[20] ^= 1;
	let r = inspect_journal_objects2(Some(&[(locator.clone(), bad)]), budget()).unwrap();
	assert!(r[0].unavailable);
	let r = inspect_journal_objects2(
		Some(&[(locator.clone(), bytes[..bytes.len() - 1].to_vec())]),
		budget(),
	)
	.unwrap();
	assert_eq!(r[0].validated_records, 6);
	assert!(r[0].unavailable);
	let r =
		inspect_journal_objects2(Some(&[(locator, fixture("journal_step_2"))]), budget()).unwrap();
	assert_eq!(r[0].validated_records, 0);
	assert!(r[0].unavailable);
}
#[test]
fn enumeration_failure_duplicate_locators_and_exhaustion_refuse() {
	assert!(inspect_journal_objects2(None, budget()).is_err());
	let a = chain();
	assert!(inspect_journal_objects2(Some(&[a.clone(), a.clone()]), budget()).is_err());
	let mut b = budget();
	b.records_per_chain = 6;
	assert!(inspect_journal_objects2(Some(&[a]), b).is_err());
}
#[test]
fn data_companion_and_bundle_dictionary_cannot_be_swapped() {
	let mut d = UnverifiedData2::decode(&fixture("data_opaque_companion")).unwrap();
	d.frame[0] ^= 1;
	assert!(d.encode().is_err());
	let mut b = UnverifiedBundle2::decode(&fixture("bundle_one_parent")).unwrap();
	b.claims[0].0.manifest.identity.sequence += 1;
	assert!(b.encode().is_err());
}
#[test]
fn total_envelope_caps_are_not_s1_payload_caps() {
	for cap in [
		4096,
		538,
		BUNDLE_MAX,
		DATA_MAX,
		WITNESS_MAX,
		JOURNAL_MAX,
		OUTCOME_MAX,
	] {
		let body = vec![0; cap - 48];
		let b = wrap(b"VISIDX02", &body, cap).unwrap();
		assert_eq!(b.len(), cap);
		assert_eq!(
			open(&b, b"VISIDX02", cap).unwrap().remaining().len(),
			cap - 48
		);
		let mut too_large = body;
		too_large.push(0);
		assert_eq!(wrap(b"VISIDX02", &too_large, cap), Err(RootError2::Limit));
		let mut b = b;
		b.push(0);
		assert!(matches!(open(&b, b"VISIDX02", cap), Err(RootError2::Limit)));
	}
}
#[test]
fn huge_claim_count_with_short_body_refuses_before_collecting() {
	let b = fixture("bundle_one_parent");
	let mut bad = b.clone();
	let provlen = u32::from_le_bytes(b[16..20].try_into().unwrap()) as usize;
	bad[20 + provlen..22 + provlen].copy_from_slice(&u16::MAX.to_le_bytes());
	assert!(UnverifiedBundle2::decode(&repaired(bad)).is_err());
}
#[test]
fn repeated_child_steps_and_independent_parent_chains_have_no_global_sequence() {
	let first = UnverifiedJournal2::decode(&fixture("journal_step_1")).unwrap();
	let second = UnverifiedJournal2::decode(&fixture("journal_step_2")).unwrap();
	let mut repeated = second.clone();
	repeated.sequence = 3;
	repeated.previous = sha(&second.encode().unwrap());
	let bytes = [
		first.encode().unwrap(),
		second.encode().unwrap(),
		repeated.encode().unwrap(),
	]
	.concat();
	let mut other = first.clone();
	let UnverifiedJournalStep2::Prepared(ref mut c) = other.step else {
		panic!()
	};
	c.manifest.identity.sequence += 1;
	other.owner.identity = c.manifest.identity;
	other.owner.claim = c.digest().unwrap();
	let mut damaged = bytes.clone();
	damaged[bytes.len() - 1] ^= 1;
	let objects = [
		(first.owner.locator().unwrap(), damaged),
		(other.owner.locator().unwrap(), other.encode().unwrap()),
	];
	let reports = inspect_journal_objects2(Some(&objects), budget()).unwrap();
	assert!(reports[0].unavailable);
	assert_eq!(reports[0].validated_records, 2);
	assert!(!reports[1].unavailable);
	assert_eq!(reports[1].validated_records, 1);
	let good = inspect_journal_objects2(Some(&[(first.owner.locator().unwrap(), bytes)]), budget())
		.unwrap();
	assert_eq!(good[0].validated_records, 3);
	assert!(!good[0].unavailable);
}
#[test]
fn journal_wrong_tuple_locator_and_conflicting_repeated_body_are_unavailable() {
	let mut first = UnverifiedJournal2::decode(&fixture("journal_step_1")).unwrap();
	let orig = first.encode().unwrap();
	let locator = first.owner.locator().unwrap();
	first.sequence = 2;
	first.previous = sha(&orig);
	let UnverifiedJournalStep2::Prepared(ref mut c) = first.step else {
		panic!()
	};
	c.signature[0] ^= 1;
	first.owner.claim = c.digest().unwrap();
	let r = inspect_journal_objects2(
		Some(&[(locator.clone(), [orig, first.encode().unwrap()].concat())]),
		budget(),
	)
	.unwrap();
	assert_eq!(r[0].validated_records, 1);
	assert!(r[0].unavailable);
	let r = inspect_journal_objects2(
		Some(&[("0".repeat(64), fixture("journal_step_1"))]),
		budget(),
	)
	.unwrap();
	assert!(r[0].unavailable);
}
#[test]
fn nested_versions_wrong_journal_bodies_and_terminal_kinds_refuse_with_repaired_checksums() {
	let b = fixture("bundle_one_parent");
	let mut bad = b.clone();
	bad[20 + 8] = 2;
	let inner_len = u32::from_le_bytes(b[16..20].try_into().unwrap()) as usize;
	let mut inner = bad[20..20 + inner_len].to_vec();
	let checksum = blake3::hash(&inner[..inner_len - 32]);
	inner[inner_len - 32..].copy_from_slice(checksum.as_bytes());
	bad[20..20 + inner_len].copy_from_slice(&inner);
	assert!(UnverifiedBundle2::decode(&repaired(bad)).is_err());
	let mut bad = b;
	let claim_start = 20 + inner_len + 2 + 4;
	bad[claim_start] = 1;
	assert!(UnverifiedBundle2::decode(&repaired(bad)).is_err());
	let mut bad = fixture("journal_step_1");
	bad[16 + 197] = 7;
	assert!(UnverifiedJournal2::decode(&repaired(bad)).is_err());
	let mut bad = fixture("outcome_accepted_unverified");
	let result_len = u32::from_le_bytes(bad[16 + 157..16 + 161].try_into().unwrap()) as usize;
	bad[16 + 161 + result_len] = 0;
	assert!(UnverifiedOutcome2::decode(&repaired(bad)).is_err());
}
#[test]
fn unsigned_receipt_is_only_unverified_and_child_binding_is_opaque_metadata() {
	let r = UnverifiedReceipt2::decode(&fixture("receipt_holder")).unwrap();
	assert!(format!("{r:?}").contains("unverified"));
	let w = UnverifiedWitness2::decode(&fixture("witness_two_receipts")).unwrap();
	let computed = child_binding2(&w.claim, r.cluster, r.reservation, 0).unwrap();
	assert_ne!(
		computed, r.binding,
		"documentation placeholders are not data-durability evidence"
	);
}
