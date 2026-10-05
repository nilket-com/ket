use super::*;

#[test]
fn snapshot_tail_equals_uninterrupted_and_event_roundtrip() {
	let a = authority();
	let b = authority();
	let events: Vec<_> = (2..12)
		.map(|i| {
			event(
				i,
				i % 3,
				if i % 2 == 0 {
					TerminalDecision::Accepted
				} else {
					TerminalDecision::Refused(RefusalCause::EpochCut)
				},
			)
		})
		.collect();
	for e in &events[..4] {
		assert_eq!(AuthorityEvent::decode(&e.encode().unwrap()).unwrap(), *e);
		a.replay_event(&e.encode().unwrap()).unwrap();
		b.apply(e).unwrap();
	}
	let restored =
		DecisionAuthority::decode_extension(&a.encode_extension().unwrap(), [1; 16]).unwrap();
	for e in &events[4..] {
		restored.replay_event(&e.encode().unwrap()).unwrap();
		b.apply(e).unwrap();
	}
	assert_eq!(
		restored.encode_extension().unwrap(),
		b.encode_extension().unwrap()
	);
	assert_eq!(
		AuthorityEvent::Coverage { log: log(20) }
			.encode()
			.unwrap()
			.len(),
		67
	);
}
#[test]
fn dormant_cuts_roundtrip_and_invalid_lineage_refuse() {
	let a = authority();
	a.apply(&AuthorityEvent::Coverage { log: log(5000) })
		.unwrap();
	for i in 1..=MAX_EPOCH_CUTS as u64 {
		a.add_reserved_cut(ReservedEpochCut {
			prior_epoch: i,
			new_epoch: i + 1,
			log: log(i),
		})
		.unwrap();
	}
	assert_eq!(
		a.add_reserved_cut(ReservedEpochCut {
			prior_epoch: 4097,
			new_epoch: 4098,
			log: log(4097)
		}),
		Err(VisibilityError::LimitExceeded)
	);
	let bytes = a.encode_extension().unwrap();
	assert_eq!(
		DecisionAuthority::decode_extension(&bytes, [1; 16])
			.unwrap()
			.encode_extension()
			.unwrap(),
		bytes
	);
	let a = authority();
	a.apply(&AuthorityEvent::Coverage { log: log(5) }).unwrap();
	for c in [
		ReservedEpochCut {
			prior_epoch: 2,
			new_epoch: 3,
			log: log(2),
		},
		ReservedEpochCut {
			prior_epoch: 1,
			new_epoch: 1,
			log: log(2),
		},
		ReservedEpochCut {
			prior_epoch: 1,
			new_epoch: 2,
			log: log(6),
		},
	] {
		assert_eq!(a.add_reserved_cut(c), Err(VisibilityError::InvalidCut));
	}
}
#[test]
fn snapshot_forgery_counters_rules_generation_order_and_checksum_refuse() {
	let a = authority();
	a.apply(&event(2, 1, TerminalDecision::Accepted)).unwrap();
	a.apply(&event(
		3,
		2,
		TerminalDecision::Refused(RefusalCause::EpochCut),
	))
	.unwrap();
	let original = a.encode_extension().unwrap();
	assert!(DecisionAuthority::decode_extension(&original, [9; 16]).is_err());
	// Payload starts at14; rules at90, count at94, stored-byte counter at98.
	for (offset, value) in [
		(90usize, vec![2]),
		(94, 0u32.to_le_bytes().to_vec()),
		(98, 0u64.to_le_bytes().to_vec()),
		(94, u32::MAX.to_le_bytes().to_vec()),
		(14, vec![0; 16]),
	] {
		let mut b = original.clone();
		overwrite_envelope(&mut b, offset, &value);
		assert!(
			DecisionAuthority::decode_extension(&b, [1; 16]).is_err(),
			"offset {offset}"
		);
	}
	let mut dup = original.clone();
	let first = dup[106..380].to_vec();
	overwrite_envelope(&mut dup, 380, &first);
	assert!(DecisionAuthority::decode_extension(&dup, [1; 16]).is_err());
	let mut swap = original.clone();
	let first = swap[106..380].to_vec();
	let second = swap[380..654].to_vec();
	overwrite_envelope(&mut swap, 106, &second);
	overwrite_envelope(&mut swap, 380, &first);
	assert!(DecisionAuthority::decode_extension(&swap, [1; 16]).is_err());
	let mut checksum = original.clone();
	checksum[150] ^= 1;
	assert!(DecisionAuthority::decode_extension(&checksum, [1; 16]).is_err());
	for n in 0..original.len() {
		assert!(DecisionAuthority::decode_extension(&original[..n], [1; 16]).is_err());
	}
	let mut trailing = original.clone();
	trailing.push(0);
	assert!(DecisionAuthority::decode_extension(&trailing, [1; 16]).is_err());
	for legacy in [b"WAL4".as_slice(), b"SNAP5", b"WAL5", b"SNAP6"] {
		assert!(DecisionAuthority::decode_extension(legacy, [1; 16]).is_err());
	}
	assert!(DecisionAuthority::decode_extension(&vec![0; 67108865], [1; 16]).is_err());
}
#[test]
fn record_coordinate_and_canonical_tags_are_validated() {
	let r = record(
		authority()
			.apply(&event(2, 1, TerminalDecision::Accepted))
			.unwrap(),
	);
	let bytes = r.encode().unwrap();
	for (offset, value) in [
		(0, u32::MAX.to_le_bytes().to_vec()),
		(248, vec![0; 8]),
		(268, vec![2, 0]),
		(268, vec![1, 3]),
		(268, vec![2, 9]),
	] {
		let mut b = bytes.clone();
		b[offset..offset + value.len()].copy_from_slice(&value);
		assert!(DecisionRecord::decode(&b).is_err());
	}
}
#[test]
fn invalid_events_never_advance_coverage() {
	let a = authority();
	let bytes = event(2, 1, TerminalDecision::Accepted).encode().unwrap();
	let before = a.encode_extension().unwrap();
	for n in 0..bytes.len() {
		assert!(a.replay_event(&bytes[..n]).is_err());
		assert_eq!(a.encode_extension().unwrap(), before);
	}
	for (offset, value) in [
		(14, vec![9]),
		(15, vec![0; 8]),
		(35, vec![2, 0]),
		(37, u32::MAX.to_le_bytes().to_vec()),
	] {
		let mut bad = bytes.clone();
		overwrite_envelope(&mut bad, offset, &value);
		assert!(a.replay_event(&bad).is_err());
		assert_eq!(a.encode_extension().unwrap(), before);
	}
	let mut bad = bytes;
	bad[50] ^= 1;
	assert_eq!(a.replay_event(&bad), Err(VisibilityError::Corrupt));
	assert_eq!(a.encode_extension().unwrap(), before);
}
#[test]
fn snapshot_does_not_contain_preimages_or_proof_secret() {
	let a = authority();
	a.apply(&event(2, 1, TerminalDecision::Accepted)).unwrap();
	let bytes = a.encode_extension().unwrap();
	for marker in [b"customer@example.com".as_slice(), &[0x99; 32]] {
		assert!(!bytes.windows(marker.len()).any(|w| w == marker));
	}
}
