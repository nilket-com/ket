use super::*;
use crate::storage::mutation_visibility::root2::provisioning::inventory::*;
use crate::storage::mutation_visibility::root2::provisioning::snapshot::*;
use SnapshotError2::*;

fn decode(b: &[u8]) -> Result<(), SnapshotError2> {
	decode_snapshot2(b).map(|_| ())
}
fn patched(name: &str, at: usize, v: &[u8]) -> Vec<u8> {
	let mut b = snapshot_bytes(name);
	b[at..at + v.len()].copy_from_slice(v);
	reseal_snapshot(b)
}
const E0: usize = 22;
const E1: usize = 22 + 320;

#[test]
fn goldens_equal_folded_exports_and_decode_round_trips() {
	let log = log_bytes();
	for (name, events) in [("empty", 0), ("prepared", 1), ("bound", 2), ("full", 6)] {
		let state = fold_events2(&log[..events * 384]).unwrap();
		let bytes = encode_snapshot2(&state).unwrap();
		assert_eq!(bytes, snapshot_bytes(name), "{name}");
		// S2: scalar equality with the folded inventory (no filesystem export is claimed).
		let s = decode_snapshot2(&bytes).unwrap();
		assert_eq!(s.export_seq, state.last_seq);
		assert_eq!(s.entries.len(), state.roots.len());
		for (e, r) in s.entries.iter().zip(&state.roots) {
			assert!(
				e.identity == r.identity
					&& e.state == r.state
					&& e.last_seq == r.last_seq
					&& e.record == r.record
			);
		}
	}
}
#[test]
fn every_invariant_is_refused() {
	let full = snapshot_bytes("full");
	let mut short = full.clone();
	short.pop();
	assert_eq!(decode(&short), Err(Length));
	assert_eq!(decode(&full[..40]), Err(Length));
	assert_eq!(decode(&patched("full", 0, b"VISINV03")), Err(Magic));
	// C-S4a: v3 exists, but these v1 entries have no profile tags.
	assert_eq!(
		decode(&patched("full", 8, &3u16.to_le_bytes())),
		Err(ProfileTag)
	);
	assert_eq!(
		decode(&patched("full", 8, &4u16.to_le_bytes())),
		Err(Version)
	);
	// M2: version 2 is readable; its untagged (version-1) entries fail at the tag.
	assert_eq!(
		decode(&patched("full", 8, &2u16.to_le_bytes())),
		Err(ProfileTag)
	);
	assert_eq!(
		decode(&patched("full", 10, &4097u32.to_le_bytes())),
		Err(Count)
	);
	assert_eq!(
		decode(&patched("full", 10, &3u32.to_le_bytes())),
		Err(Length)
	);
	let mut trailer = full.clone();
	let end = trailer.len() - 1;
	trailer[end] ^= 1;
	assert_eq!(decode(&trailer), Err(Trailer));
	assert_eq!(decode(&patched("full", E0 + 97, &[1])), Err(Reserved));
	assert_eq!(decode(&patched("full", E0 + 96, &[0])), Err(State));
	assert_eq!(decode(&patched("full", E0 + 96, &[5])), Err(State));
	for (at, w) in [(0, 16), (16, 16), (32, 32), (64, 32)] {
		assert_eq!(
			decode(&patched("full", E0 + at, &vec![0; w])),
			Err(ZeroIdentity),
			"component at {at}"
		);
	}
	let small_order = [0u8; 32]
		.iter()
		.enumerate()
		.map(|(i, _)| if i == 0 { 1 } else { 0 })
		.collect::<Vec<u8>>();
	assert_eq!(decode(&patched("full", E0 + 64, &small_order)), Err(Key));
	assert_eq!(
		decode(&patched("full", E0 + 104, &0u64.to_le_bytes())),
		Err(Sequence)
	);
	assert_eq!(
		decode(&patched("full", E0 + 104, &7u64.to_le_bytes())),
		Err(Sequence)
	);
	assert_eq!(
		decode(&patched("prepared", 14, &0u64.to_le_bytes())),
		Err(Sequence)
	);
	// Order: swap the two entries.
	let mut swapped = full.clone();
	let (a, b) = (full[E0..E1].to_vec(), full[E1..E1 + 320].to_vec());
	swapped[E0..E1].copy_from_slice(&b);
	swapped[E1..E1 + 320].copy_from_slice(&a);
	assert_eq!(decode(&reseal_snapshot(swapped)), Err(Order));
	// Shared component across entries: the second entry reuses one of the first entry's. Its
	// (generation, cluster) still sorts after the first, so Duplicate is the check that refuses.
	for (at, w) in [(0, 16), (16, 16), (32, 32), (64, 32)] {
		assert_eq!(
			decode(&patched(
				"full",
				E1 + at,
				&full[E0 + at..E0 + at + w].to_vec()
			)),
			Err(Duplicate),
			"component at {at}"
		);
	}
	// Repeating the whole (generation, cluster) pair fails the strict order first.
	assert_eq!(
		decode(&patched("full", E1, &full[E0..E0 + 32].to_vec())),
		Err(Order)
	);
	// Bound without a record; a record on a non-Bound entry.
	assert_eq!(
		decode(&patched("bound", E0 + 112, &[0u8; 208])),
		Err(Record)
	);
	let bound = snapshot_bytes("bound");
	let record = bound[E0 + 112..E0 + 320].to_vec();
	assert_eq!(decode(&patched("prepared", E0 + 112, &record)), Err(Record));
	// A nonzero record that does not decode.
	assert_eq!(decode(&patched("bound", E0 + 112, &[0xff])), Err(Record));
	// Bound record bound to the entry's generation, cluster and pin, and to last_seq.
	for (at, w) in [(0, 16), (16, 16), (32, 32)] {
		assert_eq!(
			decode(&patched("bound", E0 + at, &vec![0x55; w])),
			Err(Binding),
			"component at {at}"
		);
	}
	let mut later = patched("bound", 14, &3u64.to_le_bytes());
	later[E0 + 104..E0 + 112].copy_from_slice(&3u64.to_le_bytes());
	assert_eq!(decode(&reseal_snapshot(later)), Err(Binding));
	assert!(decode(&snapshot_bytes("empty")).is_ok());
}
#[test]
fn encode_refuses_more_roots_than_the_cap_before_any_entry_check() {
	let state = fold_events2(&log_bytes()[..384]).unwrap();
	let mut root = state.roots[0].clone();
	root.identity.generation = [0; 16];
	let with = |n: usize| LogState2 {
		roots: vec![root.clone(); n],
		last_seq: 1,
	};
	// An invalid first entry: over the cap it is never examined.
	assert_eq!(encode_snapshot2(&with(MAX_ROOTS2 + 1)), Err(Count));
	assert_eq!(encode_snapshot2(&with(MAX_ROOTS2)), Err(ZeroIdentity));
}
