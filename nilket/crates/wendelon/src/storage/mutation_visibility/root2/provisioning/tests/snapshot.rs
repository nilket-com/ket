use super::*;
use crate::storage::mutation_visibility::root2::provisioning::inventory::*;
use crate::storage::mutation_visibility::root2::provisioning::snapshot::*;

/// §5: the empty snapshot stays version 1, byte for byte (54 bytes, the kept `snapshots.empty`).
/// Snapshots with entries are version 3 or 4 (`snapshot_three.rs`); version 1 with an entry is
/// refused as `Version` (corpus N6).
#[test]
fn goldens_equal_folded_exports_and_decode_round_trips() {
	let state = v2::fold_events2(&[]).unwrap();
	let bytes = encode_snapshot2(&state).unwrap();
	assert_eq!(bytes, snapshot_bytes("empty"));
	assert_eq!(bytes.len(), 54);
	let s = decode_snapshot2(&bytes).unwrap();
	assert_eq!(s.export_seq, state.last_seq);
	assert_eq!(s.entries.len(), 0);
}
