use super::super::inventory::*;

pub(super) fn unhex(s: &str) -> Vec<u8> {
	(0..s.len())
		.step_by(2)
		.map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
		.collect()
}
pub(super) fn vectors() -> serde_json::Value {
	serde_json::from_str(include_str!("inventory_vectors.json")).unwrap()
}
pub(super) fn event_bytes(name: &str) -> Vec<u8> {
	unhex(vectors()["events"][name].as_str().unwrap())
}
pub(super) fn log_bytes() -> Vec<u8> {
	unhex(vectors()["log"].as_str().unwrap())
}
pub(super) fn snapshot_bytes(name: &str) -> Vec<u8> {
	unhex(vectors()["snapshots"][name].as_str().unwrap())
}
pub(super) fn event(name: &str) -> Event2 {
	decode_event2(&event_bytes(name)).unwrap()
}
// Recomputes the trailing SHA-256 so a test reaches the check it targets.
pub(super) fn reseal_event(mut b: Vec<u8>) -> Vec<u8> {
	let sum = super::super::super::codec::hash(b"", &b[..352]);
	b[352..].copy_from_slice(&sum);
	b
}
pub(super) fn reseal_snapshot(mut b: Vec<u8>) -> Vec<u8> {
	let n = b.len() - 32;
	let sum = super::super::super::codec::hash(b"", &b[..n]);
	b[n..].copy_from_slice(&sum);
	b
}
pub(super) fn encoded(e: &Event2) -> Vec<u8> {
	encode_event2(e).unwrap().to_vec()
}
pub(super) fn repair_at(seq: u64, op: u64) -> Event2 {
	let mut op_id = [0u8; 16];
	op_id[..8].copy_from_slice(&op.to_le_bytes());
	op_id[15] = 0xee;
	Event2 {
		seq,
		op_id,
		identity: None,
		body: EventBody2::Repair {
			tail_sha256: [0x77; 32],
			original_len: (seq - 1) * 384 + 10,
			boundary: (seq - 1) * 384,
			tail_len: 10,
		},
	}
}
