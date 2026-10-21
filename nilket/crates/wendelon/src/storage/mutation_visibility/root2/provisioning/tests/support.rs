use crate::storage::mutation_visibility::root2::codec::hash;
use crate::storage::mutation_visibility::root2::provisioning::inventory::*;

pub(super) fn unhex(s: &str) -> Vec<u8> {
	(0..s.len())
		.step_by(2)
		.map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
		.collect()
}
pub(super) fn vectors() -> serde_json::Value {
	serde_json::from_str(include_str!("inventory_vectors.json")).unwrap()
}
/// The kept empty v1 snapshot (`snapshots.empty`, §5): version 1 holds no entries.
pub(super) fn snapshot_bytes(name: &str) -> Vec<u8> {
	assert_eq!(name, "empty", "only the empty v1 snapshot is kept");
	unhex(vectors()["snapshots"][name].as_str().unwrap())
}
pub(super) fn reseal_snapshot(mut b: Vec<u8>) -> Vec<u8> {
	let n = b.len() - 32;
	let sum = hash(b"", &b[..n]);
	b[n..].copy_from_slice(&sum);
	b
}

// ---- Profile 3 over VISOEV02 (the `p3_` keys) ----
pub(super) fn p3_event_bytes(name: &str) -> Vec<u8> {
	unhex(vectors()["p3_events"][name].as_str().unwrap())
}
pub(super) fn p3_log_bytes() -> Vec<u8> {
	unhex(vectors()["p3_log"].as_str().unwrap())
}
pub(super) fn p3_snapshot_bytes(name: &str) -> Vec<u8> {
	unhex(vectors()["p3_snapshots"][name].as_str().unwrap())
}
pub(super) fn p3_event(name: &str) -> Event2 {
	v2::decode_event2(&p3_event_bytes(name)).unwrap()
}
/// Recomputes the trailing SHA-256 of a 512-byte VISOEV02 event.
pub(super) fn reseal_event3(mut b: Vec<u8>) -> Vec<u8> {
	let sum = hash(b"", &b[..480]);
	b[480..].copy_from_slice(&sum);
	b
}
pub(super) fn encoded3(e: &Event2) -> Vec<u8> {
	v2::encode_event2(e).unwrap().to_vec()
}
/// A valid VISOEV02 Repair at `seq`, claiming a 10-byte tail after the previous whole event.
pub(super) fn repair_at3(seq: u64, op: u64) -> Event2 {
	let mut op_id = [0u8; 16];
	op_id[..8].copy_from_slice(&op.to_le_bytes());
	op_id[15] = 0xee;
	Event2 {
		seq,
		op_id,
		identity: None,
		body: EventBody2::Repair {
			tail_sha256: [0x77; 32],
			original_len: (seq - 1) * v2::EVENT_BYTES2 as u64 + 10,
			boundary: (seq - 1) * v2::EVENT_BYTES2 as u64,
			tail_len: 10,
		},
	}
}
/// A VISOEV02 event framed directly (no codec round trip); the fold is the one validator.
pub(super) fn frame3(
	op: u8,
	seq: u64,
	op_id: [u8; 16],
	identity: Option<RootIdentity2>,
	payload: &[u8],
) -> Vec<u8> {
	let mut b = vec![0u8; v2::EVENT_BYTES2];
	b[..8].copy_from_slice(b"VISOEV02");
	b[8..10].copy_from_slice(&1u16.to_le_bytes());
	b[10] = op;
	b[12..20].copy_from_slice(&seq.to_le_bytes());
	b[20..36].copy_from_slice(&op_id);
	if let Some(i) = identity {
		b[36..52].copy_from_slice(&i.generation);
		b[52..68].copy_from_slice(&i.cluster);
		b[68..100].copy_from_slice(&i.pin);
		b[100..132].copy_from_slice(&i.admin_key);
	}
	b[132..134].copy_from_slice(&(payload.len() as u16).to_le_bytes());
	b[144..144 + payload.len()].copy_from_slice(payload);
	reseal_event3(b)
}
/// The second profile-3 root's manifest bytes with generation, cluster and administrator key
/// replaced: still a canonical 241-byte manifest, so a fold accepts it as a distinct root.
pub(super) fn p3_variant(generation: [u8; 16], cluster: [u8; 16], seed: [u8; 32]) -> Vec<u8> {
	let mut m = unhex(vectors()["p3_manifest_second"].as_str().unwrap());
	m[12..28].copy_from_slice(&generation);
	m[28..44].copy_from_slice(&cluster);
	m[68..100].copy_from_slice(
		&ed25519_dalek::SigningKey::from_bytes(&seed)
			.verifying_key()
			.to_bytes(),
	);
	m
}
/// A framed profile-3 Reserve of `manifest` at `seq`.
pub(super) fn p3_reserve_frame(manifest: &[u8], seq: u64, op_id: [u8; 16]) -> Vec<u8> {
	let identity = RootIdentity2 {
		generation: manifest[12..28].try_into().unwrap(),
		cluster: manifest[28..44].try_into().unwrap(),
		pin: hash(b"VISROOT2-ADMIN-MANIFEST\0", manifest),
		admin_key: manifest[68..100].try_into().unwrap(),
	};
	frame3(1, seq, op_id, Some(identity), manifest)
}
/// 4096 framed profile-3 Reserves at seq 1..=4096 (distinct generation, cluster, pin and key per
/// root): the root cap exactly. Built once and shared by the cap tests.
pub(super) fn p3_cap_roots() -> &'static [u8] {
	static ROOTS: std::sync::OnceLock<Vec<u8>> = std::sync::OnceLock::new();
	ROOTS.get_or_init(|| {
		let mut log = Vec::with_capacity(4096 * v2::EVENT_BYTES2);
		for i in 0..4096u32 {
			let n = i.to_le_bytes();
			let (mut g, mut c, mut seed, mut op) =
				([0x10; 16], [0x20; 16], [0x30u8; 32], [0x50; 16]);
			g[..4].copy_from_slice(&n);
			c[..4].copy_from_slice(&n);
			seed[..4].copy_from_slice(&n);
			op[..4].copy_from_slice(&n);
			log.extend(p3_reserve_frame(
				&p3_variant(g, c, seed),
				u64::from(i) + 1,
				op,
			));
		}
		log
	})
}
