use super::*;
use crate::storage::mutation_visibility::DecisionLogId;
use BootstrapRecordError2::*;

fn golden(kind: &str) -> Vec<u8> {
	hex(record_vectors()[kind]["bytes"].as_str().unwrap())
}
fn identity() -> BootstrapIdentity2 {
	let v = record_vectors()["identity"].clone();
	let h = |k: &str| hex(v[k].as_str().unwrap());
	BootstrapIdentity2 {
		generation: h("generation").try_into().unwrap(),
		cluster: h("cluster").try_into().unwrap(),
		pin: h("pin").try_into().unwrap(),
		initial_epoch: v["initial_epoch"].as_u64().unwrap(),
		bootstrap: DecisionLogId {
			term: v["bootstrap_term"].as_u64().unwrap(),
			node: v["bootstrap_node"].as_u64().unwrap() as u32,
			index: v["bootstrap_index"].as_u64().unwrap(),
		},
		bootstrap_fingerprint: h("bootstrap_fingerprint").try_into().unwrap(),
		bootstrap_command: h("bootstrap_command").try_into().unwrap(),
		genesis_fingerprint: h("genesis_fingerprint").try_into().unwrap(),
	}
}
fn with(kind: &str, at: usize, bytes: &[u8]) -> Vec<u8> {
	let mut b = golden(kind);
	b[at..at + bytes.len()].copy_from_slice(bytes);
	b
}
fn record(b: &[u8]) -> Result<BootstrapRecord2, BootstrapRecordError2> {
	let before = b.to_vec();
	let r = BootstrapRecord2::decode(b);
	assert_eq!(b, &before[..]);
	r
}
fn observation(b: &[u8]) -> Result<BootstrapObservation2, BootstrapRecordError2> {
	let before = b.to_vec();
	let r = BootstrapObservation2::decode(b);
	assert_eq!(b, &before[..]);
	r
}
#[test]
fn independent_goldens_round_trip_at_the_reference_offsets() {
	let v = record_vectors();
	assert_eq!(v["length"], BOOTSTRAP_RECORD_BYTES2);
	let id = identity();
	let rec = BootstrapRecord2 {
		identity: id,
		inventory_seq: v["record"]["inventory_seq"].as_u64().unwrap(),
	};
	let obs = BootstrapObservation2 { identity: id };
	assert_eq!(rec.encode().unwrap().to_vec(), golden("record"));
	assert_eq!(obs.encode().unwrap().to_vec(), golden("observation"));
	assert_eq!(record(&golden("record")).unwrap(), rec);
	assert_eq!(observation(&golden("observation")).unwrap(), obs);
	// Offsets come from the independent reference, not from the implementation.
	let at = |kind: &str, name: &str| {
		let o = &v["offsets"][name];
		let (s, w) = (
			o[0].as_u64().unwrap() as usize,
			o[1].as_u64().unwrap() as usize,
		);
		golden(kind)[s..s + w].to_vec()
	};
	assert_eq!(at("record", "magic"), b"VISADR2\0");
	assert_eq!(at("observation", "magic"), b"VISAOB2\0");
	assert_eq!(at("record", "pin"), id.pin);
	assert_eq!(
		at("record", "bootstrap_node"),
		id.bootstrap.node.to_le_bytes()
	);
	assert_eq!(at("record", "genesis_fingerprint"), id.genesis_fingerprint);
	assert_eq!(at("record", "inventory_seq"), 41u64.to_le_bytes());
	assert_eq!(at("observation", "inventory_seq"), [0u8; 8]);
}
#[test]
fn lengths_headers_and_kind_separation() {
	for kind in ["record", "observation"] {
		let g = golden(kind);
		let mut long = g.clone();
		long.push(0);
		for b in [&g[..207], &long[..], &[][..]] {
			assert_eq!(record(b).err(), Some(Length));
			assert_eq!(observation(b).err(), Some(Length));
		}
	}
	assert_eq!(record(&golden("observation")).err(), Some(Magic));
	assert_eq!(observation(&golden("record")).err(), Some(Magic));
	assert_eq!(
		record(&with("record", 8, &2u16.to_le_bytes())).err(),
		Some(UnsupportedFormat)
	);
	assert_eq!(
		record(&with("record", 10, &2u16.to_le_bytes())).err(),
		Some(UnsupportedProfile)
	);
	assert_eq!(
		observation(&with("observation", 8, &0u16.to_le_bytes())).err(),
		Some(UnsupportedFormat)
	);
}
#[test]
fn zero_fields_and_invalid_logs() {
	for (at, width) in [(12, 16), (28, 16), (76, 8)] {
		assert_eq!(
			record(&with("record", at, &vec![0; width])).err(),
			Some(ZeroField),
			"offset {at}"
		);
		assert_eq!(
			observation(&with("observation", at, &vec![0; width])).err(),
			Some(ZeroField)
		);
	}
	for (at, width) in [(84, 8), (92, 4), (96, 8), (84, 20)] {
		assert_eq!(
			record(&with("record", at, &vec![0; width])).err(),
			Some(InvalidLog),
			"offset {at}"
		);
	}
	let mut zero_log = identity();
	zero_log.bootstrap.index = 0;
	assert_eq!(
		BootstrapObservation2 { identity: zero_log }.encode().err(),
		Some(InvalidLog)
	);
}
#[test]
fn record_sequence_is_nonzero_and_observation_sequence_is_zero() {
	assert_eq!(record(&with("record", 200, &[0; 8])).err(), Some(Sequence));
	assert_eq!(
		observation(&with("observation", 200, &1u64.to_le_bytes())).err(),
		Some(Sequence)
	);
	assert_eq!(
		BootstrapRecord2 {
			identity: identity(),
			inventory_seq: 0
		}
		.encode()
		.err(),
		Some(Sequence)
	);
}
#[test]
fn each_identity_field_mismatch_is_named_individually() {
	let base = identity();
	assert_eq!(base.first_mismatch(&base), None);
	let cases: [(fn(&mut BootstrapIdentity2), BootstrapField2); 8] = [
		(|i| i.generation[0] ^= 1, BootstrapField2::Generation),
		(|i| i.cluster[15] ^= 1, BootstrapField2::Cluster),
		(|i| i.pin[31] ^= 1, BootstrapField2::Pin),
		(|i| i.initial_epoch += 1, BootstrapField2::InitialEpoch),
		(|i| i.bootstrap.term += 1, BootstrapField2::Bootstrap),
		(
			|i| i.bootstrap_fingerprint[0] ^= 1,
			BootstrapField2::BootstrapFingerprint,
		),
		(
			|i| i.bootstrap_command[0] ^= 1,
			BootstrapField2::BootstrapCommand,
		),
		(
			|i| i.genesis_fingerprint[0] ^= 1,
			BootstrapField2::GenesisFingerprint,
		),
	];
	for (change, field) in cases {
		let mut other = base;
		change(&mut other);
		assert_eq!(base.first_mismatch(&other), Some(field));
		assert_eq!(other.first_mismatch(&base), Some(field));
	}
	// Record and observation of the same identity compare equal; only sequence and magic differ.
	let rec = record(&golden("record")).unwrap();
	let obs = observation(&golden("observation")).unwrap();
	assert_eq!(rec.identity.first_mismatch(&obs.identity), None);
}
