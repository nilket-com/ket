use super::super::super::admin::manifest::AdminManifest2;
use super::super::inventory::*;
use super::*;
use CorruptReason2::*;

fn corrupt(seq: u64, reason: CorruptReason2) -> Result<(), LogError2> {
	Err(LogError2::Corrupt { seq, reason })
}
fn fold(b: &[u8]) -> Result<(), LogError2> {
	fold_events2(b).map(|_| ())
}
fn cat(parts: &[&[u8]]) -> Vec<u8> {
	parts.concat()
}
fn with_seq(mut e: Event2, seq: u64, op: u8) -> Event2 {
	e.seq = seq;
	e.op_id = [op; 16];
	e
}
// A distinct, enrollable reservation built from the second golden manifest.
fn manifest(generation: [u8; 16], cluster: [u8; 16], seed: u8) -> AdminManifest2 {
	let EventBody2::Reserve { manifest } = event("reserve_second").body else {
		unreachable!()
	};
	let mut m = manifest;
	m.generation = generation;
	m.cluster = cluster;
	m.admin_key = ed25519_dalek::SigningKey::from_bytes(&[seed; 32])
		.verifying_key()
		.to_bytes();
	m
}
fn reserve(m: AdminManifest2, seq: u64, op: [u8; 16]) -> Event2 {
	let bytes = m.encode().unwrap();
	Event2 {
		seq,
		op_id: op,
		identity: Some(RootIdentity2 {
			generation: m.generation,
			cluster: m.cluster,
			pin: super::super::super::admin::manifest::admin_manifest_digest2(&bytes),
			admin_key: m.admin_key,
		}),
		body: EventBody2::Reserve { manifest: m },
	}
}

#[test]
fn independent_goldens_round_trip_and_fold() {
	for name in [
		"reserve",
		"approve",
		"reserve_second",
		"conflict",
		"retire",
		"repair",
	] {
		let b = event_bytes(name);
		let e = decode_event2(&b).unwrap();
		assert_eq!(encoded(&e), b, "{name}");
	}
	let log = log_bytes();
	let s = fold_events2(&log).unwrap();
	assert_eq!(s.last_seq, 6);
	assert_eq!(s.roots.len(), 2);
	assert_eq!(
		(
			s.roots[0].state,
			s.roots[0].last_seq,
			s.roots[0].record.is_none()
		),
		(RootState2::Retired, 5, true)
	);
	assert_eq!(
		(
			s.roots[1].state,
			s.roots[1].last_seq,
			s.roots[1].record.is_none()
		),
		(RootState2::Conflicted, 4, true)
	);
	let p = fold_events2(&log[..384]).unwrap();
	assert_eq!((p.roots[0].state, p.last_seq), (RootState2::Prepared, 1));
	let b = fold_events2(&log[..768]).unwrap();
	assert_eq!(b.roots[0].state, RootState2::Bound);
	assert_eq!(b.roots[0].record.unwrap().inventory_seq, 2);
	let empty = fold_events2(&[]).unwrap();
	assert!(empty.roots.is_empty() && empty.last_seq == 0);
}
#[test]
fn local_checks_in_order() {
	let r = event_bytes("reserve");
	let patch = |at: usize, v: &[u8]| {
		let mut b = r.clone();
		b[at..at + v.len()].copy_from_slice(v);
		reseal_event(b)
	};
	let local = |b: &[u8]| {
		decode_event2(b).err().map(|e| match e {
			LogError2::Corrupt { reason, .. } => reason,
			other => panic!("{other:?}"),
		})
	};
	let mut bad_sum = r.clone();
	bad_sum[383] ^= 1;
	assert_eq!(local(&bad_sum), Some(Checksum));
	assert_eq!(local(&patch(0, b"VISIEV03")), Some(Magic));
	assert_eq!(local(&patch(8, &2u16.to_le_bytes())), Some(Version));
	assert_eq!(local(&patch(10, &[0])), Some(UnknownOp));
	assert_eq!(local(&patch(10, &[6])), Some(UnknownOp));
	assert_eq!(local(&patch(11, &[1])), Some(Reserved));
	assert_eq!(local(&patch(345, &[1])), Some(Reserved));
	assert_eq!(local(&patch(20, &[0; 16])), Some(ZeroOpId));
	for (at, w) in [(36, 16), (52, 16), (68, 32), (100, 32)] {
		assert_eq!(
			local(&patch(at, &vec![0; w])),
			Some(Header),
			"zero header component at {at}"
		);
	}
	assert_eq!(local(&patch(132 + 137, &[1])), Some(Payload));
	assert_eq!(local(&patch(132, &[0; 8])), Some(Payload));
	let retire = event_bytes("retire");
	let mut b = retire.clone();
	b[200] = 1;
	assert_eq!(local(&reseal_event(b)), Some(Payload));
	let mut b = event_bytes("approve");
	b[132] ^= 1;
	assert_eq!(local(&reseal_event(b)), Some(Payload));
	// Repair: zero header, zero fill, bounds.
	let repair = event_bytes("repair");
	let rp = |at: usize, v: &[u8]| {
		let mut b = repair.clone();
		b[at..at + v.len()].copy_from_slice(v);
		reseal_event(b)
	};
	assert_eq!(local(&rp(40, &[1])), Some(Header));
	assert_eq!(local(&rp(132 + 60, &[1])), Some(Payload));
	assert_eq!(
		local(&rp(132 + 40, &1u64.to_le_bytes())),
		Some(RepairBounds)
	);
	assert_eq!(
		local(&rp(132 + 32, &1u64.to_le_bytes())),
		Some(RepairBounds)
	);
	let tail = |n: u32| {
		let mut b = repair.clone();
		let boundary = u64::from_le_bytes(b[172..180].try_into().unwrap());
		b[164..172].copy_from_slice(&(boundary + n as u64).to_le_bytes());
		b[180..184].copy_from_slice(&n.to_le_bytes());
		reseal_event(b)
	};
	assert_eq!(local(&tail(0)), Some(RepairBounds));
	assert_eq!(local(&tail(767)), None);
	assert_eq!(local(&tail(768)), Some(RepairBounds));
	// Checked boundary arithmetic: seq 0 has no predecessor, and (seq - 1) * 384 overflows.
	for seq in [0, u64::MAX] {
		assert_eq!(
			decode_event2(&rp(12, &seq.to_le_bytes())).err(),
			Some(LogError2::Corrupt {
				seq,
				reason: RepairBounds
			}),
			"seq {seq}"
		);
	}
}
#[test]
fn semantic_rows() {
	let r = event("reserve");
	let a = event("approve");
	let r2 = event("reserve_second");
	let c = event("conflict");
	let t = event("retire");
	let er = encoded(&r);
	// Sequence and duplicates.
	assert_eq!(
		fold(&encoded(&with_seq(r.clone(), 2, 1))),
		corrupt(1, Sequence)
	);
	assert_eq!(
		fold(&cat(&[&er, &encoded(&with_seq(a.clone(), 2, 1))])),
		corrupt(2, DuplicateOpId)
	);
	// Approve without reservation, and with a mismatched identity.
	assert_eq!(
		fold(&encoded(&with_seq(a.clone(), 1, 9))),
		corrupt(1, NoReservation)
	);
	let mut wrong = a.clone();
	wrong.identity.as_mut().unwrap().pin[0] ^= 1;
	assert_eq!(
		fold(&cat(&[&er, &encoded(&wrong)])),
		corrupt(2, IdentityMismatch)
	);
	// Approve bindings: inventory seq, epoch.
	let EventBody2::Approve { record } = a.body.clone() else {
		unreachable!()
	};
	let mut off = record;
	off.inventory_seq = 7;
	let bad_seq = Event2 {
		body: EventBody2::Approve { record: off },
		..a.clone()
	};
	assert_eq!(fold(&cat(&[&er, &encoded(&bad_seq)])), corrupt(2, Binding));
	let mut epoch = record;
	epoch.identity.initial_epoch = 8;
	let bad_epoch = Event2 {
		body: EventBody2::Approve { record: epoch },
		..a.clone()
	};
	assert_eq!(
		fold(&cat(&[&er, &encoded(&bad_epoch)])),
		corrupt(2, Binding)
	);
	// A matching observation cannot manufacture Conflicted.
	let EventBody2::Conflict { observation } = c.body.clone() else {
		unreachable!()
	};
	let mut same = observation;
	let EventBody2::Reserve { manifest: m2 } = r2.body.clone() else {
		unreachable!()
	};
	same.identity.pin = r2.identity.unwrap().pin;
	same.identity.initial_epoch = m2.initial_epoch;
	let matching = with_seq(
		Event2 {
			body: EventBody2::Conflict { observation: same },
			..c.clone()
		},
		2,
		4,
	);
	assert_eq!(
		fold(&cat(&[
			&encoded(&with_seq(r2.clone(), 1, 3)),
			&encoded(&matching)
		])),
		corrupt(2, Binding)
	);
	// Reserve header must equal the manifest-derived identity.
	let mut lie = r.clone();
	lie.identity.as_mut().unwrap().pin[0] ^= 1;
	assert_eq!(fold(&encoded(&lie)), corrupt(1, Binding));
	// Transitions.
	let ea = encoded(&a);
	assert_eq!(
		fold(&cat(&[&er, &ea, &encoded(&with_seq(a.clone(), 3, 9))])),
		corrupt(3, Transition)
	);
	let retired = cat(&[&er, &encoded(&with_seq(t.clone(), 2, 5))]);
	assert_eq!(
		fold(&cat(&[&retired, &encoded(&with_seq(t.clone(), 3, 6))])),
		corrupt(3, Transition)
	);
	let EventBody2::Approve { record: r3 } = a.body.clone() else {
		unreachable!()
	};
	let mut r3 = r3;
	r3.inventory_seq = 3;
	let late = Event2 {
		seq: 3,
		op_id: [9; 16],
		identity: a.identity,
		body: EventBody2::Approve { record: r3 },
	};
	assert_eq!(
		fold(&cat(&[&retired, &encoded(&late)])),
		corrupt(3, Transition)
	);
	let conflicted = cat(&[
		&encoded(&with_seq(r2.clone(), 1, 3)),
		&encoded(&with_seq(c.clone(), 2, 4)),
	]);
	let retire2 = Event2 {
		seq: 3,
		op_id: [8; 16],
		identity: r2.identity,
		body: EventBody2::Retire,
	};
	assert!(
		fold(&cat(&[&conflicted, &encoded(&retire2)])).is_ok(),
		"Conflicted -> Retired is legal"
	);
	assert_eq!(
		fold(&cat(&[&conflicted, &encoded(&with_seq(c.clone(), 3, 7))])),
		corrupt(3, Transition)
	);
	let bound = cat(&[&er, &ea]);
	let c_on_bound = Event2 {
		seq: 3,
		op_id: [7; 16],
		identity: r.identity,
		body: c.body.clone(),
	};
	assert_eq!(
		fold(&cat(&[&bound, &encoded(&c_on_bound)])),
		corrupt(3, Transition)
	);
	assert!(
		fold(&cat(&[&bound, &encoded(&with_seq(t.clone(), 3, 5))])).is_ok(),
		"Bound -> Retired is legal"
	);
}
#[test]
fn retained_identities_collide_per_component_even_after_retire() {
	let r = event("reserve");
	let base = event("reserve_second");
	let EventBody2::Reserve { manifest: m1 } = r.body.clone() else {
		unreachable!()
	};
	let retired = cat(&[&encoded(&r), &encoded(&with_seq(event("retire"), 2, 5))]);
	let EventBody2::Reserve { manifest: m2 } = base.body.clone() else {
		unreachable!()
	};
	for (label, m) in [
		("generation", manifest(m1.generation, [0x31; 16], 0x51)),
		("cluster", manifest([0x32; 16], m1.cluster, 0x52)),
		("key", {
			let mut x = manifest([0x33; 16], [0x34; 16], 0x53);
			x.admin_key = m1.admin_key;
			x
		}),
		("same manifest", m1.clone()),
	] {
		let e = reserve(m, 3, [0x40; 16]);
		assert_eq!(
			fold(&cat(&[&retired, &encoded(&e)])),
			corrupt(3, IdentityTaken),
			"{label}"
		);
	}
	assert!(fold(&cat(&[&retired, &encoded(&reserve(m2, 3, [0x41; 16]))])).is_ok());
}
#[test]
fn root_and_slot_caps() {
	let mut log = Vec::new();
	for i in 0..4096u32 {
		let g = {
			let mut x = [0x10; 16];
			x[..4].copy_from_slice(&i.to_le_bytes());
			x
		};
		let c = {
			let mut x = [0x20; 16];
			x[..4].copy_from_slice(&i.to_le_bytes());
			x
		};
		let seed = {
			let mut s = [0x30u8; 32];
			s[..4].copy_from_slice(&i.to_le_bytes());
			s
		};
		let mut m = manifest(g, c, 0);
		m.admin_key = ed25519_dalek::SigningKey::from_bytes(&seed)
			.verifying_key()
			.to_bytes();
		let mut op = [0x50; 16];
		op[..4].copy_from_slice(&i.to_le_bytes());
		log.extend(encoded(&reserve(m, i as u64 + 1, op)));
	}
	assert_eq!(fold_events2(&log).unwrap().roots.len(), 4096);
	let extra = {
		let mut m = manifest([0xf1; 16], [0xf2; 16], 0xf3);
		m.admin_key = ed25519_dalek::SigningKey::from_bytes(&[0xf4; 32])
			.verifying_key()
			.to_bytes();
		m
	};
	let over = cat(&[&log, &encoded(&reserve(extra.clone(), 4097, [0xf5; 16]))]);
	assert_eq!(fold(&over), corrupt(4097, RootCap));
	// Ordinary events fit through seq 65,000; the next seq is repair-only.
	let mut pad = Vec::new();
	for seq in 1..65_000u64 {
		pad.extend(encoded(&repair_at(seq, seq)));
	}
	let at_limit = cat(&[&pad, &encoded(&reserve(extra.clone(), 65_000, [0xf6; 16]))]);
	assert!(fold(&at_limit).is_ok());
	let over_slot = cat(&[
		&pad,
		&encoded(&repair_at(65_000, 65_000)),
		&encoded(&reserve(extra, 65_001, [0xf7; 16])),
	]);
	assert_eq!(fold(&over_slot), corrupt(65_001, OrdinarySlot));
	// Repairs fill the remaining slots through seq 65,536; one more whole event is too long.
	for seq in 65_000..=TOTAL_EVENTS2 {
		pad.extend(encoded(&repair_at(seq, seq)));
	}
	assert_eq!(pad.len(), TOTAL_EVENTS2 as usize * EVENT_BYTES2);
	assert_eq!(fold_events2(&pad).unwrap().last_seq, TOTAL_EVENTS2);
	pad.extend(encoded(&repair_at(TOTAL_EVENTS2 + 1, TOTAL_EVENTS2 + 1)));
	assert_eq!(fold(&pad), Err(LogError2::TooLong));
	assert_eq!(
		fold_events2(&vec![0u8; MAX_LOG_BYTES2 + 1]).err(),
		Some(LogError2::TooLong)
	);
}
#[test]
fn error_precedence_never_lets_a_tail_mask_earlier_corruption() {
	let log = log_bytes();
	let (e1, e2, e3) = (&log[..384], &log[384..768], &log[768..1152]);
	let short = [0xabu8; 10];
	let mut bad1 = e1.to_vec();
	bad1[383] ^= 1;
	assert_eq!(fold(&cat(&[&bad1, e2, &short])), corrupt(1, Checksum));
	let dup = encoded(&with_seq(event("reserve_second"), 3, 1));
	assert_eq!(
		fold(&cat(&[e1, e2, &dup, &short])),
		corrupt(3, DuplicateOpId)
	);
	let again = encoded(&with_seq(event("approve"), 3, 9));
	assert_eq!(
		fold(&cat(&[e1, e2, &again, &short])),
		corrupt(3, Transition)
	);
	let mut bad_last = e3.to_vec();
	bad_last[383] ^= 1;
	let amb = |boundary: usize, tail: &[u8]| {
		Err(LogError2::Ambiguous {
			boundary: boundary as u64,
			tail_len: tail.len() as u32,
			tail_sha256: super::super::super::codec::hash(b"", tail),
		})
	};
	assert_eq!(fold(&cat(&[e1, e2, &bad_last])), amb(768, &bad_last));
	let with_partial = cat(&[&bad_last, &short]);
	assert_eq!(
		fold(&cat(&[e1, e2, &with_partial])),
		amb(768, &with_partial)
	);
	assert_eq!(fold(&cat(&[e1, &short])), amb(384, &short));
}
