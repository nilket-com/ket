//! Profile-3 ports of the `inventory.rs` fold and codec rows: the same typed errors at the same
//! steps over VISOEV02 events (512 bytes) and profile-3 reservations, from the independent `p3_`
//! vectors (`provisioning_reference.py`). A Bound root retires through a terminal Retire.
use super::*;
use crate::storage::mutation_visibility::root2::admin::manifest::{
	AdminManifestThree3, manifest_digest3,
};
use crate::storage::mutation_visibility::root2::codec::hash;
use crate::storage::mutation_visibility::root2::provisioning::inventory::v2::{
	EVENT_BYTES2, MAX_LOG_BYTES2, decode_event2, fold_events2,
};
use crate::storage::mutation_visibility::root2::provisioning::inventory::*;
use CorruptReason2::*;

const E: usize = EVENT_BYTES2;
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
/// The terminal Retire of the first root at `seq`, with its record's inventory sequence moved too.
fn retire_at(seq: u64, op: u8) -> Event2 {
	let mut t = with_seq(p3_event("retire"), seq, op);
	let EventBody2::RetireTerminal { terminal } = &mut t.body else {
		unreachable!()
	};
	terminal.inventory_seq = seq;
	t
}
fn three(e: &Event2) -> AdminManifestThree3 {
	let EventBody2::Reserve { manifest: m } = e.body.clone() else {
		unreachable!()
	};
	m
}
// A distinct, enrollable profile-3 reservation built from the second root's roles.
fn manifest(generation: [u8; 16], cluster: [u8; 16], seed: [u8; 32]) -> AdminManifestThree3 {
	let m = three(&p3_event("reserve_second"));
	let mut base = m.base().clone();
	base.generation = generation;
	base.cluster = cluster;
	base.admin_key = ed25519_dalek::SigningKey::from_bytes(&seed)
		.verifying_key()
		.to_bytes();
	AdminManifestThree3::new(base, m.governance(), m.recovery()).unwrap()
}
fn reserve(m: AdminManifestThree3, seq: u64, op: [u8; 16]) -> Event2 {
	let any = m;
	let a = any.base().clone();
	Event2 {
		seq,
		op_id: op,
		identity: Some(RootIdentity2 {
			generation: a.generation,
			cluster: a.cluster,
			pin: manifest_digest3(&any.encode().unwrap()),
			admin_key: a.admin_key,
		}),
		body: EventBody2::Reserve { manifest: any },
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
		let b = p3_event_bytes(name);
		let e = decode_event2(&b).unwrap();
		assert_eq!(encoded3(&e), b, "{name}");
	}
	let log = p3_log_bytes();
	let s = fold_events2(&log).unwrap();
	assert_eq!(s.last_seq, 6);
	assert_eq!(s.roots.len(), 2);
	let r0 = &s.roots[0];
	assert_eq!(
		(
			r0.state,
			r0.last_seq,
			r0.record.is_none(),
			r0.terminal.is_some()
		),
		(RootState2::Retired, 5, true, true)
	);
	let r1 = &s.roots[1];
	assert_eq!(
		(r1.state, r1.last_seq, r1.record.is_none()),
		(RootState2::Conflicted, 4, true)
	);
	let p = fold_events2(&log[..E]).unwrap();
	assert_eq!((p.roots[0].state, p.last_seq), (RootState2::Prepared, 1));
	let b = fold_events2(&log[..2 * E]).unwrap();
	assert_eq!(b.roots[0].state, RootState2::Bound);
	assert_eq!(b.roots[0].record.unwrap().inventory_seq, 2);
	let empty = fold_events2(&[]).unwrap();
	assert!(empty.roots.is_empty() && empty.last_seq == 0);
}
#[test]
fn local_checks_in_order() {
	let r = p3_event_bytes("reserve");
	let patch = |at: usize, v: &[u8]| {
		let mut b = r.clone();
		b[at..at + v.len()].copy_from_slice(v);
		reseal_event3(b)
	};
	let local = |b: &[u8]| {
		decode_event2(b).err().map(|e| match e {
			LogError2::Corrupt { reason, .. } => reason,
			other => panic!("{other:?}"),
		})
	};
	assert_eq!(local(&r), None);
	let mut bad_sum = r.clone();
	bad_sum[E - 1] ^= 1;
	assert_eq!(local(&bad_sum), Some(Checksum));
	assert_eq!(local(&patch(0, b"VISOEV03")), Some(Magic));
	assert_eq!(local(&patch(8, &2u16.to_le_bytes())), Some(Version));
	assert_eq!(local(&patch(10, &[0])), Some(UnknownOp));
	assert_eq!(local(&patch(10, &[6])), Some(UnknownOp));
	assert_eq!(local(&patch(11, &[1])), Some(Reserved));
	assert_eq!(local(&patch(140, &[1])), Some(Reserved));
	assert_eq!(local(&patch(20, &[0; 16])), Some(ZeroOpId));
	for (at, w) in [(36, 16), (52, 16), (68, 32), (100, 32)] {
		assert_eq!(
			local(&patch(at, &vec![0; w])),
			Some(Header),
			"zero header component at {at}"
		);
	}
	// The payload zero fill after the 241-byte manifest, and a length no Reserve has.
	assert_eq!(local(&patch(144 + 241, &[1])), Some(Payload));
	assert_eq!(local(&patch(132, &[0; 2])), Some(Payload));
	let retire = p3_event_bytes("retire");
	let mut b = retire.clone();
	b[144 + 156] = 1;
	assert_eq!(local(&reseal_event3(b)), Some(Payload));
	let mut b = p3_event_bytes("approve");
	b[132] ^= 1;
	assert_eq!(local(&reseal_event3(b)), Some(Payload));
	// Repair: zero header, zero fill, bounds.
	let repair = p3_event_bytes("repair");
	let rp = |at: usize, v: &[u8]| {
		let mut b = repair.clone();
		b[at..at + v.len()].copy_from_slice(v);
		reseal_event3(b)
	};
	assert_eq!(local(&repair), None);
	assert_eq!(local(&rp(40, &[1])), Some(Header));
	assert_eq!(local(&rp(144 + 60, &[1])), Some(Payload));
	assert_eq!(
		local(&rp(144 + 40, &1u64.to_le_bytes())),
		Some(RepairBounds)
	);
	assert_eq!(
		local(&rp(144 + 32, &1u64.to_le_bytes())),
		Some(RepairBounds)
	);
	let tail = |n: u32| {
		let mut b = repair.clone();
		let boundary = u64::from_le_bytes(b[184..192].try_into().unwrap());
		b[176..184].copy_from_slice(&(boundary + n as u64).to_le_bytes());
		b[192..196].copy_from_slice(&n.to_le_bytes());
		reseal_event3(b)
	};
	assert_eq!(local(&tail(0)), Some(RepairBounds));
	assert_eq!(local(&tail(v2::MAX_TAIL2 as u32)), None);
	assert_eq!(local(&tail(v2::MAX_TAIL2 as u32 + 1)), Some(RepairBounds));
	// Checked boundary arithmetic: seq 0 has no predecessor, and (seq - 1) * 512 overflows.
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
	let r = p3_event("reserve");
	let a = p3_event("approve");
	let r2 = p3_event("reserve_second");
	let c = p3_event("conflict");
	let er = encoded3(&r);
	// Sequence and duplicates.
	assert_eq!(
		fold(&encoded3(&with_seq(r.clone(), 2, 1))),
		corrupt(1, Sequence)
	);
	assert_eq!(
		fold(&cat(&[&er, &encoded3(&with_seq(a.clone(), 2, 1))])),
		corrupt(2, DuplicateOpId)
	);
	// Approve without reservation, and with a mismatched identity.
	assert_eq!(
		fold(&encoded3(&with_seq(a.clone(), 1, 9))),
		corrupt(1, NoReservation)
	);
	let mut wrong = a.clone();
	wrong.identity.as_mut().unwrap().pin[0] ^= 1;
	assert_eq!(
		fold(&cat(&[&er, &encoded3(&wrong)])),
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
	assert_eq!(fold(&cat(&[&er, &encoded3(&bad_seq)])), corrupt(2, Binding));
	let mut epoch = record;
	epoch.identity.initial_epoch = 8;
	let bad_epoch = Event2 {
		body: EventBody2::Approve { record: epoch },
		..a.clone()
	};
	assert_eq!(
		fold(&cat(&[&er, &encoded3(&bad_epoch)])),
		corrupt(2, Binding)
	);
	// A matching observation cannot manufacture Conflicted.
	let EventBody2::Conflict { observation } = c.body.clone() else {
		unreachable!()
	};
	let mut same = observation;
	same.identity.pin = r2.identity.unwrap().pin;
	same.identity.initial_epoch = three(&r2).base().initial_epoch;
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
			&encoded3(&with_seq(r2.clone(), 1, 3)),
			&encoded3(&matching)
		])),
		corrupt(2, Binding)
	);
	// Reserve header must equal the manifest-derived identity.
	let mut lie = r.clone();
	lie.identity.as_mut().unwrap().pin[0] ^= 1;
	assert_eq!(fold(&encoded3(&lie)), corrupt(1, Binding));
	// Transitions.
	let ea = encoded3(&a);
	assert_eq!(
		fold(&cat(&[&er, &ea, &encoded3(&with_seq(a.clone(), 3, 9))])),
		corrupt(3, Transition)
	);
	let retired = cat(&[&er, &ea, &encoded3(&retire_at(3, 5))]);
	assert!(fold(&retired).is_ok(), "Bound -> Retired is legal");
	assert_eq!(
		fold(&cat(&[&retired, &encoded3(&retire_at(4, 6))])),
		corrupt(4, Transition)
	);
	let EventBody2::Approve { record: r4 } = a.body.clone() else {
		unreachable!()
	};
	let mut r4 = r4;
	r4.inventory_seq = 4;
	let late = Event2 {
		seq: 4,
		op_id: [9; 16],
		identity: a.identity,
		body: EventBody2::Approve { record: r4 },
	};
	assert_eq!(
		fold(&cat(&[&retired, &encoded3(&late)])),
		corrupt(4, Transition)
	);
	let conflicted = cat(&[
		&encoded3(&with_seq(r2.clone(), 1, 3)),
		&encoded3(&with_seq(c.clone(), 2, 4)),
	]);
	assert_eq!(
		fold(&cat(&[&conflicted, &encoded3(&with_seq(c.clone(), 3, 7))])),
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
		fold(&cat(&[&bound, &encoded3(&c_on_bound)])),
		corrupt(3, Transition)
	);
}
#[test]
fn retained_identities_collide_per_component_even_after_retire() {
	let r = p3_event("reserve");
	let m1 = three(&r);
	let b1 = m1.base().clone();
	let retired = cat(&[
		&encoded3(&r),
		&encoded3(&p3_event("approve")),
		&encoded3(&retire_at(3, 5)),
	]);
	for (label, m) in [
		(
			"generation",
			manifest(b1.generation, [0x31; 16], [0x51; 32]),
		),
		("cluster", manifest([0x32; 16], b1.cluster, [0x52; 32])),
		("key", {
			let x = manifest([0x33; 16], [0x34; 16], [0x53; 32]);
			let mut base = x.base().clone();
			base.admin_key = b1.admin_key;
			AdminManifestThree3::new(base, x.governance(), x.recovery()).unwrap()
		}),
		("same manifest", m1.clone()),
	] {
		let e = reserve(m, 4, [0x40; 16]);
		assert_eq!(
			fold(&cat(&[&retired, &encoded3(&e)])),
			corrupt(4, IdentityTaken),
			"{label}"
		);
	}
	let second = three(&p3_event("reserve_second"));
	assert!(
		fold(&cat(&[
			&retired,
			&encoded3(&reserve(second, 4, [0x41; 16]))
		]))
		.is_ok()
	);
}
/// The root cap through the fold, in two tests that share the once-framed roots (each fold
/// validates all of them, so the two folds run in parallel): exactly 4096 roots are accepted...
#[test]
fn root_cap_accepts_exactly_the_cap() {
	let s = fold_events2(p3_cap_roots()).unwrap();
	assert_eq!((s.roots.len(), s.last_seq), (4096, 4096));
}
/// ...and the next Reserve is refused RootCap at seq 4097.
#[test]
fn root_cap_refuses_one_more_root() {
	let extra = p3_variant([0xf1; 16], [0xf2; 16], [0xf4; 32]);
	let over = cat(&[p3_cap_roots(), &p3_reserve_frame(&extra, 4097, [0xf5; 16])]);
	assert_eq!(fold(&over), corrupt(4097, RootCap));
}
#[test]
fn slot_caps() {
	let extra = || {
		three(
			&decode_event2(&p3_reserve_frame(
				&p3_variant([0xf1; 16], [0xf2; 16], [0xf4; 32]),
				1,
				[0xf6; 16],
			))
			.unwrap(),
		)
	};
	// Ordinary events fit through seq 65,000; the next seq is repair-only.
	let mut pad = Vec::new();
	for seq in 1..65_000u64 {
		pad.extend(encoded3(&repair_at3(seq, seq)));
	}
	let at_limit = cat(&[&pad, &encoded3(&reserve(extra(), 65_000, [0xf6; 16]))]);
	assert!(fold(&at_limit).is_ok());
	let over_slot = cat(&[
		&pad,
		&encoded3(&repair_at3(65_000, 65_000)),
		&encoded3(&reserve(extra(), 65_001, [0xf7; 16])),
	]);
	assert_eq!(fold(&over_slot), corrupt(65_001, OrdinarySlot));
	// Repairs fill the remaining slots through seq 65,536; one more whole event is too long.
	for seq in 65_000..=TOTAL_EVENTS2 {
		pad.extend(encoded3(&repair_at3(seq, seq)));
	}
	assert_eq!(pad.len(), TOTAL_EVENTS2 as usize * E);
	assert_eq!(fold_events2(&pad).unwrap().last_seq, TOTAL_EVENTS2);
	pad.extend(encoded3(&repair_at3(TOTAL_EVENTS2 + 1, TOTAL_EVENTS2 + 1)));
	assert_eq!(fold(&pad), Err(LogError2::TooLong));
	assert_eq!(
		fold_events2(&vec![0u8; MAX_LOG_BYTES2 + 1]).err(),
		Some(LogError2::TooLong)
	);
}
#[test]
fn error_precedence_never_lets_a_tail_mask_earlier_corruption() {
	let log = p3_log_bytes();
	let (e1, e2, e3) = (&log[..E], &log[E..2 * E], &log[2 * E..3 * E]);
	let short = [0xabu8; 10];
	let mut bad1 = e1.to_vec();
	bad1[E - 1] ^= 1;
	assert_eq!(fold(&cat(&[&bad1, e2, &short])), corrupt(1, Checksum));
	let dup = encoded3(&with_seq(p3_event("reserve_second"), 3, 1));
	assert_eq!(
		fold(&cat(&[e1, e2, &dup, &short])),
		corrupt(3, DuplicateOpId)
	);
	let again = encoded3(&with_seq(p3_event("approve"), 3, 9));
	assert_eq!(
		fold(&cat(&[e1, e2, &again, &short])),
		corrupt(3, Transition)
	);
	let mut bad_last = e3.to_vec();
	bad_last[E - 1] ^= 1;
	let amb = |boundary: usize, tail: &[u8]| {
		Err(LogError2::Ambiguous {
			boundary: boundary as u64,
			tail_len: tail.len() as u32,
			tail_sha256: hash(b"", tail),
		})
	};
	assert_eq!(fold(&cat(&[e1, e2, &bad_last])), amb(2 * E, &bad_last));
	let with_partial = cat(&[&bad_last, &short]);
	assert_eq!(
		fold(&cat(&[e1, e2, &with_partial])),
		amb(2 * E, &with_partial)
	);
	assert_eq!(fold(&cat(&[e1, &short])), amb(E, &short));
}
