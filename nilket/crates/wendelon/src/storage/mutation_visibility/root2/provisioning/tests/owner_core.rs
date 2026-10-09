use super::super::super::admin::manifest::{AdminManifest2, admin_manifest_digest2};
use super::super::super::admin::record::BootstrapObservation2;
use super::super::inventory::*;
use super::super::owner_core::*;
use super::*;

const B3: usize = 3 * 384;

fn sha(b: &[u8]) -> [u8; 32] {
	super::super::super::codec::hash(b"", b)
}
fn op(n: u8) -> OpId2 {
	op_id2([n; 16]).unwrap()
}
fn owner_vectors() -> serde_json::Value {
	serde_json::from_str(include_str!("owner_vectors.json")).unwrap()
}
fn manifest_of(name: &str) -> AdminManifest2 {
	let EventBody2::Reserve { manifest } = event(name).body else {
		unreachable!()
	};
	manifest
}
fn id_of(name: &str) -> RootIdentity2 {
	event(name).identity.unwrap()
}
fn approved_observation() -> BootstrapObservation2 {
	let EventBody2::Approve { record } = event("approve").body else {
		unreachable!()
	};
	BootstrapObservation2 {
		identity: record.identity,
	}
}
fn conflict_observation() -> BootstrapObservation2 {
	let EventBody2::Conflict { observation } = event("conflict").body else {
		unreachable!()
	};
	observation
}
fn approve(id: RootIdentity2, o: BootstrapObservation2) -> OwnerRequest2 {
	OwnerRequest2::Approve {
		identity: id,
		observation: o,
		confirmed: id,
	}
}
fn events(n: usize) -> Vec<u8> {
	log_bytes()[..n * 384].to_vec()
}
fn reseal(mut b: Vec<u8>) -> Vec<u8> {
	let n = b.len() - 32;
	let sum = sha(&b[..n]);
	b[n..].copy_from_slice(&sum);
	b
}
fn torn_tail() -> Vec<u8> {
	unhex(owner_vectors()["tail"].as_str().unwrap())
}

#[test]
fn decisions_reproduce_the_independent_golden_events() {
	let (id1, id2) = (id_of("reserve"), id_of("reserve_second"));
	let reserve = OwnerRequest2::Reserve {
		manifest: manifest_of("reserve"),
	};
	assert_eq!(
		decide2(&[], &op(1), &reserve),
		Decision2::Append(event("reserve"))
	);
	assert_eq!(
		decide2(&events(1), &op(2), &approve(id1, approved_observation())),
		Decision2::Append(event("approve"))
	);
	let second = OwnerRequest2::Reserve {
		manifest: manifest_of("reserve_second"),
	};
	assert_eq!(
		decide2(&events(2), &op(3), &second),
		Decision2::Append(event("reserve_second"))
	);
	// An Approve whose observation differs from the reservation becomes a Conflict.
	assert_eq!(
		decide2(&events(3), &op(4), &approve(id2, conflict_observation())),
		Decision2::Append(event("conflict"))
	);
	assert_eq!(
		decide2(&events(4), &op(5), &OwnerRequest2::Retire { identity: id1 }),
		Decision2::Append(event("retire"))
	);
}
#[test]
fn retries_are_recognised_from_the_log_alone() {
	let log = log_bytes();
	let (id1, id2) = (id_of("reserve"), id_of("reserve_second"));
	let retry = |op: EventOp2, seq: u64, current: RootState2| Decision2::Retry {
		original: EventRef2 { op, seq },
		current,
	};
	let reserve = OwnerRequest2::Reserve {
		manifest: manifest_of("reserve"),
	};
	assert_eq!(
		decide2(&log, &op(1), &reserve),
		retry(EventOp2::Reserve, 1, RootState2::Retired)
	);
	// After Retire the original Approve is still reported, with the root's state now.
	assert_eq!(
		decide2(&log, &op(2), &approve(id1, approved_observation())),
		retry(EventOp2::Approve, 2, RootState2::Retired)
	);
	assert_eq!(
		decide2(&log, &op(4), &approve(id2, conflict_observation())),
		retry(EventOp2::Conflict, 4, RootState2::Conflicted)
	);
	assert_eq!(
		decide2(&log, &op(5), &OwnerRequest2::Retire { identity: id1 }),
		retry(EventOp2::Retire, 5, RootState2::Retired)
	);
	let reused = |seq| Decision2::Refuse(OwnerError2::OpIdReused { seq });
	assert_eq!(
		decide2(&log, &op(4), &approve(id2, approved_observation())),
		reused(4)
	);
	assert_eq!(
		decide2(&log, &op(5), &OwnerRequest2::Retire { identity: id2 }),
		reused(5)
	);
	assert_eq!(
		decide2(&log, &op(2), &OwnerRequest2::Retire { identity: id1 }),
		reused(2)
	);
	// The golden Repair event's op id is never a request's.
	assert_eq!(
		decide2(&log, &op(6), &OwnerRequest2::Retire { identity: id2 }),
		reused(6)
	);
	// A bad confirmation is refused before the lookup, even for a recorded op id.
	let bad = OwnerRequest2::Approve {
		identity: id1,
		observation: approved_observation(),
		confirmed: id2,
	};
	assert_eq!(
		decide2(&log, &op(2), &bad),
		Decision2::Refuse(OwnerError2::BadConfirmation)
	);
}
#[test]
fn state_rows_no_ops_and_refusals() {
	let log = log_bytes();
	let (id1, id2) = (id_of("reserve"), id_of("reserve_second"));
	let refuse = Decision2::Refuse;
	let fresh = op(0x40);
	// Bound: the identical approval is a no-op, anything else a refusal, a bad confirmation first.
	let bound = events(2);
	assert_eq!(
		decide2(&bound, &fresh, &approve(id1, approved_observation())),
		Decision2::NoOp(RootState2::Bound)
	);
	let mut other = approved_observation();
	other.identity.bootstrap_fingerprint[0] ^= 1;
	assert_eq!(
		decide2(&bound, &fresh, &approve(id1, other)),
		refuse(OwnerError2::BoundConflict)
	);
	let bad = OwnerRequest2::Approve {
		identity: id1,
		observation: approved_observation(),
		confirmed: id2,
	};
	assert_eq!(
		decide2(&bound, &fresh, &bad),
		refuse(OwnerError2::BadConfirmation)
	);
	assert_eq!(
		decide2(&log, &fresh, &OwnerRequest2::Retire { identity: id1 }),
		Decision2::NoOp(RootState2::Retired)
	);
	for id in [id1, id2] {
		assert_eq!(
			decide2(&log, &fresh, &approve(id, approved_observation())),
			refuse(OwnerError2::NotPrepared)
		);
	}
	let again = OwnerRequest2::Reserve {
		manifest: manifest_of("reserve"),
	};
	assert_eq!(
		decide2(&log, &fresh, &again),
		refuse(OwnerError2::AlreadyReserved)
	);
	// One shared component with a Retired root is enough.
	let mut shared = manifest_of("reserve");
	shared.generation = [0x3c; 16];
	assert_eq!(
		decide2(&log, &fresh, &OwnerRequest2::Reserve { manifest: shared }),
		refuse(OwnerError2::IdentityTaken)
	);
	let mut mismatch = id1;
	mismatch.pin[0] ^= 1;
	assert_eq!(
		decide2(&log, &fresh, &OwnerRequest2::Retire { identity: mismatch }),
		refuse(OwnerError2::IdentityMismatch)
	);
	let mut unknown = id1;
	unknown.generation = [0x3d; 16];
	assert_eq!(
		decide2(&log, &fresh, &OwnerRequest2::Retire { identity: unknown }),
		refuse(OwnerError2::NoSuchRoot)
	);
	// Only a clean log is decided on.
	let mut torn = events(1);
	torn.extend([0xab; 10]);
	assert!(matches!(
		decide2(&torn, &fresh, &OwnerRequest2::Retire { identity: id1 }),
		Decision2::Refuse(OwnerError2::Log(LogError2::Ambiguous { .. }))
	));
	assert!(op_id2([0; 16]).is_none());
}
#[test]
fn invalid_typed_requests_are_refused_before_any_state() {
	let log = log_bytes();
	let id1 = id_of("reserve");
	let codec = Decision2::Refuse(OwnerError2::Codec);
	let mut m = manifest_of("reserve_second");
	m.initial_epoch = 0;
	assert_eq!(
		decide2(&log, &op(0x41), &OwnerRequest2::Reserve { manifest: m }),
		codec
	);
	let mut m = manifest_of("reserve_second");
	m.admin_key = [0; 32];
	assert_eq!(
		decide2(&log, &op(0x41), &OwnerRequest2::Reserve { manifest: m }),
		codec
	);
	let mut zero = id1;
	zero.cluster = [0; 16];
	assert_eq!(
		decide2(&log, &op(0x41), &approve(zero, approved_observation())),
		codec
	);
	assert_eq!(
		decide2(&log, &op(0x41), &OwnerRequest2::Retire { identity: zero }),
		codec
	);
	let mut o = approved_observation();
	o.identity.generation = [0; 16];
	assert_eq!(decide2(&log, &op(0x41), &approve(id1, o)), codec);
	// Invalid input is refused even on a log that is not clean.
	assert_eq!(decide2(&log[..10], &op(0x41), &approve(id1, o)), codec);
}
#[test]
fn caps_refuse_appends_but_not_retries_or_no_ops() {
	let (id1, id2) = (id_of("reserve"), id_of("reserve_second"));
	let mut log = events(5);
	for seq in 6..=ORDINARY_EVENTS2 {
		log.extend(encoded(&repair_at(seq, seq)));
	}
	let full = Decision2::Refuse(OwnerError2::LogFull);
	assert_eq!(
		decide2(&log, &op(2), &approve(id1, approved_observation())),
		Decision2::Retry {
			original: EventRef2 {
				op: EventOp2::Approve,
				seq: 2
			},
			current: RootState2::Retired
		}
	);
	assert_eq!(
		decide2(&log, &op(0x42), &OwnerRequest2::Retire { identity: id1 }),
		Decision2::NoOp(RootState2::Retired)
	);
	assert_eq!(
		decide2(&log, &op(0x42), &OwnerRequest2::Retire { identity: id2 }),
		full
	);
	let mut m = manifest_of("reserve_second");
	m.generation = [0x51; 16];
	m.cluster = [0x52; 16];
	m.admin_key = ed25519_dalek::SigningKey::from_bytes(&[0x53; 32])
		.verifying_key()
		.to_bytes();
	assert_eq!(
		decide2(
			&log,
			&op(0x42),
			&OwnerRequest2::Reserve {
				manifest: m.clone()
			}
		),
		full
	);
	// 4096 roots: a retry of the first reservation still succeeds; a new one is RootCap. The
	// roots are encoded directly; deciding each one would refold the growing log 4096 times.
	let mut roots = Vec::new();
	let mut first = None;
	for i in 0..4096u32 {
		let mut x = m.clone();
		x.generation[..4].copy_from_slice(&i.to_le_bytes());
		x.cluster[..4].copy_from_slice(&i.to_le_bytes());
		let mut seed = [0x60u8; 32];
		seed[..4].copy_from_slice(&i.to_le_bytes());
		x.admin_key = ed25519_dalek::SigningKey::from_bytes(&seed)
			.verifying_key()
			.to_bytes();
		let mut o = [0x70; 16];
		o[..4].copy_from_slice(&i.to_le_bytes());
		let bytes = x.encode().unwrap();
		roots.extend(encoded(&Event2 {
			seq: u64::from(i) + 1,
			op_id: o,
			identity: Some(RootIdentity2 {
				generation: x.generation,
				cluster: x.cluster,
				pin: admin_manifest_digest2(&bytes),
				admin_key: x.admin_key,
			}),
			body: EventBody2::Reserve {
				manifest: x.clone(),
			},
		}));
		first.get_or_insert((o, OwnerRequest2::Reserve { manifest: x }));
	}
	assert_eq!(fold_events2(&roots).unwrap().roots.len(), 4096);
	let (o, req) = first.unwrap();
	assert!(matches!(
		decide2(&roots, &op_id2(o).unwrap(), &req),
		Decision2::Retry { .. }
	));
	let mut extra = m;
	extra.generation = [0x54; 16];
	extra.cluster = [0x55; 16];
	extra.admin_key = ed25519_dalek::SigningKey::from_bytes(&[0x56; 32])
		.verifying_key()
		.to_bytes();
	assert_eq!(
		decide2(
			&roots,
			&op(0x43),
			&OwnerRequest2::Reserve { manifest: extra }
		),
		Decision2::Refuse(OwnerError2::RootCap)
	);
}
#[test]
fn independent_evidence_vectors_match_byte_for_byte() {
	let v = owner_vectors();
	let mut contexts = Vec::new();
	for case in ["base", "same_boundary_other_prefix", "other_boundary"] {
		let c = &v["cases"][case];
		let log = unhex(c["log"].as_str().unwrap());
		let evidence = unhex(c["evidence"].as_str().unwrap());
		let name = c["name"].as_str().unwrap();
		let (n, bytes) = encode_evidence2(&log).unwrap();
		assert_eq!((n.as_str(), &bytes), (name, &evidence), "{case}");
		let e = decode_evidence2(name, &evidence).unwrap();
		let context: [u8; 32] = unhex(c["context"].as_str().unwrap()).try_into().unwrap();
		assert_eq!(
			repair_context2(&e.prefix_sha256, e.boundary, e.original_len, &e.tail_sha256),
			context
		);
		assert_eq!(
			repair_op_id2(&context).to_vec(),
			unhex(c["op_id"].as_str().unwrap())
		);
		assert_eq!(evidence_name2(&context), name);
		assert_eq!(
			expected_repair_event2(&e).unwrap().to_vec(),
			unhex(c["expected_repair"].as_str().unwrap())
		);
		assert_eq!(e.boundary, c["boundary"].as_u64().unwrap());
		contexts.push(context);
	}
	// The same torn tail behind another prefix at the same boundary, and at another boundary.
	assert!(contexts[0] != contexts[1] && contexts[0] != contexts[2] && contexts[1] != contexts[2]);
	assert_eq!(
		encode_evidence2(&log_bytes()),
		Err(EvidenceError2::NotAmbiguous)
	);
	let mut corrupt = log_bytes();
	corrupt[383] ^= 1;
	corrupt.extend([1; 10]);
	assert_eq!(
		encode_evidence2(&corrupt),
		Err(EvidenceError2::NotAmbiguous)
	);
}
#[test]
fn evidence_decode_checks_in_order() {
	let c = &owner_vectors()["cases"]["base"];
	let good = unhex(c["evidence"].as_str().unwrap());
	let name = c["name"].as_str().unwrap();
	let check = |b: &[u8]| decode_evidence2(name, b).err();
	let patched = |at: usize, v: &[u8]| {
		let mut b = good.clone();
		b[at..at + v.len()].copy_from_slice(v);
		reseal(b)
	};
	let flipped = |at: usize| {
		let mut b = good.clone();
		b[at] ^= 1;
		reseal(b)
	};
	use EvidenceError2::*;
	assert_eq!(decode_evidence2("x.rep", &good).err(), Some(Name));
	assert_eq!(
		decode_evidence2(&name.to_uppercase().replace(".REP", ".rep"), &good).err(),
		Some(Name)
	);
	assert_eq!(check(&good[..156]), Some(Length));
	assert_eq!(check(&vec![0; MAX_EVIDENCE_BYTES2 + 1]), Some(Length));
	assert_eq!(check(&patched(0, b"VISREP02")), Some(Magic));
	assert_eq!(check(&patched(8, &2u16.to_le_bytes())), Some(Version));
	assert_eq!(check(&patched(10, &[1])), Some(Reserved));
	assert_eq!(check(&patched(64, &[1])), Some(Reserved));
	assert_eq!(check(&patched(60, &0u32.to_le_bytes())), Some(TailLength));
	assert_eq!(check(&patched(60, &768u32.to_le_bytes())), Some(TailLength));
	assert_eq!(check(&patched(60, &99u32.to_le_bytes())), Some(Length));
	let mut trailer = good.clone();
	let end = trailer.len() - 1;
	trailer[end] ^= 1;
	assert_eq!(check(&trailer), Some(Trailer));
	assert_eq!(check(&flipped(124)), Some(TailHash));
	assert_eq!(check(&patched(44, &1153u64.to_le_bytes())), Some(Bounds));
	assert_eq!(check(&patched(52, &1253u64.to_le_bytes())), Some(Bounds));
	for seq in [0u64, 5, 65_537] {
		assert_eq!(
			check(&patched(116, &seq.to_le_bytes())),
			Some(Sequence),
			"seq {seq}"
		);
	}
	assert_eq!(check(&flipped(12)), Some(Context));
	assert_eq!(check(&flipped(100)), Some(OpId));
	assert!(check(&good).is_none());
}
#[test]
fn classification_precedence_patterns_and_candidates() {
	let golden = log_bytes();
	let prefix = golden[..B3].to_vec();
	let tail = torn_tail();
	let torn = [prefix.as_slice(), &tail].concat();
	let (name, ev) = encode_evidence2(&torn).unwrap();
	let e = decode_evidence2(&name, &ev).unwrap();
	let context = repair_context2(&e.prefix_sha256, e.boundary, e.original_len, &e.tail_sha256);
	let expected = expected_repair_event2(&e).unwrap();
	let with = |log: &[u8]| classify2(log, &[(name.as_str(), ev.as_slice())]);
	let pending = |stage| Classification2::Pending { context, stage };
	use RepairStage2::*;
	assert_eq!(
		classify2(&torn, &[]),
		Classification2::Unclaimed {
			boundary: B3 as u64,
			tail_len: 100,
			tail_sha256: sha(&tail),
			prefix_sha256: sha(&prefix)
		}
	);
	// P1: untruncated and partially truncated tails; empty prefix after truncation.
	assert_eq!(with(&torn), pending(NotTruncated));
	assert_eq!(with(&torn[..B3 + 40]), pending(NotTruncated));
	assert_eq!(with(&prefix), pending(Truncated));
	// P2 and P3: the tool's own partial Repair, possibly followed by zero bytes.
	let partial = [prefix.as_slice(), &expected[..200]].concat();
	assert_eq!(with(&partial), pending(PartialRepair));
	let zeroed = [prefix.as_slice(), &expected[..50], &[0u8; 150]].concat();
	assert_eq!(with(&zeroed), pending(PartialRepair));
	assert_eq!(
		with(&[prefix.as_slice(), &[0u8; 30]].concat()),
		pending(PartialRepair)
	);
	// A completed repair leaves the old evidence as audit data only.
	let repaired = [prefix.as_slice(), &expected].concat();
	assert_eq!(with(&repaired), Classification2::Clean { last_seq: 4 });
	let later = [repaired.as_slice(), &[0xcd; 50]].concat();
	assert!(matches!(
		with(&later),
		Classification2::Unclaimed { boundary, .. } if boundary == (B3 + 384) as u64
	));
	// Bytes that match neither the tail nor the Repair are a new, unclaimed tail.
	assert!(matches!(
		with(&[prefix.as_slice(), &[0xee; 20]].concat()),
		Classification2::Unclaimed { .. }
	));
	// Evidence from another prefix at the same boundary never matches.
	let other = &owner_vectors()["cases"]["same_boundary_other_prefix"];
	let other_name = other["name"].as_str().unwrap();
	let other_ev = unhex(other["evidence"].as_str().unwrap());
	assert!(matches!(
		classify2(&torn, &[(other_name, other_ev.as_slice())]),
		Classification2::Unclaimed { .. }
	));
	// Overlapping patterns: a zero-led tail matches P1 and P3; P1 wins.
	let zero_led = [prefix.as_slice(), &[0u8; 20], &tail].concat();
	let (zn, zev) = encode_evidence2(&zero_led).unwrap();
	let zc = {
		let z = decode_evidence2(&zn, &zev).unwrap();
		repair_context2(&z.prefix_sha256, z.boundary, z.original_len, &z.tail_sha256)
	};
	assert_eq!(
		classify2(
			&[prefix.as_slice(), &[0u8; 10]].concat(),
			&[(zn.as_str(), zev.as_slice())]
		),
		Classification2::Pending {
			context: zc,
			stage: NotTruncated
		}
	);
	// Two distinct contexts that both match: fail closed. The same file twice is one context.
	let shared = [prefix.as_slice(), &tail[..30], &[0x01; 40]].concat();
	let (sn, sev) = encode_evidence2(&shared).unwrap();
	let both = [
		(name.as_str(), ev.as_slice()),
		(sn.as_str(), sev.as_slice()),
	];
	assert_eq!(
		classify2(&torn[..B3 + 30], &both),
		Classification2::Conflicting { count: 2 }
	);
	let twice = [
		(name.as_str(), ev.as_slice()),
		(name.as_str(), ev.as_slice()),
	];
	assert_eq!(classify2(&torn, &twice), pending(NotTruncated));
	// Temporary files with a valid name are ignored; invalid evidence is never ignored.
	let tmp = format!("{name}.tmp");
	assert!(matches!(
		classify2(&torn, &[(tmp.as_str(), b"partial".as_slice())]),
		Classification2::Unclaimed { .. }
	));
	let garbage = vec![0x5a; 200];
	assert_eq!(
		classify2(&golden, &[(name.as_str(), garbage.as_slice())]),
		Classification2::EvidenceInvalid {
			index: 0,
			error: EvidenceError2::Magic
		}
	);
}
#[test]
fn classification_input_bounds_come_first() {
	let golden = log_bytes();
	let mut corrupt = golden.clone();
	corrupt[383] ^= 1;
	corrupt.extend([1; 10]);
	let name = format!("{}.rep", "a".repeat(64));
	let small = vec![0u8; 200];
	let big = vec![0u8; MAX_EVIDENCE_BYTES2 + 1];
	let many: Vec<(&str, &[u8])> = (0..=MAX_REPAIR_ENTRIES2)
		.map(|_| ("bad", small.as_slice()))
		.collect();
	assert_eq!(classify2(&corrupt, &many), Classification2::RepairDirFull);
	let upper = format!("{}.rep", "A".repeat(64));
	let bare_tmp = format!("{}.tmp", "a".repeat(64));
	for foreign in ["notes.txt", upper.as_str(), bare_tmp.as_str()] {
		assert_eq!(
			classify2(&golden, &[(foreign, small.as_slice())]),
			Classification2::RepairDirForeign { index: 0 },
			"{foreign}"
		);
	}
	// An oversized evidence file is refused before the log is even folded...
	assert_eq!(
		classify2(&corrupt, &[(name.as_str(), big.as_slice())]),
		Classification2::EvidenceInvalid {
			index: 0,
			error: EvidenceError2::Length
		}
	);
	// ...but a corrupt log outranks decoding evidence and any tail recovery.
	assert!(matches!(
		classify2(&corrupt, &[(name.as_str(), small.as_slice())]),
		Classification2::Log(LogError2::Corrupt { .. })
	));
	assert_eq!(
		classify2(&vec![0; MAX_LOG_BYTES2 + 1], &[]),
		Classification2::Log(LogError2::TooLong)
	);
	assert_eq!(classify2(&[], &[]), Classification2::Clean { last_seq: 0 });
}
