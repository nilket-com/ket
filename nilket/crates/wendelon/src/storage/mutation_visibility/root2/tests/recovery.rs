//! G1e-1: the pure replay monitor and recovery mark. Fixture histories start at the committed
//! genesis and are reduced through the existing transitions with cfg(test) trusted fixtures; every
//! modeled step is checked. A second, separately executed replay is a determinism comparison, not
//! an independent oracle. `Ready` is progress-only; binding (install_admin2) precedes any
//! persistence or serving, which stay held.
use super::*;
use crate::storage::mutation_visibility::root2::admin::manifest::AdminManifest2;
use crate::storage::mutation_visibility::root2::admin::manifest::AnyAdminManifest2;
use crate::storage::mutation_visibility::root2::admin::record::BootstrapRecord2;
use crate::storage::mutation_visibility::root2::admin_install::install_admin2;
use crate::storage::mutation_visibility::root2::admission_registry::encode_permission_key2;
use crate::storage::mutation_visibility::root2::entry_identity::EntryPosition2;
use crate::storage::mutation_visibility::root2::incremental::Derived2;
use crate::storage::mutation_visibility::root2::origin::*;
use crate::storage::mutation_visibility::root2::provisioning::inventory::{
	LogState2, RootEntry2, RootIdentity2, RootState2,
};
use crate::storage::mutation_visibility::root2::provisioning::replica_anchor::*;
use crate::storage::mutation_visibility::root2::provisioning::snapshot::encode_snapshot2;
use crate::storage::mutation_visibility::root2::recovery::*;
use crate::storage::mutation_visibility::root2::trust::CommittedEntry2;

fn unhex(s: &str) -> Vec<u8> {
	(0..s.len())
		.step_by(2)
		.map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
		.collect()
}
fn json(text: &str) -> serde_json::Value {
	serde_json::from_str(text).unwrap()
}
fn sv() -> serde_json::Value {
	json(include_str!("statement_vectors.json"))
}
fn rv() -> serde_json::Value {
	json(include_str!("reserve_vectors.json"))
}
fn d32(field: &str) -> Digest2 {
	unhex(sv()[field].as_str().unwrap()).try_into().unwrap()
}
fn manifest() -> AdminManifest2 {
	AdminManifest2::decode(&unhex(sv()["manifest_prime"].as_str().unwrap())).unwrap()
}
fn gc() -> ([u8; 16], [u8; 16]) {
	let m = manifest();
	(m.generation, m.cluster)
}
fn deployment() -> Deployment2 {
	let (generation, cluster) = gc();
	Deployment2 {
		generation,
		cluster,
	}
}
fn generated(name: &str) -> Command2 {
	let b = unhex(rv()["commands"][name].as_str().unwrap());
	let (g, c, cmd) = Command2::decode(&b).unwrap();
	assert_eq!((g, c), gc());
	assert_eq!(cmd.encode(g, c).unwrap(), b);
	cmd
}
fn statement(name: &str) -> Command2 {
	Command2::AdmissionPermission(
		unhex(sv()[name]["bytes"].as_str().unwrap())
			.try_into()
			.unwrap(),
	)
}
fn tuple() -> PinnedBoundTuple2 {
	let bytes = unhex(sv()["manifest_prime"].as_str().unwrap());
	let m = manifest();
	let record = BootstrapRecord2::decode(&unhex(sv()["record"].as_str().unwrap())).unwrap();
	let pin = crate::storage::mutation_visibility::root2::admin::manifest::admin_manifest_digest2(
		bytes.as_slice().try_into().unwrap(),
	);
	let snapshot = encode_snapshot2(&LogState2 {
		roots: vec![RootEntry2 {
			identity: RootIdentity2 {
				generation: m.generation,
				cluster: m.cluster,
				pin,
				admin_key: m.admin_key,
			},
			manifest: AnyAdminManifest2::One(m),
			state: RootState2::Bound,
			last_seq: record.inventory_seq,
			record: Some(record),
		}],
		last_seq: record.inventory_seq,
	})
	.unwrap();
	let inventory_pin = crate::storage::mutation_visibility::root2::codec::hash(b"", &snapshot);
	validate_replica_tuple2(&ReplicaInputs2 {
		manifest: Some(&bytes),
		manifest_pin: Some(&pin),
		snapshot: Some(&snapshot),
		inventory_pin: Some(&inventory_pin),
	})
	.unwrap()
}
fn pos(term: u64, node: u32, index: u64) -> EntryPosition2 {
	EntryPosition2 { term, node, index }
}

type Permissions = Vec<([u8; 73], u64, Option<u64>)>;
type Parents = Vec<([u8; 73], u64)>;
// One committed entry of a fixture history, with its explicit expected effect.
enum Entry {
	Genesis(Digest2),
	Scanned,
	Bootstrap {
		control: Digest2,
		length: u64,
		changed: bool,
	},
	Plain(Command2, u8, ApplyResult2, Permissions, Parents),
	Admit(Command2, ApplyResult2, Permissions, Parents),
}
fn bytes_of(s: &MutationAuthorityState2) -> Vec<u8> {
	encode_authority2(s).unwrap()
}
// The checks every modeled step receives, appropriate to the state it produces.
fn checked(
	prev: &MutationAuthorityState2,
	prev_bytes: &[u8],
	next: &MutationAuthorityState2,
	i: u64,
	expected: Option<(&ApplyResult2, &ApplyResult2, &Permissions, &Parents)>,
) {
	assert_eq!(bytes_of(prev), prev_bytes, "prior state untouched at {i}");
	let ab = bytes_of(next);
	assert_eq!(
		UnverifiedAuthority2::decode(&ab).unwrap().encode().unwrap(),
		ab
	);
	let Some((root, _)) = root_and_origin2(next) else {
		assert_eq!(coverage2(next).unwrap().index, i);
		return;
	};
	let s = &root.state;
	assert_eq!(s.covered.index, i);
	assert!(
		s.derived == Derived2::build(s).unwrap(),
		"incremental == full at {i}"
	);
	validate_state(s).unwrap();
	let rb = root.encode().unwrap();
	assert_eq!(UnverifiedRoot2::decode(&rb).unwrap().encode().unwrap(), rb);
	if let Some((got, want, permissions, parents)) = expected {
		assert!(got == want, "result at {i}");
		let (g, c) = (s.generation, s.cluster);
		let b = got.encode(g, c, l(i)).unwrap();
		let (dg, dc, dl, dr) = ApplyResult2::decode(&b).unwrap();
		assert!((dg, dc, dl) == (g, c, l(i)) && dr == *got);
		let mut p = s
			.permissions
			.values()
			.map(|p| {
				(
					encode_permission_key2(&p.key).unwrap(),
					p.enrolled.index,
					p.revoked.map(|x| x.index),
				)
			})
			.collect::<Vec<_>>();
		p.sort();
		let mut wp = permissions.clone();
		wp.sort();
		assert_eq!(p, wp, "permissions at {i}");
		let q = s
			.parents
			.iter()
			.map(|(k, p)| (*k, p.reservation.index))
			.collect::<Vec<_>>();
		let mut wq = parents.clone();
		wq.sort();
		assert_eq!(q, wq, "parents at {i}");
	}
}
// Reduce a fixture history from genesis; states[i] is the state after the entry at index i.
fn replay(entries: &[Entry]) -> Vec<MutationAuthorityState2> {
	let (g, c) = gc();
	let empty = empty_authority2();
	let mut out: Vec<MutationAuthorityState2> = Vec::new();
	for (i, e) in entries.iter().enumerate() {
		let i = i as u64;
		let s = out.last().unwrap_or(&empty);
		let before = bytes_of(s);
		let next = match e {
			Entry::Genesis(genesis) => {
				let ce = CommittedEntry2::fixture(pos(0, 0, 0), *genesis, EntryClass2::Membership);
				let (n, step) = advance_pre_root2(s, &ce).unwrap();
				assert_eq!(step, PreRootStep2::Genesis);
				checked(s, &before, &n, i, None);
				n
			}
			Entry::Scanned => {
				let ce =
					CommittedEntry2::fixture(pos(1, 1, i), [i as u8; 32], EntryClass2::NonRoot2);
				let (n, step) = advance_pre_root2(s, &ce).unwrap();
				assert_eq!(step, PreRootStep2::Scanned);
				checked(s, &before, &n, i, None);
				n
			}
			Entry::Bootstrap {
				control,
				length,
				changed,
			} => {
				let cmd = Command2::Bootstrap {
					epoch: manifest().initial_epoch,
					control: *control,
					length: *length,
				};
				let mut te = TrustedEntry2::fixture(g, c, l(i), &cmd).unwrap();
				if *changed {
					te.change_fingerprint();
				}
				let (_, _, _, fp, d) = te.fields();
				let v = VerifiedCommand2::fixture(g, c, &cmd, &[], 0).unwrap();
				let ce = CommittedEntry2::fixture(pos(1, 1, i), fp, EntryClass2::Bootstrap(d));
				let (n, r) = bootstrap2(s, &ce, &te, &v, &cmd).unwrap();
				let none = Vec::new();
				let parents = Vec::new();
				checked(
					s,
					&before,
					&n,
					i,
					Some((&r, &ApplyResult2::Registered(l(i)), &none, &parents)),
				);
				n
			}
			Entry::Plain(cmd, evidence, want, permissions, parents) => {
				let te = TrustedEntry2::fixture(g, c, l(i), cmd).unwrap();
				let v = VerifiedCommand2::fixture(g, c, cmd, &[], *evidence).unwrap();
				let (n, r) = advance_root2(s, &te, &v, cmd).unwrap();
				checked(s, &before, &n, i, Some((&r, want, permissions, parents)));
				n
			}
			Entry::Admit(cmd, want, permissions, parents) => {
				let te = TrustedEntry2::fixture(g, c, l(i), cmd).unwrap();
				let v = VerifiedCommand2::fixture(g, c, cmd, &[], 0).unwrap();
				let (n, r) = advance_admission2(s, &tuple(), &te, &v, cmd).unwrap();
				checked(s, &before, &n, i, Some((&r, want, permissions, parents)));
				n
			}
		};
		out.push(next);
	}
	out
}
fn k1() -> [u8; 73] {
	let b = unhex(sv()["grant"]["bytes"].as_str().unwrap());
	b[200..273].try_into().unwrap()
}
fn claim_of(c: &Command2) -> ClaimProjection2 {
	let Command2::Reserve(x) = c else {
		panic!("reserve")
	};
	x.clone()
}
// The governed history: genesis, a scanned entry, the governed Bootstrap at 2, selector,
// authority, lineage Grant, admission grant, Reserve, admission revoke, Finalize, RevokeAuthority
// and a Cover at 11.
fn governed() -> Vec<Entry> {
	let reserve = generated("reserve");
	let c = claim_of(&reserve);
	let id = c.manifest.identity.encode().unwrap();
	let Command2::Grant {
		lineage,
		incarnation,
		lease,
		predecessor,
	} = generated("grant")
	else {
		panic!("grant")
	};
	let granted = ApplyResult2::Granted(LineageRecord2 {
		latest: OriginalGrant2 {
			lineage,
			incarnation,
			lease,
			log: l(5),
			predecessor,
		},
		closure: None,
	});
	let held = c.held_bytes().unwrap();
	let mut a = attestation();
	a.grant = c.original.reference().unwrap();
	a.reservation = l(7);
	a.claim_digest = c.digest().unwrap();
	a.authority = c.authority;
	let finalize = Command2::Finalize {
		identity: c.manifest.identity,
		digest: c.digest().unwrap(),
		reservation: l(7),
		attestation: a.clone(),
	};
	let parent = |terminal: Option<Terminal2>, new_effect: bool| ApplyResult2::Parent {
		record: ParentRecord2 {
			claim: c.clone(),
			reservation: l(7),
			held_bytes: held,
			terminal,
		},
		new_effect,
	};
	let permission = |outcome: u8, revoked: Option<u64>| ApplyResult2::Permission {
		outcome,
		key: k1(),
		enrolled: l(6),
		revoked: revoked.map(l),
	};
	let active = vec![(k1(), 6, None)];
	let revoked = vec![(k1(), 6, Some(8))];
	let parents = vec![(id, 7)];
	vec![
		Entry::Genesis(d32("genesis")),
		Entry::Scanned,
		Entry::Bootstrap {
			control: d32("manifest_prime_pin"),
			length: 137,
			changed: false,
		},
		Entry::Plain(
			generated("selector"),
			0,
			ApplyResult2::Registered(l(3)),
			vec![],
			vec![],
		),
		Entry::Plain(
			generated("authority"),
			0,
			ApplyResult2::Registered(l(4)),
			vec![],
			vec![],
		),
		Entry::Plain(generated("grant"), 0, granted, vec![], vec![]),
		Entry::Admit(
			statement("grant"),
			permission(1, None),
			active.clone(),
			vec![],
		),
		Entry::Plain(reserve, 0, parent(None, false), active, parents.clone()),
		Entry::Admit(
			statement("revoke"),
			permission(3, Some(8)),
			revoked.clone(),
			parents.clone(),
		),
		Entry::Plain(
			finalize,
			1,
			parent(
				Some(Terminal2::Accepted {
					first: l(9),
					evidence: hash(b"VISROOT2-EVIDENCE\0", &a.encode().unwrap()),
				}),
				true,
			),
			revoked.clone(),
			parents.clone(),
		),
		Entry::Plain(
			Command2::RevokeAuthority(c.authority),
			0,
			ApplyResult2::Registered(l(10)),
			revoked.clone(),
			parents.clone(),
		),
		Entry::Plain(
			Command2::Cover {
				kind: 0,
				external: [11; 32],
			},
			0,
			ApplyResult2::Covered,
			revoked,
			parents,
		),
	]
}
// A short history ending at the Bootstrap plus `covers` Cover entries.
fn short(
	genesis: Digest2,
	control: Digest2,
	length: u64,
	changed: bool,
	covers: u64,
) -> Vec<Entry> {
	let mut h = vec![
		Entry::Genesis(genesis),
		Entry::Scanned,
		Entry::Bootstrap {
			control,
			length,
			changed,
		},
	];
	for i in 3..3 + covers {
		h.push(Entry::Plain(
			Command2::Cover {
				kind: 0,
				external: [i as u8; 32],
			},
			0,
			ApplyResult2::Covered,
			vec![],
			vec![],
		));
	}
	h
}
fn tip(s: &MutationAuthorityState2) -> ReplayTarget2 {
	let (root, _) = root_and_origin2(s).unwrap();
	ReplayTarget2 {
		cut: root.state.covered,
		fingerprint: root.state.fingerprint,
		command: crate::storage::mutation_visibility::root2::recovery::TargetCommand2::Expected(
			root.state.command_digest,
		),
	}
}
fn mark(s: &MutationAuthorityState2) -> RecoveryMark2 {
	mark_of2(s).unwrap()
}
// Observe states in order; returns every progress result (or the first error and its index).
fn run<'a>(
	m: &mut ReplayMonitor2,
	states: impl IntoIterator<Item = &'a MutationAuthorityState2>,
) -> Result<Vec<Progress2>, (usize, RecoveryError2)> {
	let mut out = Vec::new();
	for (i, s) in states.into_iter().enumerate() {
		out.push(observe2(m, s).map_err(|e| (i, e))?);
	}
	Ok(out)
}

#[test]
fn recovery_mark_codec_is_canonical() {
	let v = json(include_str!("recovery_vectors.json"));
	let provisioned = unhex(v["provisioned"].as_str().unwrap());
	assert!(decode_recovery_mark2(&provisioned).unwrap() == Baseline2::Provisioned);
	assert_eq!(
		encode_recovery_mark2(&Baseline2::Provisioned)
			.unwrap()
			.to_vec(),
		provisioned
	);
	let f = &v["sample_fields"];
	let sample = RecoveryMark2 {
		generation: unhex(f["generation"].as_str().unwrap()).try_into().unwrap(),
		cluster: unhex(f["cluster"].as_str().unwrap()).try_into().unwrap(),
		cut: l(7),
		fingerprint: unhex(f["fingerprint"].as_str().unwrap())
			.try_into()
			.unwrap(),
		command: unhex(f["command"].as_str().unwrap()).try_into().unwrap(),
		state: [0; 32],
	};
	let bytes = unhex(v["sample"].as_str().unwrap());
	assert!(
		decode_recovery_mark2(&bytes).unwrap() == Baseline2::Mark(sample),
		"zero digest accepted"
	);
	assert_eq!(
		encode_recovery_mark2(&Baseline2::Mark(sample))
			.unwrap()
			.to_vec(),
		bytes
	);
	for (name, value) in v["refusals"].as_object().unwrap() {
		let err = decode_recovery_mark2(&unhex(value.as_str().unwrap())).unwrap_err();
		assert_eq!(
			format!("{err:?}"),
			v["expected"][name].as_str().unwrap(),
			"{name}"
		);
	}
	// Invalid in-memory values: encode is fallible and shares the validation.
	for (bad, want) in [
		(
			RecoveryMark2 {
				generation: [0; 16],
				..sample
			},
			FieldError2::Generation,
		),
		(
			RecoveryMark2 {
				cluster: [0; 16],
				..sample
			},
			FieldError2::Cluster,
		),
		(
			RecoveryMark2 {
				cut: Log2 {
					term: 1,
					node: 0,
					index: 7,
				},
				..sample
			},
			FieldError2::Cut,
		),
	] {
		assert_eq!(
			encode_recovery_mark2(&Baseline2::Mark(bad)).err(),
			Some(RecoveryError2::Field(want))
		);
	}
}

#[test]
fn genesis_replay_reaches_the_mark_and_target() {
	let states = replay(&governed());
	assert_eq!(states.len(), 12);
	// Determinism comparison (not an independent oracle): a second, separate replay.
	let again = replay(&governed());
	for (a, b) in states.iter().zip(&again) {
		assert_eq!(bytes_of(a), bytes_of(b));
	}
	let mut m = replay_monitor2(
		deployment(),
		Some(Baseline2::Mark(mark(&states[7]))),
		tip(&states[11]),
	)
	.unwrap();
	assert!(
		observe2(&mut m, &empty_authority2()).unwrap() == Progress2::Replaying,
		"Empty before genesis"
	);
	let progress = run(&mut m, &states).unwrap();
	for (i, p) in progress.iter().enumerate() {
		let want = match i {
			7 => Progress2::MarkMatched,
			11 => Progress2::Ready(mark(&states[11])),
			_ => Progress2::Replaying,
		};
		assert!(*p == want, "progress at {i}");
	}
	assert_eq!(
		encodings_for_test(&m),
		2,
		"full encodings only at the mark and the target"
	);
}

#[test]
fn replay_refusals_latch_and_leave_marks_untouched() {
	use RecoveryError2 as E;
	let states = replay(&governed());
	let m7 = mark(&states[7]);
	let t11 = tip(&states[11]);
	let dep = deployment();
	let fresh = |b: Baseline2, t: ReplayTarget2| replay_monitor2(dep, Some(b), t).unwrap();
	// Construction.
	assert_eq!(replay_monitor2(dep, None, t11).err(), Some(E::Unavailable));
	assert_eq!(
		replay_monitor2(
			Deployment2 {
				generation: [0; 16],
				..dep
			},
			Some(Baseline2::Provisioned),
			t11
		)
		.err(),
		Some(E::Field(FieldError2::Deployment))
	);
	assert_eq!(
		replay_monitor2(
			dep,
			Some(Baseline2::Provisioned),
			ReplayTarget2 {
				cut: Log2 {
					term: 0,
					node: 1,
					index: 11
				},
				..t11
			}
		)
		.err(),
		Some(E::Field(FieldError2::Target))
	);
	assert_eq!(
		replay_monitor2(
			dep,
			Some(Baseline2::Mark(RecoveryMark2 {
				cluster: [0; 16],
				..m7
			})),
			t11
		)
		.err(),
		Some(E::Field(FieldError2::Cluster))
	);
	assert_eq!(
		replay_monitor2(dep, Some(Baseline2::Mark(m7)), tip(&states[5])).err(),
		Some(E::TargetBelowMark)
	);
	assert_eq!(
		replay_monitor2(
			dep,
			Some(Baseline2::Mark(m7)),
			ReplayTarget2 {
				fingerprint: [0x5A; 32],
				..tip(&states[7])
			}
		)
		.err(),
		Some(E::Conflict),
		"same index, different tip"
	);
	assert_eq!(
		replay_monitor2(
			Deployment2 {
				generation: [0x55; 16],
				..dep
			},
			Some(Baseline2::Mark(m7)),
			t11
		)
		.err(),
		Some(E::Conflict)
	);
	// The first observation must be the genesis pre-root at (0,0,0).
	let mut m = fresh(Baseline2::Provisioned, t11);
	assert_eq!(
		observe2(&mut m, &states[11]).err(),
		Some(E::NotGenesis),
		"first rooted (Provisioned bypass)"
	);
	assert_eq!(observe2(&mut m, &states[0]).err(), Some(E::Latched));
	let mut m = fresh(Baseline2::Provisioned, t11);
	assert_eq!(
		observe2(&mut m, &states[1]).err(),
		Some(E::NotGenesis),
		"first non-genesis pre-root"
	);
	// Contiguity: repeat, skip, regression, pre-root after root, Empty after progress.
	for (seq, at) in [(vec![0, 0], 1), (vec![0, 2], 1), (vec![0, 1, 2, 1], 3)] {
		let mut m = fresh(Baseline2::Provisioned, t11);
		let picked = seq.iter().map(|i| &states[*i]);
		assert_eq!(
			run(&mut m, picked).err(),
			Some((at, E::Regressed)),
			"{seq:?}"
		);
	}
	let pre = replay(&[
		Entry::Genesis(d32("genesis")),
		Entry::Scanned,
		Entry::Scanned,
		Entry::Scanned,
	]);
	let mut m = fresh(Baseline2::Provisioned, t11);
	let mixed = [&states[0], &states[1], &states[2], &pre[3]];
	assert_eq!(
		run(&mut m, mixed).err(),
		Some((3, E::Regressed)),
		"pre-root after root"
	);
	let mut m = fresh(Baseline2::Provisioned, t11);
	run(&mut m, &states[..2]).unwrap();
	assert_eq!(
		observe2(&mut m, &empty_authority2()).err(),
		Some(E::Regressed),
		"Empty after progress"
	);
	// Synthetic setup: contiguity overflow at u64::MAX.
	let mut m = fresh(Baseline2::Provisioned, t11);
	place_last_for_test(&mut m, u64::MAX, true);
	assert_eq!(observe2(&mut m, &states[3]).err(), Some(E::Regressed));
	// Overshoot, pre-root and rooted.
	let low = ReplayTarget2 {
		cut: l(1),
		fingerprint: [1; 32],
		command: crate::storage::mutation_visibility::root2::recovery::TargetCommand2::Expected(
			[1; 32],
		),
	};
	let mut m = fresh(Baseline2::Provisioned, low);
	assert_eq!(
		run(&mut m, &pre).err(),
		Some((2, E::Overshoot)),
		"an unbootstrapped history cannot pass the target"
	);
	let mut m = fresh(Baseline2::Provisioned, low);
	assert_eq!(
		run(&mut m, &states).err(),
		Some((2, E::Overshoot)),
		"the first rooted state passes a pre-root target"
	);
	// Target mismatches: coordinate, fingerprint, command.
	for bad in [
		ReplayTarget2 {
			cut: Log2 { term: 2, ..t11.cut },
			..t11
		},
		ReplayTarget2 {
			fingerprint: [0x5B; 32],
			..t11
		},
		ReplayTarget2 {
			command: crate::storage::mutation_visibility::root2::recovery::TargetCommand2::Expected(
				[0x5C; 32],
			),
			..t11
		},
	] {
		let mut m = fresh(Baseline2::Provisioned, bad);
		assert_eq!(run(&mut m, &states).err(), Some((11, E::Conflict)));
	}
	// Mark mismatches: a different state, command or fingerprint at the same cut.
	for bad in [
		RecoveryMark2 {
			state: [0x5D; 32],
			..m7
		},
		RecoveryMark2 {
			command: [0x5E; 32],
			..m7
		},
		RecoveryMark2 {
			fingerprint: [0x5F; 32],
			..m7
		},
	] {
		let mut m = fresh(Baseline2::Mark(bad), t11);
		assert_eq!(run(&mut m, &states).err(), Some((7, E::Conflict)));
	}
	// A fork reaching the mark: no admission grant at 6, so the Reserve at 7 is denied and the
	// state differs while the entry at 7 is the same command.
	let mut forked = governed();
	forked[6] = Entry::Plain(
		Command2::Cover {
			kind: 0,
			external: [6; 32],
		},
		0,
		ApplyResult2::Covered,
		vec![],
		vec![],
	);
	forked[7] = Entry::Plain(
		generated("reserve"),
		0,
		ApplyResult2::Denied {
			reason: 18,
			reference: None,
		},
		vec![],
		vec![],
	);
	let fork = replay(&forked[..8]);
	let mut m = fresh(Baseline2::Mark(m7), tip(&fork[7]));
	assert_eq!(run(&mut m, &fork).err(), Some((7, E::Conflict)));
	// Another generation reaches the root.
	let mut m = replay_monitor2(
		Deployment2 {
			generation: [0x55; 16],
			..dep
		},
		Some(Baseline2::Provisioned),
		t11,
	)
	.unwrap();
	assert_eq!(run(&mut m, &states).err(), Some((2, E::Conflict)));
	assert_eq!(observe2(&mut m, &states[3]).err(), Some(E::Latched));
	// A finished monitor is latched too; the baseline used is unchanged.
	let b = Baseline2::Mark(m7);
	let mut m = fresh(b, t11);
	run(&mut m, &states).unwrap();
	assert_eq!(observe2(&mut m, &states[11]).err(), Some(E::Latched));
	assert!(b == Baseline2::Mark(m7));
}

#[test]
fn provisioned_baseline_target_equals_mark_and_no_regression() {
	let states = replay(&governed());
	let dep = deployment();
	// Provisioned: Ready at the target, with one full encoding.
	let mut m = replay_monitor2(dep, Some(Baseline2::Provisioned), tip(&states[10])).unwrap();
	let p = run(&mut m, &states[..11]).unwrap();
	assert!(p[10] == Progress2::Ready(mark(&states[10])));
	assert_eq!(encodings_for_test(&m), 1);
	// Restart with that mark and a higher target: MarkMatched, then Ready, never regressing.
	let m10 = mark(&states[10]);
	let mut m = replay_monitor2(dep, Some(Baseline2::Mark(m10)), tip(&states[11])).unwrap();
	let p = run(&mut m, &states).unwrap();
	assert!(p[10] == Progress2::MarkMatched);
	let Progress2::Ready(new) = p[11] else {
		panic!("ready")
	};
	assert!(new.cut.index > m10.cut.index && new == mark(&states[11]));
	// Target == mark: Ready directly at that observation.
	let mut m = replay_monitor2(dep, Some(Baseline2::Mark(m10)), tip(&states[10])).unwrap();
	let p = run(&mut m, &states[..11]).unwrap();
	assert!(p[10] == Progress2::Ready(m10));
	assert!(!p.contains(&Progress2::MarkMatched));
	// An exact repeat replay gives an identical new mark.
	let again = replay(&governed());
	let mut m = replay_monitor2(dep, Some(Baseline2::Mark(m10)), tip(&again[11])).unwrap();
	assert!(run(&mut m, &again).unwrap()[11] == Progress2::Ready(new));
}

#[test]
fn binding_before_persistence() {
	let dep = deployment();
	let ready = |states: &[MutationAuthorityState2]| {
		let last = states.last().unwrap();
		let mut m = replay_monitor2(dep, Some(Baseline2::Provisioned), tip(last)).unwrap();
		assert!(matches!(
			run(&mut m, states).unwrap().last(),
			Some(Progress2::Ready(_))
		));
	};
	// Governed: Ready, then binding succeeds, so a mark could be persisted.
	let governed = replay(&governed());
	ready(&governed);
	assert!(install_admin2(governed.last().unwrap(), &tuple()).is_ok());
	// A foreign genesis or a foreign original Bootstrap reaches Ready (progress only) but fails
	// binding: no mark would be persisted and nothing served.
	let pin = d32("manifest_prime_pin");
	for history in [
		short([0x77; 32], pin, 137, false, 2),
		short(d32("genesis"), pin, 137, true, 2),
	] {
		let states = replay(&history);
		ready(&states);
		assert!(install_admin2(states.last().unwrap(), &tuple()).is_err());
	}
	// Legacy (ungoverned) Ready never implies governed eligibility.
	let legacy = replay(&short(d32("genesis"), [9; 32], 0, false, 2));
	ready(&legacy);
	let legacy = legacy.last().unwrap();
	assert_ne!(root_and_origin2(legacy).unwrap().0.state.control_len, 137);
	assert!(install_admin2(legacy, &tuple()).is_err());
}
