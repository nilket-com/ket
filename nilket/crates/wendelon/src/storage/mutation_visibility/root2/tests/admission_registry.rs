//! Semantic tests of the dormant permission registry. Calling a transition directly is a test of
//! lifecycle semantics over consistent inputs, never an authorization: no issuer decision exists.
use super::*;
use crate::storage::mutation_visibility::MutationScope;
use crate::storage::mutation_visibility::root2::admin_statement::*;
use crate::storage::mutation_visibility::root2::admission_registry::*;
use crate::storage::mutation_visibility::root2::persistent::Map;
use crate::storage::mutation_visibility::root2::validation::authority_key;
use std::collections::BTreeMap;

const G: [u8; 16] = [0xA1; 16];
type E = RegistryError2;
type O = PermissionOutcome2;

fn unhex(s: &str) -> Vec<u8> {
	(0..s.len())
		.step_by(2)
		.map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
		.collect()
}
fn vectors(field: &str) -> Vec<u8> {
	let v: serde_json::Value =
		serde_json::from_str(include_str!("admission_registry_vectors.json")).unwrap();
	unhex(v[field].as_str().unwrap())
}
fn grant_statement() -> Vec<u8> {
	let v: serde_json::Value =
		serde_json::from_str(include_str!("statement_vectors.json")).unwrap();
	unhex(v["grant"]["bytes"].as_str().unwrap())
}
fn auth(id: u8, revision: u64) -> AuthorityRef2 {
	AuthorityRef2 {
		id: [id; 16],
		revision,
	}
}
fn reg(a: AuthorityRef2, enrolled: u64, revoked: Option<u64>) -> AuthorityRegistration2 {
	AuthorityRegistration2 {
		authority: a,
		node: NodeRef2 {
			node: 1,
			incarnation: [1; 16],
		},
		public_key: [2; 32],
		enrolled: l(enrolled),
		revoked: revoked.map(l),
	}
}
fn key(a: AuthorityRef2, tenant: u64) -> PermissionKey2 {
	PermissionKey2 {
		authority: a,
		namespace: AdmissionNamespace2 {
			generation: G,
			tenant,
			scope: MutationScope::Raw,
			table: [0; 16],
			schema: 0,
		},
	}
}
fn table_key(a: AuthorityRef2, tenant: u64) -> PermissionKey2 {
	let mut k = key(a, tenant);
	k.namespace.scope = MutationScope::Table;
	k.namespace.table = [0x7A; 16];
	k.namespace.schema = 2;
	k
}
fn rec(k: PermissionKey2, enrolled: u64, revoked: Option<u64>) -> PermissionRecord2 {
	PermissionRecord2 {
		key: k,
		enrolled: l(enrolled),
		revoked: revoked.map(l),
	}
}
fn kb(k: &PermissionKey2) -> [u8; 73] {
	encode_permission_key2(k).unwrap()
}
fn map_of(rs: &[PermissionRecord2]) -> PermissionMap2 {
	let mut m = PermissionMap2::new();
	for r in rs {
		m.insert(kb(&r.key), *r);
	}
	m
}
fn lim() -> PermissionLimits2 {
	permission_limits2()
}
fn bytes_used(m: &PermissionMap2) -> usize {
	117 * m.len()
}
fn grant(
	m: &PermissionMap2,
	a: Option<&AuthorityRegistration2>,
	k: &PermissionKey2,
	at: Log2,
) -> Result<(PermissionMap2, O, PermissionDelta2), E> {
	let before = m.clone();
	let r = grant_permission2(m, a, k, at, bytes_used(m), lim());
	assert!(
		*m == before && m.shares_root(&before),
		"prior map preserved"
	);
	r
}
fn revoke(
	m: &PermissionMap2,
	k: &PermissionKey2,
	at: Log2,
) -> Result<(PermissionMap2, O, PermissionDelta2), E> {
	let before = m.clone();
	let r = revoke_permission2(m, k, at);
	assert!(
		*m == before && m.shares_root(&before),
		"prior map preserved"
	);
	r
}
fn outcome(r: Result<(PermissionMap2, O, PermissionDelta2), E>) -> O {
	r.unwrap().1
}
const ZERO: PermissionDelta2 = PermissionDelta2 {
	records: 0,
	registry: 0,
	coordinate: None,
};
fn label(o: &O) -> &'static str {
	match o {
		O::Granted(_) => "granted",
		O::GrantDuplicate(_) => "grant-duplicate",
		O::Revoked(_) => "revoked",
		O::RevokeDuplicate(_) => "revoke-duplicate",
		O::UnknownAuthority => "unknown-authority",
		O::AuthorityRevoked(_) => "authority-revoked",
		O::Capacity => "capacity",
		O::UnknownPermission => "unknown-permission",
	}
}
fn xorshift(s: &mut u64) -> u64 {
	*s ^= *s << 13;
	*s ^= *s >> 7;
	*s ^= *s << 17;
	*s
}

#[test]
fn key_and_record_codecs_are_canonical_and_match_statement_bytes() {
	for name in ["raw_key", "table_key", "statement_key"] {
		let b = vectors(name);
		assert_eq!(
			kb(&decode_permission_key2(&b).unwrap()).to_vec(),
			b,
			"{name}"
		);
	}
	let statement = grant_statement();
	assert_eq!(statement[200..273].to_vec(), vectors("statement_key"));
	assert_eq!(
		decode_permission_key2(&vectors("raw_key")).unwrap(),
		key(auth(0x41, 1), 0)
	);
	assert_eq!(
		decode_permission_key2(&vectors("table_key")).unwrap(),
		table_key(auth(0x42, 3), 5)
	);
	for name in ["active_record", "revoked_record", "statement_record"] {
		let b = vectors(name);
		let r = decode_permission_record2(&b).unwrap();
		assert_eq!(encode_permission_record2(&r).unwrap().to_vec(), b, "{name}");
	}
	let active = decode_permission_record2(&vectors("active_record")).unwrap();
	assert_eq!(active, rec(key(auth(0x41, 1), 0), 3, None));
	let revoked = decode_permission_record2(&vectors("revoked_record")).unwrap();
	assert_eq!(
		revoked.revoked,
		Some(Log2 {
			term: 2,
			node: 3,
			index: 6
		})
	);
	// Record refusals.
	let good = vectors("revoked_record");
	assert_eq!(decode_permission_record2(&good[..112]), Err(E::Length));
	assert_eq!(
		decode_permission_record2(&[good.clone(), vec![0]].concat()),
		Err(E::Length)
	);
	assert_eq!(decode_permission_key2(&good[..72]), Err(E::Length));
	let mut b = good.clone();
	b[73..93].fill(0);
	assert_eq!(
		decode_permission_record2(&b),
		Err(E::Log),
		"absent enrollment"
	);
	let mut b = good.clone();
	b[81..85].fill(0);
	assert_eq!(
		decode_permission_record2(&b),
		Err(E::Log),
		"partially zero enrollment"
	);
	let mut b = good.clone();
	b[93..101].fill(0);
	assert_eq!(
		decode_permission_record2(&b),
		Err(E::Log),
		"partially zero revoked"
	);
	let mut b = good.clone();
	b[105..113].copy_from_slice(&3u64.to_le_bytes());
	assert_eq!(
		decode_permission_record2(&b),
		Err(E::Coordinate),
		"revoked before enrolled"
	);
	b[105..113].copy_from_slice(&4u64.to_le_bytes());
	assert_eq!(
		decode_permission_record2(&b),
		Err(E::Coordinate),
		"revoked at enrolled index"
	);
	assert_eq!(
		encode_permission_record2(&rec(key(auth(0x41, 1), 0), 3, Some(3))),
		Err(E::Coordinate)
	);
	assert_eq!(
		encode_permission_record2(&PermissionRecord2 {
			enrolled: Log2 {
				term: 1,
				node: 0,
				index: 3
			},
			..active
		}),
		Err(E::Log)
	);
	// A finite, seeded differential sample (not a proof over all 73-byte strings) against the
	// accepted statement structural parser, through its public preimage function with the vector's
	// fixed valid context. Every known structural class is enumerated, then randomized.
	let parse = |region: &[u8]| -> Result<(), E> {
		let mut s: [u8; 337] = statement.clone().try_into().unwrap();
		s[200..273].copy_from_slice(region);
		admin_statement_preimage2(&s)
			.map(|_| ())
			.map_err(|e| match e {
				StatementError2::ZeroField(z) => E::ZeroField(z),
				StatementError2::Namespace => E::Namespace,
				other => panic!("unexpected statement class {other:?}"),
			})
	};
	let raw = vectors("raw_key");
	let base = vectors("statement_key");
	let mut cases: Vec<Vec<u8>> = Vec::new();
	let set = |b: &[u8], at: usize, v: &[u8]| {
		let mut x = b.to_vec();
		x[at..at + v.len()].copy_from_slice(v);
		x
	};
	for b in [&raw, &base] {
		cases.push(b.clone());
		cases.push(set(b, 0, &[0; 16]));
		cases.push(set(b, 16, &[0; 8]));
		cases.push(set(b, 24, &[0; 16]));
		cases.push(set(b, 40, &[0; 8]));
		cases.push(set(b, 40, &[9; 8]));
		for scope in [0u8, 1, 2, 255] {
			cases.push(set(b, 48, &[scope]));
		}
		cases.push(set(b, 49, &[0; 16]));
		cases.push(set(b, 49, &[3; 16]));
		cases.push(set(b, 65, &[0; 8]));
		cases.push(set(b, 65, &[1, 0, 0, 0, 0, 0, 0, 0]));
		cases.push(set(&set(b, 0, &[0; 16]), 48, &[7]));
		cases.push(set(&set(b, 24, &[0; 16]), 49, &[0; 16]));
	}
	let mut seed = 0x9E37_79B9_7F4A_7C15u64;
	for i in 0..2000 {
		let mut x = if i % 2 == 0 {
			raw.clone()
		} else {
			base.clone()
		};
		for _ in 0..1 + xorshift(&mut seed) % 3 {
			let r = xorshift(&mut seed);
			match r % 6 {
				0 => x[(r >> 8) as usize % 73] = (r >> 16) as u8,
				1 => {
					let (at, n) = [(0usize, 16usize), (16, 8), (24, 16)][(r >> 8) as usize % 3];
					x[at..at + n].fill(0)
				}
				2 => x[48] = [0, 1, 2, 255][(r >> 8) as usize % 4],
				3 => x[49..65].fill([0u8, 0x5c][(r >> 8) as usize % 2]),
				4 => x[65..73]
					.copy_from_slice(&[0u64, 1, r >> 32][(r >> 8) as usize % 3].to_le_bytes()),
				_ => x[40..48].copy_from_slice(&[0u64, r][(r >> 8) as usize % 2].to_le_bytes()),
			}
		}
		cases.push(x);
	}
	let mut classes = BTreeMap::new();
	for c in &cases {
		let ours = decode_permission_key2(c).map(|k| {
			assert_eq!(kb(&k).as_slice(), c.as_slice(), "canonical");
		});
		assert_eq!(ours, parse(c), "differential {c:02x?}");
		*classes.entry(format!("{ours:?}")).or_insert(0) += 1;
	}
	for class in [
		"Ok(())",
		"Err(Namespace)",
		"Err(ZeroField(AuthorityId))",
		"Err(ZeroField(AuthorityRevision))",
		"Err(ZeroField(NamespaceGeneration))",
	] {
		assert!(
			classes.contains_key(class),
			"class {class} sampled: {classes:?}"
		);
	}
	// Tenant zero is an exact tenant in both scopes.
	assert!(decode_permission_key2(&raw).is_ok() && decode_permission_key2(&base).is_ok());
}

#[test]
fn section_codec_bounds_order_and_framing() {
	let section = vectors("section");
	let m = decode_permission_section2(&section, lim()).unwrap();
	assert_eq!(m.len(), 3);
	let order = |k: &[u8; 73]| k.iter().map(|b| format!("{b:02x}")).collect::<String>();
	let v: serde_json::Value =
		serde_json::from_str(include_str!("admission_registry_vectors.json")).unwrap();
	let expected = v["section_order"]
		.as_array()
		.unwrap()
		.iter()
		.map(|x| x.as_str().unwrap().to_owned())
		.collect::<Vec<_>>();
	assert_eq!(
		m.iter().map(|(k, _)| order(k)).collect::<Vec<_>>(),
		expected
	);
	assert_eq!(encode_permission_section2(&m, lim()).unwrap(), section);
	let dec = |b: &[u8]| decode_permission_section2(b, lim());
	assert_eq!(
		dec(&[section.clone(), vec![0]].concat()),
		Err(E::Length),
		"trailing"
	);
	assert_eq!(
		dec(&section[..section.len() - 1]),
		Err(E::Length),
		"truncated"
	);
	assert_eq!(dec(&[1, 0, 0]), Err(E::Length));
	assert_eq!(dec(&[0, 0, 0, 0]).unwrap().len(), 0);
	let mut b = section.clone();
	b[0] = 2;
	assert_eq!(dec(&b), Err(E::Length), "count disagrees with length");
	let mut b = section.clone();
	b[4] = 112;
	assert_eq!(dec(&b), Err(E::Framing));
	b[4] = 114;
	assert_eq!(dec(&b), Err(E::Framing));
	let mut b = section.clone();
	b[8..24].fill(0);
	assert_eq!(dec(&b), Err(E::ZeroField(ZeroField2::AuthorityId)));
	let (f1, f2) = (section[4..121].to_vec(), section[121..238].to_vec());
	let swapped = [&section[..4], &f2[..], &f1[..], &section[238..]].concat();
	assert_eq!(dec(&swapped), Err(E::Order), "descending");
	let dup = [&section[..4], &f1[..], &f1[..], &section[238..]].concat();
	assert_eq!(dec(&dup), Err(E::Order), "duplicate");
	// Lowered caller limits; precedence Length (input over section) before Count.
	let lowered = |count, section| PermissionLimits2 {
		count,
		section,
		..lim()
	};
	assert_eq!(
		decode_permission_section2(&section, lowered(2, 524288)),
		Err(E::Count)
	);
	assert_eq!(
		decode_permission_section2(&section, lowered(2, 4 + 117 * 2)),
		Err(E::Length)
	);
	assert_eq!(
		decode_permission_section2(&section, lowered(4097, 524288)),
		Err(E::Limits)
	);
	assert_eq!(
		decode_permission_section2(&section, lowered(4096, 524289)),
		Err(E::Limits)
	);
	assert_eq!(
		encode_permission_section2(&m, lowered(2, 524288)),
		Err(E::Count)
	);
	assert_eq!(
		encode_permission_section2(&m, lowered(4096, 4 + 117 * 2)),
		Err(E::Count)
	);
	let mut wrong = m.clone();
	let r = *m.values().next().unwrap();
	wrong.insert([0x55; 73], r);
	assert_eq!(encode_permission_section2(&wrong, lim()), Err(E::MapKey));
	// Disclosed: at the default limits the section byte bound cannot be the binding one.
	assert!(4 + 117 * PERMISSIONS2 < PERMISSION_SECTION_BYTES2);
}

#[test]
fn grant_outcomes_follow_the_selected_order() {
	let a1 = auth(0x41, 1);
	let r1 = reg(a1, 2, None);
	let k = key(a1, 5);
	let empty = PermissionMap2::new();
	let mut over = lim();
	over.count += 1;
	assert_eq!(
		grant_permission2(&empty, Some(&r1), &k, l(3), 0, over).err(),
		Some(E::Limits)
	);
	let mut bad = k;
	bad.authority.revision = 0;
	assert_eq!(
		grant(&empty, Some(&r1), &bad, l(3)).err(),
		Some(E::ZeroField(ZeroField2::AuthorityRevision))
	);
	assert_eq!(
		grant(
			&empty,
			Some(&r1),
			&k,
			Log2 {
				term: 0,
				node: 1,
				index: 3
			}
		)
		.err(),
		Some(E::Log)
	);
	// A registration for another id or another revision never satisfies the grant.
	assert_eq!(
		grant(&empty, Some(&reg(auth(0x42, 1), 2, None)), &k, l(3)).err(),
		Some(E::Reference)
	);
	assert_eq!(
		grant(&empty, Some(&reg(auth(0x41, 2), 2, None)), &k, l(3)).err(),
		Some(E::Reference)
	);
	// Fresh grant.
	let (m, o, d) = grant(&empty, Some(&r1), &k, l(3)).unwrap();
	assert_eq!(o, O::Granted(rec(k, 3, None)));
	assert_eq!(
		d,
		PermissionDelta2 {
			records: 1,
			registry: 117,
			coordinate: Some(3)
		}
	);
	assert_eq!(m.len(), 1);
	assert_eq!(
		grant_permission2(&m, Some(&r1), &key(a1, 6), l(4), 116, lim()).err(),
		Some(E::Accounting),
		"total registry usage must cover the permission bytes"
	);
	// Duplicate, before the authority checks.
	let (same, o, d) = grant(&m, Some(&r1), &k, l(4)).unwrap();
	assert_eq!((o, d), (O::GrantDuplicate(rec(k, 3, None)), ZERO));
	assert!(same.shares_root(&m));
	assert_eq!(
		grant(&m, None, &k, l(4)).map(|x| x.1),
		Ok(O::GrantDuplicate(rec(k, 3, None)))
	);
	assert_eq!(
		outcome(grant(&m, Some(&reg(a1, 2, Some(4))), &k, l(5))),
		O::GrantDuplicate(rec(k, 3, None)),
		"a retired authority does not turn a duplicate into a refusal"
	);
	for at in [
		l(3),
		l(2),
		Log2 {
			term: 9,
			node: 1,
			index: 3,
		},
	] {
		assert_eq!(grant(&m, Some(&r1), &k, at).err(), Some(E::NotLater));
	}
	let (revoked, ..) = revoke(&m, &k, l(5)).unwrap();
	assert_eq!(
		grant(&revoked, Some(&r1), &k, l(5)).err(),
		Some(E::NotLater)
	);
	assert_eq!(
		outcome(grant(&revoked, Some(&r1), &k, l(6))),
		O::GrantDuplicate(rec(k, 3, Some(5)))
	);
	// Authority outcomes for a new key.
	let k2 = table_key(a1, 5);
	assert_eq!(outcome(grant(&m, None, &k2, l(4))), O::UnknownAuthority);
	assert_eq!(
		grant(&m, Some(&reg(a1, 4, None)), &k2, l(4)).err(),
		Some(E::NotLater)
	);
	assert_eq!(
		grant(&m, Some(&reg(a1, 5, None)), &k2, l(4)).err(),
		Some(E::NotLater)
	);
	assert_eq!(
		outcome(grant(&m, Some(&reg(a1, 2, Some(3))), &k2, l(5))),
		O::AuthorityRevoked(l(3))
	);
	assert_eq!(
		grant(&m, Some(&reg(a1, 2, Some(5))), &k2, l(5)).err(),
		Some(E::NotLater)
	);
	assert_eq!(
		grant(&m, Some(&reg(a1, 2, Some(6))), &k2, l(5)).err(),
		Some(E::NotLater)
	);
	assert_eq!(
		grant_permission2(&m, Some(&r1), &k2, l(5), usize::MAX, lim()).err(),
		Some(E::Overflow)
	);
	let (two, o, _) = grant(&m, Some(&r1), &k2, l(5)).unwrap();
	assert_eq!(o, O::Granted(rec(k2, 5, None)));
	assert_eq!(two.len(), 2);
	assert_eq!(m.len(), 1, "prior map untouched");
}

#[test]
fn revoke_outcomes_follow_the_selected_order() {
	let a1 = auth(0x41, 1);
	let k = key(a1, 5);
	let m = map_of(&[rec(k, 3, None)]);
	let mut bad = k;
	bad.namespace.generation = [0; 16];
	assert_eq!(
		revoke(&m, &bad, l(4)).err(),
		Some(E::ZeroField(ZeroField2::NamespaceGeneration))
	);
	assert_eq!(
		revoke(
			&m,
			&k,
			Log2 {
				term: 1,
				node: 1,
				index: 0
			}
		)
		.err(),
		Some(E::Log)
	);
	let (same, o, d) = revoke(&m, &key(a1, 6), l(4)).unwrap();
	assert_eq!((o, d), (O::UnknownPermission, ZERO));
	assert!(same.shares_root(&m), "unknown revoke writes nothing");
	for at in [
		l(3),
		l(2),
		Log2 {
			term: 9,
			node: 1,
			index: 3,
		},
	] {
		assert_eq!(
			revoke(&m, &k, at).err(),
			Some(E::NotLater),
			"no revoke at or before enrollment"
		);
	}
	let (r, o, d) = revoke(&m, &k, l(5)).unwrap();
	assert_eq!(o, O::Revoked(rec(k, 3, Some(5))));
	assert_eq!(
		d,
		PermissionDelta2 {
			records: 0,
			registry: 0,
			coordinate: Some(5)
		}
	);
	assert_eq!(m.get(&kb(&k)).unwrap().revoked, None, "prior map untouched");
	let (same, o, d) = revoke(&r, &k, l(6)).unwrap();
	assert_eq!((o, d), (O::RevokeDuplicate(rec(k, 3, Some(5))), ZERO));
	assert!(same.shares_root(&r));
	for at in [
		l(5),
		l(4),
		Log2 {
			term: 2,
			node: 7,
			index: 5,
		},
	] {
		assert_eq!(revoke(&r, &k, at).err(), Some(E::NotLater));
	}
}

#[test]
fn t1_to_t6_timelines_are_pinned() {
	let (a1, a2, a3) = (auth(1, 1), auth(2, 1), auth(3, 1));
	let r1 = reg(a1, 2, None);
	let k = key(a1, 0);
	// T1: a duplicate returns the original enrollment.
	let (m, o, _) = grant(&PermissionMap2::new(), Some(&r1), &k, l(3)).unwrap();
	assert_eq!(o, O::Granted(rec(k, 3, None)));
	assert_eq!(
		outcome(grant(&m, Some(&r1), &k, l(4))),
		O::GrantDuplicate(rec(k, 3, None))
	);
	// T2: no revival after revocation.
	let (m, o, _) = revoke(&m, &k, l(5)).unwrap();
	assert_eq!(o, O::Revoked(rec(k, 3, Some(5))));
	let (m, o, d) = grant(&m, Some(&r1), &k, l(6)).unwrap();
	assert_eq!((o, d), (O::GrantDuplicate(rec(k, 3, Some(5))), ZERO));
	// T3: a duplicate revoke returns the original revocation.
	assert_eq!(
		outcome(revoke(&m, &k, l(7))),
		O::RevokeDuplicate(rec(k, 3, Some(5)))
	);
	// T4: an unknown-key revoke writes nothing and cannot pre-cancel a later grant; the same
	// revoke resubmitted after the grant revokes.
	let k2 = table_key(a1, 0);
	let (m, o, _) = revoke(&m, &k2, l(8)).unwrap();
	assert_eq!(o, O::UnknownPermission);
	let (m, o, _) = grant(&m, Some(&r1), &k2, l(9)).unwrap();
	assert_eq!(o, O::Granted(rec(k2, 9, None)));
	assert_eq!(
		outcome(revoke(&m, &k2, l(10))),
		O::Revoked(rec(k2, 9, Some(10)))
	);
	let (m, ..) = revoke(&m, &k2, l(10)).unwrap();
	// T5: a refusal for an unknown authority is not consumed.
	let k3 = key(a2, 0);
	let (m, o, _) = grant(&m, None, &k3, l(11)).unwrap();
	assert_eq!(o, O::UnknownAuthority);
	let r2 = reg(a2, 12, None);
	let (m, o, _) = grant(&m, Some(&r2), &k3, l(13)).unwrap();
	assert_eq!(o, O::Granted(rec(k3, 13, None)));
	// T6: a revoked authority revision is never eligible again; a new revision is another key.
	let r3 = reg(a3, 14, Some(15));
	let k4 = key(a3, 0);
	assert_eq!(
		outcome(grant(&m, Some(&r3), &k4, l(16))),
		O::AuthorityRevoked(l(15))
	);
	assert_eq!(
		outcome(grant(&m, Some(&r3), &k4, l(40))),
		O::AuthorityRevoked(l(15))
	);
	let a3b = auth(3, 2);
	let k4b = key(a3b, 0);
	assert_ne!(kb(&k4), kb(&k4b));
	let (m, o, _) = grant(&m, Some(&reg(a3b, 17, None)), &k4b, l(18)).unwrap();
	assert_eq!(o, O::Granted(rec(k4b, 18, None)));
	assert_eq!(
		outcome(grant(&m, Some(&r3), &k4, l(19))),
		O::AuthorityRevoked(l(15))
	);
	assert_eq!(m.len(), 4);
}

#[test]
fn capacity_refuses_new_grants_but_not_revoke_or_duplicates() {
	let a1 = auth(0x41, 1);
	let r1 = reg(a1, 2, None);
	let mut m = PermissionMap2::new();
	let mut at = 3;
	for tenant in 0..PERMISSIONS2 as u64 {
		let (next, o, _) = grant(&m, Some(&r1), &key(a1, tenant), l(at)).unwrap();
		assert!(matches!(o, O::Granted(_)));
		m = next;
		at += 1;
	}
	assert_eq!(m.len(), PERMISSIONS2);
	let fresh = key(a1, 1 << 40);
	let (same, o, d) = grant(&m, Some(&r1), &fresh, l(at)).unwrap();
	assert_eq!((o, d), (O::Capacity, ZERO));
	assert!(same.shares_root(&m));
	let first = key(a1, 0);
	assert_eq!(
		outcome(grant(&m, Some(&r1), &first, l(at))),
		O::GrantDuplicate(rec(first, 3, None))
	);
	let (revoked, o, d) = revoke(&m, &first, l(at)).unwrap();
	assert_eq!(o, O::Revoked(rec(first, 3, Some(at))));
	assert_eq!(
		d,
		PermissionDelta2 {
			records: 0,
			registry: 0,
			coordinate: Some(at)
		}
	);
	assert_eq!(
		outcome(revoke(&revoked, &first, l(at + 1))),
		O::RevokeDuplicate(rec(first, 3, Some(at)))
	);
	assert_eq!(
		outcome(grant(&revoked, Some(&r1), &fresh, l(at + 1))),
		O::Capacity,
		"no reclamation"
	);
	// Revoke at the cap: the summary gains exactly one coordinate and no bytes.
	let mut auths = Map::new();
	auths.insert(authority_key(a1).unwrap(), r1.clone());
	let mut existing = Map::new();
	existing.insert(1u64, ());
	existing.insert(2u64, ());
	let full = |m: &PermissionMap2, covered| {
		validate_permissions2(m, G, l(1), l(covered), &auths, &existing, lim()).unwrap()
	};
	let before = full(&m, at - 1);
	let after = apply_permission_delta2(&before, d, lim()).unwrap();
	assert_eq!(after, full(&revoked, at));
	assert_eq!(
		(after.count, after.registry, after.coordinates.len()),
		(before.count, before.registry, before.coordinates.len() + 1)
	);
	// Lowered caller limits each refuse independently.
	let small = map_of(&[rec(key(a1, 0), 3, None), rec(key(a1, 1), 4, None)]);
	let with = |count, section, registry| PermissionLimits2 {
		count,
		section,
		registry,
	};
	let try_grant =
		|lims, used| grant_permission2(&small, Some(&r1), &fresh, l(5), used, lims).map(|x| x.1);
	assert_eq!(
		try_grant(with(2, PERMISSION_SECTION_BYTES2, REGISTRY_BYTES2), 234),
		Ok(O::Capacity)
	);
	assert_eq!(
		try_grant(with(4096, 4 + 117 * 2, REGISTRY_BYTES2), 234),
		Ok(O::Capacity)
	);
	assert_eq!(
		try_grant(with(4096, PERMISSION_SECTION_BYTES2, 1100), 1000),
		Ok(O::Capacity)
	);
	assert!(matches!(
		try_grant(with(4096, PERMISSION_SECTION_BYTES2, 1117), 1000),
		Ok(O::Granted(_))
	));
	assert_eq!(try_grant(lim(), usize::MAX), Err(E::Overflow));
	assert_eq!(
		validate_permissions2(
			&small,
			G,
			l(1),
			l(9),
			&auths,
			&existing,
			with(4096, 524288, 233)
		),
		Err(E::Count)
	);
	// Summary/delta accounting refusals, in order.
	let s = full(&small, 9);
	let granted = PermissionDelta2 {
		records: 1,
		registry: 117,
		coordinate: Some(7),
	};
	assert_eq!(
		apply_permission_delta2(&s, granted, with(4097, 524288, 0)),
		Err(E::Limits)
	);
	let mut skew = s.clone();
	skew.registry += 1;
	assert_eq!(
		apply_permission_delta2(&skew, ZERO, lim()),
		Err(E::Accounting)
	);
	let mut skew = s.clone();
	skew.section -= 4;
	assert_eq!(
		apply_permission_delta2(&skew, ZERO, lim()),
		Err(E::Accounting)
	);
	let mut huge = s.clone();
	huge.count = usize::MAX;
	assert_eq!(
		apply_permission_delta2(&huge, ZERO, lim()),
		Err(E::Overflow)
	);
	assert_eq!(
		apply_permission_delta2(&s, ZERO, with(1, 524288, REGISTRY_BYTES2)),
		Err(E::Count),
		"even a zero-byte revoke needs a summary within limits"
	);
	for shape in [
		PermissionDelta2 {
			records: 1,
			registry: 0,
			coordinate: Some(7),
		},
		PermissionDelta2 {
			records: 1,
			registry: 117,
			coordinate: None,
		},
		PermissionDelta2 {
			records: 0,
			registry: 117,
			coordinate: Some(7),
		},
		PermissionDelta2 {
			records: 2,
			registry: 234,
			coordinate: Some(7),
		},
	] {
		assert_eq!(
			apply_permission_delta2(&s, shape, lim()),
			Err(E::Accounting)
		);
	}
	let zero_index = PermissionDelta2 {
		coordinate: Some(0),
		..granted
	};
	assert_eq!(
		apply_permission_delta2(&s, zero_index, lim()),
		Err(E::Coordinate)
	);
	assert_eq!(
		apply_permission_delta2(&s, granted, with(2, 524288, REGISTRY_BYTES2)),
		Err(E::Count)
	);
	assert_eq!(
		apply_permission_delta2(&s, granted, with(4096, 4 + 117 * 2, REGISTRY_BYTES2)),
		Err(E::Count)
	);
	assert_eq!(
		apply_permission_delta2(&s, granted, with(4096, 524288, 234)),
		Err(E::Count)
	);
	let taken = PermissionDelta2 {
		coordinate: Some(3),
		..granted
	};
	assert_eq!(
		apply_permission_delta2(&s, taken, lim()),
		Err(E::Coordinate)
	);
	assert_eq!(
		apply_permission_delta2(&s, granted, lim()).unwrap().count,
		3
	);
}

#[test]
fn full_validation_refuses_bad_coordinates_and_authority_references() {
	let (a1, a2) = (auth(0x41, 1), auth(0x42, 1));
	let k1 = key(a1, 0);
	let k2 = table_key(a1, 0);
	let auths_with = |rs: &[(AuthorityRef2, AuthorityRegistration2)]| {
		let mut m = Map::new();
		for (under, r) in rs {
			m.insert(authority_key(*under).unwrap(), r.clone());
		}
		m
	};
	let auths = auths_with(&[(a1, reg(a1, 2, None))]);
	let mut existing = Map::new();
	existing.insert(1u64, ());
	existing.insert(2u64, ());
	let check =
		|m: &PermissionMap2| validate_permissions2(m, G, l(1), l(20), &auths, &existing, lim());
	let good = map_of(&[rec(k1, 3, Some(5)), rec(k2, 4, None)]);
	let s = check(&good).unwrap();
	assert_eq!((s.count, s.registry, s.section), (2, 234, 238));
	assert_eq!(
		s.coordinates.iter().map(|(i, _)| *i).collect::<Vec<_>>(),
		vec![3, 4, 5]
	);
	let empty = PermissionMap2::new();
	assert_eq!(
		validate_permissions2(&empty, [0; 16], l(1), l(20), &auths, &existing, lim()),
		Err(E::Generation),
		"zero generation refused even for an empty map"
	);
	let mut over = lim();
	over.registry += 1;
	assert_eq!(
		validate_permissions2(&empty, G, l(1), l(20), &auths, &existing, over),
		Err(E::Limits)
	);
	assert_eq!(
		validate_permissions2(
			&empty,
			G,
			Log2 {
				term: 1,
				node: 0,
				index: 1
			},
			l(20),
			&auths,
			&existing,
			lim()
		),
		Err(E::Log)
	);
	assert_eq!(
		validate_permissions2(&empty, G, l(21), l(20), &auths, &existing, lim()),
		Err(E::Coordinate)
	);
	let mut lowered = lim();
	lowered.count = 1;
	assert_eq!(
		validate_permissions2(&good, G, l(1), l(20), &auths, &existing, lowered),
		Err(E::Count)
	);
	let mut wrong = PermissionMap2::new();
	wrong.insert(kb(&k2), rec(k1, 3, None));
	assert_eq!(check(&wrong), Err(E::MapKey));
	let mut other = k1;
	other.namespace.generation = [0xB2; 16];
	assert_eq!(check(&map_of(&[rec(other, 3, None)])), Err(E::Generation));
	let partial = PermissionRecord2 {
		enrolled: Log2 {
			term: 1,
			node: 0,
			index: 3,
		},
		..rec(k1, 3, None)
	};
	assert_eq!(check(&map_of(&[partial])), Err(E::Log));
	for bad in [
		rec(k1, 1, None),     // at bootstrap
		rec(k1, 21, None),    // above covered
		rec(k1, 3, Some(3)),  // revoked at enrolled
		rec(k1, 4, Some(3)),  // revoked before enrolled
		rec(k1, 3, Some(21)), // revoked above covered
		rec(k1, 2, None),     // collides with an existing (authority) coordinate
	] {
		assert_eq!(
			check(&map_of(&[bad])),
			Err(E::Coordinate),
			"{:?}",
			(bad.enrolled.index, bad.revoked.map(|x| x.index))
		);
	}
	assert_eq!(
		check(&map_of(&[rec(k1, 3, Some(5)), rec(k2, 5, None)])),
		Err(E::Coordinate),
		"cross-permission"
	);
	let other_term = PermissionRecord2 {
		enrolled: Log2 {
			term: 7,
			node: 1,
			index: 3,
		},
		..rec(k2, 3, None)
	};
	assert_eq!(
		check(&map_of(&[rec(k1, 3, None), other_term])),
		Err(E::Coordinate),
		"same index, different term"
	);
	let refs = |auths: &Map<[u8; 24], AuthorityRegistration2>, r: PermissionRecord2| {
		validate_permissions2(&map_of(&[r]), G, l(1), l(20), auths, &existing, lim())
	};
	assert_eq!(
		refs(&auths, rec(key(a2, 0), 3, None)),
		Err(E::Reference),
		"absent authority"
	);
	let id_mismatch = auths_with(&[(a1, reg(a2, 2, None))]);
	assert_eq!(refs(&id_mismatch, rec(k1, 3, None)), Err(E::Reference));
	let revision_mismatch = auths_with(&[(a1, reg(auth(0x41, 2), 2, None))]);
	assert_eq!(
		refs(&revision_mismatch, rec(k1, 3, None)),
		Err(E::Reference)
	);
	assert_eq!(
		refs(&auths_with(&[(a1, reg(a1, 4, None))]), rec(k1, 4, None)),
		Err(E::Reference),
		"authority enrolled at the grant"
	);
	assert_eq!(
		refs(&auths_with(&[(a1, reg(a1, 6, None))]), rec(k1, 4, None)),
		Err(E::Reference),
		"authority enrolled after"
	);
	assert_eq!(
		refs(&auths_with(&[(a1, reg(a1, 2, Some(3)))]), rec(k1, 4, None)),
		Err(E::Reference),
		"revoked before"
	);
	assert_eq!(
		refs(&auths_with(&[(a1, reg(a1, 2, Some(4)))]), rec(k1, 4, None)),
		Err(E::Reference),
		"revoked at"
	);
	assert!(
		refs(
			&auths_with(&[(a1, reg(a1, 2, Some(6)))]),
			rec(k1, 4, Some(9))
		)
		.is_ok(),
		"the two revocations stay distinct"
	);
}

#[test]
fn incremental_delta_matches_full_rebuild_over_random_sequences() {
	for (seed, count) in [
		(0x1234_5678_9ABC_DEF1u64, PERMISSIONS2),
		(0xDEAD_BEEF_0BAD_F00D, 6),
		(7, 12),
	] {
		let limits = PermissionLimits2 { count, ..lim() };
		let pool = [auth(1, 1), auth(1, 2), auth(2, 1), auth(3, 1)];
		let keys = (0..16)
			.map(|i| {
				let a = pool[i % 4];
				if i % 3 == 0 {
					table_key(a, (i / 4) as u64)
				} else {
					key(a, (i / 4) as u64)
				}
			})
			.collect::<Vec<_>>();
		let mut s = seed;
		let mut auths = Map::new();
		let mut existing = Map::new();
		existing.insert(1u64, ());
		let mut m = PermissionMap2::new();
		let mut summary =
			validate_permissions2(&m, G, l(1), l(1), &auths, &existing, limits).unwrap();
		// Independent model written from the contract tables; it never calls a transition.
		let mut model: BTreeMap<[u8; 73], (u64, Option<u64>)> = BTreeMap::new();
		let mut model_auth: BTreeMap<[u8; 24], (u64, Option<u64>)> = BTreeMap::new();
		let mut seen = BTreeMap::new();
		for at in 2..602u64 {
			let r = xorshift(&mut s);
			let pick = (r >> 8) as usize;
			match r % 10 {
				0 | 1 => {
					let a = pool[pick % 4];
					let ak = authority_key(a).unwrap();
					match model_auth.get(&ak).copied() {
						None => {
							model_auth.insert(ak, (at, None));
							auths.insert(ak, reg(a, at, None));
						}
						Some((e, None)) if r % 10 == 1 => {
							model_auth.insert(ak, (e, Some(at)));
							auths.insert(ak, reg(a, e, Some(at)));
						}
						_ => continue,
					}
					existing.insert(at, ());
					continue;
				}
				_ => {}
			}
			let k = keys[pick % keys.len()];
			let kbytes = kb(&k);
			let ak = authority_key(k.authority).unwrap();
			let (next, o, d) = if r % 10 < 6 {
				let expected = match (model.get(&kbytes), model_auth.get(&ak)) {
					(Some(&(e, x)), _) => O::GrantDuplicate(rec(k, e, x)),
					(None, None) => O::UnknownAuthority,
					(None, Some(&(_, Some(x)))) => O::AuthorityRevoked(l(x)),
					(None, Some(_)) if model.len() >= count => O::Capacity,
					(None, Some(_)) => O::Granted(rec(k, at, None)),
				};
				let got = grant_permission2(&m, auths.get(&ak), &k, l(at), bytes_used(&m), limits)
					.unwrap();
				assert_eq!(got.1, expected, "grant step {at}");
				if let O::Granted(_) = expected {
					model.insert(kbytes, (at, None));
				}
				got
			} else {
				let expected = match model.get(&kbytes) {
					None => O::UnknownPermission,
					Some(&(e, Some(x))) => O::RevokeDuplicate(rec(k, e, Some(x))),
					Some(&(e, None)) => O::Revoked(rec(k, e, Some(at))),
				};
				let got = revoke_permission2(&m, &k, l(at)).unwrap();
				assert_eq!(got.1, expected, "revoke step {at}");
				if let O::Revoked(_) = expected {
					model.insert(kbytes, (model[&kbytes].0, Some(at)));
				}
				got
			};
			*seen.entry(label(&o)).or_insert(0) += 1;
			summary = apply_permission_delta2(&summary, d, limits).unwrap();
			m = next;
			// The oracle rebuilds from the map's records alone and never consults the delta.
			let full =
				validate_permissions2(&m, G, l(1), l(at), &auths, &existing, limits).unwrap();
			assert_eq!(full, summary, "step {at}");
			assert_eq!(m.len(), model.len());
		}
		assert!(seen.len() >= 4, "seed {seed:#x} exercised {seen:?}");
	}
}
