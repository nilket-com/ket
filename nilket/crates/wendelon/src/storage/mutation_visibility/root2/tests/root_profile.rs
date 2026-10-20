//! Current root profile 4: permission accounting and the frozen profile-2 oracle projection.
//! States marked "synthetic accounting" exercise arithmetic only and are not authentic roots.
use super::*;
use crate::storage::mutation_visibility::MutationScope;
use crate::storage::mutation_visibility::root2::admin_statement::{
	AdmissionNamespace2, PermissionKey2,
};
use crate::storage::mutation_visibility::root2::admission_registry::*;
use crate::storage::mutation_visibility::root2::incremental::{Derived2, cached_capacity};
use crate::storage::mutation_visibility::root2::origin::UnverifiedAuthority2;
use crate::storage::mutation_visibility::root2::persistent::Map;

fn unhex(s: &str) -> Vec<u8> {
	(0..s.len())
		.step_by(2)
		.map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
		.collect()
}
fn vectors() -> serde_json::Value {
	serde_json::from_str(include_str!("root_profile_vectors.json")).unwrap()
}
fn with_permissions() -> Vec<u8> {
	unhex(
		vectors()["p3_root_with_permissions"]["hex"]
			.as_str()
			.unwrap(),
	)
}
fn installed(b: &[u8]) -> Root2 {
	let u = UnverifiedRoot2::decode(b).unwrap();
	let (g, c, l, fp) = u.fixture_fields();
	u.fixture_install(g, c, l, fp, hash(b"", b)).unwrap()
}
fn body(b: &[u8]) -> Vec<u8> {
	open_versioned(b, b"VISAUT04", 4, ROOT_BYTES2)
		.unwrap()
		.to_vec()
}
fn reseal(body: &[u8]) -> Vec<u8> {
	envelope_versioned(b"VISAUT04", 4, body, ROOT_BYTES2).unwrap()
}
fn decode_err(body: &[u8]) -> Option<RootError2> {
	UnverifiedRoot2::decode(&reseal(body)).err()
}
fn key(s: &State2, a: AuthorityRef2, tenant: u64) -> PermissionKey2 {
	PermissionKey2 {
		authority: a,
		namespace: AdmissionNamespace2 {
			generation: s.generation,
			tenant,
			scope: MutationScope::Raw,
			table: [0; 16],
			schema: 0,
		},
	}
}
fn with_record(s: &State2, r: PermissionRecord2) -> State2 {
	let mut s = s.clone();
	s.permissions = PermissionMap2::new();
	s.permissions
		.insert(encode_permission_key2(&r.key).unwrap(), r);
	s
}
// Offset of the permission count word after selectors, authorities and issuers.
fn permission_at(body: &[u8]) -> usize {
	let mut r = Read2::new(body);
	r.take(212).unwrap();
	for _ in 0..3 {
		for _ in 0..r.count(4096, 4).unwrap() {
			r.blob(ROOT_BYTES2).unwrap();
		}
	}
	body.len() - r.remaining().len()
}

#[test]
fn profile3_goldens_round_trip_and_profile2_bytes_are_refused() {
	let v = vectors();
	let refused = v["refused"].as_array().unwrap();
	assert_eq!(refused.len(), 7);
	for item in refused {
		let name = item["name"].as_str().unwrap();
		let old = unhex(item["hex"].as_str().unwrap());
		let new = fixture(name);
		assert_eq!(new.len(), old.len() + 8, "{name}");
		assert_eq!(&new[..8], b"VISAUT04");
		assert_eq!(
			UnverifiedRoot2::decode(&new).unwrap().encode().unwrap(),
			new,
			"{name}"
		);
		// Old bytes: refused by production, still genuine to the frozen oracle.
		assert_eq!(
			UnverifiedRoot2::decode(&old).err(),
			Some(RootError2::Codec),
			"{name}"
		);
		let legacy = super::s1c_oracle::state::UnverifiedRoot2::decode(&old).unwrap();
		assert_eq!(legacy.encode().unwrap(), old, "{name}");
		// The Rust bridge and the independent Python transform agree, both ways.
		assert_eq!(super::s1c_oracle::profile3(&old).unwrap(), new, "{name}");
		assert_eq!(
			super::s1c_oracle::legacy_profile2(&new).unwrap(),
			old,
			"{name}"
		);
	}
	let old_origin = unhex(v["origin_root_refused"].as_str().unwrap());
	assert_eq!(
		UnverifiedAuthority2::decode(&old_origin).err(),
		Some(RootError2::Codec)
	);
	// A current root with permissions has no profile-2 projection.
	assert_eq!(
		super::s1c_oracle::legacy_profile2(&with_permissions()).err(),
		Some(RootError2::Codec)
	);
	// No other version or magic is accepted for a root.
	let mut b = fixture("root_bootstrap_only");
	b[8] = 2;
	let n = b.len();
	let sum = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, &b[..n - 32]);
	b[n - 32..].copy_from_slice(sum.as_ref());
	assert_eq!(UnverifiedRoot2::decode(&b).err(), Some(RootError2::Codec));
}

#[test]
fn permission_section_decodes_bounded_and_maps_errors() {
	let b = with_permissions();
	let v = &vectors()["p3_root_with_permissions"];
	assert_eq!(b.len() as u64, v["length"].as_u64().unwrap());
	assert_eq!(UnverifiedRoot2::decode(&b).unwrap().encode().unwrap(), b);
	let r = installed(&b);
	assert_eq!(r.state.permissions.len(), 2);
	assert_eq!(
		r.usage().unwrap().registry as u64,
		v["registry"].as_u64().unwrap()
	);
	let body = body(&b);
	let at = permission_at(&body);
	assert_eq!(at, 212 + 4 + 4 + 4 + 117 + 4);
	let set = |at: usize, x: &[u8]| {
		let mut b = body.clone();
		b[at..at + x.len()].copy_from_slice(x);
		b
	};
	assert_eq!(
		decode_err(&set(at, &4097u32.to_le_bytes())),
		Some(RootError2::Limit)
	);
	assert_eq!(
		decode_err(&set(at, &3u32.to_le_bytes())),
		Some(RootError2::Codec),
		"short section"
	);
	assert_eq!(
		decode_err(&set(at + 4, &112u32.to_le_bytes())),
		Some(RootError2::Codec),
		"framing"
	);
	assert_eq!(
		decode_err(&set(at + 8, &[0; 16])),
		Some(RootError2::Codec),
		"key fault"
	);
	let first = at + 8;
	let second = first + 117;
	let mut swapped = body.clone();
	swapped[first..first + 113].copy_from_slice(&body[second..second + 113]);
	swapped[second..second + 113].copy_from_slice(&body[first..first + 113]);
	assert_eq!(decode_err(&swapped), Some(RootError2::Codec), "order");
	// Revoked at or before enrolled inside a record.
	let revoked_index = first + 73 + 20 + 12;
	assert_eq!(
		decode_err(&set(revoked_index, &3u64.to_le_bytes())),
		Some(RootError2::Codec)
	);
	// Saved usage that disagrees with the section.
	assert_eq!(
		decode_err(&set(204, &354u64.to_le_bytes())),
		Some(RootError2::RootInvariant)
	);
	assert_eq!(decode_err(&body), None);
	// The count is consumed exactly once: the following sections are read at their exact place.
	let mut trailing = body.clone();
	trailing.push(0);
	assert_eq!(decode_err(&trailing), Some(RootError2::Codec));
	assert_eq!(decode_err(&body[..body.len() - 1]), Some(RootError2::Codec));
}

#[test]
fn validate_state_joins_permission_coordinates_and_references() {
	let mut joined = 0;
	for name in [
		"root_reserved",
		"root_accepted",
		"root_cut_first_refused",
		"root_precut_accepted_preserved",
		"root_renew_then_reserve_old_interval",
	] {
		let r = root(name);
		let mut s = r.state.clone();
		let a = s.authorities.values().next().unwrap().clone();
		let free = s.covered.index + 1;
		s.covered = l(free);
		let at = |i: u64| PermissionRecord2 {
			key: key(&s, a.authority, 9),
			enrolled: l(i),
			revoked: None,
		};
		if a.revoked.is_none() {
			validate_state(&with_record(&s, at(free))).unwrap();
		}
		let mut claimed = vec![s.bootstrap.index];
		claimed.extend(s.selectors.values().map(|x| x.enrolled.index));
		for x in s.authorities.values() {
			claimed.push(x.enrolled.index);
			claimed.extend(x.revoked.map(|l| l.index));
		}
		claimed.extend(s.cuts.iter().map(|c| c.log.index));
		for g in s.lineages.values() {
			claimed.push(g.latest.log.index);
			claimed.extend(g.closure.map(|c| c.log.index));
		}
		for p in s.parents.values() {
			claimed.push(p.reservation.index);
			claimed.extend(p.terminal.map(|t| t.first().index));
		}
		for i in claimed {
			assert_eq!(
				validate_state(&with_record(&s, at(i))).err(),
				Some(RootError2::RootInvariant),
				"{name}: permission at claimed coordinate {i}"
			);
		}
		// A parent's original grant may not sit at a permission coordinate, even when that index
		// is otherwise unclaimed: the join with non-grant coordinates is what refuses it.
		for p in s.parents.values() {
			let o = p.claim.original.log.index;
			if a.revoked.is_none()
				&& a.enrolled.index < o
				&& !s.lineages.values().any(|g| g.latest.log.index == o)
			{
				let mut without = with_record(&s, at(o));
				validate_state(&{
					let mut x = without.clone();
					x.parents = Map::new();
					x
				})
				.unwrap();
				assert_eq!(
					validate_state(&without).err(),
					Some(RootError2::RootInvariant),
					"{name}"
				);
				without.permissions = PermissionMap2::new();
				validate_state(&without).unwrap();
				joined += 1;
			}
		}
		// References and generation.
		let mut other = at(free);
		other.key.namespace.generation = [0xEE; 16];
		assert_eq!(
			validate_state(&with_record(&s, other)).err(),
			Some(RootError2::RootInvariant)
		);
		let mut unknown = at(free);
		unknown.key.authority.revision += 7;
		assert_eq!(
			validate_state(&with_record(&s, unknown)).err(),
			Some(RootError2::RootInvariant)
		);
	}
	// The renewal golden supplies an old-interval original grant, so the join is exercised.
	assert!(joined > 0);
}

#[test]
fn root_total_and_registry_include_permission_bytes() {
	let r = installed(&with_permissions());
	let s = &r.state;
	let u = usage(s).unwrap();
	assert_eq!(
		u.registry,
		s.selectors.len() * 68 + s.authorities.len() * 121 + s.permissions.len() * 117
	);
	assert_eq!(u.registry, 355);
	assert_eq!(ROOT_BASE2, 212 + 7 * 4);
	assert_eq!(
		ROOT_BASE2 - 232,
		8,
		"the oracle's base lacks issuer and permission count words"
	);
	// Disclosed: with the component caps in force the hard root total cannot be the binding bound,
	// so 240 versus 232 changes no capacity outcome.
	assert!(PARENT_BYTES2 + HISTORY_BYTES2 + REGISTRY_BYTES2 + ROOT_BASE2 < ROOT_BYTES2);
	let both = |s: &State2, l: Limits2| {
		let full = capacity(s, l).err();
		let mut cached = s.clone();
		cached.derived = Derived2::build(s).unwrap();
		assert_eq!(cached.derived.usage, usage(s).unwrap());
		assert_eq!(
			cached_capacity(&cached, l).err(),
			full,
			"full and cached agree"
		);
		full
	};
	let d = Limits2::default();
	assert_eq!(both(s, d), None);
	assert_eq!(
		both(
			s,
			Limits2 {
				permissions: 2,
				..d
			}
		),
		None
	);
	assert_eq!(
		both(
			s,
			Limits2 {
				permissions: 1,
				..d
			}
		),
		Some(RootError2::Limit)
	);
	assert_eq!(both(s, Limits2 { registry: 355, ..d }), None);
	assert_eq!(
		both(s, Limits2 { registry: 354, ..d }),
		Some(RootError2::Limit)
	);
	// Synthetic accounting states (not authentic roots): component maxima pass, one byte over each
	// component refuses, in both paths; cached usage is set equal to full usage.
	let synthetic = |held: usize, history: usize, registry: usize| {
		let mut x = s.clone();
		x.derived.usage = Usage2 {
			parents: 0,
			held,
			history,
			registry,
		};
		x
	};
	assert_eq!(
		cached_capacity(
			&synthetic(PARENT_BYTES2, HISTORY_BYTES2, REGISTRY_BYTES2),
			d
		)
		.err(),
		None
	);
	assert_eq!(
		cached_capacity(&synthetic(PARENT_BYTES2 + 1, 0, 0), d).err(),
		Some(RootError2::Limit)
	);
	assert_eq!(
		cached_capacity(&synthetic(0, HISTORY_BYTES2 + 1, 0), d).err(),
		Some(RootError2::Limit)
	);
	assert_eq!(
		cached_capacity(&synthetic(0, 0, REGISTRY_BYTES2 + 1), d).err(),
		Some(RootError2::Limit)
	);
	let mut held = root("root_reserved").state;
	let k = *held.parents.iter().next().unwrap().0;
	for (bytes, expected) in [
		(PARENT_BYTES2 as u64, None),
		(PARENT_BYTES2 as u64 + 1, Some(RootError2::Limit)),
	] {
		let mut p = held.parents.get(&k).unwrap().clone();
		p.held_bytes = bytes;
		held.parents.insert(k, p);
		assert_eq!(
			capacity(&held, d).err(),
			expected,
			"synthetic full-path held {bytes}"
		);
	}
}

#[test]
fn derived_build_records_permission_non_grants() {
	let r = installed(&with_permissions());
	let d = Derived2::build(&r.state).unwrap();
	for i in [3u64, 4, 5] {
		assert!(d.non_grants.contains_key(&i));
	}
	assert_eq!(r.state.derived, d);
	// Revoke at the cap: exactly one more non-grant entry and no registry change.
	let mut s = r.state.clone();
	let a = s.authorities.values().next().unwrap().clone();
	s.covered = l(10_000);
	let mut m = PermissionMap2::new();
	for (i, tenant) in (0..PERMISSIONS2 as u64).enumerate() {
		let k = key(&s, a.authority, 100 + tenant);
		let used = 121 + 117 * m.len();
		let (next, o, _) = grant_permission2(
			&m,
			Some(&a),
			&k,
			l(6 + i as u64),
			used,
			permission_limits2(),
		)
		.unwrap();
		assert!(matches!(o, PermissionOutcome2::Granted(_)));
		m = next;
	}
	s.permissions = m.clone();
	validate_state(&s).unwrap();
	let before = Derived2::build(&s).unwrap();
	let first = *m.values().next().unwrap();
	let (revoked, _, delta) = revoke_permission2(&m, &first.key, l(9_000)).unwrap();
	assert_eq!(delta.coordinate, Some(9_000));
	s.permissions = revoked;
	let after = Derived2::build(&s).unwrap();
	assert_eq!(after.non_grants.len(), before.non_grants.len() + 1);
	assert!(after.non_grants.contains_key(&9_000));
	assert_eq!(after.usage.registry, before.usage.registry);
	assert_eq!(before.usage.registry, 121 + 117 * PERMISSIONS2);
}

#[test]
fn bootstrap_and_reduce_keep_permissions_empty() {
	let (g, c) = ([3; 16], [5; 16]);
	let boot = Command2::Bootstrap {
		epoch: 1,
		control: [9; 32],
		length: 241,
	};
	let e = TrustedEntry2::fixture(g, c, l(1), &boot).unwrap();
	let v = VerifiedCommand2::fixture(g, c, &boot, &[], 0).unwrap();
	let (b, _) = Root2::reduce(None, &e, &v, &boot).unwrap();
	assert_eq!(b.state.permissions.len(), 0);
	assert_eq!(&b.encode().unwrap()[..8], b"VISAUT04");
	let mut r = root("root_reserved");
	for step in 0..6u8 {
		let cmd = match step % 3 {
			0 => Command2::Cover {
				kind: step % 3,
				external: [step; 32],
			},
			1 => Command2::Cut {
				expected: epoch(&r.state),
				next: epoch(&r.state) + 1,
				configuration: [step; 32],
			},
			_ => finalized(&r),
		};
		r = next(&r, &cmd, &[], 1).0;
		assert_eq!(r.state.permissions.len(), 0, "step {step}");
	}
}
