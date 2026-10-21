//! Independent S2 expectations: never project issuer state into the frozen S1 oracle.
use super::*;
use crate::storage::mutation_visibility::root2::incremental::Derived2;
use crate::storage::mutation_visibility::root2::persistent::Map;

fn key(seed: u8) -> Digest2 {
	ed25519_dalek::SigningKey::from_bytes(&[seed; 32])
		.verifying_key()
		.to_bytes()
}
fn enroll(node: u32, key: Digest2) -> Command2 {
	Command2::EnrollLeaseIssuer { node, key }
}
fn revoke(node: u32) -> Command2 {
	Command2::RevokeLeaseIssuer { node }
}
fn step(r: &Root2, c: &Command2) -> (Root2, ApplyResult2) {
	let (g, cluster) = (r.state.generation, r.state.cluster);
	let e = TrustedEntry2::fixture(g, cluster, l(r.covered().index + 1), c).unwrap();
	let v = VerifiedCommand2::fixture(g, cluster, c, &[], 0).unwrap();
	let before = r.encode().unwrap();
	let (n, out) = Root2::reduce(Some(r), &e, &v, c).unwrap();
	assert_eq!(r.encode().unwrap(), before);
	validate_state(&n.state).unwrap();
	assert_eq!(n.state.derived, Derived2::build(&n.state).unwrap());
	assert_eq!(n.usage().unwrap(), usage(&n.state).unwrap());
	assert_eq!(n.covered(), l(r.covered().index + 1));
	assert_eq!(n.state.command_digest, c.digest(g, cluster).unwrap());
	assert_eq!(n.state.fingerprint, e.fields().3);
	assert_eq!(
		n.encode().unwrap(),
		crate::storage::mutation_visibility::root2::state::encode_state_reference(&n.state)
			.unwrap()
	);
	(n, out)
}
fn deny(r: &Root2, c: &Command2, reason: u16, reference: Option<Log2>) -> Root2 {
	let (n, out) = step(r, c);
	assert_eq!(out, ApplyResult2::Denied { reason, reference });
	assert_eq!(r.state.issuers, n.state.issuers);
	assert_eq!(r.state.selectors, n.state.selectors);
	assert_eq!(r.state.authorities, n.state.authorities);
	assert_eq!(r.state.permissions, n.state.permissions);
	assert_eq!(r.state.lineages, n.state.lineages);
	assert_eq!(r.state.cuts, n.state.cuts);
	assert_eq!(r.state.parents, n.state.parents);
	assert_eq!(r.usage().unwrap(), n.usage().unwrap());
	assert_ne!(r.encode().unwrap(), n.encode().unwrap());
	n
}
#[test]
fn issuer_lifecycle_first_coordinates_and_irreversibility() {
	let r = root("root_bootstrap_only");
	let r = deny(&r, &revoke(7), 9, None);
	let first = l(r.covered().index + 1);
	let (r, o) = step(&r, &enroll(7, key(1)));
	assert_eq!(o, ApplyResult2::Registered(first));
	assert_eq!(r.usage().unwrap().registry, 81);
	let (r, o) = step(&r, &enroll(7, key(1)));
	assert_eq!(o, ApplyResult2::Registered(first));
	let r = deny(&r, &enroll(7, key(2)), 2, Some(first));
	let revoked = l(r.covered().index + 1);
	let (r, o) = step(&r, &revoke(7));
	assert_eq!(o, ApplyResult2::Registered(revoked));
	let (r, o) = step(&r, &revoke(7));
	assert_eq!(o, ApplyResult2::Registered(revoked));
	let r = deny(&r, &enroll(7, key(1)), 2, Some(first));
	let r = deny(&r, &enroll(7, key(2)), 2, Some(first));
	assert_eq!(
		r.state.issuers.get(&7u32.to_be_bytes()).unwrap().revoked,
		Some(revoked)
	);
	assert_eq!(r.usage().unwrap().registry, 81);
	assert_eq!(r.state.derived.nodes.len(), 0);
	assert_eq!(r.state.derived.non_grants.len(), 3); // bootstrap, first enrollment, first revoke
}
#[test]
fn issuer_first_error_order_and_capacity_are_independent() {
	let r = root("root_bootstrap_only");
	let r = deny(&r, &enroll(7, [0; 32]), 9, None);
	let (r, _) = step(&r, &enroll(7, key(1)));
	let first = r.state.issuers.values().next().unwrap().enrolled;
	let r = r.lower_limits(Limits2 {
		registrations: 1,
		registry: 81,
		..Limits2::default()
	});
	let (r, o) = step(&r, &enroll(7, key(1)));
	assert_eq!(o, ApplyResult2::Registered(first)); // duplicate before both caps
	let r = deny(&r, &enroll(7, [0; 32]), 9, None); // invalid before existing/capacity
	let r = deny(&r, &enroll(7, key(2)), 2, Some(first)); // conflict before capacity
	let r = deny(&r, &enroll(8, key(1)), 11, None);
	let (r, _) = step(&r, &revoke(7));
	let r = deny(&r, &enroll(7, [0; 32]), 9, None); // invalid before revoked
	let _ = deny(&r, &enroll(7, key(1)), 2, Some(first));
	let r = root("root_bootstrap_only").lower_limits(Limits2 {
		registry: 80,
		..Limits2::default()
	});
	let _ = deny(&r, &enroll(9, key(1)), 11, None); // registry bytes alone
	let r = root("root_bootstrap_only").lower_limits(Limits2 {
		registrations: 0,
		..Limits2::default()
	});
	let _ = deny(&r, &enroll(9, key(1)), 11, None); // section count alone
	// Selector/authority occupancy does not consume the independent issuer count cap.
	let r = root("root_reserved").lower_limits(Limits2 {
		registrations: 1,
		..Limits2::default()
	});
	assert_eq!(step(&r, &enroll(9, key(1))).0.state.issuers.len(), 1);
}
#[test]
fn issuer_record_and_command_codec_structural_faults_are_atomic() {
	let b = fixture("issuer_registration2");
	assert_eq!(b.len(), 77);
	assert_eq!(
		IssuerRegistration2::decode(&b).unwrap().encode().unwrap(),
		b
	);
	for i in 0..b.len() {
		assert_eq!(
			IssuerRegistration2::decode(&b[..i]).err(),
			Some(RootError2::Codec)
		);
	}
	for (at, bytes) in [
		(0, vec![0; 4]),
		(56, vec![2]),
		(56, vec![1]),
		(57, vec![1]),
		(36, vec![0; 20]),
	] {
		let mut x = b.clone();
		x[at..at + bytes.len()].copy_from_slice(&bytes);
		assert_eq!(
			IssuerRegistration2::decode(&x).err(),
			Some(RootError2::Codec)
		);
	}
	let mut a = IssuerRegistration2::decode(&b).unwrap();
	a.revoked = Some(a.enrolled);
	assert_eq!(a.encode().err(), Some(RootError2::Codec));
	let r = root("root_bootstrap_only");
	let before = r.encode().unwrap();
	for name in ["command_enroll_lease_issuer", "command_revoke_lease_issuer"] {
		let b = fixture(name);
		let (g, c, cmd) = Command2::decode(&b).unwrap();
		assert_eq!(cmd.encode(g, c).unwrap(), b);
		let mut payload = cmd.payload(g, c).unwrap();
		payload[33..37].fill(0);
		assert_eq!(
			Command2::decode(&envelope(b"VISEVT02", &payload, COMMAND_BYTES2).unwrap()).err(),
			Some(RootError2::Codec)
		);
		payload = cmd.payload(g, c).unwrap();
		payload.push(0);
		assert_eq!(
			Command2::decode(&envelope(b"VISEVT02", &payload, COMMAND_BYTES2).unwrap()).err(),
			Some(RootError2::Codec)
		);
	}
	for cmd in [enroll(0, key(1)), revoke(0)] {
		assert_eq!(
			cmd.encode(r.state.generation, r.state.cluster).err(),
			Some(RootError2::Codec)
		);
	}
	assert_eq!(r.encode().unwrap(), before);
}
#[test]
fn issuer_validate_state_maps_semantic_and_structural_faults() {
	let r = root("root_one_issuer");
	let a = r.state.issuers.values().next().unwrap().clone();
	let mutate = |a: IssuerRegistration2, mapkey: [u8; 4]| {
		let mut s = r.state.clone();
		s.issuers = Map::new();
		s.issuers.insert(mapkey, a);
		validate_state(&s).err()
	};
	assert_eq!(
		mutate(a.clone(), 1u32.to_be_bytes()),
		Some(RootError2::RootInvariant)
	);
	let mut x = a.clone();
	x.key = [0; 32];
	assert_eq!(
		mutate(x, a.node.to_be_bytes()),
		Some(RootError2::RootInvariant)
	);
	let mut x = a.clone();
	x.node = 0;
	assert_eq!(mutate(x, [0; 4]), Some(RootError2::Codec));
	let mut x = a.clone();
	x.enrolled = l(r.covered().index + 1);
	assert_eq!(
		mutate(x, a.node.to_be_bytes()),
		Some(RootError2::RootInvariant)
	);
	let mut x = a.clone();
	x.enrolled = r.state.bootstrap;
	assert_eq!(
		mutate(x, a.node.to_be_bytes()),
		Some(RootError2::RootInvariant)
	);
	let mut x = a.clone();
	x.revoked = Some(x.enrolled);
	assert_eq!(mutate(x, a.node.to_be_bytes()), Some(RootError2::Codec));
	let mut x = a.clone();
	x.enrolled.term = 0;
	assert_eq!(mutate(x, a.node.to_be_bytes()), Some(RootError2::Codec));
	let mut collision = root("root_numeric_issuers").state;
	collision
		.issuers
		.get_mut(&1u32.to_be_bytes())
		.unwrap()
		.revoked = Some(l(3));
	assert_eq!(
		validate_state(&collision).err(),
		Some(RootError2::RootInvariant)
	);
	let mut s = root("root_numeric_issuers").state;
	let k = 256u32.to_be_bytes();
	s.issuers.get_mut(&k).unwrap().enrolled = l(2);
	assert_eq!(validate_state(&s).err(), Some(RootError2::RootInvariant));
}
#[test]
fn issuer_profile04_numeric_order_and_profile03_refusal() {
	for (name, nodes) in [
		("root_bootstrap_only", vec![]),
		("root_one_issuer", vec![256]),
		("root_numeric_issuers", vec![1, 256, 0x01000000]),
	] {
		let r = root(name);
		assert_eq!(
			r.state.issuers.values().map(|a| a.node).collect::<Vec<_>>(),
			nodes
		);
		assert_eq!(r.encode().unwrap(), fixture(name));
		assert_eq!(r.usage().unwrap().registry, 81 * nodes.len());
		assert_eq!(
			r.encode().unwrap(),
			crate::storage::mutation_visibility::root2::state::encode_state_reference(&r.state)
				.unwrap()
		);
	}
	let v: serde_json::Value =
		serde_json::from_str(include_str!("root_profile_vectors.json")).unwrap();
	for row in v["refused_profile3"].as_array().unwrap() {
		let hex = row["hex"].as_str().unwrap();
		let b = (0..hex.len())
			.step_by(2)
			.map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
			.collect::<Vec<_>>();
		assert_eq!(&b[..8], b"VISAUT03");
		assert_eq!(UnverifiedRoot2::decode(&b).err(), Some(RootError2::Codec));
	}
	let r = root("root_numeric_issuers");
	let encoded = r.encode().unwrap();
	let mut b = encoded[16..encoded.len() - 32].to_vec();
	// Header212 + empty selector4 + authority4 + issuer count4: first framed record starts224.
	let first = b[224..305].to_vec();
	let second = b[305..386].to_vec();
	b[224..305].copy_from_slice(&second);
	b[305..386].copy_from_slice(&first);
	assert_eq!(
		UnverifiedRoot2::decode(&envelope_versioned(b"VISAUT04", 4, &b, ROOT_BYTES2).unwrap())
			.err(),
		Some(RootError2::Codec)
	);
}
#[test]
fn issuer_oracle_boundary_refuses_nonempty_sections_and_new_commands() {
	let r = root("root_one_issuer");
	assert_eq!(
		super::s1c_oracle::legacy_profile2(&r.encode().unwrap()).err(),
		Some(RootError2::Codec)
	);
	let r = root("root_bootstrap_only");
	let bytes = super::s1c_oracle::legacy_profile2(&r.encode().unwrap()).unwrap();
	for cmd in [enroll(1, key(1)), revoke(1)] {
		let e = TrustedEntry2::fixture(
			r.state.generation,
			r.state.cluster,
			l(r.covered().index + 1),
			&cmd,
		)
		.unwrap();
		let v =
			VerifiedCommand2::fixture(r.state.generation, r.state.cluster, &cmd, &[], 0).unwrap();
		assert_eq!(
			super::s1c_oracle::reduce(&bytes, Limits2::default(), &e, &v, &cmd).err(),
			Some(RootError2::TrustMismatch)
		);
	}
}
#[test]
fn issuer_coordinates_join_lifecycle_and_retained_original_grants() {
	let mut retained_join = 0;
	for name in [
		"root_reserved",
		"root_accepted",
		"root_cut_first_refused",
		"root_precut_accepted_preserved",
		"root_renew_then_reserve_old_interval",
	] {
		let r = root(name);
		let mut s = r.state.clone();
		let free = s.covered.index + 1;
		s.covered = l(free + 1);
		let mut claimed = vec![s.bootstrap.index];
		claimed.extend(s.selectors.values().map(|a| a.enrolled.index));
		for a in s.authorities.values() {
			claimed.push(a.enrolled.index);
			claimed.extend(a.revoked.map(|l| l.index));
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
			for (enrolled, revoked) in [(l(i), None), (l(free), Some(l(i)))] {
				if revoked.is_some() && i <= free {
					continue;
				}
				let mut x = s.clone();
				x.issuers.insert(
					1u32.to_be_bytes(),
					IssuerRegistration2 {
						node: 1,
						key: key(1),
						enrolled,
						revoked,
					},
				);
				assert_eq!(validate_state(&x).err(), Some(RootError2::RootInvariant));
			}
		}
		for p in s.parents.values() {
			let original = p.claim.original.log.index;
			if !s.lineages.values().any(|g| g.latest.log.index == original) {
				let mut x = s.clone();
				x.issuers.insert(
					1u32.to_be_bytes(),
					IssuerRegistration2 {
						node: 1,
						key: key(1),
						enrolled: l(original),
						revoked: None,
					},
				);
				assert_eq!(validate_state(&x).err(), Some(RootError2::RootInvariant));
				x.parents = Map::new();
				validate_state(&x).unwrap();
				retained_join += 1;
			}
		}
	}
	assert!(retained_join > 0);
	// Permission enrollment/revocation also owns non-grant coordinates.
	let v: serde_json::Value =
		serde_json::from_str(include_str!("root_profile_vectors.json")).unwrap();
	let hex = v["p3_root_with_permissions"]["hex"].as_str().unwrap();
	let b = (0..hex.len())
		.step_by(2)
		.map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
		.collect::<Vec<_>>();
	let u = UnverifiedRoot2::decode(&b).unwrap();
	let (g, c, l, fp) = u.fixture_fields();
	let r = u.fixture_install(g, c, l, fp, hash(b"", &b)).unwrap();
	for i in [3, 4, 5] {
		let mut s = r.state.clone();
		s.issuers.insert(
			1u32.to_be_bytes(),
			IssuerRegistration2 {
				node: 1,
				key: key(1),
				enrolled: super::support::l(i),
				revoked: None,
			},
		);
		assert_eq!(validate_state(&s).err(), Some(RootError2::RootInvariant));
	}
}
#[test]
fn issuer_default_section_limit_and_slow_cached_usage_agree() {
	let mut r = root("root_bootstrap_only");
	let k = key(1);
	for node in 1..=REGISTRY_COUNT2 as u32 {
		r.state.issuers.insert(
			node.to_be_bytes(),
			IssuerRegistration2 {
				node,
				key: k,
				enrolled: l(node as u64 + 1),
				revoked: None,
			},
		);
	}
	r.state.covered = l(REGISTRY_COUNT2 as u64 + 1);
	r.state.derived = Derived2::build(&r.state).unwrap();
	assert_eq!(r.usage().unwrap().registry, REGISTRY_COUNT2 * 81);
	let _ = deny(&r, &enroll(REGISTRY_COUNT2 as u32 + 1, k), 11, None);
	let (n, out) = step(&r, &enroll(1, k));
	assert_eq!(out, ApplyResult2::Registered(l(2)));
	assert_eq!(n.state.issuers.len(), REGISTRY_COUNT2);
	let (n, _) = step(&n, &revoke(1));
	assert_eq!(n.usage().unwrap().registry, REGISTRY_COUNT2 * 81);
	let mut excessive = r.state.clone();
	excessive.issuers.insert(
		999999u32.to_be_bytes(),
		IssuerRegistration2 {
			node: 999999,
			key: k,
			enrolled: l(999999),
			revoked: None,
		},
	);
	assert_eq!(validate_state(&excessive).err(), Some(RootError2::Limit));
}

#[test]
fn issuer_revoke_and_cut_preserve_each_others_lifecycle() {
	let r = root("root_reserved");
	let (r, _) = step(&r, &enroll(123, key(1)));
	let lineages = r.state.lineages.clone();
	let (r, _) = step(&r, &revoke(123));
	assert_eq!(r.state.lineages, lineages);
	let issuers = r.state.issuers.clone();
	let current = epoch(&r.state);
	let (r, _) = step(
		&r,
		&Command2::Cut {
			expected: current,
			next: current + 1,
			configuration: [4; 32],
		},
	);
	assert_eq!(r.state.issuers, issuers);
}

#[test]
fn issuer_revoke_coordinates_collide_with_original_latest_and_parent_lifecycle() {
	// Explicit synthetic invariant fixture: move the parent lifecycle after a free issuer
	// enrollment. Validate it before each collision so no unrelated malformed field can pass.
	let mut s = root("root_renew_then_reserve_old_interval").state;
	let free = s.covered.index + 1;
	s.covered = l(free + 5);
	let parentkey = *s.parents.iter().next().unwrap().0;
	let lineage = s.parents.get(&parentkey).unwrap().claim.original.lineage;
	let p = s.parents.get_mut(&parentkey).unwrap();
	std::sync::Arc::make_mut(&mut p.claim).original.log = l(free + 1);
	p.reservation = l(free + 3);
	p.terminal = Some(Terminal2::Accepted {
		first: l(free + 4),
		evidence: [5; 32],
	});
	s.lineages.get_mut(&lineage).unwrap().latest.log = l(free + 2);
	s.issuers.insert(
		999u32.to_be_bytes(),
		IssuerRegistration2 {
			node: 999,
			key: key(1),
			enrolled: l(free),
			revoked: None,
		},
	);
	validate_state(&s).unwrap();
	for index in [free + 1, free + 2, free + 3, free + 4] {
		let mut x = s.clone();
		x.issuers.get_mut(&999u32.to_be_bytes()).unwrap().revoked = Some(l(index));
		assert_eq!(
			validate_state(&x).err(),
			Some(RootError2::RootInvariant),
			"revoke collides at {index}"
		);
		if index == free + 1 {
			x.parents = Map::new();
			validate_state(&x).unwrap(); // retained-original non_grants join
		}
	}
}
