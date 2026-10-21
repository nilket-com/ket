use super::*;
#[test]
fn s1c_existing_goldens_equal_legacy_decoder_and_codec() {
	for name in ["root_reserved", "root_renewed_no_parents"] {
		let r = root(name);
		let bytes = super::s1c_oracle::s1c_projection(&r.encode().unwrap()).unwrap();
		let legacy = super::s1c_oracle::state::UnverifiedRoot2::decode(&bytes).unwrap();
		assert_eq!(legacy.encode().unwrap(), bytes);
		assert!(
			r.state.derived
				== crate::storage::mutation_visibility::root2::incremental::Derived2::build(
					&r.state
				)
				.unwrap()
		);
	}
}

pub(super) fn checked_step(
	root: &Root2,
	e: &TrustedEntry2,
	v: &VerifiedCommand2,
	c: &Command2,
) -> Result2<(Root2, ApplyResult2)> {
	// Existing fixtures can edit private DTO-like fields. Re-seal through the full validator first.
	let mut sealed = root.clone();
	sealed.state.derived =
		crate::storage::mutation_visibility::root2::incremental::Derived2::build(&sealed.state)?;
	let before = sealed.encode()?;
	let expected = super::s1c_oracle::reduce(
		&super::s1c_oracle::s1c_projection(&before)?,
		sealed.limits,
		e,
		v,
		c,
	);
	let actual = Root2::reduce(Some(&sealed), e, v, c);
	// D-G2 precondition: the projection drops the permission section, so a step that a permission
	// decides is never compared.
	if let Ok((_, ApplyResult2::Denied { reason, .. })) = &actual {
		assert!(
			!matches!(reason, 18 | 19),
			"Denied({reason}) is a permission refusal; the S1c oracle cannot compare it"
		);
	}
	assert_eq!(sealed.encode()?, before);
	match (&actual, expected) {
		(Ok((n, o)), Ok((bytes, old_o))) => {
			assert!(o == &old_o, "outcome differs from frozen S1 oracle");
			assert_eq!(super::s1c_oracle::s1c_projection(&n.encode()?)?, bytes);
			assert_eq!(
				o.encode(n.state.generation, n.state.cluster, n.covered())?,
				old_o.encode(n.state.generation, n.state.cluster, n.covered())?
			);
			validate_state(&n.state)?;
			assert!(
				n.state.derived
					== crate::storage::mutation_visibility::root2::incremental::Derived2::build(
						&n.state
					)?
			);
			n.state.parents.audit();
			n.state.lineages.audit();
			n.state.selectors.audit();
			n.state.authorities.audit();
		}
		(Err(a), Err(b)) => assert_eq!(*a, b),
		_ => panic!("function result differs from legacy S1 oracle"),
	}
	actual
}
#[test]
fn s1c_serial_sequences_match_legacy_outcomes_bytes_and_summaries() {
	for seed in 1..=8u64 {
		let mut rng = seed;
		let mut r = root("root_reserved");
		let mut grants = vec![original()];
		for step in 0..256 {
			rng ^= rng << 13;
			rng ^= rng >> 7;
			rng ^= rng << 17;
			let choice = (rng % 11) as usize;
			let c = match choice {
				0 => Command2::Cover {
					kind: (rng % 3) as u8,
					external: [step as u8; 32],
				},
				1 => {
					let g = r.state.lineages.values().next().unwrap().latest.clone();
					let mut lease = g.lease.clone();
					lease.start.physical_ms += 1;
					lease.expiry.physical_ms += 1;
					Command2::Grant {
						lineage: g.lineage,
						incarnation: g.incarnation,
						lease,
						predecessor: g.predecessor,
					}
				}
				2 => {
					let mut c = claim();
					c.manifest.identity.caller = [(step % 128 + 1) as u8; 16];
					Command2::Reserve(c)
				}
				3 => Command2::EnrollSelector(claim().manifest.binding()),
				4 => {
					let a = r.state.authorities.values().next().unwrap();
					Command2::EnrollAuthority {
						authority: a.authority,
						node: a.node,
						key: a.public_key,
					}
				}
				5 => Command2::RevokeAuthority(claim().authority),
				6 => {
					let g = r.state.lineages.values().next().unwrap().latest.clone();
					Command2::Release {
						lineage: g.lineage,
						latest: g.reference().unwrap(),
					}
				}
				7 => {
					let e = epoch(&r.state);
					Command2::Cut {
						expected: e,
						next: e + 1,
						configuration: [step as u8; 32],
					}
				}
				8 => finalized(&r),
				9 => closure(&r, 1, l(1)),
				_ => command("command_bootstrap"),
			};
			let e = TrustedEntry2::fixture(
				r.state.generation,
				r.state.cluster,
				l(r.covered().index + 1),
				&c,
			)
			.unwrap();
			let mut v = VerifiedCommand2::fixture(
				r.state.generation,
				r.state.cluster,
				&c,
				&grants,
				(rng % 3) as u8,
			)
			.unwrap();
			if rng % 13 == 0 {
				v = v.unauthorized();
			}
			if let Ok((n, o)) = checked_step(&r, &e, &v, &c) {
				if let ApplyResult2::Granted(g) = o {
					grants.push(g.latest);
				}
				r = n;
			}
		}
	}
}
#[test]
fn s1c_trust_gap_and_replay_match_legacy_errors() {
	let r = root("root_reserved");
	let c = Command2::Cover {
		kind: 0,
		external: [7; 32],
	};
	for (idx, error) in [
		(r.covered().index - 1, RootError2::LineageGap),
		(r.covered().index, RootError2::TrustMismatch),
		(r.covered().index + 2, RootError2::LineageGap),
	] {
		let e = TrustedEntry2::fixture(r.state.generation, r.state.cluster, l(idx), &c).unwrap();
		let v = VerifiedCommand2::fixture(r.state.generation, r.state.cluster, &c, &[], 1).unwrap();
		assert_eq!(checked_step(&r, &e, &v, &c).err(), Some(error), "{idx}");
	}
	let cmd = command("command_reserve"); // use a self-consistent replay built from a completed new entry
	let (n, _) = next(&r, &cmd, &[original()], 1);
	let e = TrustedEntry2::fixture(n.state.generation, n.state.cluster, n.covered(), &cmd).unwrap();
	let v = VerifiedCommand2::fixture(n.state.generation, n.state.cluster, &cmd, &[original()], 1)
		.unwrap();
	assert!(matches!(
		checked_step(&n, &e, &v, &cmd).unwrap().1,
		ApplyResult2::AlreadyProcessed
	));
}
#[test]
fn s1c_persistent_map_order_balance_replacement_and_old_versions() {
	use crate::storage::mutation_visibility::root2::persistent::Map;
	use std::collections::BTreeMap;
	for pattern in 0..3 {
		let mut actual = Map::new();
		let mut expected = BTreeMap::new();
		let mut saved = Vec::new();
		for i in 0..2048u64 {
			let key = match pattern {
				0 => i,
				1 => 2047 - i,
				_ => {
					if i % 2 == 0 {
						i / 2
					} else {
						2047 - i / 2
					}
				}
			};
			actual.insert(key, key * 3);
			expected.insert(key, key * 3);
			if i % 64 == 0 {
				actual.audit();
				assert!(actual.iter().eq(expected.iter()));
				saved.push((actual.clone(), expected.clone()));
			}
		}
		for i in 0..2048u64 {
			actual.insert(i, i * 7);
			expected.insert(i, i * 7);
		}
		actual.audit();
		assert!(actual.iter().eq(expected.iter()));
		for (a, e) in saved {
			a.audit();
			assert!(a.iter().eq(e.iter()));
		}
	}
	let mut actual = Map::<u64, u64>::new();
	let mut expected = BTreeMap::new();
	let mut seed = 0x6295_43b2_91e7_a10du64;
	let mut saved = Vec::new();
	for op in 0..10000 {
		seed = seed
			.wrapping_mul(6364136223846793005)
			.wrapping_add(1442695040888963407);
		let key = (seed >> 17) % 1024;
		let old = actual.clone();
		let previous = expected.clone();
		match seed % 3 {
			0 => {
				actual.insert(key, seed);
				expected.insert(key, seed);
			}
			1 => {
				assert_eq!(actual.remove(&key), expected.remove(&key));
				for other in expected.keys() {
					assert!(actual.shares_value(&old, other));
				}
			}
			_ => assert_eq!(actual.get(&key), expected.get(&key)),
		}
		actual.audit();
		old.audit();
		assert!(actual.iter().eq(expected.iter()));
		assert!(old.iter().eq(previous.iter()));
		let unchanged = actual.clone();
		assert_eq!(actual.remove(&2048), None);
		assert!(actual.shares_root(&unchanged));
		if op % 256 == 0 {
			saved.push((actual.clone(), expected.clone()));
		}
	}
	for (map, reference) in saved {
		map.audit();
		assert!(map.iter().eq(reference.iter()));
	}
}
#[test]
fn s1c_shared_values_are_released_after_all_retained_roots_drop() {
	use crate::storage::mutation_visibility::root2::persistent::Map;
	use std::sync::Arc;
	let payload = Arc::new(vec![9u8; 4096]);
	let weak = Arc::downgrade(&payload);
	let mut map: Map<u64, Arc<Vec<u8>>> = Map::new();
	map.insert(1u64, payload.clone());
	drop(payload);
	let old = map.clone();
	map.insert(1u64, Arc::new(vec![8u8; 4096]));
	assert!(weak.upgrade().is_some());
	drop(map);
	assert!(weak.upgrade().is_some());
	drop(old);
	assert!(weak.upgrade().is_none());
}
#[test]
fn s1c_default_node_limit_rolls_back_record_and_all_derived_changes() {
	let mut r = root("root_reserved");
	let mut node = 3000u32;
	while r.state.derived.nodes.len() < 256 {
		let mut id = [0x77; 16];
		id[..4].copy_from_slice(&node.to_le_bytes());
		let c = Command2::EnrollAuthority {
			authority: AuthorityRef2 { id, revision: 1 },
			node: NodeRef2 {
				node,
				incarnation: [0x7a; 16],
			},
			key: [5; 32],
		};
		let (n, o) = next(&r, &c, &[], 1);
		assert!(matches!(o, ApplyResult2::Registered(_)));
		r = n;
		node += 1;
	}
	let before = r.state.derived.clone();
	let old_len = r.state.authorities.len();
	let mut id = [0x77; 16];
	id[..4].copy_from_slice(&node.to_le_bytes());
	let c = Command2::EnrollAuthority {
		authority: AuthorityRef2 { id, revision: 1 },
		node: NodeRef2 {
			node,
			incarnation: [0x7a; 16],
		},
		key: [5; 32],
	};
	let (n, o) = next(&r, &c, &[], 1);
	assert_denied(&o, 11);
	assert_eq!(n.state.authorities.len(), old_len);
	assert!(n.state.derived == before);
	assert_eq!(n.covered().index, r.covered().index + 1);
}
#[test]
fn s1c_terminal_update_retains_one_shared_claim_and_does_not_modify_old_root() {
	let r = root("root_reserved");
	let key = claim().manifest.identity.encode().unwrap();
	let original_claim = r.state.parents.get(&key).unwrap().claim.clone();
	let weak = std::sync::Arc::downgrade(&original_claim);
	let (n, o) = next(&r, &finalized(&r), &[], 1);
	assert!(matches!(
		o,
		ApplyResult2::Parent {
			new_effect: true,
			..
		}
	));
	assert!(std::sync::Arc::ptr_eq(
		&original_claim,
		&n.state.parents.get(&key).unwrap().claim
	));
	assert!(r.state.parents.get(&key).unwrap().terminal.is_none());
	assert!(n.state.parents.get(&key).unwrap().terminal.is_some());
	drop(original_claim);
	drop(r);
	assert!(weak.upgrade().is_some());
	drop(n);
	assert!(weak.upgrade().is_none());
}
#[test]
fn s1c_path_copy_mutation_never_changes_a_retained_version() {
	use crate::storage::mutation_visibility::root2::persistent::Map;
	let mut m: Map<u64, Vec<u8>> = Map::new();
	for i in 0..128 {
		m.insert(i, vec![i as u8]);
	}
	let old = m.clone();
	m.get_mut(&64).unwrap().push(9);
	assert_eq!(old.get(&64).unwrap(), &vec![64]);
	assert_eq!(m.get(&64).unwrap(), &vec![64, 9]);
	old.audit();
	m.audit();
}
#[test]
fn s1c_bootstrap_and_unauthorized_bootstrap_match_legacy() {
	let c = command("command_bootstrap_p3");
	let g = [4; 16];
	let cluster = [5; 16];
	let e = TrustedEntry2::fixture(g, cluster, l(1), &c).unwrap();
	for authorized in [true, false] {
		let mut v = VerifiedCommand2::fixture(g, cluster, &c, &[], 1).unwrap();
		if !authorized {
			v = v.unauthorized();
		}
		let expected = super::s1c_oracle::bootstrap(&e, &v, &c);
		let actual = Root2::reduce(None, &e, &v, &c);
		match (actual, expected) {
			(Ok((n, o)), Ok((bytes, old_o))) => {
				assert_eq!(
					super::s1c_oracle::s1c_projection(&n.encode().unwrap()).unwrap(),
					bytes
				);
				assert!(o == old_o);
				assert!(
					n.state.derived
						== crate::storage::mutation_visibility::root2::incremental::Derived2::build(&n.state).unwrap()
				);
			}
			(Err(a), Err(b)) => assert_eq!(a, b),
			_ => panic!("bootstrap differs"),
		}
	}
}
#[test]
fn s1c_every_root_golden_and_corrupt_envelope_agree_with_legacy() {
	let goldens: serde_json::Value = serde_json::from_str(include_str!("goldens.json")).unwrap();
	let (mut legacy_count, mut issuer_count) = (0, 0);
	for item in goldens["fixtures"].as_array().unwrap() {
		let name = item["name"].as_str().unwrap();
		if !name.starts_with("root_") {
			continue;
		}
		let bytes = fixture(name);
		if matches!(name, "root_one_issuer" | "root_numeric_issuers") {
			assert_eq!(
				super::s1c_oracle::s1c_projection(&bytes),
				Err(RootError2::Codec)
			);
			assert_eq!(
				UnverifiedRoot2::decode(&bytes).unwrap().encode().unwrap(),
				bytes
			);
			let mut corrupt = bytes.clone();
			let last = corrupt.len() - 1;
			corrupt[last] ^= 1;
			assert_eq!(
				UnverifiedRoot2::decode(&corrupt).err(),
				Some(RootError2::Codec)
			);
			issuer_count += 1;
			continue;
		}
		legacy_count += 1;
		let legacy = super::s1c_oracle::s1c_projection(&bytes).unwrap();
		let old = super::s1c_oracle::state::UnverifiedRoot2::decode(&legacy).unwrap();
		let n = UnverifiedRoot2::decode(&bytes).unwrap();
		assert_eq!(old.encode().unwrap(), legacy);
		assert_eq!(n.encode().unwrap(), bytes);
		// The projection strips exactly the twin's permission section (one record on the six
		// parent-lineage roots, none on root_bootstrap_only) and the two section counts, and keeps
		// the profile-3 control length opaque.
		let permissions = root(name).state.permissions.len();
		assert_eq!(
			permissions,
			usize::from(name != "root_bootstrap_only"),
			"{name}"
		);
		assert_eq!(legacy.len(), bytes.len() - 8 - 117 * permissions, "{name}");
		assert_eq!(legacy[192..200], 241u64.to_le_bytes(), "{name}");
		// The same checksum mutation, applied to each profile's own bytes.
		let flip = |b: &[u8]| {
			let mut b = b.to_vec();
			let last = b.len() - 1;
			b[last] ^= 1;
			b
		};
		let a = UnverifiedRoot2::decode(&flip(&bytes)).err();
		let b = super::s1c_oracle::state::UnverifiedRoot2::decode(&flip(&legacy)).err();
		assert_eq!(a, b);
		assert!(a.is_some());
	}
	assert_eq!(legacy_count, 7);
	assert_eq!(issuer_count, 2);
}
