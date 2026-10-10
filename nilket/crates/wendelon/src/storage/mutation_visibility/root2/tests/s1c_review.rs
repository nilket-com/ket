use super::*;
fn grant(g: &OriginalGrant2) -> Command2 {
	Command2::Grant {
		lineage: g.lineage,
		incarnation: g.incarnation,
		lease: g.lease.clone(),
		predecessor: g.predecessor,
	}
}
// The corrupted profile-3 body goes to production; the oracle receives its bounded projection,
// which keeps every mutated byte.
fn compare_decode(bytes: &[u8], expected: RootError2) {
	assert_eq!(UnverifiedRoot2::decode(bytes).err(), Some(expected));
	assert_eq!(
		s1c_oracle::state::UnverifiedRoot2::decode(&s1c_oracle::legacy_profile2(bytes).unwrap())
			.err(),
		Some(expected)
	);
}
#[test]
fn s1c_closed_and_idempotent_records_remain_shared() {
	let r = root("root_reserved");
	let g = r.state.lineages.values().next().unwrap().latest.clone();
	let release = Command2::Release {
		lineage: g.lineage,
		latest: g.reference().unwrap(),
	};
	let (closed, _) = next(&r, &release, &[], 1);
	let (again, _) = next(&closed, &release, &[], 1);
	assert!(closed.state.lineages.shares_root(&again.state.lineages));
	let mut stale = g.reference().unwrap();
	stale.digest = [3; 32];
	let (denied, o) = next(
		&r,
		&Command2::Release {
			lineage: g.lineage,
			latest: stale,
		},
		&[],
		1,
	);
	assert_denied(&o, 7);
	assert!(r.state.lineages.shares_root(&denied.state.lineages));
	let (cut, _) = next(
		&closed,
		&Command2::Cut {
			expected: epoch(&closed.state),
			next: epoch(&closed.state) + 1,
			configuration: [5; 32],
		},
		&[],
		1,
	);
	assert!(closed.state.lineages.shares_root(&cut.state.lineages));
	let a = claim().authority;
	let (revoked, _) = next(&r, &Command2::RevokeAuthority(a), &[], 1);
	let (again, _) = next(&revoked, &Command2::RevokeAuthority(a), &[], 1);
	assert!(
		revoked
			.state
			.authorities
			.shares_root(&again.state.authorities)
	);
	let mut fresh = g.clone();
	fresh.lineage = [93; 16];
	fresh.incarnation = [94; 16];
	fresh.predecessor = None;
	fresh.lease.range_start = g.lease.range_end + 1;
	fresh.lease.range_end = g.lease.range_end + 100;
	let (mixed, o) = next(&closed, &grant(&fresh), &[], 1);
	assert!(matches!(o, ApplyResult2::Granted(_)));
	let (cut, _) = next(
		&mixed,
		&Command2::Cut {
			expected: epoch(&mixed.state),
			next: epoch(&mixed.state) + 1,
			configuration: [5; 32],
		},
		&[],
		1,
	);
	assert!(
		mixed
			.state
			.lineages
			.shares_value(&cut.state.lineages, &g.lineage)
	);
	assert!(
		!mixed
			.state
			.lineages
			.shares_value(&cut.state.lineages, &fresh.lineage)
	);
}
#[test]
fn s1c_reserve_original_before_bootstrap_matches_legacy_invariant_error() {
	let mut r = root("root_renewed_no_parents");
	r.state.bootstrap.index += 100;
	r.state.covered.index += 100;
	for key in r
		.state
		.selectors
		.iter()
		.map(|(k, _)| *k)
		.collect::<Vec<_>>()
	{
		r.state.selectors.get_mut(&key).unwrap().enrolled.index += 100;
	}
	for key in r
		.state
		.authorities
		.iter()
		.map(|(k, _)| *k)
		.collect::<Vec<_>>()
	{
		r.state.authorities.get_mut(&key).unwrap().enrolled.index += 100;
	}
	for key in r.state.lineages.iter().map(|(k, _)| *k).collect::<Vec<_>>() {
		r.state.lineages.get_mut(&key).unwrap().latest.log.index += 100;
	}
	r.state.derived = super::super::incremental::Derived2::build(&r.state).unwrap();
	let c = Command2::Reserve(claim());
	let e = TrustedEntry2::fixture(
		r.state.generation,
		r.state.cluster,
		l(r.covered().index + 1),
		&c,
	)
	.unwrap();
	let v = VerifiedCommand2::fixture(r.state.generation, r.state.cluster, &c, &[original()], 1)
		.unwrap();
	assert_eq!(
		s1c::checked_step(&r, &e, &v, &c).err(),
		Some(RootError2::RootInvariant)
	);
}
#[test]
fn s1c_multi_defect_decode_keeps_legacy_precedence() {
	let mut r = root("root_reserved");
	for _ in 0..2 {
		let ep = epoch(&r.state);
		r = next(
			&r,
			&Command2::Cut {
				expected: ep,
				next: ep + 1,
				configuration: [5; 32],
			},
			&[],
			1,
		)
		.0;
	}
	let encoded = r.encode().unwrap();
	let first = r.state.cuts.iter().next().unwrap();
	let second = r.state.cuts.iter().nth(1).unwrap();
	let from = second.bytes().unwrap();
	let mut dup = second.clone();
	dup.log.index = first.log.index;
	let to = dup.bytes().unwrap();
	let mut body = open_versioned(&encoded, b"VISAUT03", 3, ROOT_BYTES2)
		.unwrap()
		.to_vec();
	let hits = body
		.windows(from.len())
		.enumerate()
		.filter_map(|(i, b)| (b == from).then_some(i))
		.collect::<Vec<_>>();
	assert_eq!(hits.len(), 1);
	body[hits[0]..hits[0] + to.len()].copy_from_slice(&to);
	compare_decode(
		&envelope_versioned(b"VISAUT03", 3, &body, ROOT_BYTES2).unwrap(),
		RootError2::RootInvariant,
	);
	// Parse vector boundaries after the fixed header (selectors, authorities, permissions, lineages,
	// cuts), then force a later parent-count Limit while the earlier cuts still have duplicate
	// coordinates.
	let mut pos = 212usize;
	for _ in 0..5 {
		let count = u32::from_le_bytes(body[pos..pos + 4].try_into().unwrap()) as usize;
		pos += 4;
		for _ in 0..count {
			let len = u32::from_le_bytes(body[pos..pos + 4].try_into().unwrap()) as usize;
			pos += 4 + len;
		}
	}
	let mut excessive = body.clone();
	excessive[pos..pos + 4].copy_from_slice(&u32::MAX.to_le_bytes());
	compare_decode(
		&envelope_versioned(b"VISAUT03", 3, &excessive, ROOT_BYTES2).unwrap(),
		RootError2::Limit,
	);
	let mut zero = body.clone();
	zero[..16].fill(0);
	compare_decode(
		&envelope_versioned(b"VISAUT03", 3, &zero, ROOT_BYTES2).unwrap(),
		RootError2::Codec,
	);
	let mut trailing = body.clone();
	trailing.push(0);
	compare_decode(
		&envelope_versioned(b"VISAUT03", 3, &trailing, ROOT_BYTES2).unwrap(),
		RootError2::Codec,
	);
	let mut bad_wire = body.clone();
	bad_wire[16..32].fill(0);
	compare_decode(
		&envelope_versioned(b"VISAUT03", 3, &bad_wire, ROOT_BYTES2).unwrap(),
		RootError2::Codec,
	);
}
#[test]
fn s1c_lowered_limit_final_error_agrees_and_input_is_unchanged() {
	let mut r = root("root_reserved");
	r.limits.history = r.usage().unwrap().history - 1;
	let before = r.encode().unwrap();
	let c = command("command_cover_blank");
	let e = TrustedEntry2::fixture(
		r.state.generation,
		r.state.cluster,
		l(r.covered().index + 1),
		&c,
	)
	.unwrap();
	let v = VerifiedCommand2::fixture(r.state.generation, r.state.cluster, &c, &[], 1).unwrap();
	assert_eq!(
		s1c::checked_step(&r, &e, &v, &c).err(),
		Some(RootError2::Limit)
	);
	assert_eq!(before, r.encode().unwrap());
}
#[test]
fn s1c_lookup_at_and_epoch_at_agree_with_legacy_at_every_boundary() {
	let r = root("root_reserved");
	let r = next(&r, &finalized(&r), &[], 1).0;
	let ep = epoch(&r.state);
	let r = next(
		&r,
		&Command2::Cut {
			expected: ep,
			next: ep + 1,
			configuration: [7; 32],
		},
		&[],
		1,
	)
	.0;
	let old =
		s1c_oracle::install(&s1c_oracle::legacy_profile2(&r.encode().unwrap()).unwrap()).unwrap();
	for i in 0..=r.covered().index + 1 {
		for generation in [r.state.generation, [77; 16]] {
			let v = TrustedView2::fixture(generation, r.state.cluster, r.state.bootstrap, i);
			assert!(
				r.lookup_at(&claim().manifest.identity, &v)
					== old.lookup_at(&claim().manifest.identity, &v)
			);
			assert_eq!(r.epoch_at(&v), old.epoch_at(&v));
		}
	}
}
fn filled(mut r: Root2, count: usize) -> Root2 {
	let mut node = 3000u32;
	while r.state.derived.nodes.len() < count {
		let mut id = [0x77; 16];
		id[..4].copy_from_slice(&node.to_le_bytes());
		let c = Command2::EnrollAuthority {
			authority: AuthorityRef2 { id, revision: 1 },
			node: NodeRef2 {
				node,
				incarnation: id,
			},
			key: [5; 32],
		};
		let (n, o) = next(&r, &c, &[], 1);
		assert!(matches!(o, ApplyResult2::Registered(_)));
		r = n;
		node += 1;
	}
	r
}
#[test]
fn s1c_grant_node_cap_and_shared_pairs_match_legacy() {
	let r = filled(root("root_reserved"), 255);
	let g = r.state.lineages.values().next().unwrap().latest.clone();
	let mut other = g.clone();
	other.lineage = [90; 16];
	other.incarnation = [91; 16];
	other.predecessor = None;
	other.lease.holder = 2000;
	other.lease.range_start = g.lease.range_end + 1;
	other.lease.range_end = g.lease.range_end + 100;
	let (r, o) = next(&r, &grant(&other), &[], 1);
	assert!(matches!(o, ApplyResult2::Granted(_)));
	assert_eq!(r.state.derived.nodes.len(), 256);
	let mut third = other.clone();
	third.lineage = [92; 16];
	third.incarnation = [93; 16];
	third.lease.holder = 2001;
	third.lease.range_start = other.lease.range_end + 1;
	third.lease.range_end = other.lease.range_end + 100;
	let (denied, o) = next(&r, &grant(&third), &[], 1);
	assert_denied(&o, 11);
	assert!(denied.state.derived == r.state.derived);
	assert_eq!(denied.state.lineages, r.state.lineages);
	third.lease.holder = other.lease.holder;
	third.incarnation = other.incarnation;
	let (shared, o) = next(&r, &grant(&third), &[], 1);
	assert!(matches!(o, ApplyResult2::Granted(_)));
	assert_eq!(shared.state.derived.nodes.len(), 256);
}
#[test]
fn s1c_reserve_plan_node_cap_and_cross_table_shared_pairs_match_legacy() {
	let r = filled(root("root_renewed_no_parents"), 254);
	let holder = NodeRef2 {
		node: claim().original.lease.holder,
		incarnation: claim().original.incarnation,
	};
	let mut c = claim();
	for p in &mut c.plans {
		p.nodes = vec![
			holder,
			NodeRef2 {
				node: 2000,
				incarnation: [90; 16],
			},
			NodeRef2 {
				node: 2001,
				incarnation: [91; 16],
			},
		];
		p.nodes.sort();
	}
	let (r, o) = next(&r, &Command2::Reserve(c.clone()), &[original()], 1);
	assert!(matches!(
		o,
		ApplyResult2::Parent {
			new_effect: false,
			..
		}
	));
	assert_eq!(r.state.derived.nodes.len(), 256);
	assert_eq!(r.state.parents.len(), 1);
	c.manifest.identity.caller = [92; 16];
	for p in &mut c.plans {
		p.nodes[2] = NodeRef2 {
			node: 2002,
			incarnation: [93; 16],
		};
		p.nodes.sort();
	}
	let (denied, o) = next(&r, &Command2::Reserve(c.clone()), &[original()], 1);
	assert_denied(&o, 11);
	assert!(denied.state.derived == r.state.derived);
	assert_eq!(denied.state.parents, r.state.parents);
	// Reuse a node already recorded in the authority table and in the earlier plan.
	for p in &mut c.plans {
		p.nodes = vec![
			holder,
			NodeRef2 {
				node: 2000,
				incarnation: [90; 16],
			},
			r.state
				.authorities
				.values()
				.find(|a| a.node.node >= 3000)
				.unwrap()
				.node,
		];
		p.nodes.sort();
	}
	let (shared, o) = next(&r, &Command2::Reserve(c), &[original()], 1);
	assert!(matches!(
		o,
		ApplyResult2::Parent {
			new_effect: false,
			..
		}
	));
	assert_eq!(shared.state.derived.nodes.len(), 256);
	assert_eq!(shared.state.parents.len(), 2);
}
#[test]
fn s1c_seeded_successful_new_records_supersession_successor_renew_and_multi_cut() {
	for seed in 1..=8u64 {
		let mut r = root("root_reserved");
		let base = original();
		let mut random = seed;
		let mut counts = [0usize; 6];
		for cycle in 0..8u64 {
			random ^= random << 13;
			random ^= random >> 7;
			random ^= random << 17;
			let mut binding = claim().manifest.binding();
			binding.key_id = [(cycle + 80) as u8; 16];
			binding.key_version = 1 + (random % 5) as u32;
			let (n, o) = next(&r, &Command2::EnrollSelector(binding), &[], 1);
			assert!(matches!(o, ApplyResult2::Registered(_)));
			r = n;
			counts[0] += 1;
			let authority = AuthorityRef2 {
				id: [(cycle + 100) as u8; 16],
				revision: 1 + (random % 5),
			};
			let (n, o) = next(
				&r,
				&Command2::EnrollAuthority {
					authority,
					node: NodeRef2 {
						node: base.lease.holder,
						incarnation: base.incarnation,
					},
					key: [(random % 255 + 1) as u8; 32],
				},
				&[],
				1,
			);
			assert!(matches!(o, ApplyResult2::Registered(_)));
			r = n;
			counts[1] += 1;
			let mut a = base.clone();
			a.lineage = [(cycle * 3 + 170) as u8; 16];
			a.predecessor = None;
			a.lease.epoch = epoch(&r.state);
			a.lease.range_start = base.lease.range_end + 1 + cycle * 200;
			a.lease.range_end = a.lease.range_start + 99;
			let (n, o) = next(&r, &grant(&a), &[], 1);
			let ApplyResult2::Granted(g) = o else {
				panic!("new lineage denied")
			};
			r = n;
			a = g.latest;
			counts[2] += 1;
			let mut companion = a.clone();
			companion.lineage = [(cycle * 3 + 172) as u8; 16];
			companion.predecessor = None;
			companion.lease.range_start = a.lease.range_end + 1;
			companion.lease.range_end = a.lease.range_end + 100;
			let (n, o) = next(&r, &grant(&companion), &[], 1);
			assert!(matches!(o, ApplyResult2::Granted(_)));
			r = n;
			counts[2] += 1;

			let mut b = a.clone();
			b.lineage = [(cycle * 3 + 171) as u8; 16];
			b.predecessor = Some(a.reference().unwrap());
			b.lease.start.physical_ms += 1;
			b.lease.expiry.physical_ms += 1;
			let (n, o) = next(&r, &grant(&b), &[], 1);
			let ApplyResult2::Granted(g) = o else {
				panic!("supersession denied")
			};
			r = n;
			b = g.latest;
			counts[3] += 1;
			b.lease.start.physical_ms += 1;
			b.lease.expiry.physical_ms += 1;
			let (n, o) = next(&r, &grant(&b), &[], 1);
			assert!(matches!(o, ApplyResult2::Granted(_)));
			r = n;
			counts[4] += 1;
			let ep = epoch(&r.state);
			let open = r
				.state
				.lineages
				.values()
				.filter(|g| g.closure.is_none())
				.count();
			assert!(open >= 2);
			// Each cut closes two newly opened lineages while retaining all closed histories.
			if cycle == 0 {
				assert!(open >= 2);
			}
			let (n, o) = next(
				&r,
				&Command2::Cut {
					expected: ep,
					next: ep + 1,
					configuration: [(random % 255 + 1) as u8; 32],
				},
				&[],
				1,
			);
			assert!(matches!(o, ApplyResult2::Registered(_)));
			r = n;
			counts[5] += 1;
		}
		assert_eq!(counts, [8, 8, 16, 8, 8, 8]);
	}
}
