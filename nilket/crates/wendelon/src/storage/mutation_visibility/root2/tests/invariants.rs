use super::*;
#[test]
fn s1_generation_tenant_schema_caller_identity_and_selector_are_bound() {
	let r = root("root_renewed_no_parents");
	for dimension in 0..4 {
		let mut c = claim();
		match dimension {
			0 => c.manifest.identity.generation = [3; 16],
			1 => c.manifest.identity.tenant = 2,
			2 => {
				c.manifest.identity.scope = super::super::super::MutationScope::Table;
				c.manifest.identity.table = [2; 16];
				c.manifest.identity.schema_generation = 1;
				c.manifest.operation = super::super::super::MutationOperation::Insert;
			}
			_ => c.manifest.identity.caller = [0; 16],
		};
		if dimension == 3 {
			assert!(c.encode().is_err())
		} else if dimension == 2 {
			let id = c.manifest.identity;
			let (n, o) = next(&r, &Command2::Reserve(c), &[original()], 1);
			assert!(matches!(o, ApplyResult2::Parent { .. }));
			assert!(n.lookup(&id).unwrap().is_some());
			assert!(n.lookup(&claim().manifest.identity).unwrap().is_none());
		} else {
			let (_, o) = next(&r, &Command2::Reserve(c), &[original()], 1);
			assert_denied(&o, 9);
		}
	}
	let r = root("root_reserved");
	let mut c = claim();
	c.manifest.identity.caller = [4; 16];
	let (_, o) = next(&r, &Command2::Reserve(c), &[original()], 1);
	assert!(matches!(o, ApplyResult2::Parent { .. }));
}
#[test]
fn s1_codec_rejects_wrong_hlc_coordinates_and_original_grant_term_node() {
	let r = root("root_renewed_no_parents");
	for dimension in 0..5 {
		let mut c = claim();
		match dimension {
			0 => c.stamps[0].child = 1,
			1 => c.stamps[0].row = 1,
			2 => c.stamps[0].hlc = c.original.lease.expiry,
			3 => c.stamps[0].hlc.node = 0,
			_ => c.plans[0].nodes[1] = c.plans[0].nodes[0],
		};
		assert!(c.encode().is_err());
	}
	let mut c = claim();
	let latest = r.state.lineages.values().next().unwrap().latest.clone();
	c.original = latest.clone();
	c.stamps[0].hlc = latest.lease.start;
	c.original.log.term += 1;
	assert!(!old_original_fits(&c.original, &latest));
}
#[test]
fn s1_zero_or_overflow_limits_do_not_admit_new_parent_and_release_needs_no_capacity() {
	let r = root("root_renewed_no_parents");
	for delta in [0usize, 1] {
		let mut limits = Limits2::default();
		limits.slots = 1;
		limits.parent = claim().held_bytes().unwrap() as usize - 1 - delta;
		let x = r.clone().lower_limits(limits);
		let (n, o) = next(&x, &Command2::Reserve(claim()), &[original()], 1);
		assert_denied(&o, 10);
		assert_eq!(n.usage().unwrap(), x.usage().unwrap());
	}
	let mut limits = Limits2::default();
	limits.history = r.usage().unwrap().history;
	limits.lineages = 1;
	let r = r.lower_limits(limits);
	let g = r.state.lineages.values().next().unwrap().latest.clone();
	let (n, o) = next(
		&r,
		&Command2::Release {
			lineage: g.lineage,
			latest: g.reference().unwrap(),
		},
		&[],
		1,
	);
	assert!(matches!(o, ApplyResult2::Registered(_)));
	assert_eq!(n.usage().unwrap(), r.usage().unwrap());
	let mut corrupt = r.state.clone();
	corrupt.parents.insert(
		claim().manifest.identity.encode().unwrap(),
		ParentRecord2 {
			claim: claim(),
			reservation: l(6),
			held_bytes: u64::MAX,
			terminal: None,
		},
	);
	assert!(capacity(&corrupt, Limits2::default()).is_err());
}
#[test]
fn s1_resolver_requires_actual_earliest_closure_and_keeps_reserved_without_one() {
	let r = root("root_reserved");
	let (_, o) = next(&r, &closure(&r, 1, l(4)), &[], 1);
	assert_denied(&o, 14);
	let (r, _) = next(&r, &command("command_release_grant"), &[], 1);
	let (_, o) = next(&r, &closure(&r, 1, r.covered()), &[], 1);
	assert_denied(&o, 15);
	let (r, _) = next(&r, &command("command_advance_epoch"), &[], 1);
	let (r, o) = next(&r, &finalized(&r), &[], 1);
	assert!(matches!(
		o,
		ApplyResult2::Parent {
			new_effect: false,
			..
		}
	));
	assert!(matches!(
		r.lookup(&claim().manifest.identity)
			.unwrap()
			.unwrap()
			.terminal,
		Some(Terminal2::Refused { cause: 2, .. })
	));
}
#[test]
fn s1_rf1_has_real_holder_threshold_and_duplicate_ack_cannot_make_quorum() {
	let r = root("root_renewed_no_parents");
	let mut c = claim();
	c.plans[0].nodes.truncate(1);
	let (r, o) = next(&r, &Command2::Reserve(c.clone()), &[original()], 1);
	assert!(matches!(o, ApplyResult2::Parent { .. }));
	let p = r.lookup(&c.manifest.identity).unwrap().unwrap();
	let mut a = attestation();
	a.children[0].nodes.truncate(1);
	a.claim_digest = c.digest().unwrap();
	a.reservation = p.reservation;
	let cmd = Command2::Finalize {
		identity: c.manifest.identity,
		digest: c.digest().unwrap(),
		reservation: p.reservation,
		attestation: a.clone(),
	};
	assert!(matches!(
		next(&r, &cmd, &[], 1).1,
		ApplyResult2::Parent {
			new_effect: true,
			..
		}
	));
	let duplicate = a.children[0].nodes[0];
	a.children[0].nodes.push(duplicate);
	assert!(a.encode().is_err());
}
#[test]
fn s1_restoration_refuses_missing_selector_lineage_and_grant_as_non_grant_entry() {
	let r = root("root_reserved");
	for k in 0..3 {
		let mut s = r.state.clone();
		match k {
			0 => s.selectors.clear(),
			1 => s.lineages.clear(),
			_ => {
				let key = *s.parents.iter().next().unwrap().0;
				let p = s.parents.get_mut(&key).unwrap();
				std::sync::Arc::make_mut(&mut p.claim).original.log = l(3);
			}
		}
		assert!(validate_state(&s).is_err());
	}
}
