use super::*;
#[test]
fn s1_renewal_churn_is_constant_size_even_at_history_capacity() {
	let mut r = root("root_renewed_no_parents");
	let before = r.usage().unwrap();
	let mut lim = Limits2::default();
	lim.history = before.history;
	lim.lineages = 1;
	r = r.lower_limits(lim);
	for i in 0..2048 {
		let g = r.state.lineages.values().next().unwrap().latest.clone();
		let mut lease = g.lease.clone();
		lease.start.physical_ms += 1;
		lease.expiry.physical_ms += 1;
		let c = Command2::Grant {
			lineage: g.lineage,
			incarnation: g.incarnation,
			lease,
			predecessor: g.predecessor,
		};
		let (n, o) = next(&r, &c, &[], 1);
		assert!(matches!(o, ApplyResult2::Granted(_)), "{i}");
		assert_eq!(n.usage().unwrap(), before);
		r = n;
	}
}
#[test]
fn s1_older_in_lineage_grant_survives_renewal_with_its_own_interval() {
	let r = root("root_renewed_no_parents");
	let c = claim();
	assert!(c.stamps[0].hlc < r.state.lineages.values().next().unwrap().latest.lease.start);
	let (n, o) = next(&r, &Command2::Reserve(c.clone()), &[original()], 1);
	assert!(matches!(
		o,
		ApplyResult2::Parent {
			new_effect: false,
			..
		}
	));
	assert_eq!(
		n.lookup(&c.manifest.identity)
			.unwrap()
			.unwrap()
			.claim
			.original,
		original()
	);
	let (n2, o) = next(&n, &finalized(&n), &[], 1);
	assert!(matches!(
		o,
		ApplyResult2::Parent {
			new_effect: true,
			..
		}
	));
	assert_eq!(n2.usage().unwrap(), n.usage().unwrap());
}
#[test]
fn s1_signed_uncommitted_old_snapshot_has_no_historical_authority() {
	let r = root("root_renewed_no_parents");
	let (n, o) = next(&r, &Command2::Reserve(claim()), &[], 1);
	assert_denied(&o, 9);
	assert_eq!(n.usage().unwrap(), r.usage().unwrap());
	let mut c = claim();
	c.original.log = l(3);
	let (n, o) = next(&r, &Command2::Reserve(c), &[], 1);
	assert_denied(&o, 9);
	assert_eq!(n.usage().unwrap(), r.usage().unwrap());
}
#[test]
fn s1_reserved_claim_is_immutable_after_later_renewal() {
	let r = root("root_reserved");
	let parent = r.lookup(&claim().manifest.identity).unwrap().unwrap();
	let (n, o) = next(&r, &command("command_grant_renewal"), &[], 1);
	assert!(matches!(o, ApplyResult2::Granted(_)));
	assert_eq!(
		n.lookup(&parent.claim.manifest.identity).unwrap().unwrap(),
		parent
	);
	let (n2, o) = next(&n, &Command2::Reserve(parent.claim.clone()), &[], 1);
	assert!(matches!(
		o,
		ApplyResult2::Parent {
			new_effect: false,
			..
		}
	));
	assert_eq!(n2.usage().unwrap(), n.usage().unwrap());
}
#[test]
fn s1_capacity_equality_then_plus_one_and_finalize_at_exhaustion() {
	let r = root("root_renewed_no_parents");
	let charge = claim().held_bytes().unwrap() as usize;
	let mut lim = Limits2::default();
	lim.slots = 1;
	lim.parent = charge;
	let r = r.lower_limits(lim);
	let (r, o) = next(&r, &Command2::Reserve(claim()), &[original()], 1);
	assert!(matches!(o, ApplyResult2::Parent { .. }));
	let mut c = claim();
	c.manifest.identity.sequence += 1;
	let (n, o) = next(&r, &Command2::Reserve(c), &[original()], 1);
	assert_denied(&o, 10);
	let (n2, o) = next(&n, &finalized(&n), &[], 1);
	assert!(matches!(
		o,
		ApplyResult2::Parent {
			new_effect: true,
			..
		}
	));
	assert_eq!(n2.usage().unwrap(), r.usage().unwrap());
}
#[test]
fn s1_first_terminal_and_single_acceptance_effect_survive_epoch_cut() {
	let r = root("root_reserved");
	let (r, o) = next(&r, &finalized(&r), &[], 1);
	assert!(matches!(
		o,
		ApplyResult2::Parent {
			new_effect: true,
			..
		}
	));
	let accepted = r.lookup(&claim().manifest.identity).unwrap().unwrap();
	let (r, _) = next(&r, &command("command_advance_epoch"), &[], 1);
	let (r, o) = next(&r, &finalized(&r), &[], 0);
	assert!(matches!(
		o,
		ApplyResult2::Parent {
			new_effect: false,
			..
		}
	));
	assert_eq!(
		r.lookup(&claim().manifest.identity).unwrap().unwrap(),
		accepted
	);
}
#[test]
fn s1_monotone_full_hlc_grant_orders_and_exact_latest_replay() {
	let r = root("root_renewed_no_parents");
	let before = r.usage().unwrap();
	let (_, o) = next(&r, &command("command_grant_initial"), &[], 1);
	assert_denied(&o, 5);
	let mut c = command("command_grant_renewal");
	if let Command2::Grant { lease, .. } = &mut c {
		lease.start.logical = 1;
		lease.expiry.logical = 1
	};
	let (r, o) = next(&r, &c, &[], 1);
	assert!(matches!(o, ApplyResult2::Granted(_)));
	let (r, o) = next(&r, &c, &[], 1);
	assert!(matches!(o, ApplyResult2::Granted(_)));
	assert_eq!(r.usage().unwrap(), before);
	let mut c2 = c.clone();
	if let Command2::Grant { lease, .. } = &mut c2 {
		lease.start.physical_ms += 1;
		lease.expiry.physical_ms -= 1
	};
	assert_denied(&next(&r, &c2, &[], 1).1, 5);
}
#[test]
fn s1_unknown_evidence_does_not_commit_or_advance_acceptance() {
	let r = root("root_reserved");
	let (r, o) = next(&r, &finalized(&r), &[], 0);
	assert!(matches!(o, ApplyResult2::PendingEvidence(2)));
	assert!(
		r.lookup(&claim().manifest.identity)
			.unwrap()
			.unwrap()
			.terminal
			.is_none()
	);
	let (r, o) = next(&r, &finalized(&r), &[], 2);
	assert!(matches!(o, ApplyResult2::RejectedEvidence(1)));
	assert!(
		r.lookup(&claim().manifest.identity)
			.unwrap()
			.unwrap()
			.terminal
			.is_none()
	);
}
