use super::*;
#[test]
fn s1_cut_before_finalize_refuses_at_current_log_not_at_cut() {
	let r = root("root_reserved");
	let (r, _) = next(&r, &command("command_advance_epoch"), &[], 1);
	let anchor = r.covered();
	let (r, o) = next(&r, &finalized(&r), &[], 0);
	assert!(matches!(
		o,
		ApplyResult2::Parent {
			new_effect: false,
			..
		}
	));
	let p = r.lookup(&claim().manifest.identity).unwrap().unwrap();
	assert_eq!(
		p.terminal,
		Some(Terminal2::Refused {
			first: r.covered(),
			cause: 1,
			anchor
		})
	);
	let v = TrustedView2::fixture(
		r.state.generation,
		r.state.cluster,
		r.state.bootstrap,
		anchor.index,
	);
	assert!(
		r.lookup_at(&p.claim.manifest.identity, &v)
			.unwrap()
			.unwrap()
			.terminal
			.is_none()
	);
}
#[test]
fn s1_release_and_successor_resolve_without_epoch_change() {
	for successor in [false, true] {
		let r = root("root_reserved");
		let action = if successor {
			command("command_grant_successor")
		} else {
			command("command_release_grant")
		};
		let (r, o) = next(&r, &action, &[], 1);
		assert!(matches!(
			o,
			ApplyResult2::Granted(_) | ApplyResult2::Registered(_)
		));
		assert_eq!(epoch(&r.state), 1);
		let anchor = r.covered();
		let cause = if successor { 3 } else { 2 };
		let (r, o) = next(&r, &closure(&r, cause, anchor), &[], 0);
		assert!(matches!(
			o,
			ApplyResult2::Parent {
				new_effect: false,
				..
			}
		));
		assert_eq!(
			r.lookup(&claim().manifest.identity)
				.unwrap()
				.unwrap()
				.terminal,
			Some(Terminal2::Refused {
				first: r.covered(),
				cause,
				anchor
			})
		);
		let (_, o) = next(&r, &Command2::Reserve(claim()), &[], 0);
		assert!(matches!(o, ApplyResult2::Parent { .. }));
	}
}
#[test]
fn s1_closure_denies_new_reservation_but_preserves_first_terminal() {
	let r = root("root_renewed_no_parents");
	let g = r.state.lineages.values().next().unwrap().latest.clone();
	let (r, _) = next(
		&r,
		&Command2::Release {
			lineage: g.lineage,
			latest: g.reference().unwrap(),
		},
		&[],
		1,
	);
	let (_, o) = next(&r, &Command2::Reserve(claim()), &[original()], 1);
	assert_denied(&o, 4);
	let r = root("root_accepted");
	let accepted = r.lookup(&claim().manifest.identity).unwrap().unwrap();
	let (r, _) = next(&r, &command("command_release_grant"), &[], 1);
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
fn s1_stale_release_and_delayed_old_holder_grant_do_not_close_successor() {
	let r = root("root_renewed_no_parents");
	let (_, o) = next(&r, &command("command_release_grant"), &[], 1);
	assert_denied(&o, 7);
	let r = root("root_reserved");
	let (r, _) = next(&r, &command("command_grant_successor"), &[], 1);
	let (r, o) = next(&r, &command("command_grant_initial"), &[], 1);
	assert!(matches!(o, ApplyResult2::Closed { .. }));
	assert_eq!(r.state.lineages.len(), 2);
}
#[test]
fn s1_revocation_is_ordered_and_conflicts_do_not_overwrite_claims() {
	let r = root("root_reserved");
	let (r, _) = next(&r, &Command2::RevokeAuthority(claim().authority), &[], 1);
	let (r, _) = next(&r, &finalized(&r), &[], 1);
	assert!(matches!(
		r.lookup(&claim().manifest.identity)
			.unwrap()
			.unwrap()
			.terminal,
		Some(Terminal2::Refused { cause: 4, .. })
	));
	let mut changed = claim();
	changed.accounting_delta += 1;
	let (n, o) = next(&r, &Command2::Reserve(changed), &[], 1);
	assert_denied(&o, 2);
	assert_eq!(n.usage().unwrap(), r.usage().unwrap());
}
#[test]
fn s1_missing_reservation_and_malformed_quorums_never_imply_acceptance() {
	let r = root("root_reserved");
	let mut c = finalized(&r);
	if let Command2::Finalize { identity, .. } = &mut c {
		identity.sequence += 1
	}
	assert_denied(&next(&r, &c, &[], 1).1, 12);
	let mut c = finalized(&r);
	if let Command2::Finalize { attestation, .. } = &mut c {
		attestation.children[0].nodes.pop();
	}
	assert!(matches!(
		next(&r, &c, &[], 1).1,
		ApplyResult2::PendingEvidence(1)
	));
	let mut c = finalized(&r);
	if let Command2::Finalize { attestation, .. } = &mut c {
		attestation.children[0].nodes[1].incarnation[0] ^= 1;
	}
	assert!(matches!(
		next(&r, &c, &[], 1).1,
		ApplyResult2::RejectedEvidence(3)
	));
}
#[test]
fn s1_caller_or_envelope_conflict_and_unauthorized_apply_are_atomic() {
	let r = root("root_reserved");
	let c = finalized(&r);
	let (g, cl) = (r.state.generation, r.state.cluster);
	let e = TrustedEntry2::fixture(g, cl, l(6), &c).unwrap();
	let v = VerifiedCommand2::fixture(g, cl, &c, &[], 1)
		.unwrap()
		.unauthorized();
	let (n, o) = Root2::reduce(Some(&r), &e, &v, &c).unwrap();
	assert_denied(&o, 8);
	assert_eq!(n.usage().unwrap(), r.usage().unwrap());
	let v = VerifiedCommand2::fixture(g, cl, &command("command_cover_blank"), &[], 1).unwrap();
	assert!(Root2::reduce(Some(&r), &e, &v, &c).is_err());
}
