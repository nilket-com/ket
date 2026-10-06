use super::*;
fn grant_of(g: &OriginalGrant2) -> Command2 {
	Command2::Grant {
		lineage: g.lineage,
		incarnation: g.incarnation,
		lease: g.lease.clone(),
		predecessor: g.predecessor,
	}
}
fn replace_lineage(root: &Root2, old: &LineageRecord2, new: &LineageRecord2) -> Vec<u8> {
	let encoded = root.encode().unwrap();
	let mut body = open(&encoded, b"VISAUT02", ROOT_BYTES2).unwrap().to_vec();
	let from = old.bytes().unwrap();
	let to = new.bytes().unwrap();
	assert_eq!(from.len(), to.len());
	let hits = body
		.windows(from.len())
		.enumerate()
		.filter_map(|(i, b)| (b == from).then_some(i))
		.collect::<Vec<_>>();
	assert_eq!(hits.len(), 1);
	body[hits[0]..hits[0] + to.len()].copy_from_slice(&to);
	envelope(b"VISAUT02", &body, ROOT_BYTES2).unwrap()
}
#[test]
fn s1_d1_released_ranges_cannot_bypass_same_epoch_floors_by_widening() {
	let r = root("root_reserved");
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
	for (lo, hi) in [
		(g.lease.range_start, g.lease.range_end + 1),
		(g.lease.range_start + 1, g.lease.range_end),
		(g.lease.range_end, g.lease.range_end + 1),
	] {
		let mut other = g.clone();
		other.lineage = [99; 16];
		other.incarnation = [98; 16];
		other.predecessor = None;
		other.lease.range_start = lo;
		other.lease.range_end = hi;
		other.lease.start.physical_ms -= 1;
		other.lease.min_commit_index = 0;
		for predecessor in [None, Some(g.reference().unwrap())] {
			other.predecessor = predecessor;
			let (n, o) = next(&r, &grant_of(&other), &[], 1);
			assert_denied(&o, 6);
			assert_eq!(n.usage().unwrap(), r.usage().unwrap());
			assert_eq!(n.state.lineages, r.state.lineages);
			assert_eq!(n.covered().index, r.covered().index + 1);
		}
	}
	let mut other = g.clone();
	other.lineage = [99; 16];
	other.incarnation = [98; 16];
	other.predecessor = None;
	other.lease.range_start = g.lease.range_end + 1;
	other.lease.range_end = g.lease.range_end + 100;
	let (disjoint, outcome) = next(&r, &grant_of(&other), &[], 1);
	assert!(matches!(outcome, ApplyResult2::Granted(_)));
	let old = disjoint.state.lineages.get(&other.lineage).unwrap().clone();
	let mut overlap = old.clone();
	overlap.latest.lease.range_start = g.lease.range_end;
	assert!(matches!(
		UnverifiedRoot2::decode(&replace_lineage(&disjoint, &old, &overlap)),
		Err(RootError2::RootInvariant)
	));
}
#[test]
fn s1_d2_committed_semantic_grant_refusals_advance_coverage() {
	for kind in 0..4 {
		let r = root("root_bootstrap_only");
		let mut c = command("command_grant_initial");
		if let Command2::Grant {
			lineage,
			lease,
			predecessor,
			..
		} = &mut c
		{
			lease.min_commit_index = 0;
			if kind == 0 {
				lease.min_commit_index = r.covered().index + 2;
			} else {
				*predecessor = Some(GrantRef2 {
					lineage: if kind == 1 { *lineage } else { [99; 16] },
					log: l(if kind == 1 { 1 } else { kind as u64 }),
					digest: [7; 32],
				});
			}
		}
		let frame = c.encode(r.state.generation, r.state.cluster).unwrap();
		assert_eq!(Command2::decode(&frame).unwrap().2, c);
		let (n, o) = next(&r, &c, &[], 1);
		assert_denied(&o, if kind == 0 { 17 } else { 7 });
		assert_eq!(n.usage().unwrap(), r.usage().unwrap());
		assert_eq!(n.covered().index, r.covered().index + 1);
		let (n, o) = next(&n, &command("command_cover_blank"), &[], 1);
		assert!(matches!(o, ApplyResult2::Covered));
		assert_eq!(n.covered().index, 3);
	}
}
#[test]
fn s1_d3_exact_closed_replay_reports_closed_after_epoch_cut() {
	let r = root("root_reserved");
	let (r, _) = next(&r, &command("command_advance_epoch"), &[], 1);
	let (n, o) = next(&r, &command("command_grant_initial"), &[], 1);
	assert!(matches!(o,ApplyResult2::Closed{cause:3,first,..} if first==r.covered()));
	assert_eq!(n.state.lineages, r.state.lineages);
	let mut changed = command("command_grant_initial");
	if let Command2::Grant { lease, .. } = &mut changed {
		lease.expiry.physical_ms += 1;
	}
	assert_denied(&next(&r, &changed, &[], 1).1, 4);
}
#[test]
fn s1_d4_release_closure_cannot_collide_with_reservation_or_follow_its_epoch_cut() {
	let r = root("root_reserved");
	let (released, _) = next(&r, &command("command_release_grant"), &[], 1);
	let old = released.state.lineages.values().next().unwrap().clone();
	let mut bad = old.clone();
	bad.closure.as_mut().unwrap().log = l(5);
	assert!(UnverifiedRoot2::decode(&replace_lineage(&released, &old, &bad)).is_err());
	let (cut, _) = next(&released, &command("command_advance_epoch"), &[], 1);
	let (after, _) = next(&cut, &command("command_cover_blank"), &[], 1);
	let mut bad = old.clone();
	bad.closure.as_mut().unwrap().log = after.covered();
	assert!(UnverifiedRoot2::decode(&replace_lineage(&after, &old, &bad)).is_err());
	assert!(UnverifiedRoot2::decode(&after.encode().unwrap()).is_ok());
}
#[test]
fn s1_d4_supersession_coordinate_is_unique_except_its_exact_successor_grant() {
	let r = root("root_reserved");
	let (r, _) = next(&r, &command("command_grant_successor"), &[], 1);
	assert!(UnverifiedRoot2::decode(&r.encode().unwrap()).is_ok());
	let old = r
		.state
		.lineages
		.values()
		.find(|g| g.closure.is_some())
		.unwrap()
		.clone();
	let mut bad = old.clone();
	bad.closure.as_mut().unwrap().log.term += 1;
	assert!(UnverifiedRoot2::decode(&replace_lineage(&r, &old, &bad)).is_err());
	let mut grant = r
		.state
		.lineages
		.values()
		.find(|g| g.closure.is_none())
		.unwrap()
		.latest
		.clone();
	grant.lease.expiry.physical_ms += 1;
	let (renewed, _) = next(&r, &grant_of(&grant), &[], 1);
	assert!(UnverifiedRoot2::decode(&renewed.encode().unwrap()).is_ok());
	let mut bad = old.clone();
	bad.closure.as_mut().unwrap().log = l(5);
	assert!(UnverifiedRoot2::decode(&replace_lineage(&renewed, &old, &bad)).is_err());
}
#[test]
fn s1_d5_missing_or_extra_attestation_children_are_rejected_not_pending() {
	for extra in [false, true] {
		for evidence in [0, 1] {
			let r = root("root_reserved");
			let mut c = finalized(&r);
			if let Command2::Finalize { attestation, .. } = &mut c {
				if extra {
					let mut child = attestation.children[0].clone();
					child.child = 1;
					attestation.children.push(child);
				} else {
					attestation.children.clear();
				}
			}
			let (n, o) = next(&r, &c, &[], evidence);
			assert!(matches!(o, ApplyResult2::RejectedEvidence(3)));
			assert!(
				n.lookup(&claim().manifest.identity)
					.unwrap()
					.unwrap()
					.terminal
					.is_none()
			);
			let (_, o) = next(&n, &finalized(&n), &[], 1);
			assert!(matches!(
				o,
				ApplyResult2::Parent {
					new_effect: true,
					..
				}
			));
		}
	}
}
