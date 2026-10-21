use super::*;
use openraft::{CommittedLeaderId, LogId, Membership, StoredMembership};

fn membership(
	configs: Vec<BTreeSet<u32>>,
	address: Option<&str>,
) -> StoredMembership<u32, BasicNode> {
	let nodes = address
		.into_iter()
		.map(|addr| (3, BasicNode { addr: addr.into() }))
		.collect::<BTreeMap<_, _>>();
	StoredMembership::new(
		Some(LogId::new(CommittedLeaderId::new(1, 1), 9)),
		Membership::new(configs, nodes),
	)
}

#[test]
fn join_forwarding_uses_only_applied_uniform_voter_address() {
	let applied = membership(
		vec![BTreeSet::from([1, 2, 3])],
		Some("loopback://trusted-3"),
	);
	assert_eq!(
		join_forwarding_address(2, 3, Some(3), false, &applied).unwrap(),
		Some("loopback://trusted-3".into())
	);
}

#[test]
fn join_forwarding_preserves_visible_epoch_route() {
	// A normal epoch route needs no fallback membership proof, even while a
	// legitimate subsequent membership transition is joint.
	let joint = membership(
		vec![BTreeSet::from([1, 2]), BTreeSet::from([1, 2, 3])],
		None,
	);
	assert_eq!(
		join_forwarding_address(2, 3, Some(3), true, &joint).unwrap(),
		None
	);
}

#[test]
fn join_forwarding_refuses_learner_joint_missing_or_empty_address() {
	for applied in [
		membership(vec![BTreeSet::from([1, 2])], Some("loopback://learner-3")),
		membership(
			vec![BTreeSet::from([1, 2]), BTreeSet::from([1, 2, 3])],
			Some("loopback://joint-3"),
		),
		membership(vec![BTreeSet::from([1, 2, 3])], None),
		membership(vec![BTreeSet::from([1, 2, 3])], Some("  ")),
	] {
		assert!(join_forwarding_address(2, 3, Some(3), false, &applied).is_err());
	}
	let applied = membership(
		vec![BTreeSet::from([1, 2, 3])],
		Some("loopback://trusted-3"),
	);
	let uncommitted = StoredMembership::new(None, applied.membership().clone());
	assert!(join_forwarding_address(2, 3, Some(3), false, &uncommitted).is_err());
}

#[test]
fn join_forwarding_refuses_changed_missing_self_or_zero_leader_hint() {
	let applied = membership(
		vec![BTreeSet::from([1, 2, 3])],
		Some("loopback://trusted-3"),
	);
	for (leader, current) in [(3, Some(1)), (3, None), (2, Some(2)), (0, Some(0))] {
		for visible in [false, true] {
			assert!(join_forwarding_address(2, leader, current, visible, &applied).is_err());
		}
	}
}
