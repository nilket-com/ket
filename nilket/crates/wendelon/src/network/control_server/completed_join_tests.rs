use super::*;
use openraft::{CommittedLeaderId, LogId, Membership, StoredMembership};

fn fixture() -> (
	JoinClusterRequestWire,
	StoredMembership<u32, BasicNode>,
	(u64, EpochConfig),
	MembershipEpoch,
) {
	let request = JoinClusterRequestWire {
		node_id: 3,
		advertise_address: "loopback://completed-3".into(),
		tier_bits: 2,
		role_bits: 3,
		capability_bits: 3,
		protocol: ProtocolVersionWindow::default(),
	};
	let membership = StoredMembership::new(
		Some(LogId::new(CommittedLeaderId::new(1, 1), 9)),
		Membership::new(
			vec![BTreeSet::from([1, 2, 3])],
			BTreeMap::from([(
				3,
				BasicNode {
					addr: request.advertise_address.clone(),
				},
			)]),
		),
	);
	let epoch = EpochConfig {
		epoch: 3,
		peers: BTreeMap::from([(
			3,
			EpochPeer {
				node_id: NodeId {
					id: 3,
					_reserved: 0,
				},
				address: request.advertise_address.clone(),
				tier_bits: 2,
				role_bits: 3,
				capability_bits: 3,
			},
		)]),
	};
	let visible = membership_from_epoch_config(&epoch);
	(request, membership, (3, epoch), visible)
}

#[test]
fn completed_self_join_requires_every_request_field_including_unknown_bits() {
	let (request, membership, epoch, visible) = fixture();
	let check = |r: &JoinClusterRequestWire| {
		validate_completed_self_join(
			r,
			3,
			ProtocolVersionWindow::default(),
			&membership,
			Some(&epoch),
			&visible,
		)
	};
	assert!(check(&request).is_ok());
	let mut variants = Vec::new();
	for id in [0, 2] {
		let mut r = request.clone();
		r.node_id = id;
		variants.push(r);
	}
	for address in ["", "loopback://different", "loopback://completed-3 "] {
		let mut r = request.clone();
		r.advertise_address = address.into();
		variants.push(r);
	}
	for bits in [0, 1, request.tier_bits | 128] {
		let mut r = request.clone();
		r.tier_bits = bits;
		variants.push(r);
	}
	for bits in [0, 1, 2, request.role_bits | 128] {
		let mut r = request.clone();
		r.role_bits = bits;
		variants.push(r);
	}
	for bits in [0, 1, request.capability_bits | 128] {
		let mut r = request.clone();
		r.capability_bits = bits;
		variants.push(r);
	}
	for protocol in [
		ProtocolVersionWindow {
			current: 0,
			min_compatible: 1,
		},
		ProtocolVersionWindow {
			current: 2,
			min_compatible: 2,
		},
		ProtocolVersionWindow {
			current: 2,
			min_compatible: 1,
		},
	] {
		let mut r = request.clone();
		r.protocol = protocol;
		variants.push(r);
	}
	for changed in variants {
		assert!(
			check(&changed).is_err(),
			"accepted changed request: {changed:?}"
		);
	}
}

#[test]
fn completed_self_join_rejects_uncommitted_learner_absent_joint_and_wrong_address() {
	let (request, membership, epoch, visible) = fixture();
	let check = |m: &StoredMembership<u32, BasicNode>| {
		validate_completed_self_join(&request, 3, request.protocol, m, Some(&epoch), &visible)
	};
	assert!(check(&membership).is_ok());
	let nodes = BTreeMap::from([(
		3,
		BasicNode {
			addr: request.advertise_address.clone(),
		},
	)]);
	let log = *membership.log_id();
	for rejected in [
		StoredMembership::default(),
		StoredMembership::new(None, membership.membership().clone()),
		StoredMembership::new(
			log,
			Membership::new(vec![BTreeSet::from([1, 2])], nodes.clone()),
		),
		StoredMembership::new(
			log,
			Membership::new(
				vec![BTreeSet::from([1, 2, 3]), BTreeSet::from([1, 2])],
				nodes,
			),
		),
		StoredMembership::new(
			log,
			Membership::new(
				vec![BTreeSet::from([1, 2, 3])],
				BTreeMap::from([(
					3,
					BasicNode {
						addr: "wrong".into(),
					},
				)]),
			),
		),
	] {
		assert!(
			check(&rejected).is_err(),
			"accepted incomplete membership: {rejected:?}"
		);
	}
}

#[test]
fn completed_self_join_rejects_missing_epoch_visible_lag_and_full_config_divergence() {
	let (request, membership, epoch, visible) = fixture();
	let check = |e: Option<&(u64, EpochConfig)>, v: &MembershipEpoch| {
		validate_completed_self_join(&request, 3, request.protocol, &membership, e, v)
	};
	assert!(check(Some(&epoch), &visible).is_ok());
	assert!(check(None, &visible).is_err());
	let mut old = visible.clone();
	old.epoch = 2;
	old.peers.clear();
	assert!(
		check(Some(&epoch), &old).is_err(),
		"durable epoch ahead of visible"
	);
	let mut incomplete = epoch.clone();
	incomplete.0 = 2;
	incomplete.1.epoch = 2;
	incomplete.1.peers.clear();
	assert!(
		check(Some(&incomplete), &old).is_err(),
		"voter promotion alone is not completed join"
	);
	let mut wrong_key = epoch.clone();
	wrong_key.0 = 4;
	assert!(check(Some(&wrong_key), &visible).is_err());
	let mut different_peer = visible.clone();
	different_peer.peers.insert(
		NodeId {
			id: 1,
			_reserved: 0,
		},
		PeerCapabilities {
			address: "other".into(),
			tiers: TransportTierSet::from_u8(2),
			roles: NodeRoleSet::from_u8(3),
			capabilities: NodeCapabilitySet::from_u8(3),
		},
	);
	assert!(
		check(Some(&epoch), &different_peer).is_err(),
		"full config must match, not just self peer"
	);
}
