//! ADR 0699 S2 controls for the parent decision reducer. Signatures are the
//! test-only synthetic kind; a non-test build verifies nothing.

use super::*;
use crate::consensus::raft_storage::{IncarnationRegistry, apply_request_at_index_for_test};
use crate::consensus::{
	AdvanceEpochRecord, EpochConfig, EpochPeer, MigrationLedgerEntry, MigrationState,
	RegisterIncarnation, TenantLifecycleState, TenantRecord, WendelonRequest, WendelonResponse,
};
use crate::network::ring::ReplicaSegment;

pub(crate) const TENANT: TenantId = TenantId(7);
/// The table generic fixture parents write to. A KV parent has its own shape
/// (one key), so generic multi-child parents are table parents.
pub(crate) const TABLE: crate::schema::TableObjectId =
	crate::schema::TableObjectId::new([0x0f; 16]);

pub(crate) fn fixture_table() -> crate::schema::TableSchemaDescriptor {
	use arrow::datatypes::{DataType, Field, Schema};
	crate::schema::TableSchemaDescriptor::new_with_layout(
		TABLE,
		1,
		"fixture",
		"rows",
		&Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("v", DataType::Utf8, true),
		]),
		crate::schema::RowIdentityPolicy::PrimaryKey(crate::schema::PrimaryKeyDescriptor::single(
			"id",
		)),
		crate::schema::LocalityPolicy::IdentityProjection,
		crate::schema::RecordLayoutPolicy::SingleRow,
		crate::schema::SchemaEvolutionPolicy::AdditiveOnly,
	)
	.unwrap()
}
pub(crate) const EPOCH: u64 = 1;
/// A plausible wall-clock time, so the expiry arithmetic never saturates.
pub(crate) const T0: u64 = 1_800_000_000_000;

pub(crate) fn node(id: u32) -> NodeId {
	NodeId { id, _reserved: 0 }
}

pub(crate) fn inc(id: u32) -> StoreIncarnation {
	StoreIncarnation::from_raw(1_000 + u128::from(id))
}

/// Each node's decision key: the dev key, as the mode requires.
pub(crate) fn test_key(node: u32) -> ed25519_dalek::SigningKey {
	ed25519_dalek::SigningKey::from_bytes(&dev_decision_signing_secret(node))
}

/// Signs with the real Ed25519 scheme under the node's key.
pub(crate) fn sign_for_test<T: evidence::Evidence>(body: T, signer: Signer) -> Signed<T> {
	evidence::sign(&test_key(signer.node), body, signer)
}

pub(crate) fn signer(id: u32) -> Signer {
	Signer {
		node: id,
		incarnation: inc(id),
	}
}

fn hts(ms: u64) -> HybridTimestamp {
	HybridTimestamp {
		physical_ms: ms,
		logical: 0,
		node_id: 1,
	}
}

/// A uniform applied membership whose voters are `voters`, with no learner.
pub(crate) fn formed_membership(
	voters: &[u32],
) -> openraft::StoredMembership<u32, openraft::BasicNode> {
	membership_of(&[voters], &[])
}

/// An applied membership with the given voter configurations (more than one is
/// a joint configuration) and learners.
pub(crate) fn membership_of(
	configs: &[&[u32]],
	learners: &[u32],
) -> openraft::StoredMembership<u32, openraft::BasicNode> {
	let configs: Vec<std::collections::BTreeSet<u32>> = configs
		.iter()
		.map(|config| config.iter().copied().collect())
		.collect();
	let nodes: std::collections::BTreeMap<u32, openraft::BasicNode> = configs
		.iter()
		.flatten()
		.chain(learners)
		.map(|id| {
			(
				*id,
				openraft::BasicNode {
					addr: format!("127.0.0.1:{}", 7000 + u64::from(*id)),
				},
			)
		})
		.collect();
	openraft::StoredMembership::new(
		Some(openraft::LogId::new(
			openraft::CommittedLeaderId::new(1, 1),
			1,
		)),
		openraft::Membership::new(configs, nodes),
	)
}

pub(crate) fn epoch_config(epoch: u64) -> EpochConfig {
	let peers = [1, 2, 3]
		.into_iter()
		.map(|id| {
			(
				id,
				EpochPeer {
					node_id: node(id),
					address: format!("127.0.0.1:{}", 7000 + id),
					tier_bits: 0,
					role_bits: 1,
					capability_bits: 0,
				},
			)
		})
		.collect();
	EpochConfig { epoch, peers }
}

fn ring(rf: usize) -> RingState {
	RingState::build(&membership_from_epoch_config(&epoch_config(EPOCH)), rf)
}

/// A `ParentDecisions` state: three registered data nodes, one tenant.
pub(crate) fn base(rf: u32) -> SmState {
	let mut state = SmState {
		decisions: Some(DecisionState::default()),
		..SmState::default()
	};
	state.epoch_history.insert(EPOCH, epoch_config(EPOCH));
	// ADR 0720: a formed cluster: the voters are the epoch's peers.
	state.last_membership = formed_membership(&[1, 2, 3]);
	state.tenants.insert(
		TENANT,
		TenantRecord {
			tenant_id: TENANT,
			state: TenantLifecycleState::Active,
			quota_bytes: u64::MAX,
			used_bytes: 0,
			replication_factor: rf,
		},
	);
	for id in 1..=3 {
		state.incarnations.insert(
			id,
			IncarnationRegistry {
				current: Some(inc(id)),
				..IncarnationRegistry::default()
			},
		);
		state.decisions.as_mut().unwrap().signers.insert(
			(id, inc(id)),
			decision_verifying_key_bytes(dev_decision_signing_secret(id)),
		);
	}
	state.table_schemas.upsert(TENANT, fixture_table()).unwrap();
	state
}

pub(crate) fn lease(holder: u32, range: (u64, u64), start: u64, expiry: u64) -> LeaseDescriptor {
	LeaseDescriptor {
		epoch: EPOCH,
		holder: node(holder),
		range_start: range.0,
		range_end: range.1,
		lease_start: hts(start),
		lease_expiry: hts(expiry),
		min_commit_index: 0,
		issuer: node(1),
		signature: [0; 64],
	}
}

pub(crate) fn universe() -> Vec<(u32, (u64, u64))> {
	let ring = ring(1);
	(1..=3)
		.flat_map(|id| {
			ring.primary_ranges_for(node(id))
				.into_iter()
				.map(move |range| (id, range))
		})
		.collect()
}

thread_local! {
	/// Each test thread's Raft log position: every applied request takes the
	/// next index, as committed entries do.
	static NEXT_INDEX: std::cell::Cell<u64> = const { std::cell::Cell::new(1) };
}

/// Applies `request` at the next log index of this test thread.
pub(crate) fn apply_next(state: &mut SmState, request: WendelonRequest) -> WendelonResponse {
	let index = NEXT_INDEX.with(|next| {
		let index = next.get();
		next.set(index + 1);
		index
	});
	apply_request_at_index_for_test(state, request, index)
}

/// The index the next applied request will take.
pub(crate) fn peek_index() -> u64 {
	NEXT_INDEX.with(|next| next.get())
}

/// ADR 0729: the batch a holder's lease pass would build for `leases` from
/// this state: each grant names the generation of the lineage the state holds
/// for its range (`None` where it holds none), stamped `stamp_ms`.
pub(crate) fn lease_batch_at(
	state: &SmState,
	stamp_ms: u64,
	leases: Vec<LeaseDescriptor>,
) -> WendelonRequest {
	let grants = leases
		.into_iter()
		.map(|lease| crate::consensus::LeaseBatchGrant {
			lease,
			expected_generation: state
				.decisions
				.as_ref()
				.and_then(|decisions| decisions.lineages().get(&lease.range_start))
				.map(|lineage| lineage.generation),
			// ADR 0108 (wendelon): the holder carries its own store, here the
			// slot's current one.
			holder_incarnation: state
				.incarnations
				.get(&lease.holder.id)
				.and_then(|registry| registry.current)
				.unwrap_or(inc(lease.holder.id)),
		})
		.collect();
	WendelonRequest::AcquireLeases(crate::consensus::LeaseBatch::new(stamp_ms, grants).unwrap())
}

/// One grant, as this mode carries it (ADR 0729): a batch of one, answered as
/// the single grant used to be.
pub(crate) fn grant_request(state: &SmState, lease: LeaseDescriptor) -> WendelonRequest {
	lease_batch_at(state, 0, vec![lease])
}

fn granted(response: WendelonResponse) -> WendelonResponse {
	match response {
		WendelonResponse::LeaseResults(results) if results.results().len() == 1 => {
			if results.results()[0].granted {
				WendelonResponse::Success
			} else {
				WendelonResponse::LeaseDenied
			}
		}
		other => other,
	}
}

fn grant(state: &mut SmState, lease: LeaseDescriptor) -> WendelonResponse {
	// Today keeps the single grant; this mode carries a grant in its batch.
	if state.decisions.is_none() {
		return apply_next(state, WendelonRequest::AcquireLease(lease));
	}
	let request = grant_request(state, lease);
	granted(apply_next(state, request))
}

/// Closes `range`'s generation the way this mode does without a takeover: its
/// holder's regrant after its lease lapsed opens a new one (ADR 0729 keeps
/// that boundary; `ReleaseLease` is denied in the mode).
fn close_by_gap_regrant(state: &mut SmState, holder: u32, range: (u64, u64)) {
	let expiry = state.leases[&range.0].lease_expiry.physical_ms;
	let before = decisions(state).lineages[&range.0].generation;
	assert_eq!(
		grant(state, lease(holder, range, expiry + 1, expiry + 1_000)),
		WendelonResponse::Success
	);
	assert_ne!(decisions(state).lineages[&range.0].generation, before);
}

/// Grants every range of the universe to its primary.
pub(crate) fn grant_all(state: &mut SmState) {
	for (holder, range) in universe() {
		assert_eq!(
			grant(state, lease(holder, range, 0, 1_000)),
			WendelonResponse::Success
		);
	}
}

fn decisions(state: &SmState) -> &DecisionState {
	state.decisions.as_ref().unwrap()
}

fn run(
	state: &mut SmState,
	entries: &mut Vec<(u64, WendelonRequest)>,
	request: WendelonRequest,
) -> u64 {
	let index = entries.len() as u64 + 1;
	apply_request_at_index_for_test(state, request.clone(), index);
	entries.push((index, request));
	index
}

/// A fixed history of `(log index, request)` from `base(3)`: two lineages
/// kept, an accepted two-child parent coordinated by its second child whose
/// Reserve and Finalize are separated by a lease renewal, a reserved parent,
/// and a floor that retired the accepted key. Built on a scratch state so each
/// acknowledgement binds the Reserve index the replay will produce.
pub(crate) fn golden_history() -> Vec<(u64, WendelonRequest)> {
	let mut scratch = base(3);
	let mut entries = Vec::new();
	for (holder, range) in universe() {
		let request = grant_request(&scratch, lease(holder, range, 0, 1_000));
		run(&mut scratch, &mut entries, request);
	}
	let kids = children(&scratch, 2);
	let mut kept = Vec::new();
	for (holder, range) in universe() {
		if kids
			.iter()
			.any(|child| child.lineage.range_start == range.0)
		{
			kept.push((holder, range));
			continue;
		}
		// ADR 0729: denied in this mode, with no effect. Kept in the history
		// so every later entry keeps its log index, and with them every
		// recorded claim and acknowledgement its bytes.
		run(
			&mut scratch,
			&mut entries,
			WendelonRequest::ReleaseLease(ReleaseLeaseRequestMeta {
				range_start: range.0,
				range_end: range.1,
				epoch: EPOCH,
				holder: node(holder),
			}),
		);
	}
	let accepted = claim(key(T0, 1), kids.clone(), 1);
	let reserved = claim(key(T0, 2), vec![kids[0].clone()], 0);
	let rsv = run(
		&mut scratch,
		&mut entries,
		WendelonRequest::Decision(DecisionCommand::Reserve {
			claim: sign_for_test(accepted.clone(), kids[1].holder),
			stamp_ms: T0,
		}),
	);
	// An unrelated renewal between the Reserve and its decision.
	let (holder, range) = kept[0];
	let request = grant_request(&scratch, lease(holder, range, 500, 2_000));
	run(&mut scratch, &mut entries, request);
	let commands = [
		DecisionCommand::Finalize {
			key: accepted.key,
			request_digest: accepted.request_digest,
			acks: quorum_acks(&accepted, rsv),
		},
		DecisionCommand::Reserve {
			claim: sign_for_test(reserved.clone(), kids[0].holder),
			stamp_ms: T0,
		},
		DecisionCommand::AdvanceFloor {
			floor_ms: T0,
			stamp_ms: T0 + DECISION_EXPIRY_MS + 10,
		},
	];
	for command in commands {
		run(
			&mut scratch,
			&mut entries,
			WendelonRequest::Decision(command),
		);
	}
	let decisions = scratch.decisions.as_ref().unwrap();
	assert_eq!(
		(
			decisions.lineages.len(),
			decisions.retained_bodies(),
			decisions.retry_keys()
		),
		// ADR 0729: every range keeps its lineage; a release is denied.
		(universe().len(), 2, 1)
	);
	assert_eq!(
		decisions.body(rsv).unwrap().status,
		ParentStatus::Accepted { dec: rsv + 2 }
	);
	entries
}

/// Stands in for a store replacement, which this checkpoint cannot perform
/// (registration refuses it under this authority): the registry moves and the
/// apply arms' pruning runs, so closure by incarnation stays covered.
fn replace_incarnation(state: &mut SmState, node: u32, incarnation: StoreIncarnation) {
	state.incarnations.get_mut(&node).unwrap().current = Some(incarnation);
	prune_lineages(state);
}

pub(crate) fn decide(state: &mut SmState, command: DecisionCommand) -> DecisionResult {
	match apply_next(state, WendelonRequest::Decision(command)) {
		WendelonResponse::Decision(result) => result,
		other => panic!("unexpected response {other:?}"),
	}
}

pub(crate) fn uuid_v7(ms: u64, n: u8) -> [u8; 16] {
	let mut bytes = [n; 16];
	bytes[..6].copy_from_slice(&ms.to_be_bytes()[2..]);
	bytes[6] = 0x70;
	bytes[8] = 0x80 | (n & 0x3f);
	bytes
}

pub(crate) fn key(issued_ms: u64, sequence_no: u64) -> ParentKey {
	ParentKey {
		tenant: TENANT,
		namespace: ParentNamespace::Table(TABLE),
		client_uuid: uuid_v7(issued_ms, 1),
		sequence_no,
	}
}

/// A child for a whole replica segment whose replicas are exactly `rf` nodes.
fn segment_child(state: &SmState, segment: &ReplicaSegment, content: u8) -> ChildClaim {
	let (range_start, lineage) = decisions(state)
		.lineages
		.range(..=segment.start)
		.next_back()
		.map(|(start, lineage)| (*start, *lineage))
		.unwrap();
	// ADR 0729: the holder is the lineage's, which is the segment's first
	// replica until a takeover moves it.
	let holder = lineage.holder.id;
	assert!(segment.end <= lineage.range_end);
	ChildClaim {
		range_start: segment.start,
		range_end: segment.end,
		lineage: LineageRef {
			range_start,
			generation: lineage.generation,
		},
		holder: signer(holder),
		plan: segment.replicas.iter().map(|n| signer(n.id)).collect(),
		rows: 2,
		content: [content; 32],
	}
}

/// The first `n` replica segments whose primaries differ from one another.
pub(crate) fn children(state: &SmState, n: usize) -> Vec<ChildClaim> {
	let mut out: Vec<ChildClaim> = Vec::new();
	let mut primaries: Vec<u32> = Vec::new();
	for segment in ring(3).replica_segments() {
		if out.len() == n {
			break;
		}
		if primaries.contains(&segment.replicas[0].id) {
			continue;
		}
		primaries.push(segment.replicas[0].id);
		out.push(segment_child(state, &segment, out.len() as u8 + 1));
	}
	assert_eq!(out.len(), n);
	out
}

pub(crate) fn claim(key: ParentKey, children: Vec<ChildClaim>, coordinator: u16) -> Claim {
	let first_row_position = children[usize::from(coordinator)].range_start;
	Claim {
		key,
		request_digest: [9; 32],
		commit_hlc: hts(T0),
		coordinator,
		first_row_position,
		children,
		operation: Operation::Write,
	}
}

pub(crate) fn reserve(state: &mut SmState, claim: &Claim, stamp_ms: u64) -> DecisionResult {
	let holder = claim.children[usize::from(claim.coordinator)].holder;
	decide(
		state,
		DecisionCommand::Reserve {
			claim: sign_for_test(claim.clone(), holder),
			stamp_ms,
		},
	)
}

pub(crate) fn ack(claim: &Claim, reservation: u64, ordinal: u16, by: u32) -> Signed<ChildAckBody> {
	let child = &claim.children[usize::from(ordinal)];
	sign_for_test(
		ChildAckBody {
			key: claim.key,
			reservation,
			claim_digest: claim_digest(claim),
			ordinal,
			range_start: child.range_start,
			range_end: child.range_end,
			lineage: child.lineage,
			holder_incarnation: child.holder.incarnation,
			rows: child.rows,
			content: child.content,
			acker: signer(by),
		},
		signer(by),
	)
}

/// Holder plus one more plan member, for every child: a quorum of three.
pub(crate) fn quorum_acks(claim: &Claim, reservation: u64) -> Vec<Signed<ChildAckBody>> {
	let mut acks = Vec::new();
	for (ordinal, child) in claim.children.iter().enumerate() {
		let other = child
			.plan
			.iter()
			.find(|member| **member != child.holder)
			.unwrap();
		acks.push(ack(claim, reservation, ordinal as u16, child.holder.node));
		acks.push(ack(claim, reservation, ordinal as u16, other.node));
	}
	acks
}

pub(crate) fn finalize(
	state: &mut SmState,
	claim: &Claim,
	acks: Vec<Signed<ChildAckBody>>,
) -> DecisionResult {
	decide(
		state,
		DecisionCommand::Finalize {
			key: claim.key,
			request_digest: claim.request_digest,
			acks,
		},
	)
}

fn resolve(state: &mut SmState, claim: &Claim) -> DecisionResult {
	decide(
		state,
		DecisionCommand::Resolve {
			key: claim.key,
			request_digest: claim.request_digest,
		},
	)
}

fn abort(state: &mut SmState, claim: &Claim, reservation: u64) -> DecisionResult {
	decide(state, abort_command(claim, reservation))
}

pub(crate) fn abort_command(claim: &Claim, reservation: u64) -> DecisionCommand {
	let coordinator = claim.children[usize::from(claim.coordinator)].clone();
	DecisionCommand::Abort {
		abort: sign_for_test(
			AbortBody {
				key: claim.key,
				reservation,
				claim_digest: claim_digest(claim),
				coordinator: coordinator.holder,
				lineage: coordinator.lineage,
			},
			coordinator.holder,
		),
	}
}

fn floor(state: &mut SmState, floor_ms: u64, stamp_ms: u64) -> DecisionResult {
	decide(state, DecisionCommand::AdvanceFloor { floor_ms, stamp_ms })
}

fn refused(reason: DecisionRefusal) -> DecisionResult {
	DecisionResult::Refused(reason)
}

/// A granted state with one two-child reserved parent.
pub(crate) fn reserved_parent(sequence_no: u64) -> (SmState, Claim, u64) {
	let mut state = base(3);
	grant_all(&mut state);
	let claim = claim(key(T0, sequence_no), children(&state, 2), 0);
	let DecisionResult::Reserved { reservation } = reserve(&mut state, &claim, T0) else {
		panic!("reserve");
	};
	(state, claim, reservation)
}

// ============================================================
//  LINEAGES
// ============================================================

#[test]
fn lineages_follow_only_exact_ranges_of_the_universe_granted_to_their_primary() {
	let mut state = base(3);
	grant_all(&mut state);
	let universe = universe();
	assert!(universe.len() <= 3 * crate::network::ring::VNODES_PER_NODE + 1);
	assert_eq!(decisions(&state).lineages.len(), universe.len());
	let before = state.clone();
	let (holder, (start, end)) = universe[0];
	let other = if holder == 1 { 2 } else { 1 };
	let mut denied = vec![
		("not the primary", lease(other, (start, end), 0, 1_000)),
		(
			"start not before expiry",
			lease(holder, (start, end), 1_000, 1_000),
		),
		(
			"another epoch",
			LeaseDescriptor {
				epoch: 2,
				..lease(holder, (start, end), 0, 1_000)
			},
		),
	];
	if start < end {
		denied.push(("a sub-range", lease(holder, (start, start), 0, 1_000)));
	}
	if end < u64::MAX {
		denied.push(("a super-range", lease(holder, (start, end + 1), 0, 1_000)));
	}
	for (label, lease) in denied {
		assert_eq!(
			grant(&mut state, lease),
			WendelonResponse::LeaseDenied,
			"{label}"
		);
		assert_eq!(decisions(&state), decisions(&before), "{label}");
		assert_eq!(state.leases, before.leases, "{label}");
	}
	// A holder without a registered incarnation is denied.
	let mut unregistered = base(3);
	unregistered.incarnations.remove(&holder);
	assert_eq!(
		grant(&mut unregistered, lease(holder, (start, end), 0, 1_000)),
		WendelonResponse::LeaseDenied
	);
	assert!(decisions(&unregistered).lineages.is_empty());
}

#[test]
fn a_renewal_continues_its_generation_and_a_gap_or_a_shortening_does_not() {
	let mut state = base(3);
	let (holder, range) = universe()[0];
	assert_eq!(
		grant(&mut state, lease(holder, range, 0, 1_000)),
		WendelonResponse::Success
	);
	let first = decisions(&state).lineages[&range.0];
	// Continuous and not shorter: the same generation, whichever leader
	// issues it (ADR 0700: a new leader's renewal continues the lineage).
	let mut renewal = lease(holder, range, 500, 2_000);
	renewal.issuer = node(2);
	assert_eq!(grant(&mut state, renewal), WendelonResponse::Success);
	assert_eq!(decisions(&state).lineages[&range.0], first);
	// Shorter than the grant it continues: denied, nothing changes.
	let before = state.clone();
	assert_eq!(
		grant(&mut state, lease(holder, range, 600, 1_500)),
		WendelonResponse::LeaseDenied
	);
	assert_eq!(decisions(&state), decisions(&before));
	assert_eq!(state.leases, before.leases);
	// After a gap: a new generation, even for the same holder.
	assert_eq!(
		grant(&mut state, lease(holder, range, 2_000, 3_000)),
		WendelonResponse::Success
	);
	let after_gap = decisions(&state).lineages[&range.0];
	assert!(after_gap.generation > first.generation);
	assert_eq!(decisions(&state).next_generation, after_gap.generation + 1);
}

/// ADR 0700 delta 4: the mode's lease source is the applied record: this
/// holder's unexpired grants of the current epoch, whoever issued them. A
/// gapped grant still replaces the lineage, and its old parent cannot be
/// accepted.
#[test]
fn the_applied_lease_source_follows_the_record_not_the_issuer() {
	use crate::network::data_server::child_writes::applied_leases;
	let covers = |state: &SmState, holder: u32, range: (u64, u64), now: u64| {
		applied_leases(state, holder, now)
			.iter()
			.any(|lease| lease.range_start == range.0 && lease.range_end == range.1)
	};
	let mut state = base(3);
	let (holder, range) = universe()[0];
	let other = universe()
		.into_iter()
		.find(|(id, _)| *id != holder)
		.unwrap()
		.0;
	assert!(!covers(&state, holder, range, T0));
	assert_eq!(
		grant(&mut state, lease(holder, range, 0, 1_000)),
		WendelonResponse::Success
	);
	assert!(covers(&state, holder, range, 500));
	assert!(!covers(&state, other, range, 500), "another holder's grant");
	// The same bound as before: `now < lease_expiry` as an HLC.
	assert!(!covers(&state, holder, range, 1_001), "expired");
	// A former leader's grant still covers; a new leader's overlapping
	// renewal keeps the lineage and extends it.
	let first = decisions(&state).lineages[&range.0];
	let mut renewal = lease(holder, range, 500, 2_000);
	renewal.issuer = node(2);
	assert_eq!(grant(&mut state, renewal), WendelonResponse::Success);
	assert_eq!(decisions(&state).lineages[&range.0], first);
	assert!(covers(&state, holder, range, 1_500));
	// A previous epoch's grant does not cover.
	let mut later = state.clone();
	apply_next(
		&mut later,
		WendelonRequest::AdvanceEpoch(AdvanceEpochRecord {
			from: epoch_config(EPOCH),
			to: epoch_config(EPOCH + 1),
		}),
	);
	assert!(!covers(&later, holder, range, 1_500), "a previous epoch");

	// A gapped grant replaces the lineage; the parent reserved under the old
	// one is refused, never accepted.
	let (mut state, claim, rsv) = reserved_parent(1);
	let child = claim.children[usize::from(claim.coordinator)].clone();
	let range_end = decisions(&state).lineages[&child.lineage.range_start].range_end;
	let mut gapped = lease(
		child.holder.node,
		(child.lineage.range_start, range_end),
		2_000,
		3_000,
	);
	gapped.issuer = node(2);
	assert_eq!(grant(&mut state, gapped), WendelonResponse::Success);
	assert_ne!(
		decisions(&state).lineages[&child.lineage.range_start].generation,
		child.lineage.generation
	);
	assert!(matches!(
		finalize(&mut state, &claim, quorum_acks(&claim, rsv)),
		DecisionResult::Terminal {
			status: ParentStatus::Refused {
				cause: RefusalCause::LineageClosed,
				..
			},
			..
		}
	));
}

/// ADR 0729: a release names no lineage generation, so this mode denies it
/// whoever sends it and changes nothing. A generation ends without a takeover
/// only by its holder's regrant after a gap, or a prune.
#[test]
fn a_release_is_denied_in_the_mode_and_a_gap_regrant_is_what_closes_a_generation() {
	let mut state = base(3);
	let (holder, range) = universe()[0];
	grant(&mut state, lease(holder, range, 0, 1_000));
	let release = |holder: u32, epoch: u64| {
		WendelonRequest::ReleaseLease(ReleaseLeaseRequestMeta {
			range_start: range.0,
			range_end: range.1,
			epoch,
			holder: node(holder),
		})
	};
	let before = state.clone();
	let other = if holder == 1 { 2 } else { 1 };
	for (who, epoch) in [(other, EPOCH), (holder, EPOCH), (holder, EPOCH + 1)] {
		assert_eq!(
			apply_next(&mut state, release(who, epoch)),
			WendelonResponse::LeaseDenied
		);
		assert_eq!(decisions(&state), decisions(&before));
		assert_eq!(state.leases, before.leases);
	}
	let generation = decisions(&state).next_generation;
	close_by_gap_regrant(&mut state, holder, range);
	assert_eq!(decisions(&state).lineages.len(), 1);
	assert_eq!(decisions(&state).next_generation, generation + 1);
}

#[test]
fn a_replaced_incarnation_or_a_new_epoch_ends_lineages() {
	let mut state = base(3);
	grant_all(&mut state);
	let (holder, range) = universe()[0];
	let held = decisions(&state)
		.lineages
		.values()
		.filter(|l| l.holder.id == holder)
		.count();
	let total = decisions(&state).lineages.len();
	let replacement = StoreIncarnation::from_raw(9_999);
	replace_incarnation(&mut state, holder, replacement);
	assert_eq!(decisions(&state).lineages.len(), total - held);
	// The reused node id regrants under its new incarnation: a new generation.
	grant(&mut state, lease(holder, range, 0, 1_000));
	assert_eq!(
		decisions(&state).lineages[&range.0].incarnation,
		replacement
	);

	apply_next(
		&mut state,
		WendelonRequest::AdvanceEpoch(AdvanceEpochRecord {
			from: epoch_config(EPOCH),
			to: epoch_config(EPOCH + 1),
		}),
	);
	assert!(decisions(&state).lineages.is_empty());
}

#[test]
fn generation_overflow_denies_the_grant_and_changes_nothing() {
	let mut state = base(3);
	state.decisions.as_mut().unwrap().next_generation = u64::MAX;
	let (holder, range) = universe()[0];
	let before = state.clone();
	assert_eq!(
		grant(&mut state, lease(holder, range, 0, 1_000)),
		WendelonResponse::LeaseDenied
	);
	assert_eq!(decisions(&state), decisions(&before));
	assert_eq!(state.leases, before.leases);
}

#[test]
fn twenty_thousand_regrants_keep_one_lineage_per_range() {
	let mut state = base(3);
	grant_all(&mut state);
	let ranges: Vec<_> = universe().into_iter().take(4).collect();
	let entries = decisions(&state).lineages.len();
	let generation = decisions(&state).next_generation;
	for round in 0..5_000u64 {
		for (holder, range) in &ranges {
			// Each regrant starts after the last lease ended: a gap, so a new
			// generation (ADR 0729 keeps this boundary).
			let start = 2_000 + round * 10;
			assert_eq!(
				grant(&mut state, lease(*holder, *range, start, start + 5)),
				WendelonResponse::Success
			);
		}
	}
	assert_eq!(decisions(&state).lineages.len(), entries);
	assert_eq!(decisions(&state).next_generation, generation + 20_000);
}

#[test]
fn today_keeps_its_lease_rules_and_refuses_decisions_without_effect() {
	let mut state = SmState::default();
	// An arbitrary range, as today has always allowed.
	assert_eq!(
		grant(&mut state, lease(1, (5, 10), 0, 1_000)),
		WendelonResponse::Success
	);
	assert!(state.decisions.is_none());
	let (_, claim, _) = reserved_parent(1);
	let result = decide(
		&mut state,
		DecisionCommand::Resolve {
			key: claim.key,
			request_digest: claim.request_digest,
		},
	);
	assert_eq!(result, refused(DecisionRefusal::ModeToday));
	assert!(state.decisions.is_none());
}

// ============================================================
//  IDENTITY AND TIME
// ============================================================

#[test]
fn identity_form_and_future_issuance_are_refused_without_moving_the_clock() {
	let mut state = base(3);
	grant_all(&mut state);
	let mut not_v7 = claim(key(T0, 1), children(&state, 1), 0);
	not_v7.key.client_uuid[6] = 0x40;
	let future = claim(
		key(T0 + DECISION_CLOCK_SKEW_MS + 1, 2),
		children(&state, 1),
		0,
	);
	let before = decisions(&state).clone();
	assert_eq!(
		reserve(&mut state, &not_v7, T0),
		refused(DecisionRefusal::InvalidIdentity)
	);
	assert_eq!(
		reserve(&mut state, &future, T0),
		refused(DecisionRefusal::IdentityFromFuture)
	);
	assert_eq!(decisions(&state), &before);
	// At the edge of the skew allowance it is accepted.
	let edge = claim(key(T0 + DECISION_CLOCK_SKEW_MS, 3), children(&state, 1), 0);
	assert!(matches!(
		reserve(&mut state, &edge, T0),
		DecisionResult::Reserved { .. }
	));
	assert_eq!(decisions(&state).clock_ms, T0);
}

#[test]
fn a_new_claims_commit_hlc_may_lead_the_stamp_only_by_the_gate_bound() {
	let mut state = base(3);
	grant_all(&mut state);
	let with_hlc = |state: &SmState, sequence_no: u64, physical_ms: u64| {
		let mut claim = claim(key(T0, sequence_no), children(state, 1), 0);
		claim.commit_hlc = hts(physical_ms);
		claim
	};
	// Just beyond the bound: refused, and nothing changes.
	let beyond = with_hlc(&state, 1, T0 + DECISION_COMMIT_HLC_LEAD_MS + 1);
	let before = decisions(&state).clone();
	assert_eq!(
		reserve(&mut state, &beyond, T0),
		refused(DecisionRefusal::CommitFromFuture)
	);
	assert_eq!(decisions(&state), &before);
	// Overflow-safe: a stamp at the top of the range refuses, never wraps.
	assert_eq!(
		commit_hlc_not_ahead(&beyond, u64::MAX),
		Err(DecisionRefusal::CounterOverflow)
	);
	// Exactly at the bound: admitted.
	let edge = with_hlc(&state, 2, T0 + DECISION_COMMIT_HLC_LEAD_MS);
	let DecisionResult::Reserved { reservation } = reserve(&mut state, &edge, T0) else {
		panic!("the exact bound is admitted");
	};
	// An older claim, far behind the stamp but otherwise valid: admitted.
	let older = with_hlc(&state, 3, T0 - 60_000);
	assert!(matches!(
		reserve(&mut state, &older, T0 + 10_000),
		DecisionResult::Reserved { .. }
	));
	// A recorded claim answers as recorded after the stamp falls behind it
	// (a new leader's clock) and after its authority ends.
	assert_eq!(
		reserve(&mut state, &edge, T0 - 1),
		DecisionResult::Reserved { reservation }
	);
	let child = edge.children[0].clone();
	let range_end = decisions(&state).lineages[&child.lineage.range_start].range_end;
	close_by_gap_regrant(
		&mut state,
		child.holder.node,
		(child.lineage.range_start, range_end),
	);
	assert_eq!(
		reserve(&mut state, &edge, T0 - 1),
		DecisionResult::Reserved { reservation }
	);
	let status = resolve(&mut state, &edge);
	assert!(
		matches!(status, DecisionResult::Terminal { .. }),
		"{status:?}"
	);
	assert_eq!(reserve(&mut state, &edge, T0 - 1), status);
}

#[test]
fn lookup_compares_the_digest_before_returning_any_status() {
	let (mut state, claim_a, reservation) = reserved_parent(1);
	let mut other = claim_a.clone();
	other.request_digest = [1; 32];
	assert_eq!(
		reserve(&mut state, &claim_a, T0),
		DecisionResult::Reserved { reservation }
	);
	assert_eq!(
		reserve(&mut state, &other, T0),
		refused(DecisionRefusal::Conflict)
	);
	assert_eq!(
		answer(decisions(&state), &claim_a.key, &other.request_digest),
		Answer::Conflict
	);
	assert_eq!(
		answer(decisions(&state), &claim_a.key, &claim_a.request_digest),
		Answer::Reserved
	);
	assert_eq!(
		finalize(&mut state, &claim_a, quorum_acks(&claim_a, reservation)),
		DecisionResult::Terminal {
			reservation,
			status: ParentStatus::Accepted {
				dec: peek_index() - 1
			}
		}
	);
	// Terminal records too.
	assert_eq!(
		reserve(&mut state, &other, T0),
		refused(DecisionRefusal::Conflict)
	);
	assert_eq!(
		answer(decisions(&state), &claim_a.key, &other.request_digest),
		Answer::Conflict
	);
	assert_eq!(
		resolve(&mut state, &other),
		refused(DecisionRefusal::Conflict)
	);
	assert_eq!(
		answer(decisions(&state), &claim_a.key, &claim_a.request_digest),
		Answer::Accepted
	);
}

#[test]
fn the_floor_retires_terminal_keys_and_keeps_every_body() {
	let (mut state, accepted, accepted_rsv) = reserved_parent(1);
	finalize(&mut state, &accepted, quorum_acks(&accepted, accepted_rsv));
	let refused_claim = claim(key(T0, 2), children(&state, 1), 0);
	let DecisionResult::Reserved {
		reservation: refused_rsv,
	} = reserve(&mut state, &refused_claim, T0)
	else {
		panic!()
	};
	abort(&mut state, &refused_claim, refused_rsv);
	let live = claim(key(T0, 3), children(&state, 1), 0);
	let DecisionResult::Reserved {
		reservation: live_rsv,
	} = reserve(&mut state, &live, T0)
	else {
		panic!()
	};

	// Refusals change nothing, the clock included.
	let before = decisions(&state).clone();
	assert_eq!(
		floor(&mut state, T0, T0),
		refused(DecisionRefusal::FloorTooRecent)
	);
	assert_eq!(
		floor(&mut state, 0, T0 + DECISION_EXPIRY_MS),
		refused(DecisionRefusal::FloorNotAdvanced)
	);
	assert_eq!(decisions(&state), &before);
	// A clock younger than the expiry cannot place any floor.
	let mut young = base(3);
	let young_before = decisions(&young).clone();
	assert_eq!(
		floor(&mut young, 1, 5),
		refused(DecisionRefusal::FloorTooRecent)
	);
	assert_eq!(decisions(&young), &young_before);

	let later = T0 + DECISION_EXPIRY_MS + 10;
	assert_eq!(
		floor(&mut state, T0, later),
		DecisionResult::FloorAdvanced {
			floor_ms: T0,
			retired_keys: 2
		}
	);
	assert_eq!(
		floor(&mut state, T0, later),
		refused(DecisionRefusal::FloorNotAdvanced)
	);
	let after = decisions(&state);
	assert_eq!(
		after.retained_bodies(),
		3,
		"Accepted and Refused bodies stay until S5"
	);
	assert_eq!(after.retry_keys(), 1);
	assert_eq!(after.retained_claim_bytes, before.retained_claim_bytes);
	assert!(matches!(
		after.body(refused_rsv).unwrap().status,
		ParentStatus::Refused {
			cause: RefusalCause::Aborted,
			dec
		} if dec > refused_rsv
	));
	// A retired key is never fresh again.
	for claim in [&accepted, &refused_claim] {
		assert_eq!(
			answer(after, &claim.key, &claim.request_digest),
			Answer::OutcomeUnavailable
		);
	}
	assert_eq!(
		reserve(&mut state, &accepted, later),
		refused(DecisionRefusal::OutcomeUnavailable)
	);
	assert_eq!(
		finalize(&mut state, &accepted, Vec::new()),
		refused(DecisionRefusal::OutcomeUnavailable)
	);
	// A reserved parent survives the floor and can still conclude.
	assert_eq!(
		answer(decisions(&state), &live.key, &live.request_digest),
		Answer::Reserved
	);
	assert_eq!(
		finalize(&mut state, &live, quorum_acks(&live, live_rsv)),
		DecisionResult::Terminal {
			reservation: live_rsv,
			status: ParentStatus::Accepted {
				dec: peek_index() - 1
			}
		}
	);
	decisions(&state).validate().unwrap();
}

#[test]
fn an_unrecorded_key_at_or_below_the_cutoff_is_never_new() {
	let mut state = base(3);
	grant_all(&mut state);
	let later = T0 + DECISION_EXPIRY_MS + 1_000;
	let fresh = claim(key(later, 1), children(&state, 1), 0);
	reserve(&mut state, &fresh, later);
	let cutoff = decisions(&state).retirement_cutoff();
	for issued in [cutoff - 1, cutoff] {
		let old = claim(key(issued, 2), children(&state, 1), 0);
		assert_eq!(
			answer(decisions(&state), &old.key, &old.request_digest),
			Answer::OutcomeUnavailable
		);
		assert_eq!(
			reserve(&mut state, &old, later),
			refused(DecisionRefusal::OutcomeUnavailable)
		);
	}
	let new = claim(key(cutoff + 1, 3), children(&state, 1), 0);
	assert_eq!(
		answer(decisions(&state), &new.key, &new.request_digest),
		Answer::New
	);
}

// ============================================================
//  EVIDENCE AND FIRST TERMINAL
// ============================================================

#[test]
fn unverifiable_or_misbound_signatures_change_nothing() {
	let mut state = base(3);
	grant_all(&mut state);
	let claim = claim(key(T0, 1), children(&state, 2), 0);
	let mut forged = sign_for_test(claim.clone(), claim.children[0].holder);
	forged.signature[0] ^= 1;
	let before = decisions(&state).clone();
	let result = decide(
		&mut state,
		DecisionCommand::Reserve {
			claim: forged,
			stamp_ms: T0,
		},
	);
	assert_eq!(result, refused(DecisionRefusal::Unverifiable));
	let by_other = sign_for_test(claim.clone(), claim.children[1].holder);
	let result = decide(
		&mut state,
		DecisionCommand::Reserve {
			claim: by_other,
			stamp_ms: T0,
		},
	);
	assert_eq!(result, refused(DecisionRefusal::SignerMismatch));
	assert_eq!(decisions(&state), &before);
}

#[test]
fn acknowledgements_bind_one_reservation_of_one_claim() {
	let (mut state, a, rsv_a) = reserved_parent(1);
	// Same children and content, another key: another reservation.
	let b = claim(key(T0, 2), a.children.clone(), 0);
	let DecisionResult::Reserved { reservation: rsv_b } = reserve(&mut state, &b, T0) else {
		panic!()
	};
	let before = decisions(&state).clone();
	let mismatch = refused(DecisionRefusal::EvidenceMismatch);
	assert_eq!(
		finalize(&mut state, &b, quorum_acks(&a, rsv_a)),
		mismatch,
		"A's acks for B"
	);
	let mut relabelled = quorum_acks(&a, rsv_a);
	for signed in &mut relabelled {
		signed.body.reservation = rsv_b;
		*signed = sign_for_test(signed.body.clone(), signed.signer);
	}
	assert_eq!(
		finalize(&mut state, &b, relabelled),
		mismatch,
		"A's claim digest under B"
	);
	// An acknowledging store that is not the node's current incarnation.
	let mut stale = quorum_acks(&b, rsv_b);
	let mut body = stale[1].body.clone();
	body.acker.incarnation = StoreIncarnation::from_raw(42);
	stale[1] = sign_for_test(body.clone(), body.acker);
	// An incarnation with no bound key verifies nothing.
	assert_eq!(
		finalize(&mut state, &b, stale),
		refused(DecisionRefusal::Unverifiable)
	);
	// The same acknowledgement twice.
	let mut doubled = quorum_acks(&b, rsv_b);
	doubled.push(doubled[0].clone());
	assert_eq!(finalize(&mut state, &b, doubled), mismatch);
	assert_eq!(decisions(&state), &before);
	// Two members without the holder is not a quorum.
	let child = &b.children[0];
	let others: Vec<u32> = child
		.plan
		.iter()
		.map(|m| m.node)
		.filter(|n| *n != child.holder.node)
		.collect();
	let mut no_holder = quorum_acks(&b, rsv_b);
	no_holder.retain(|signed| signed.body.ordinal != 0);
	no_holder.extend(others.iter().map(|n| ack(&b, rsv_b, 0, *n)));
	assert_eq!(
		finalize(&mut state, &b, no_holder),
		DecisionResult::PendingEvidence
	);
	assert_eq!(
		finalize(&mut state, &b, quorum_acks(&b, rsv_b)),
		DecisionResult::Terminal {
			reservation: rsv_b,
			status: ParentStatus::Accepted {
				dec: peek_index() - 1
			}
		}
	);
}

#[test]
fn the_first_terminal_status_wins_and_keeps_its_decision_index() {
	let (mut state, claim, rsv) = reserved_parent(1);
	assert_eq!(resolve(&mut state, &claim), DecisionResult::StillOpen);
	let accepted = finalize(&mut state, &claim, quorum_acks(&claim, rsv));
	let DecisionResult::Terminal {
		reservation,
		status: ParentStatus::Accepted { dec },
	} = accepted
	else {
		panic!("{accepted:?}");
	};
	assert_eq!((reservation, dec), (rsv, peek_index() - 1));
	assert_eq!(resolve(&mut state, &claim), accepted);
	assert_eq!(abort(&mut state, &claim, rsv), accepted);
	assert_eq!(
		finalize(&mut state, &claim, quorum_acks(&claim, rsv)),
		accepted
	);
	assert_eq!(reserve(&mut state, &claim, T0), accepted);
	assert_eq!(decisions(&state).live, 0);

	let (mut state, claim, rsv) = reserved_parent(1);
	let aborted = abort(&mut state, &claim, rsv);
	assert_eq!(
		aborted,
		DecisionResult::Terminal {
			reservation: rsv,
			status: ParentStatus::Refused {
				cause: RefusalCause::Aborted,
				dec: peek_index() - 1
			}
		}
	);
	assert_eq!(
		finalize(&mut state, &claim, quorum_acks(&claim, rsv)),
		aborted
	);
	assert_eq!(resolve(&mut state, &claim), aborted);
}

#[test]
fn a_closed_lineage_refuses_the_parent_before_any_evidence_counts() {
	let closed = |rsv: u64| DecisionResult::Terminal {
		reservation: rsv,
		status: ParentStatus::Refused {
			cause: RefusalCause::LineageClosed,
			dec: peek_index() - 1,
		},
	};
	let (mut state, claim, rsv) = reserved_parent(1);
	let child = claim.children[1].clone();
	let lineage = decisions(&state).lineages[&child.lineage.range_start];
	close_by_gap_regrant(
		&mut state,
		child.holder.node,
		(child.lineage.range_start, lineage.range_end),
	);
	// Re-granting the same range to the same holder does not reopen it (no ABA).
	grant(
		&mut state,
		lease(
			child.holder.node,
			(child.lineage.range_start, lineage.range_end),
			0,
			1_000,
		),
	);
	assert_eq!(
		finalize(&mut state, &claim, quorum_acks(&claim, rsv)),
		closed(rsv)
	);
	assert_eq!(
		answer(decisions(&state), &claim.key, &claim.request_digest),
		Answer::Refused(RefusalCause::LineageClosed)
	);

	let (mut state, claim, rsv) = reserved_parent(1);
	apply_next(
		&mut state,
		WendelonRequest::AdvanceEpoch(AdvanceEpochRecord {
			from: epoch_config(EPOCH),
			to: epoch_config(EPOCH + 1),
		}),
	);
	assert_eq!(resolve(&mut state, &claim), closed(rsv));
}

// ============================================================
//  PLACEMENT AND LIMITS
// ============================================================

#[test]
fn the_coordinator_is_the_first_rows_child_not_the_smallest_range() {
	let mut state = base(3);
	grant_all(&mut state);
	let children = children(&state, 2);
	let routed = claim(key(T0, 1), children.clone(), 1);
	assert!(routed.first_row_position > children[0].range_end);
	let signed_by_child0 = sign_for_test(routed.clone(), children[0].holder);
	assert_eq!(
		decide(
			&mut state,
			DecisionCommand::Reserve {
				claim: signed_by_child0,
				stamp_ms: T0
			}
		),
		refused(DecisionRefusal::SignerMismatch)
	);
	let mut outside = routed.clone();
	outside.first_row_position = children[0].range_start;
	assert_eq!(
		reserve(&mut state, &outside, T0),
		refused(DecisionRefusal::MalformedClaim)
	);
	let DecisionResult::Reserved { reservation } = reserve(&mut state, &routed, T0) else {
		panic!()
	};
	assert_eq!(
		decisions(&state)
			.body(reservation)
			.unwrap()
			.claim
			.coordinator,
		1
	);
}

#[test]
fn malformed_claims_and_wrong_placement_are_refused_without_effect() {
	let mut state = base(3);
	grant_all(&mut state);
	let good = children(&state, 2);
	let lineage_end = decisions(&state).lineages[&good[0].lineage.range_start].range_end;
	let mut cases: Vec<(&str, Vec<ChildClaim>, DecisionRefusal)> = Vec::new();
	cases.push((
		"unsorted",
		vec![good[1].clone(), good[0].clone()],
		DecisionRefusal::MalformedClaim,
	));
	cases.push((
		"duplicate",
		vec![good[0].clone(), good[0].clone()],
		DecisionRefusal::MalformedClaim,
	));
	let mut reversed = good[0].clone();
	(reversed.range_start, reversed.range_end) = (reversed.range_end, reversed.range_start);
	if reversed.range_start != reversed.range_end {
		cases.push(("reversed", vec![reversed], DecisionRefusal::MalformedClaim));
	}
	let mut beyond = good[0].clone();
	beyond.range_end = lineage_end.saturating_add(1);
	if lineage_end != u64::MAX {
		cases.push((
			"outside its lineage",
			vec![beyond],
			DecisionRefusal::LineageClosed,
		));
	}
	let mut stale_generation = good[0].clone();
	stale_generation.lineage.generation += 1_000;
	cases.push((
		"unknown generation",
		vec![stale_generation],
		DecisionRefusal::LineageClosed,
	));
	let mut short_plan = good[0].clone();
	short_plan.plan.retain(|m| *m == short_plan.holder);
	cases.push((
		"plan not the ring's",
		vec![short_plan],
		DecisionRefusal::PlanMismatch,
	));
	let mut stale_member = good[0].clone();
	stale_member.plan[1].incarnation = StoreIncarnation::from_raw(5);
	cases.push((
		"stale plan incarnation",
		vec![stale_member],
		DecisionRefusal::PlanMismatch,
	));
	let mut no_rows = good[0].clone();
	no_rows.rows = 0;
	cases.push(("no rows", vec![no_rows], DecisionRefusal::MalformedClaim));
	cases.push(("no children", Vec::new(), DecisionRefusal::MalformedClaim));
	let before = decisions(&state).clone();
	for (n, (label, children, expected)) in cases.into_iter().enumerate() {
		let mut claim = Claim {
			key: key(T0, n as u64 + 10),
			request_digest: [9; 32],
			commit_hlc: hts(T0),
			coordinator: 0,
			first_row_position: 0,
			children,
			operation: Operation::Write,
		};
		let signer = claim.children.first().map_or(signer(1), |c| c.holder);
		claim.first_row_position = claim
			.children
			.first()
			.map_or(0, |c| c.range_start.min(c.range_end));
		let result = decide(
			&mut state,
			DecisionCommand::Reserve {
				claim: sign_for_test(claim, signer),
				stamp_ms: T0,
			},
		);
		let expected = if label == "no children" {
			DecisionRefusal::SignerMismatch
		} else {
			expected
		};
		assert_eq!(result, refused(expected), "{label}");
		assert_eq!(decisions(&state), &before, "{label}");
	}
}

/// Codex asked that a lease range crossing replica plans be split or refused.
/// In this ring a merged primary range never crosses one: adjacent vnodes of one
/// primary walk to the same successors. The reducer still checks every segment a
/// child touches, so a different ring could not slip a crossing child through.
#[test]
fn no_primary_range_crosses_a_replica_plan_in_this_ring() {
	for rf in 1..=3 {
		let segments = ring(rf).replica_segments();
		for (holder, (start, end)) in universe() {
			let plans: std::collections::BTreeSet<Vec<u32>> = segments
				.iter()
				.filter(|segment| segment.start <= end && start <= segment.end)
				.map(|segment| {
					assert_eq!(segment.replicas[0].id, holder, "rf {rf}");
					let mut ids: Vec<u32> = segment.replicas.iter().map(|n| n.id).collect();
					ids.sort_unstable();
					ids
				})
				.collect();
			assert_eq!(plans.len(), 1, "rf {rf}: range {start}..={end}");
		}
	}
}

#[test]
fn a_child_spanning_two_lineages_is_refused() {
	let mut state = base(3);
	grant_all(&mut state);
	let kids = children(&state, 2);
	let mut wide = kids[0].clone();
	wide.range_end = kids[1].range_end;
	let before = decisions(&state).clone();
	let c = claim(key(T0, 1), vec![wide], 0);
	assert_eq!(
		reserve(&mut state, &c, T0),
		refused(DecisionRefusal::LineageClosed)
	);
	assert_eq!(decisions(&state), &before);
}

#[test]
fn tenant_table_and_migration_admission() {
	let mut state = base(3);
	grant_all(&mut state);
	let one = children(&state, 1);
	state.tenants.get_mut(&TENANT).unwrap().state = TenantLifecycleState::Suspended;
	let kv = claim(key(T0, 1), one.clone(), 0);
	assert_eq!(
		reserve(&mut state, &kv, T0),
		refused(DecisionRefusal::TenantNotWritable)
	);
	state.tenants.get_mut(&TENANT).unwrap().state = TenantLifecycleState::Active;

	let mut table = claim(key(T0, 2), one.clone(), 0);
	table.key.namespace = ParentNamespace::Table(TableObjectId::new([3; 16]));
	assert_eq!(
		reserve(&mut state, &table, T0),
		refused(DecisionRefusal::TableUnavailable)
	);

	state.migrations.insert(
		1,
		MigrationLedgerEntry {
			migration_id: 1,
			range_start: one[0].range_start,
			range_end: one[0].range_start,
			from_epoch: 0,
			to_epoch: EPOCH,
			logical_owner: node(1),
			upstream_migration_id: None,
			source_replicas: vec![node(2)],
			migration_hlc_floor: hts(1),
			state: MigrationState::Pending,
			completed_by: None,
			completed_at_hlc: None,
		},
	);
	assert_eq!(
		reserve(&mut state, &kv, T0),
		refused(DecisionRefusal::MigrationActive)
	);
	state.migrations.get_mut(&1).unwrap().state = MigrationState::Completed;
	assert!(matches!(
		reserve(&mut state, &kv, T0),
		DecisionResult::Reserved { .. }
	));
}

#[test]
fn live_slots_and_retained_bytes_are_checked_before_any_change() {
	let mut state = base(3);
	grant_all(&mut state);
	let one = children(&state, 1);
	state.decisions.as_mut().unwrap().live = LIVE_PARENT_SLOTS;
	let before = decisions(&state).clone();
	let c = claim(key(T0, 1), one.clone(), 0);
	assert_eq!(
		reserve(&mut state, &c, T0),
		refused(DecisionRefusal::LiveSlotsFull)
	);
	assert_eq!(decisions(&state), &before);
	let decisions_mut = state.decisions.as_mut().unwrap();
	decisions_mut.live = 0;
	decisions_mut.retained_claim_bytes = MAX_RETAINED_CLAIM_BYTES - 1;
	assert_eq!(
		reserve(&mut state, &c, T0),
		refused(DecisionRefusal::RetainedFull)
	);
}

/// Shows only that the old lifetime admission cap (32,768 parent slots) is gone.
/// Retained bodies grow with history until S5 reclaims them: this is not a
/// bounded-state control.
#[test]
fn more_parents_than_the_old_lifetime_cap_are_admitted() {
	let mut state = base(3);
	grant_all(&mut state);
	let one = children(&state, 1);
	for sequence_no in 0..32_769u64 {
		let c = claim(key(T0, sequence_no), one.clone(), 0);
		let DecisionResult::Reserved { reservation } = reserve(&mut state, &c, T0) else {
			panic!("parent {sequence_no} was not admitted");
		};
		let DecisionResult::Terminal {
			status: ParentStatus::Accepted { .. },
			..
		} = finalize(&mut state, &c, quorum_acks(&c, reservation))
		else {
			panic!("parent {sequence_no} was not accepted");
		};
	}
	let decisions = decisions(&state);
	assert_eq!(decisions.retained_bodies(), 32_769);
	assert_eq!(decisions.live, 0);
	decisions.validate().unwrap();
}

// ============================================================
//  DECODE VALIDATION
// ============================================================

fn golden_decisions() -> DecisionState {
	let mut state = base(3);
	for (index, request) in golden_history() {
		apply_request_at_index_for_test(&mut state, request, index);
	}
	state.decisions.unwrap()
}

#[test]
fn a_snapshot_cannot_substitute_an_identity_or_lose_a_coordinate() {
	let good = golden_decisions();
	good.validate().unwrap();
	let (live_key, live_entry) = good
		.keys
		.iter()
		.map(|(key, entry)| (*key, *entry))
		.next()
		.unwrap();
	let accepted_rsv = *good
		.bodies
		.iter()
		.find(|(_, body)| body.status.is_terminal())
		.unwrap()
		.0;
	let mut cases: Vec<(&str, DecisionState, &str)> = Vec::new();
	let mut digest = good.clone();
	digest.keys.get_mut(&live_key).unwrap().request_digest = [7; 32];
	cases.push((
		"a retry key with another digest",
		digest,
		"of that key and digest",
	));
	let mut lost = good.clone();
	lost.keys.remove(&live_key);
	cases.push((
		"a reserved body without its retry key",
		lost,
		"lost its retry key",
	));
	let mut moved = good.clone();
	let body = moved.bodies.remove(&live_entry.reservation).unwrap();
	moved.bodies.insert(live_entry.reservation + 1_000, body);
	moved.keys.get_mut(&live_key).unwrap().reservation = live_entry.reservation + 1_000;
	cases.push((
		"a body away from its reservation index",
		moved,
		"at its reservation index",
	));
	let mut early = good.clone();
	early.bodies.get_mut(&accepted_rsv).unwrap().status =
		ParentStatus::Accepted { dec: accepted_rsv };
	cases.push((
		"a decision not after its reservation",
		early,
		"does not follow",
	));
	let mut claim = good.clone();
	claim
		.bodies
		.get_mut(&accepted_rsv)
		.unwrap()
		.claim
		.request_digest = [8; 32];
	cases.push((
		"a claim that no longer matches its digest",
		claim,
		"digest or size",
	));
	let mut twin = good.clone();
	let mut copy = twin.bodies[&accepted_rsv].clone();
	copy.rsv = 9_999;
	copy.status = ParentStatus::Accepted { dec: 10_000 };
	twin.retained_claim_bytes += copy.claim_bytes;
	twin.bodies.insert(9_999, copy);
	cases.push(("two bodies with one key", twin, "share a key"));
	for (label, state, reason) in cases {
		let error = state.validate().unwrap_err();
		assert!(error.contains(reason), "{label}: {error}");
	}
}

#[test]
fn eligibility_at_a_cut_follows_rsv_and_the_first_decision() {
	let decisions = golden_decisions();
	let (rsv, body) = decisions
		.bodies
		.iter()
		.find(|(_, body)| body.status.is_terminal())
		.unwrap();
	let dec = body.status.dec().unwrap();
	assert!(dec > rsv + 1, "an unrelated entry sits between them");
	assert_eq!(body.status_at(rsv - 1), None);
	assert_eq!(body.status_at(*rsv), Some(ParentStatus::Reserved));
	assert_eq!(body.status_at(dec - 1), Some(ParentStatus::Reserved));
	assert_eq!(body.status_at(dec), Some(body.status));
	assert_eq!(
		body.status_at(u64::MAX),
		Some(ParentStatus::Accepted { dec })
	);
	// The floor retired its key; its body and both coordinates remain.
	assert!(!decisions.keys.contains_key(&body.claim.key));
}

// ============================================================
//  SIGNER BINDING (S3a)
// ============================================================

fn bind(state: &mut SmState, signer: Signer, key: [u8; 32]) -> DecisionResult {
	decide(state, DecisionCommand::BindSigner { signer, key })
}

#[test]
fn a_signer_binds_once_for_its_current_incarnation_only() {
	let mut state = base(3);
	let key4 = decision_verifying_key_bytes(dev_decision_signing_secret(4));
	let before = decisions(&state).clone();
	// Already bound: the same key is idempotent, another key is a rebind.
	let key1 = decisions(&state).signer_key(signer(1)).unwrap();
	assert_eq!(
		bind(&mut state, signer(1), key1),
		DecisionResult::SignerBound
	);
	assert_eq!(
		bind(&mut state, signer(1), key4),
		refused(DecisionRefusal::SignerRebind)
	);
	// A stale incarnation, an unknown node, a malformed key, a key of another node.
	let stale = Signer {
		node: 2,
		incarnation: StoreIncarnation::from_raw(77),
	};
	assert_eq!(
		bind(&mut state, stale, key4),
		refused(DecisionRefusal::SignerNotCurrent)
	);
	assert_eq!(
		bind(
			&mut state,
			Signer {
				node: 9,
				incarnation: inc(9)
			},
			key4
		),
		refused(DecisionRefusal::SignerNotCurrent)
	);
	let malformed = (0u8..=255)
		.map(|first| {
			let mut bytes = [0xEEu8; 32];
			bytes[0] = first;
			bytes
		})
		.find(|bytes| ed25519_dalek::VerifyingKey::from_bytes(bytes).is_err())
		.expect("some byte string is not a curve point");
	assert_eq!(
		bind(&mut state, signer(3), malformed),
		refused(DecisionRefusal::MalformedKey)
	);
	assert_eq!(decisions(&state), &before);

	// A replaced incarnation needs its own binding; node 1's key cannot serve node 2.
	let replacement = StoreIncarnation::from_raw(6_000);
	replace_incarnation(&mut state, 2, replacement);
	let fresh = Signer {
		node: 2,
		incarnation: replacement,
	};
	let key1 = decisions(&state).signer_key(signer(1)).unwrap();
	assert_eq!(
		bind(&mut state, fresh, key1),
		refused(DecisionRefusal::SignerRebind)
	);
	let key2 = decision_verifying_key_bytes(dev_decision_signing_secret(2));
	assert_eq!(bind(&mut state, fresh, key2), DecisionResult::SignerBound);
	// The old binding is kept for retained evidence.
	assert_eq!(decisions(&state).signer_key(signer(2)), Some(key2));
	decisions(&state).validate().unwrap();
}

#[test]
fn real_signatures_are_required_and_an_old_incarnation_cannot_start_new_work() {
	let mut state = base(3);
	grant_all(&mut state);
	let c = claim(key(T0, 1), children(&state, 1), 0);
	let holder = c.children[0].holder;
	// Signed by another node's key under the holder's identity.
	let wrong_key = evidence::sign(&test_key(holder.node % 3 + 1), c.clone(), holder);
	let before = decisions(&state).clone();
	assert_eq!(
		decide(
			&mut state,
			DecisionCommand::Reserve {
				claim: wrong_key,
				stamp_ms: T0
			}
		),
		refused(DecisionRefusal::Unverifiable)
	);
	let mut truncated = sign_for_test(c.clone(), holder);
	truncated.signature.pop();
	assert_eq!(
		decide(
			&mut state,
			DecisionCommand::Reserve {
				claim: truncated,
				stamp_ms: T0
			}
		),
		refused(DecisionRefusal::Unverifiable)
	);
	assert_eq!(decisions(&state), &before);

	// Reserve and accept, then replace the coordinator's store.
	let DecisionResult::Reserved { reservation } = reserve(&mut state, &c, T0) else {
		panic!()
	};
	let accepted = finalize(&mut state, &c, quorum_acks(&c, reservation));
	assert!(matches!(
		accepted,
		DecisionResult::Terminal {
			status: ParentStatus::Accepted { .. },
			..
		}
	));
	let replacement = StoreIncarnation::from_raw(7_000);
	replace_incarnation(&mut state, holder.node, replacement);
	// The recorded outcome is still retrievable, with no evidence at all.
	assert_eq!(reserve(&mut state, &c, T0), accepted);
	assert_eq!(finalize(&mut state, &c, Vec::new()), accepted);
	// New work signed by the old incarnation is refused: its lineage is closed.
	let fresh = claim(key(T0, 2), c.children.clone(), 0);
	assert_eq!(
		reserve(&mut state, &fresh, T0),
		refused(DecisionRefusal::LineageClosed)
	);
}

#[test]
fn this_checkpoint_never_replaces_an_established_incarnation() {
	let mut state = base(3);
	grant_all(&mut state);
	let before = state.clone();
	let replacement = StoreIncarnation::from_raw(9_000);
	for supersedes in [Some(inc(2)), None] {
		let response = apply_next(
			&mut state,
			WendelonRequest::RegisterIncarnation(RegisterIncarnation {
				node: 2,
				incarnation: replacement,
				supersedes,
			}),
		);
		assert!(
			matches!(response, WendelonResponse::Refused(_)),
			"{supersedes:?}"
		);
		assert_eq!(state.incarnations[&2].current, Some(inc(2)));
		assert_eq!(decisions(&state), decisions(&before));
	}
	// The exact retry of the established incarnation is accepted and changes nothing.
	state
		.incarnations
		.get_mut(&2)
		.unwrap()
		.accepted
		.insert((inc(2), None));
	assert_eq!(
		apply_next(
			&mut state,
			WendelonRequest::RegisterIncarnation(RegisterIncarnation {
				node: 2,
				incarnation: inc(2),
				supersedes: None,
			}),
		),
		WendelonResponse::Success
	);
	assert_eq!(decisions(&state), decisions(&before));
}

// ============================================================
//  AUTHORITATIVE LOOKUP (S3c)
// ============================================================

#[test]
fn a_lookup_returns_the_original_claim_or_says_whether_a_new_one_may_be_made() {
	let (mut state, claim, rsv) = reserved_parent(1);
	let lookup = |state: &SmState, claim: &Claim| {
		lookup_parent(decisions(state), &claim.key, &claim.request_digest)
	};
	let Lookup::Recorded { record } = lookup(&state, &claim) else {
		panic!("recorded");
	};
	assert_eq!(
		(record.rsv, &record.claim, record.status),
		(rsv, &claim, ParentStatus::Reserved)
	);
	// The original claim, even after its lineages and leases moved on.
	apply_next(
		&mut state,
		WendelonRequest::AdvanceEpoch(AdvanceEpochRecord {
			from: epoch_config(EPOCH),
			to: epoch_config(EPOCH + 1),
		}),
	);
	assert!(matches!(lookup(&state, &claim), Lookup::Recorded { record } if record.claim == claim));
	// Another request under the same key is a conflict, not the record.
	let mut other = claim.clone();
	other.request_digest = [1; 32];
	assert_eq!(lookup(&state, &other), Lookup::Conflict);
	// Terminal records come back as recorded.
	resolve(&mut state, &claim);
	assert!(matches!(
		lookup(&state, &claim),
		Lookup::Recorded { record } if matches!(record.status, ParentStatus::Refused { .. })
	));
	// A fresh key may be claimed; one past the cutoff may not; a non-v7 key is invalid.
	let fresh = self::claim(key(T0, 99), claim.children.clone(), 0);
	assert_eq!(lookup(&state, &fresh), Lookup::Absent { eligible: true });
	let mut aged = state.clone();
	aged.decisions.as_mut().unwrap().floor_ms = T0 + 1;
	assert_eq!(lookup(&aged, &fresh), Lookup::Absent { eligible: false });
	let mut invalid = fresh.clone();
	invalid.key.client_uuid[6] = 0x40;
	assert_eq!(lookup(&state, &invalid), Lookup::InvalidIdentity);
}

#[test]
fn only_the_coordinator_ranges_holder_drives_resolve_of_a_closed_parent() {
	let (mut state, claim, rsv) = reserved_parent(1);
	let coordinator = claim.children[usize::from(claim.coordinator)].holder.node;
	assert!(
		resolvable_by(&state, coordinator, 8).is_empty(),
		"nothing closed yet"
	);
	// Close the non-coordinator child's lineage.
	let child = claim
		.children
		.iter()
		.find(|c| c.holder.node != coordinator)
		.unwrap()
		.clone();
	let end = decisions(&state).lineages[&child.lineage.range_start].range_end;
	close_by_gap_regrant(
		&mut state,
		child.holder.node,
		(child.lineage.range_start, end),
	);
	assert_eq!(
		resolvable_by(&state, coordinator, 8),
		vec![(claim.key, claim.request_digest)]
	);
	for node_id in (1..=3).filter(|n| *n != coordinator) {
		assert!(
			resolvable_by(&state, node_id, 8).is_empty(),
			"node {node_id} does not drive it"
		);
	}
	resolve(&mut state, &claim);
	assert!(
		resolvable_by(&state, coordinator, 8).is_empty(),
		"terminal now"
	);
	let _ = rsv;
}

// ============================================================
//  ADR 0699 S4b: KV PARENTS, MATCHED DELETES AND NOOPS
// ============================================================

/// A one-row child at `position`: the lineage covering it, its segment's plan.
fn point_child(state: &SmState, position: u64) -> ChildClaim {
	let segment = ring(3)
		.replica_segments()
		.into_iter()
		.find(|segment| segment.start <= position && position <= segment.end)
		.unwrap();
	let (range_start, lineage) = decisions(state)
		.lineages
		.range(..=position)
		.next_back()
		.map(|(start, lineage)| (*start, *lineage))
		.unwrap();
	ChildClaim {
		range_start: position,
		range_end: position,
		lineage: LineageRef {
			range_start,
			generation: lineage.generation,
		},
		holder: Signer {
			node: lineage.holder.id,
			incarnation: lineage.incarnation,
		},
		plan: segment.replicas.iter().map(|n| signer(n.id)).collect(),
		rows: 1,
		content: [3; 32],
	}
}

fn row(n: u8) -> Identity {
	Identity::new([n; 32])
}

fn delete_request(n: u8) -> DeleteRequest {
	DeleteRequest {
		table: TABLE,
		schema_generation: 1,
		rows: vec![row(n)],
	}
}

fn position_of(n: u8) -> u64 {
	delete_row_position(TENANT, &fixture_table(), row(n)).unwrap()
}

/// A matched delete of row `n` above a winner at `winner_ms`.
fn delete_claim(state: &SmState, sequence_no: u64, n: u8, winner_ms: u64) -> Claim {
	let request = delete_request(n);
	let position = position_of(n);
	Claim {
		key: key(T0, sequence_no),
		request_digest: delete_request_digest(TENANT, &request),
		commit_hlc: hts(T0),
		coordinator: 0,
		first_row_position: position,
		children: vec![point_child(state, position)],
		operation: Operation::Delete {
			request,
			selection: DeleteSelection {
				cut: peek_index() - 1,
				outcomes: vec![DeleteOutcome::Deleted {
					winner_rsv: 1,
					winner_hlc: hts(winner_ms),
				}],
			},
		},
	}
}

pub(crate) fn noop_body(state: &SmState, sequence_no: u64, n: u8) -> NoopBody {
	let request = delete_request(n);
	let child = point_child(state, position_of(n));
	NoopBody {
		key: key(T0, sequence_no),
		request_digest: delete_request_digest(TENANT, &request),
		request,
		selection: DeleteSelection {
			cut: peek_index() - 1,
			outcomes: vec![DeleteOutcome::NotFound],
		},
		position: position_of(n),
		coordinator: child.holder,
		lineage: child.lineage,
	}
}

fn record_noop(state: &mut SmState, body: NoopBody) -> DecisionResult {
	let coordinator = body.coordinator;
	decide(
		state,
		DecisionCommand::RecordNoop {
			noop: sign_for_test(body, coordinator),
			stamp_ms: T0,
		},
	)
}

#[test]
fn a_kv_parent_writes_exactly_one_key_and_never_deletes() {
	let mut state = base(3);
	grant_all(&mut state);
	let kv = |sequence_no| ParentKey {
		namespace: ParentNamespace::Kv,
		..key(T0, sequence_no)
	};
	let one = point_child(&state, position_of(1));
	let good = Claim {
		first_row_position: one.range_start,
		..claim(kv(1), vec![one.clone()], 0)
	};
	assert!(matches!(
		reserve(&mut state, &good, T0),
		DecisionResult::Reserved { .. }
	));
	let mut two_rows = one.clone();
	two_rows.rows = 2;
	let mut wide = one.clone();
	wide.range_end += 1;
	let before = decisions(&state).clone();
	for (label, children) in [
		("two rows", vec![two_rows]),
		("a range", vec![wide]),
		("two children", children(&state, 2)),
	] {
		let claim = Claim {
			first_row_position: children[0].range_start,
			..claim(kv(2), children, 0)
		};
		assert_eq!(
			reserve(&mut state, &claim, T0),
			refused(DecisionRefusal::MalformedClaim),
			"{label}"
		);
	}
	let deleting = Claim {
		key: kv(3),
		..delete_claim(&state, 3, 1, T0 - 1)
	};
	assert_eq!(
		reserve(&mut state, &deleting, T0),
		refused(DecisionRefusal::MalformedClaim)
	);
	assert_eq!(decisions(&state), &before);
}

#[test]
fn a_matched_delete_publishes_one_tombstone_strictly_above_its_winner() {
	let mut state = base(3);
	grant_all(&mut state);
	let before = decisions(&state).clone();
	let not_above = delete_claim(&state, 1, 1, T0);
	let mut wrong_digest = delete_claim(&state, 1, 1, T0 - 1);
	wrong_digest.request_digest = [7; 32];
	let mut not_found = delete_claim(&state, 1, 1, T0 - 1);
	if let Operation::Delete { selection, .. } = &mut not_found.operation {
		selection.outcomes = vec![DeleteOutcome::NotFound];
	}
	let mut future_cut = delete_claim(&state, 1, 1, T0 - 1);
	if let Operation::Delete { selection, .. } = &mut future_cut.operation {
		// At or after any index this command can apply at.
		selection.cut = u64::MAX;
	}
	let mut other_table = delete_claim(&state, 1, 1, T0 - 1);
	other_table.key.namespace = ParentNamespace::Table(TableObjectId::new([0x0e; 16]));
	let mut moved = delete_claim(&state, 1, 1, T0 - 1);
	moved.children = vec![point_child(&state, position_of(2))];
	moved.first_row_position = moved.children[0].range_start;
	use DecisionRefusal::{MalformedClaim, TableUnavailable};
	for (label, claim, expected) in [
		("tombstone not above the winner", not_above, MalformedClaim),
		("digest", wrong_digest, MalformedClaim),
		(
			"a reserve that publishes nothing",
			not_found,
			MalformedClaim,
		),
		("cut not before the reserve", future_cut, MalformedClaim),
		// The namespace's table is checked first: it does not exist.
		("namespace is another table", other_table, TableUnavailable),
		("child not at the row's position", moved, MalformedClaim),
	] {
		assert_eq!(
			reserve(&mut state, &claim, T0),
			refused(expected),
			"{label}"
		);
	}
	assert_eq!(decisions(&state), &before);
	let good = delete_claim(&state, 1, 1, T0 - 1);
	assert!(matches!(
		reserve(&mut state, &good, T0),
		DecisionResult::Reserved { .. }
	));
}

#[test]
fn the_first_record_of_a_key_wins_between_a_delete_and_a_noop() {
	// A noop first: a later Reserve of the same request creates no body.
	let mut state = base(3);
	grant_all(&mut state);
	let noop = noop_body(&state, 1, 1);
	let DecisionResult::Terminal {
		reservation: dec,
		status: ParentStatus::Noop { dec: noop_dec },
	} = record_noop(&mut state, noop.clone())
	else {
		panic!("noop");
	};
	assert_eq!(dec, noop_dec);
	let digest = noop.request_digest;
	assert!(matches!(
		lookup_parent(decisions(&state), &noop.key, &digest),
		Lookup::Noop { record } if record.dec == dec && record.body == noop
	));
	assert_eq!(answer(decisions(&state), &noop.key, &digest), Answer::Noop);
	assert_eq!(
		{
			let body = noop_body(&state, 1, 1);
			record_noop(&mut state, body)
		},
		DecisionResult::Terminal {
			reservation: dec,
			status: ParentStatus::Noop { dec }
		},
		"a retry answers from the record"
	);
	let bodies = decisions(&state).bodies.len();
	assert_eq!(
		{
			let claim = delete_claim(&state, 1, 1, T0 - 1);
			reserve(&mut state, &claim, T0)
		},
		DecisionResult::Terminal {
			reservation: dec,
			status: ParentStatus::Noop { dec }
		}
	);
	assert_eq!(
		decisions(&state).bodies.len(),
		bodies,
		"no children after a noop"
	);

	// A reservation first: a later noop of the same request changes nothing.
	let mut state = base(3);
	grant_all(&mut state);
	let claim = delete_claim(&state, 1, 1, T0 - 1);
	let DecisionResult::Reserved { reservation } = reserve(&mut state, &claim, T0) else {
		panic!("reserve");
	};
	assert_eq!(
		{
			let body = noop_body(&state, 1, 1);
			record_noop(&mut state, body)
		},
		DecisionResult::Reserved { reservation }
	);
	assert!(decisions(&state).noops.is_empty());

	// Another request under the same key is a conflict.
	let mut other = noop_body(&state, 1, 2);
	other.key = key(T0, 1);
	assert_eq!(
		record_noop(&mut state, other),
		refused(DecisionRefusal::Conflict)
	);
	decisions(&state).validate().unwrap();
}

#[test]
fn a_noop_is_refused_without_its_structure_or_its_coordinators_authority() {
	let mut state = base(3);
	grant_all(&mut state);
	let before = decisions(&state).clone();
	let mut deleted = noop_body(&state, 1, 1);
	deleted.selection.outcomes = vec![DeleteOutcome::Deleted {
		winner_rsv: 1,
		winner_hlc: hts(T0 - 1),
	}];
	let mut future_cut = noop_body(&state, 1, 1);
	future_cut.selection.cut = u64::MAX;
	let mut moved = noop_body(&state, 1, 1);
	moved.position += 1;
	let mut digest = noop_body(&state, 1, 1);
	digest.request_digest = [7; 32];
	for (label, body) in [
		("a noop that deleted", deleted),
		("cut not before the noop", future_cut),
		("position", moved),
		("digest", digest),
	] {
		assert_eq!(
			record_noop(&mut state, body),
			refused(DecisionRefusal::MalformedClaim),
			"{label}"
		);
	}
	// Signed by a node that does not hold the row's lineage.
	let mut stranger = noop_body(&state, 1, 1);
	stranger.coordinator = signer(stranger.coordinator.node % 3 + 1);
	assert_eq!(
		record_noop(&mut state, stranger),
		refused(DecisionRefusal::LineageClosed)
	);
	assert_eq!(decisions(&state), &before);
}

#[test]
fn the_floor_retires_a_noops_key_and_keeps_its_record_and_bytes() {
	let mut state = base(3);
	grant_all(&mut state);
	let bytes_before = decisions(&state).retained_claim_bytes;
	assert!(matches!(
		{
			let body = noop_body(&state, 1, 1);
			record_noop(&mut state, body)
		},
		DecisionResult::Terminal { .. }
	));
	let record = decisions(&state).noops.values().next().unwrap().clone();
	assert_eq!(
		decisions(&state).retained_claim_bytes,
		bytes_before + record.bytes
	);
	assert!(matches!(
		floor(&mut state, T0, T0 + DECISION_EXPIRY_MS + 10),
		DecisionResult::FloorAdvanced {
			retired_keys: 1,
			..
		}
	));
	assert_eq!(decisions(&state).retry_keys(), 0);
	assert_eq!(decisions(&state).noops.len(), 1, "the record stays");
	assert_eq!(
		decisions(&state).retained_claim_bytes,
		bytes_before + record.bytes
	);
	decisions(&state).validate().unwrap();
	// A snapshot that lost the record's bytes, or a key naming a missing noop,
	// does not validate.
	let mut lost = decisions(&state).clone();
	lost.retained_claim_bytes -= 1;
	assert!(lost.validate().is_err());
	let mut dangling = decisions(&state).clone();
	dangling.keys.insert(
		record.body.key,
		KeyEntry {
			reservation: record.dec + 1,
			request_digest: record.body.request_digest,
			record: RecordKind::Noop,
		},
	);
	assert!(dangling.validate().is_err());
}

/// CODEX-3 R4: new wire variants are appended, so every earlier variant keeps
/// its positional tag.
#[test]
fn new_wire_variants_are_appended_after_the_existing_tags() {
	use crate::network::decision_proposals::ForwardedDecision;
	let tag = |bytes: Vec<u8>| bytes[0];
	assert_eq!(
		tag(postcard::to_stdvec(&ForwardedDecision::BindSigner {
			incarnation: inc(1),
			key: [0; 32],
		})
		.unwrap()),
		4
	);
	assert_eq!(
		tag(
			postcard::to_stdvec(&ForwardedDecision::RegisterIncarnation {
				incarnation: inc(1)
			})
			.unwrap()
		),
		5
	);
	let mut state = base(3);
	grant_all(&mut state);
	let body = noop_body(&state, 1, 1);
	let signer = body.coordinator;
	assert_eq!(
		tag(postcard::to_stdvec(&ForwardedDecision::RecordNoop {
			noop: sign_for_test(body, signer)
		})
		.unwrap()),
		6
	);
	assert_eq!(
		tag(postcard::to_stdvec(&DecisionRefusal::BarrierIncomplete).unwrap()),
		27
	);
	assert_eq!(
		tag(postcard::to_stdvec(&DecisionRefusal::CommitFromFuture).unwrap()),
		28
	);
	assert_eq!(tag(postcard::to_stdvec(&Lookup::Conflict).unwrap()), 2);
	assert_eq!(
		tag(postcard::to_stdvec(&Lookup::InvalidIdentity).unwrap()),
		3
	);
}

// ============================================================
//  ADR 0699 S5a: CERTIFIED GENERATIONS
// ============================================================

/// An Accepted parent whose retry key the floor has retired.
fn accepted_retired() -> (SmState, u64, u64) {
	let (mut state, claim, rsv) = reserved_parent(1);
	let DecisionResult::Terminal {
		status: ParentStatus::Accepted { dec },
		..
	} = finalize(&mut state, &claim, quorum_acks(&claim, rsv))
	else {
		panic!("accept");
	};
	assert!(matches!(
		floor(&mut state, T0, T0 + DECISION_EXPIRY_MS + 10),
		DecisionResult::FloorAdvanced { .. }
	));
	(state, rsv, dec)
}

fn target(state: &SmState, gid: u64, t: u64, predecessor_gid: u64) -> generations::Target {
	generations::Target {
		gid,
		t,
		predecessor_gid,
		members: generations::current_members(state).unwrap(),
		chunk_bits: generations::CHUNK_BITS,
		survivor_policy: generations::SURVIVOR_POLICY,
	}
}

fn propose(state: &mut SmState, gid: u64, t: u64, predecessor_gid: u64) -> DecisionResult {
	let target = target(state, gid, t, predecessor_gid);
	decide(state, DecisionCommand::ProposeTarget { target })
}

fn report_of(state: &SmState, member: Signer, chunk: u8) -> DecisionCommand {
	let pending = decisions(state).generations.pending.clone().unwrap();
	let body = generations::ReportBody {
		gid: pending.target.gid,
		target_digest: pending.digest,
		reporter: member,
		chunks: vec![
			generations::ChunkSum {
				count: u64::from(chunk),
				root: [chunk; 32],
			};
			generations::GENERATION_CHUNKS
		],
	};
	DecisionCommand::Report {
		report: sign_for_test(body, member),
	}
}

/// CODEX-3 R1: a member restarting while a target is pending, after every
/// member reported. The full report set is stale, never committed and never
/// counted as agreement; the driver replaces the target from the same
/// predecessor at a fresh gid with the current members, which then commits.
#[test]
fn a_member_restart_replaces_its_pending_target_rather_than_stalling() {
	let (mut state, rsv, dec) = accepted_retired();
	assert_eq!(
		propose(&mut state, 1, dec, 0),
		DecisionResult::TargetRecorded { gid: 1 }
	);
	let old = decisions(&state).generations.pending.clone().unwrap();
	for member in &old.target.members {
		let report = report_of(&state, *member, 3);
		assert!(matches!(
			decide(&mut state, report),
			DecisionResult::ReportRecorded { .. }
		));
	}
	assert_eq!(generations::ready_commit(&state), Some(1));
	// The driver runs on applied state: the next entry's index bounds T.
	let applied = |_: &SmState| peek_index();
	assert_eq!(
		generations::next_target(&state, applied(&state)),
		None,
		"a current pending target is not replaced"
	);

	// Node 3's store is replaced (a new incarnation; a same-store restart keeps
	// its incarnation and its target stays current).
	let restarted = StoreIncarnation::from_raw(9_003);
	replace_incarnation(&mut state, 3, restarted);
	state.decisions.as_mut().unwrap().signers.insert(
		(3, restarted),
		decision_verifying_key_bytes(dev_decision_signing_secret(3)),
	);
	assert_eq!(
		generations::ready_commit(&state),
		None,
		"a stale full report set is not committable"
	);
	assert_eq!(
		decide(&mut state, DecisionCommand::CommitGeneration { gid: 1 }),
		refused(DecisionRefusal::StaleGeneration)
	);
	assert!(decisions(&state).body(rsv).is_some(), "nothing erased");

	// The driver replaces it: same predecessor, fresh gid, current members.
	let replacement = generations::next_target(&state, applied(&state)).expect("a replacement");
	assert_eq!((replacement.gid, replacement.predecessor_gid), (2, 0));
	assert!(replacement.members.contains(&Signer {
		node: 3,
		incarnation: restarted
	}));
	assert_eq!(
		decide(
			&mut state,
			DecisionCommand::ProposeTarget {
				target: replacement.clone()
			}
		),
		DecisionResult::TargetRecorded { gid: 2 }
	);
	// A late report for the replaced target is refused.
	let late = generations::ReportBody {
		gid: 1,
		target_digest: old.digest,
		reporter: replacement.members[0],
		chunks: vec![
			generations::ChunkSum {
				count: 3,
				root: [3; 32]
			};
			generations::GENERATION_CHUNKS
		],
	};
	assert_eq!(
		decide(
			&mut state,
			DecisionCommand::Report {
				report: sign_for_test(late, replacement.members[0])
			}
		),
		refused(DecisionRefusal::StaleGeneration)
	);
	for member in &replacement.members {
		let report = report_of(&state, *member, 3);
		assert!(matches!(
			decide(&mut state, report),
			DecisionResult::ReportRecorded { .. }
		));
	}
	assert_eq!(generations::ready_commit(&state), Some(2));
	assert_eq!(
		decide(&mut state, DecisionCommand::CommitGeneration { gid: 2 }),
		DecisionResult::GenerationCommitted { gid: 2 }
	);
	assert!(
		decisions(&state).body(rsv).is_none(),
		"erased by the replacement"
	);
	let generations = &decisions(&state).generations;
	assert_eq!(
		(
			generations.current_gid(),
			generations.current.as_ref().unwrap().predecessor_gid,
			generations.pending.is_none(),
			generations.next_gid
		),
		(2, 0, true, 3)
	);
}

#[test]
fn a_cut_is_closed_over_decisions_not_reservations() {
	let (mut state, rsv, dec) = accepted_retired();
	// The counterexample: reserved at rsv, decided at dec > T: not admissible.
	let between = rsv;
	assert!(between < dec);
	let before = decisions(&state).clone();
	assert_eq!(
		propose(&mut state, 1, between, 0),
		refused(DecisionRefusal::TargetNotAdmissible)
	);
	assert_eq!(decisions(&state), &before);
	// Closed at or after the decision: admissible.
	assert_eq!(
		propose(&mut state, 1, dec, 0),
		DecisionResult::TargetRecorded { gid: 1 }
	);
}

#[test]
fn a_generation_commits_only_on_every_members_agreeing_report_and_erases_its_cut() {
	let (mut state, rsv, dec) = accepted_retired();
	let bytes = decisions(&state).retained_claim_bytes;
	assert!(bytes > 0);
	let proposed = target(&state, 1, dec, 0);
	let members = proposed.members.clone();
	assert_eq!(
		decide(
			&mut state,
			DecisionCommand::ProposeTarget { target: proposed }
		),
		DecisionResult::TargetRecorded { gid: 1 }
	);
	// Reports: wrong digest, partial coverage and a non-member are refused.
	let mut wrong = decisions(&state).generations.pending.clone().unwrap();
	wrong.digest = [1; 32];
	let stale = generations::ReportBody {
		gid: 1,
		target_digest: wrong.digest,
		reporter: members[0],
		chunks: vec![
			generations::ChunkSum {
				count: 0,
				root: [0; 32]
			};
			generations::GENERATION_CHUNKS
		],
	};
	assert_eq!(
		decide(
			&mut state,
			DecisionCommand::Report {
				report: sign_for_test(stale, members[0])
			}
		),
		refused(DecisionRefusal::StaleGeneration)
	);
	let DecisionCommand::Report { report } = report_of(&state, members[0], 3) else {
		unreachable!()
	};
	let mut partial = report.body.clone();
	partial.chunks.pop();
	assert_eq!(
		decide(
			&mut state,
			DecisionCommand::Report {
				report: sign_for_test(partial, members[0])
			}
		),
		refused(DecisionRefusal::ReportMismatch)
	);
	// Two of three reported: the barrier stalls.
	for member in &members[..2] {
		let report = report_of(&state, *member, 3);
		assert!(matches!(
			decide(&mut state, report),
			DecisionResult::ReportRecorded { .. }
		));
	}
	assert_eq!(
		decide(&mut state, DecisionCommand::CommitGeneration { gid: 1 }),
		refused(DecisionRefusal::BarrierIncomplete)
	);
	// A disagreeing third report: no commit.
	let disagreeing = report_of(&state, members[2], 4);
	assert!(matches!(
		decide(&mut state, disagreeing),
		DecisionResult::ReportRecorded { .. }
	));
	assert_eq!(
		decide(&mut state, DecisionCommand::CommitGeneration { gid: 1 }),
		refused(DecisionRefusal::ReportMismatch)
	);
	// A different re-report from the same member is refused.
	let changed = report_of(&state, members[2], 3);
	assert_eq!(
		decide(&mut state, changed),
		refused(DecisionRefusal::ReportMismatch)
	);
	assert!(decisions(&state).body(rsv).is_some(), "nothing erased yet");

	// A fresh target from the same predecessor abandons gid 1 (skipping it).
	let next = target(&state, 2, dec, 0);
	assert_eq!(
		decide(&mut state, DecisionCommand::ProposeTarget { target: next }),
		DecisionResult::TargetRecorded { gid: 2 }
	);
	// A late report for the abandoned target is refused.
	let late = generations::ReportBody {
		gid: 1,
		target_digest: [0; 32],
		reporter: members[0],
		chunks: vec![
			generations::ChunkSum {
				count: 3,
				root: [3; 32]
			};
			generations::GENERATION_CHUNKS
		],
	};
	assert_eq!(
		decide(
			&mut state,
			DecisionCommand::Report {
				report: sign_for_test(late, members[0])
			}
		),
		refused(DecisionRefusal::StaleGeneration)
	);
	for member in &members {
		let report = report_of(&state, *member, 3);
		assert!(matches!(
			decide(&mut state, report),
			DecisionResult::ReportRecorded { .. }
		));
	}
	let commit_index = peek_index();
	assert_eq!(
		decide(&mut state, DecisionCommand::CommitGeneration { gid: 2 }),
		DecisionResult::GenerationCommitted { gid: 2 }
	);
	let decisions = decisions(&state);
	assert!(decisions.body(rsv).is_none(), "the cut is erased");
	assert_eq!(decisions.retained_claim_bytes, 0, "and its bytes");
	let generations = decisions.generations();
	assert_eq!(generations.current_gid(), 2);
	assert_eq!(
		generations.current.as_ref().unwrap().predecessor_gid,
		0,
		"recorded predecessor, not gid arithmetic"
	);
	assert_eq!(generations.k_min(), commit_index);
	assert_eq!(generations.fr(), dec);
	assert!(generations.pending.is_none());
	decisions.validate().unwrap();
}

/// A generation commit refused for its byte accounting leaves the decision
/// state exactly as it was: the freed bytes are checked before anything is
/// removed. The retained-bytes invariant is broken on purpose here, the only
/// way to reach that refusal; with it restored, the same commit then succeeds.
#[test]
fn a_generation_commit_refused_for_its_byte_accounting_changes_nothing() {
	let (mut state, rsv, dec) = accepted_retired();
	let bytes = decisions(&state).retained_claim_bytes;
	assert!(bytes > 0);
	let proposed = target(&state, 1, dec, 0);
	let members = proposed.members.clone();
	assert_eq!(
		decide(
			&mut state,
			DecisionCommand::ProposeTarget { target: proposed }
		),
		DecisionResult::TargetRecorded { gid: 1 }
	);
	for member in &members {
		let report = report_of(&state, *member, 3);
		assert!(matches!(
			decide(&mut state, report),
			DecisionResult::ReportRecorded { .. }
		));
	}
	// Fewer retained bytes than the cut frees: the subtraction cannot hold.
	state.decisions.as_mut().unwrap().retained_claim_bytes = bytes - 1;
	let before = decisions(&state).clone();
	assert_eq!(
		decide(&mut state, DecisionCommand::CommitGeneration { gid: 1 }),
		refused(DecisionRefusal::CounterOverflow)
	);
	assert_eq!(
		decisions(&state),
		&before,
		"the refusal removed nothing and committed nothing"
	);
	assert!(decisions(&state).body(rsv).is_some(), "the cut is intact");
	// The invariant restored, the same commit erases the cut.
	state.decisions.as_mut().unwrap().retained_claim_bytes = bytes;
	assert_eq!(
		decide(&mut state, DecisionCommand::CommitGeneration { gid: 1 }),
		DecisionResult::GenerationCommitted { gid: 1 }
	);
	assert!(decisions(&state).body(rsv).is_none(), "the cut is erased");
	assert_eq!(decisions(&state).retained_claim_bytes, 0);
	decisions(&state).validate().unwrap();
}

#[test]
fn a_reserved_parent_or_an_unretired_key_blocks_the_cut() {
	// Reserved: blocks.
	let (mut state, _claim, rsv) = reserved_parent(1);
	assert_eq!(
		propose(&mut state, 1, rsv, 0),
		refused(DecisionRefusal::TargetNotAdmissible)
	);
	// Accepted but its key not yet retired by the floor: blocks.
	let (mut state, claim, rsv) = reserved_parent(1);
	let DecisionResult::Terminal {
		status: ParentStatus::Accepted { dec },
		..
	} = finalize(&mut state, &claim, quorum_acks(&claim, rsv))
	else {
		panic!("accept");
	};
	assert_eq!(
		propose(&mut state, 1, dec, 0),
		refused(DecisionRefusal::TargetNotAdmissible)
	);
}

// ============================================================
//  ADR 0701: A HOLDER'S BATCHED LEASE RENEWAL
// ============================================================

fn lease_batch(state: &SmState, leases: Vec<LeaseDescriptor>) -> WendelonRequest {
	lease_batch_at(state, 0, leases)
}

fn batch_results(response: WendelonResponse) -> Vec<(u64, bool)> {
	match response {
		WendelonResponse::LeaseResults(results) => results
			.results()
			.iter()
			.map(|result| (result.range_start, result.granted))
			.collect(),
		other => panic!("not lease results: {other:?}"),
	}
}

/// The same leases applied as one `AcquireLeases` and as separate
/// `AcquireLease` entries, from equal states: equal per-lease outcomes, leases,
/// lineages and generations. Returns the outcomes and the batched state.
fn batch_matches_singles(
	setup: impl Fn(&mut SmState),
	leases: Vec<LeaseDescriptor>,
) -> (Vec<(u64, bool)>, SmState) {
	let (mut one, mut many) = (base(3), base(3));
	setup(&mut one);
	setup(&mut many);
	let singles: Vec<(u64, bool)> = leases
		.iter()
		.map(|lease| {
			let response = grant(&mut one, *lease);
			(lease.range_start, response == WendelonResponse::Success)
		})
		.collect();
	let request = lease_batch(&many, leases);
	let batched = batch_results(apply_next(&mut many, request));
	assert_eq!(batched, singles);
	assert_eq!(one.leases, many.leases);
	assert_eq!(decisions(&one).lineages, decisions(&many).lineages);
	assert_eq!(
		decisions(&one).next_generation(),
		decisions(&many).next_generation()
	);
	(singles, many)
}

/// ADR 0701: for a WELL-FORMED batch, one entry applies exactly as its leases
/// one at a time, in declared order, so a later denial undoes nothing.
#[test]
fn a_well_formed_lease_batch_applies_as_its_leases_one_at_a_time() {
	let holder = universe()[0].0;
	let ranges: Vec<(u64, u64)> = universe()
		.into_iter()
		.filter(|(id, _)| *id == holder)
		.map(|(_, range)| range)
		.collect();
	assert!(ranges.len() >= 3, "{ranges:?}");
	let other = universe()
		.into_iter()
		.find(|(id, _)| *id != holder)
		.unwrap()
		.0;
	let fresh = |range: (u64, u64)| lease(holder, range, 0, 1_000);

	// Mixed outcome. SYNTHESIZED fixture: a live lease of another holder over
	// the second range, put straight into state to exercise the shared reducer.
	// A batch of canonical primary ranges cannot overlap within itself, and
	// mode admission would not grant this lease; it is not claimed reachable.
	let synthesized = |state: &mut SmState| {
		state
			.leases
			.insert(ranges[1].0, lease(other, ranges[1], 0, 10_000));
	};
	let (outcomes, _) =
		batch_matches_singles(synthesized, ranges[..3].iter().map(|r| fresh(*r)).collect());
	assert_eq!(
		outcomes,
		vec![
			(ranges[0].0, true),
			(ranges[1].0, false),
			(ranges[2].0, true)
		]
	);

	// Continuous renewal by a new issuer keeps the generation.
	let granted = |state: &mut SmState| {
		for range in &ranges[..2] {
			assert_eq!(grant(state, fresh(*range)), WendelonResponse::Success);
		}
	};
	let mut before = base(3);
	granted(&mut before);
	let renewals = ranges[..2]
		.iter()
		.map(|range| {
			let mut renewal = lease(holder, *range, 500, 2_000);
			renewal.issuer = node(2);
			renewal
		})
		.collect();
	let (outcomes, after) = batch_matches_singles(granted, renewals);
	assert!(outcomes.iter().all(|(_, granted)| *granted));
	assert_eq!(decisions(&after).lineages, decisions(&before).lineages);

	// A grant after a gap replaces the lineage.
	let gapped = ranges[..2]
		.iter()
		.map(|range| lease(holder, *range, 2_000, 3_000))
		.collect();
	let (outcomes, after) = batch_matches_singles(granted, gapped);
	assert!(outcomes.iter().all(|(_, granted)| *granted));
	for range in &ranges[..2] {
		assert!(
			decisions(&after).lineages[&range.0].generation
				> decisions(&before).lineages[&range.0].generation
		);
	}
}

/// ADR 0701: a gapped batch closes the lineage a reserved parent is bound to;
/// that parent becomes Refused only through the Finalize that finds it closed.
#[test]
fn a_gapped_lease_batch_leaves_the_old_parent_to_be_refused_at_finalize() {
	let (mut state, claim, rsv) = reserved_parent(1);
	let child = claim.children[usize::from(claim.coordinator)].clone();
	let range_end = decisions(&state).lineages[&child.lineage.range_start].range_end;
	let mut gapped = lease(
		child.holder.node,
		(child.lineage.range_start, range_end),
		2_000,
		3_000,
	);
	gapped.issuer = node(2);
	assert_eq!(
		batch_results({
			let request = lease_batch(&state, vec![gapped]);
			apply_next(&mut state, request)
		}),
		vec![(child.lineage.range_start, true)]
	);
	assert_eq!(
		decisions(&state).body(rsv).unwrap().status,
		ParentStatus::Reserved,
		"a grant alone records no outcome"
	);
	assert!(matches!(
		finalize(&mut state, &claim, quorum_acks(&claim, rsv)),
		DecisionResult::Terminal {
			status: ParentStatus::Refused {
				cause: RefusalCause::LineageClosed,
				..
			},
			..
		}
	));
}

/// ADR 0701: an invalid envelope applies nothing, and Today refuses any batch.
/// This is all-or-nothing refusal, not an equivalence with single grants.
#[test]
fn a_malformed_lease_batch_or_one_under_today_applies_nothing() {
	let (holder, range) = universe()[0];
	let (other, other_range) = universe()
		.into_iter()
		.find(|(id, _)| *id != holder)
		.unwrap();
	let good = lease(holder, range, 0, 1_000);
	let mut later_epoch = lease(holder, (range.1 + 1, range.1 + 10), 0, 1_000);
	later_epoch.epoch += 1;
	let mut reversed = lease(holder, (range.1 + 10, range.1 + 1), 0, 1_000);
	reversed.range_start = range.1 + 10;
	for (why, leases) in [
		("empty", vec![]),
		("duplicate start", vec![good, good]),
		(
			"two holders",
			vec![good, lease(other, other_range, 0, 1_000)],
		),
		("two epochs", vec![good, later_epoch]),
		("reversed", vec![good, reversed]),
	] {
		let mut state = base(3);
		let (leases_before, decisions_before) = (state.leases.clone(), decisions(&state).clone());
		let request = lease_batch(&state, leases);
		let response = apply_next(&mut state, request);
		assert!(
			matches!(&response, WendelonResponse::Error(message) if message.starts_with("lease batch malformed")),
			"{why}: {response:?}"
		);
		assert_eq!(state.leases, leases_before, "{why}");
		assert_eq!(decisions(&state), &decisions_before, "{why}");
	}
	let mut today = SmState::default();
	let request = lease_batch(&today, vec![good]);
	let response = apply_next(&mut today, request);
	assert!(
		matches!(response, WendelonResponse::Error(_)),
		"{response:?}"
	);
	assert!(today.leases.is_empty() && today.decisions.is_none());
}

/// ADR 0701: decoding refuses a batch or a result list longer than the cap
/// from its declared length, before reading or allocating an element.
#[test]
fn an_oversized_lease_batch_is_refused_from_its_length() {
	use crate::consensus::{LeaseBatch, LeaseGrantResults};
	use crate::schema::MAX_LEASES_PER_BATCH;
	let one = lease(universe()[0].0, universe()[0].1, 0, 1_000);
	let one = crate::consensus::LeaseBatchGrant {
		lease: one,
		expected_generation: None,
		holder_incarnation: inc(universe()[0].0),
	};
	// The wire shape: the stamp, then the bounded sequence of grants.
	let at_cap = postcard::to_stdvec(&(7_u64, vec![one; MAX_LEASES_PER_BATCH])).unwrap();
	let decoded = postcard::from_bytes::<LeaseBatch>(&at_cap).unwrap();
	assert_eq!(decoded.grants().len(), MAX_LEASES_PER_BATCH);
	assert_eq!(decoded.stamp_ms(), 7);
	let over = postcard::to_stdvec(&(7_u64, vec![one; MAX_LEASES_PER_BATCH + 1])).unwrap();
	assert!(postcard::from_bytes::<LeaseBatch>(&over).is_err());
	assert!(LeaseBatch::new(7, vec![one; MAX_LEASES_PER_BATCH + 1]).is_none());
	// A declared length of a billion with no elements behind it is refused
	// from the length alone.
	let huge = postcard::to_stdvec(&1_000_000_000u32).unwrap();
	assert!(postcard::from_bytes::<LeaseBatch>(&huge).is_err());
	assert!(postcard::from_bytes::<LeaseGrantResults>(&huge).is_err());
}

// ============================================================
//  ADR 0720: MEMBERSHIP IS SEALED BY THE FIRST ADMITTED WRITE
// ============================================================

fn epoch_with(epoch: u64, peers: &[(u32, u8)]) -> EpochConfig {
	let mut config = epoch_config(epoch);
	config.peers = peers
		.iter()
		.map(|(id, role_bits)| {
			let mut peer = epoch_config(epoch).peers[&1].clone();
			peer.node_id = node(*id);
			peer.address = format!("127.0.0.1:{}", 7000 + id);
			peer.role_bits = *role_bits;
			(*id, peer)
		})
		.collect();
	config
}

fn advance(state: &mut SmState, from: EpochConfig, to: EpochConfig) -> WendelonResponse {
	apply_next(
		state,
		WendelonRequest::AdvanceEpoch(AdvanceEpochRecord { from, to }),
	)
}

fn sealed_placement(state: &SmState) -> Option<Vec<(u32, bool)>> {
	decisions(state)
		.sealed()
		.map(|sealed| sealed.placement().iter().copied().collect())
}

#[test]
fn the_first_admitted_reserve_seals_and_a_refused_one_does_not() {
	let mut state = base(3);
	grant_all(&mut state);
	assert_eq!(sealed_placement(&state), None);

	// Refused for its own reasons: nothing is sealed, nothing is counted.
	let before = decisions(&state).clone();
	let mut not_v7 = claim(key(T0, 1), children(&state, 1), 0);
	not_v7.key.client_uuid[6] = 0x40;
	assert_eq!(
		reserve(&mut state, &not_v7, T0),
		refused(DecisionRefusal::InvalidIdentity)
	);
	assert_eq!(decisions(&state), &before);

	let first = claim(key(T0, 2), children(&state, 1), 0);
	assert!(matches!(
		reserve(&mut state, &first, T0),
		DecisionResult::Reserved { .. }
	));
	assert_eq!(
		sealed_placement(&state),
		Some(vec![(1, true), (2, true), (3, true)])
	);
	decisions(&state).validate().unwrap();
}

#[test]
fn an_unformed_cluster_admits_nothing_and_seals_nothing() {
	for (what, membership, epoch) in [
		(
			"a joint configuration",
			membership_of(&[&[1, 2, 3], &[1, 2, 3, 4]], &[]),
			epoch_config(EPOCH),
		),
		(
			"a learner",
			membership_of(&[&[1, 2, 3]], &[4]),
			epoch_config(EPOCH),
		),
		(
			"a voter the epoch does not name",
			formed_membership(&[1, 2, 3, 4]),
			epoch_config(EPOCH),
		),
		(
			"an epoch peer that is not a voter",
			formed_membership(&[1, 2]),
			epoch_config(EPOCH),
		),
		(
			"no applied membership",
			openraft::StoredMembership::default(),
			epoch_config(EPOCH),
		),
	] {
		let mut state = base(3);
		state.epoch_history.insert(EPOCH, epoch);
		grant_all(&mut state);
		state.last_membership = membership;
		let before = decisions(&state).clone();
		let claim = claim(key(T0, 1), children(&state, 1), 0);
		assert_eq!(
			reserve(&mut state, &claim, T0),
			refused(DecisionRefusal::FormationIncomplete),
			"{what}"
		);
		assert_eq!(decisions(&state), &before, "{what}");
		// Once formed, the same claim is admitted and seals.
		state.last_membership = formed_membership(&[1, 2, 3]);
		assert!(
			matches!(
				reserve(&mut state, &claim, T0),
				DecisionResult::Reserved { .. }
			),
			"{what}"
		);
		assert!(decisions(&state).sealed().is_some(), "{what}");
	}
}

#[test]
fn a_sealed_store_refuses_an_epoch_that_changes_placement_and_changes_nothing() {
	let (mut state, _claim, _reservation) = reserved_parent(1);
	let sealed = [(1, 1u8), (2, 1), (3, 1)];
	let cases = [
		(
			"a fourth peer",
			epoch_config(EPOCH),
			epoch_with(EPOCH + 1, &[(1, 1), (2, 1), (3, 1), (4, 1)]),
		),
		(
			"a removed peer",
			epoch_config(EPOCH),
			epoch_with(EPOCH + 1, &[(1, 1), (2, 1)]),
		),
		(
			"a peer that lost the Data role",
			epoch_config(EPOCH),
			epoch_with(EPOCH + 1, &[(1, 1), (2, 1), (3, 2)]),
		),
		(
			// Both ends are recorded with or_insert, so an unrecorded `from` at the
			// higher epoch would become the serving configuration.
			"an incompatible from at the higher epoch",
			epoch_with(EPOCH + 2, &[(1, 1), (2, 1), (3, 1), (4, 1)]),
			epoch_with(EPOCH + 1, &sealed),
		),
	];
	for (what, from, to) in cases {
		let (epochs, lineages, decisions_before) = (
			state.epoch_history.clone(),
			decisions(&state).lineages.clone(),
			decisions(&state).clone(),
		);
		assert!(!lineages.is_empty());
		let response = advance(&mut state, from, to);
		assert!(
			matches!(&response, WendelonResponse::Error(why) if why.starts_with("MembershipSealed")),
			"{what}: {response:?}"
		);
		assert_eq!(state.epoch_history, epochs, "{what}");
		assert_eq!(decisions(&state), &decisions_before, "{what}");
	}
}

#[test]
fn a_sealed_store_takes_a_same_placement_epoch_and_writes_resume_after_regrant() {
	let (mut state, _claim, _reservation) = reserved_parent(1);
	// Same peers and roles; an address and a capability changed.
	let mut next = epoch_config(EPOCH + 1);
	next.peers.get_mut(&2).unwrap().address = "127.0.0.1:9999".into();
	next.peers.get_mut(&3).unwrap().capability_bits = 0b1100;
	assert_eq!(
		advance(&mut state, epoch_config(EPOCH), next),
		WendelonResponse::Success
	);
	assert_eq!(state.epoch_history.len(), 2);
	assert!(
		decisions(&state).lineages.is_empty(),
		"lineages are regranted"
	);
	for (holder, range) in universe() {
		let mut regrant = lease(holder, range, 0, 1_000);
		regrant.epoch = EPOCH + 1;
		grant(&mut state, regrant);
	}
	// ADR 0729: the regranted lineages began after the first write, so their
	// floor is above its stamp: a claim at that stamp is refused, one above
	// the floor is reserved.
	let floor = decisions(&state)
		.lineages
		.values()
		.map(|lineage| lineage.floor_ms)
		.max()
		.unwrap();
	assert_eq!(floor, T0 + DECISION_COMMIT_HLC_LEAD_MS);
	let below = claim(key(T0, 2), children(&state, 1), 0);
	assert_eq!(
		reserve(&mut state, &below, T0),
		refused(DecisionRefusal::BelowTenureFloor)
	);
	let mut second = claim(key(T0, 3), children(&state, 1), 0);
	second.commit_hlc = hts(floor + 1);
	assert!(matches!(
		reserve(&mut state, &second, T0 + 1),
		DecisionResult::Reserved { .. }
	));
}

#[test]
fn a_voter_change_after_the_seal_fences_new_parents_and_nothing_else() {
	let (mut state, first, reservation) = reserved_parent(1);
	let second = claim(key(T0, 2), children(&state, 1), 0);
	assert!(matches!(
		reserve(&mut state, &second, T0),
		DecisionResult::Reserved { .. }
	));
	let sealed = decisions(&state).sealed().cloned();

	for (what, membership) in [
		("a stray voter", formed_membership(&[1, 2, 3, 4])),
		(
			"a joint configuration",
			membership_of(&[&[1, 2, 3], &[1, 2, 3, 4]], &[]),
		),
		("a learner", membership_of(&[&[1, 2, 3]], &[4])),
		("a lost voter", formed_membership(&[1, 2])),
	] {
		state.last_membership = membership;
		let before = decisions(&state).clone();
		let third = claim(key(T0, 3), children(&state, 1), 0);
		assert_eq!(
			reserve(&mut state, &third, T0),
			refused(DecisionRefusal::MembershipSealed),
			"{what}"
		);
		assert_eq!(decisions(&state), &before, "{what}");
		// A recorded parent still answers as recorded: the fence never hides an
		// outcome from its retry.
		assert_eq!(
			reserve(&mut state, &first, T0),
			DecisionResult::Reserved { reservation },
			"{what}"
		);
	}

	// With the stray voter still applied, parents reserved earlier finish: one
	// is finalized by its acknowledgements and one is aborted.
	state.last_membership = formed_membership(&[1, 2, 3, 4]);
	assert!(matches!(
		finalize(&mut state, &first, quorum_acks(&first, reservation)),
		DecisionResult::Terminal {
			status: ParentStatus::Accepted { .. },
			..
		}
	));
	let DecisionResult::Reserved {
		reservation: second_reservation,
	} = reserve(&mut state, &second, T0)
	else {
		panic!("the second parent is on record");
	};
	assert!(matches!(
		abort(&mut state, &second, second_reservation),
		DecisionResult::Terminal { .. }
	));
	assert_eq!(decisions(&state).sealed().cloned(), sealed);

	// The sealed membership back in effect: parents are admitted again.
	state.last_membership = formed_membership(&[1, 2, 3]);
	let fourth = claim(key(T0, 4), children(&state, 1), 0);
	assert!(matches!(
		reserve(&mut state, &fourth, T0),
		DecisionResult::Reserved { .. }
	));
}

#[test]
fn the_seal_outlives_every_record_and_a_snapshot_cannot_drop_it() {
	let golden = golden_decisions();
	golden.validate().unwrap();
	assert!(golden.sealed().is_some());
	let bytes = postcard::to_stdvec(&golden).unwrap();
	let mut decoded: DecisionState = postcard::from_bytes(&bytes).unwrap();
	// Wendelon ADR 0113: as the snapshot reader does, the derived index is rebuilt.
	decoded.restore_table_operations().unwrap();
	assert_eq!(decoded.sealed(), golden.sealed());

	// Records without a seal were not written by this build.
	let mut unsealed = golden.clone();
	unsealed.sealed = None;
	assert!(
		unsealed
			.validate()
			.unwrap_err()
			.contains("no membership is sealed")
	);
	let mut empty = golden.clone();
	empty.sealed.as_mut().unwrap().placement.clear();
	assert!(empty.validate().unwrap_err().contains("is empty"));

	// Every record retired: the seal is all that says writes began.
	let mut pruned = golden.clone();
	pruned.bodies.clear();
	pruned.keys.clear();
	pruned.noops.clear();
	pruned.live = 0;
	pruned.retained_claim_bytes = 0;
	// Wendelon ADR 0113: the derived index follows the keys cleared by hand.
	pruned.restore_table_operations().unwrap();
	pruned.validate().unwrap();
	let mut state = base(3);
	state.decisions = Some(pruned);
	grant_all(&mut state);
	state.last_membership = formed_membership(&[1, 2, 3, 4]);
	// Above the regranted lineages' floors (ADR 0729), so the seal is what refuses.
	let mut claim = claim(key(T0 + 10_000_000, 9), children(&state, 1), 0);
	claim.commit_hlc = hts(T0 + 10_000_000);
	assert_eq!(
		reserve(&mut state, &claim, T0 + 10_000_000),
		refused(DecisionRefusal::MembershipSealed)
	);
}

// ============================================================
//  ADR 0721: THE KEY A STORE SIGNS WITH AGAINST ITS BINDING
// ============================================================

#[test]
fn a_local_signer_is_not_current_unbound_bound_or_mismatched() {
	let mut state = base(3);
	let mine = signer(1);
	let key = decision_verifying_key_bytes(dev_decision_signing_secret(1));
	let other = decision_verifying_key_bytes(dev_decision_signing_secret(9));
	assert_eq!(
		local_signer_binding(&state, mine, key),
		LocalSignerBinding::Bound
	);
	assert_eq!(
		local_signer_binding(&state, mine, other),
		LocalSignerBinding::Mismatch
	);
	state
		.decisions
		.as_mut()
		.unwrap()
		.signers
		.remove(&(1, inc(1)));
	assert_eq!(
		local_signer_binding(&state, mine, key),
		LocalSignerBinding::Unbound
	);
	// A store that is not its node's current incarnation is judged on nothing
	// else, whatever key is bound for the incarnation that is.
	let stale = Signer {
		node: 1,
		incarnation: StoreIncarnation::from_raw(4_242),
	};
	assert_eq!(
		local_signer_binding(&state, stale, key),
		LocalSignerBinding::NotCurrent
	);
	state.incarnations.remove(&1);
	assert_eq!(
		local_signer_binding(&state, mine, key),
		LocalSignerBinding::NotCurrent
	);
}

// ============================================================
//  ADR 0729: A RANGE'S AUTHORITY MOVES TO A REPLICA
// ============================================================

fn takeover(
	state: &mut SmState,
	range_start: u64,
	generation: u64,
	successor: u32,
	stamp_ms: u64,
) -> DecisionResult {
	decide(
		state,
		DecisionCommand::TakeOverLineage {
			takeover: sign_for_test(
				TakeOverBody {
					range_start,
					generation,
					successor: signer(successor),
				},
				signer(successor),
			),
			stamp_ms,
		},
	)
}

/// A sealed store with one reserved parent, the range of its coordinator
/// child, that range's holder, and its lease renewed to expire at
/// `T0 + 10_000` (so it is unexpired against the ring clock, which the
/// Reserve set to `T0`).
fn sealed_with_a_live_lease() -> (SmState, Claim, u64, (u64, u64), u32) {
	let (mut state, claim, rsv) = reserved_parent(1);
	let child = claim.children[usize::from(claim.coordinator)].clone();
	let start = child.lineage.range_start;
	let end = decisions(&state).lineages[&start].range_end;
	let holder = child.holder.node;
	assert_eq!(
		grant(&mut state, lease(holder, (start, end), 500, T0 + 10_000)),
		WendelonResponse::Success,
		"a renewal"
	);
	assert_eq!(decisions(&state).clock_ms(), T0);
	(state, claim, rsv, (start, end), holder)
}

const LAPSED: u64 = T0 + 10_000 + TAKEOVER_GRACE_MS + 1;

#[test]
fn a_takeover_needs_a_lapsed_lease_the_current_generation_and_an_eligible_successor() {
	let (mut state, _claim, _rsv, range, holder) = sealed_with_a_live_lease();
	let generation = decisions(&state).lineages[&range.0].generation;
	let successor = (1..=3).find(|id| *id != holder).unwrap();
	let third = (1..=3)
		.find(|id| *id != holder && *id != successor)
		.unwrap();
	let before = state.clone();
	let unchanged = |state: &SmState, what: &str| {
		assert_eq!(decisions(state), decisions(&before), "{what}");
		assert_eq!(state.leases, before.leases, "{what}");
	};
	// The lease has not lapsed on the ring clock: at its expiry, and through
	// the grace.
	for stamp in [T0, T0 + 10_000, T0 + 10_000 + TAKEOVER_GRACE_MS] {
		assert_eq!(
			takeover(&mut state, range.0, generation, successor, stamp),
			refused(DecisionRefusal::TakeoverNotLapsed),
			"{stamp}"
		);
		unchanged(&state, "not lapsed");
	}
	// Past the grace: another generation, no such lineage, the holder itself.
	assert_eq!(
		takeover(&mut state, range.0, generation + 1, successor, LAPSED),
		refused(DecisionRefusal::TakeoverStale)
	);
	assert_eq!(
		takeover(&mut state, range.0 + 1, generation, successor, LAPSED),
		refused(DecisionRefusal::TakeoverStale)
	);
	assert_eq!(
		takeover(&mut state, range.0, generation, holder, LAPSED),
		refused(DecisionRefusal::TakeoverIneligible)
	);
	unchanged(&state, "stale or ineligible");
	// A successor at an incarnation that is not its node's current one.
	let mut replaced = state.clone();
	replace_incarnation(&mut replaced, successor, StoreIncarnation::from_raw(0xDEAD));
	let replaced_before = replaced.clone();
	assert_eq!(
		takeover(&mut replaced, range.0, generation, successor, LAPSED),
		refused(DecisionRefusal::SignerNotCurrent)
	);
	assert_eq!(
		decisions(&replaced).lineages,
		decisions(&replaced_before).lineages
	);
	// Some tenant's replicas are not every data member: unsupported.
	let mut narrow = state.clone();
	narrow.tenants.get_mut(&TENANT).unwrap().replication_factor = 2;
	assert_eq!(
		takeover(&mut narrow, range.0, generation, successor, LAPSED),
		refused(DecisionRefusal::UnsupportedShape)
	);
	// A membership that is not the sealed one.
	let mut moved = state.clone();
	moved.last_membership = formed_membership(&[1, 2, 3, 4]);
	assert_eq!(
		takeover(&mut moved, range.0, generation, successor, LAPSED),
		refused(DecisionRefusal::MembershipSealed)
	);
	// No write admitted yet: nothing is sealed, nothing is taken over.
	let mut unsealed = base(3);
	grant_all(&mut unsealed);
	let (first_holder, first_range) = universe()[0];
	let other = (1..=3).find(|id| *id != first_holder).unwrap();
	let unsealed_generation = decisions(&unsealed).lineages[&first_range.0].generation;
	assert_eq!(
		takeover(
			&mut unsealed,
			first_range.0,
			unsealed_generation,
			other,
			LAPSED
		),
		refused(DecisionRefusal::FormationIncomplete)
	);

	// Admitted: the lineage is the successor's at a new generation, with a
	// floor above everything the ring clock has seen, protected, awaiting its
	// first grant; the old holder's lease record is gone; the clock is the stamp.
	let next = decisions(&state).next_generation;
	assert_eq!(
		takeover(&mut state, range.0, generation, successor, LAPSED),
		DecisionResult::TookOver {
			range_start: range.0,
			generation: next
		}
	);
	let lineage = decisions(&state).lineages[&range.0];
	assert_eq!(
		lineage,
		Lineage {
			range_end: range.1,
			epoch: EPOCH,
			holder: node(successor),
			incarnation: inc(successor),
			generation: next,
			floor_ms: LAPSED + DECISION_COMMIT_HLC_LEAD_MS,
			// The lapsed lease ran from 500 to T0 + 10_000: clamped to the maximum.
			protected_until_ms: LAPSED + TAKEOVER_PROTECT_MAX_MS,
			first_grant_pending: true,
		}
	);
	assert_eq!(decisions(&state).next_generation, next + 1);
	assert_eq!(decisions(&state).clock_ms(), LAPSED);
	assert!(!state.leases.contains_key(&range.0));
	assert_eq!(
		decisions(&state).lineages.len(),
		decisions(&before).lineages.len()
	);

	// The consumed generation cannot be taken again; the new one is protected.
	assert_eq!(
		takeover(&mut state, range.0, generation, third, LAPSED + 1),
		refused(DecisionRefusal::TakeoverStale)
	);
	assert_eq!(
		takeover(&mut state, range.0, next, third, lineage.protected_until_ms),
		refused(DecisionRefusal::TakeoverNotLapsed)
	);
	// The successor never got a grant: past its protection the range moves again.
	let again = decisions(&state).next_generation;
	assert_eq!(
		takeover(
			&mut state,
			range.0,
			next,
			third,
			lineage.protected_until_ms + 1
		),
		DecisionResult::TookOver {
			range_start: range.0,
			generation: again
		}
	);
	let lineage = decisions(&state).lineages[&range.0];
	assert_eq!(lineage.holder, node(third));
	// No lease was on record this time: the default protection.
	assert_eq!(
		lineage.protected_until_ms,
		decisions(&state).clock_ms() + TAKEOVER_PROTECT_DEFAULT_MS
	);
}

#[test]
fn after_a_takeover_only_the_successor_is_granted_and_its_first_grant_keeps_the_generation() {
	let (mut state, _claim, _rsv, range, holder) = sealed_with_a_live_lease();
	let old = decisions(&state).lineages[&range.0].generation;
	let successor = (1..=3).find(|id| *id != holder).unwrap();
	takeover(&mut state, range.0, old, successor, LAPSED);
	let taken = decisions(&state).lineages[&range.0];
	let batch = |state: &mut SmState, who: u32, expected: Option<u64>, start: u64, expiry: u64| {
		let grants = vec![crate::consensus::LeaseBatchGrant {
			lease: lease(who, range, start, expiry),
			expected_generation: expected,
			holder_incarnation: inc(who),
		}];
		granted(apply_next(
			state,
			WendelonRequest::AcquireLeases(
				crate::consensus::LeaseBatch::new(LAPSED + 5_000, grants).unwrap(),
			),
		))
	};
	let before = state.clone();
	// The old holder, whatever generation it names; the successor naming the
	// generation it replaced, or none; the single grant; a release.
	for (who, expected) in [
		(holder, Some(old)),
		(holder, Some(taken.generation)),
		(holder, None),
		(successor, Some(old)),
		(successor, None),
	] {
		assert_eq!(
			batch(&mut state, who, expected, LAPSED, LAPSED + 3_000),
			WendelonResponse::LeaseDenied,
			"{who} {expected:?}"
		);
		assert_eq!(decisions(&state), decisions(&before), "{who} {expected:?}");
		assert_eq!(state.leases, before.leases);
	}
	assert_eq!(
		apply_next(
			&mut state,
			WendelonRequest::AcquireLease(lease(successor, range, LAPSED, LAPSED + 3_000))
		),
		WendelonResponse::LeaseDenied,
		"the single grant names no generation"
	);
	assert_eq!(decisions(&state), decisions(&before));

	// The successor's first grant: the takeover's generation and floor stand.
	assert_eq!(
		batch(
			&mut state,
			successor,
			Some(taken.generation),
			LAPSED,
			LAPSED + 3_000
		),
		WendelonResponse::Success
	);
	let first = decisions(&state).lineages[&range.0];
	assert_eq!(
		first,
		Lineage {
			first_grant_pending: false,
			..taken
		}
	);
	assert_eq!(state.leases[&range.0].holder, node(successor));
	// A renewal continues it.
	assert_eq!(
		batch(
			&mut state,
			successor,
			Some(taken.generation),
			LAPSED + 1_000,
			LAPSED + 4_000
		),
		WendelonResponse::Success
	);
	assert_eq!(decisions(&state).lineages[&range.0], first);
	// A regrant after a gap opens a new generation for the same holder, with a
	// floor from the ring clock and the batch's stamp.
	let clock = decisions(&state).clock_ms();
	assert_eq!(
		batch(
			&mut state,
			successor,
			Some(taken.generation),
			LAPSED + 4_001,
			LAPSED + 7_000
		),
		WendelonResponse::Success
	);
	let regranted = decisions(&state).lineages[&range.0];
	assert_eq!(regranted.holder, node(successor));
	assert_eq!(regranted.generation, taken.generation + 1);
	assert_eq!(decisions(&state).clock_ms(), clock.max(LAPSED + 5_000));
	assert_eq!(
		regranted.floor_ms,
		clock.max(LAPSED + 5_000) + DECISION_COMMIT_HLC_LEAD_MS
	);
	assert_eq!(regranted.protected_until_ms, 0);
	// A delayed request of the earlier tenure cannot act on this one.
	let settled = state.clone();
	assert_eq!(
		batch(
			&mut state,
			successor,
			Some(taken.generation),
			LAPSED + 5_000,
			LAPSED + 9_000
		),
		WendelonResponse::LeaseDenied
	);
	assert_eq!(decisions(&state), decisions(&settled));
	assert_eq!(state.leases, settled.leases);
}

#[test]
fn a_renewal_applied_first_refuses_the_takeover_and_a_takeover_applied_first_denies_the_renewal() {
	let (state, _claim, _rsv, range, holder) = sealed_with_a_live_lease();
	let generation = decisions(&state).lineages[&range.0].generation;
	let successor = (1..=3).find(|id| *id != holder).unwrap();
	let renewal = lease(holder, range, T0 + 9_000, LAPSED + 3_000);

	let mut renewal_first = state.clone();
	assert_eq!(
		grant(&mut renewal_first, renewal),
		WendelonResponse::Success
	);
	assert_eq!(
		takeover(&mut renewal_first, range.0, generation, successor, LAPSED),
		refused(DecisionRefusal::TakeoverNotLapsed)
	);
	assert_eq!(
		decisions(&renewal_first).lineages[&range.0].holder,
		node(holder)
	);

	let mut takeover_first = state.clone();
	// The renewal as its holder built it, naming the generation it held.
	let late = WendelonRequest::AcquireLeases(
		crate::consensus::LeaseBatch::new(
			LAPSED,
			vec![crate::consensus::LeaseBatchGrant {
				lease: renewal,
				expected_generation: Some(generation),
				holder_incarnation: inc(renewal.holder.id),
			}],
		)
		.unwrap(),
	);
	assert!(matches!(
		takeover(&mut takeover_first, range.0, generation, successor, LAPSED),
		DecisionResult::TookOver { .. }
	));
	let taken = takeover_first.clone();
	assert_eq!(
		granted(apply_next(&mut takeover_first, late)),
		WendelonResponse::LeaseDenied
	);
	assert_eq!(decisions(&takeover_first), decisions(&taken));
	assert_eq!(takeover_first.leases, taken.leases);
}

#[test]
fn a_takeover_refuses_what_was_reserved_under_the_old_generation_and_keeps_what_was_accepted() {
	// Accepted before the takeover: its outcome stands.
	let (mut state, accepted, accepted_rsv, range, holder) = sealed_with_a_live_lease();
	let accepted_status = finalize(&mut state, &accepted, quorum_acks(&accepted, accepted_rsv));
	assert!(matches!(
		accepted_status,
		DecisionResult::Terminal {
			status: ParentStatus::Accepted { .. },
			..
		}
	));
	// Reserved, with every acknowledgement gathered, not yet finalized. Its
	// stamp does not move the ring clock past the lease.
	let mut pending = claim(key(T0, 2), children(&state, 2), 0);
	pending.commit_hlc = hts(T0 + 1);
	let DecisionResult::Reserved { reservation } = reserve(&mut state, &pending, T0 + 1) else {
		panic!("reserve");
	};
	let coordinator = pending.children[0].clone();
	assert_eq!(coordinator.lineage.range_start, range.0, "the same range");
	let generation = decisions(&state).lineages[&range.0].generation;
	let successor = (1..=3).find(|id| *id != holder).unwrap();
	let third = (1..=3)
		.find(|id| *id != holder && *id != successor)
		.unwrap();
	assert!(resolvable_by(&state, successor, 8).is_empty());
	assert!(matches!(
		takeover(&mut state, range.0, generation, successor, LAPSED),
		DecisionResult::TookOver { .. }
	));

	// The accepted parent answers as recorded to every command.
	assert_eq!(
		finalize(&mut state, &accepted, quorum_acks(&accepted, accepted_rsv)),
		accepted_status
	);
	assert_eq!(resolve(&mut state, &accepted), accepted_status);
	assert_eq!(abort(&mut state, &accepted, accepted_rsv), accepted_status);
	assert_eq!(reserve(&mut state, &accepted, LAPSED), accepted_status);

	// The pending one: only the node now holding its coordinator's range
	// drives its resolution, and whatever evidence arrives it is refused, once.
	assert_eq!(
		resolvable_by(&state, successor, 8),
		vec![(pending.key, pending.request_digest)]
	);
	assert!(resolvable_by(&state, holder, 8).is_empty());
	assert!(resolvable_by(&state, third, 8).is_empty());
	let closed = finalize(&mut state, &pending, quorum_acks(&pending, reservation));
	assert!(
		matches!(
			closed,
			DecisionResult::Terminal {
				status: ParentStatus::Refused {
					cause: RefusalCause::LineageClosed,
					..
				},
				..
			}
		),
		"{closed:?}"
	);
	assert_eq!(resolve(&mut state, &pending), closed);
	assert_eq!(reserve(&mut state, &pending, LAPSED), closed);
	assert!(resolvable_by(&state, successor, 8).is_empty());

	// A new claim by the old holder for the range it lost is refused.
	let mut stale = claim(key(T0, 3), vec![coordinator], 0);
	stale.commit_hlc = hts(LAPSED + DECISION_COMMIT_HLC_LEAD_MS + 1);
	assert_eq!(
		reserve(&mut state, &stale, LAPSED + 1),
		refused(DecisionRefusal::LineageClosed)
	);
}

#[test]
fn a_successors_claim_is_reserved_only_above_its_generations_floor() {
	let (mut state, _claim, _rsv, range, holder) = sealed_with_a_live_lease();
	let generation = decisions(&state).lineages[&range.0].generation;
	let successor = (1..=3).find(|id| *id != holder).unwrap();
	takeover(&mut state, range.0, generation, successor, LAPSED);
	let lineage = decisions(&state).lineages[&range.0];
	// The successor is not its segment's first replica; it holds the lineage
	// and is in the plan, which is what Reserve requires.
	let kids = children(&state, 2);
	let own: Vec<ChildClaim> = kids
		.iter()
		.filter(|child| child.lineage.range_start == range.0)
		.cloned()
		.collect();
	assert_eq!(own.len(), 1);
	assert_eq!(own[0].holder, signer(successor));
	assert_eq!(own[0].lineage.generation, lineage.generation);
	// A leader whose clock is behind the ring's: the stamp does not lower the
	// floor, and a claim at or below it is refused whatever its logical part.
	for (n, physical_ms) in [T0, LAPSED, lineage.floor_ms].into_iter().enumerate() {
		let mut below = claim(key(T0, 10 + n as u64), own.clone(), 0);
		below.commit_hlc = HybridTimestamp {
			physical_ms,
			logical: u32::MAX,
			node_id: u32::MAX,
		};
		let before = state.clone();
		assert_eq!(
			reserve(&mut state, &below, lineage.floor_ms),
			refused(DecisionRefusal::BelowTenureFloor),
			"{physical_ms}"
		);
		assert_eq!(decisions(&state), decisions(&before));
	}
	// Above the floor, with a stamp that permits it.
	let mut above = claim(key(T0, 20), own.clone(), 0);
	above.commit_hlc = hts(lineage.floor_ms + 1);
	assert_eq!(
		reserve(&mut state, &above, LAPSED),
		refused(DecisionRefusal::CommitFromFuture),
		"the stamp must pass the earlier high-water first"
	);
	assert!(matches!(
		reserve(&mut state, &above, LAPSED + 1),
		DecisionResult::Reserved { .. }
	));
	// A claim spanning the taken range and another must clear both floors;
	// the other range's floor is its first grant's.
	let other_floor = kids
		.iter()
		.filter(|child| child.lineage.range_start != range.0)
		.map(|child| decisions(&state).lineages[&child.lineage.range_start].floor_ms)
		.max()
		.unwrap();
	assert!(other_floor < lineage.floor_ms);
	let mut spanning = claim(key(T0, 21), kids.clone(), 0);
	spanning.commit_hlc = hts(other_floor + 1);
	assert_eq!(
		reserve(&mut state, &spanning, LAPSED + 1),
		refused(DecisionRefusal::BelowTenureFloor)
	);
}

#[test]
fn takeover_arithmetic_overflow_is_a_refusal_and_the_state_round_trips() {
	let (mut state, _claim, _rsv, range, holder) = sealed_with_a_live_lease();
	let generation = decisions(&state).lineages[&range.0].generation;
	let successor = (1..=3).find(|id| *id != holder).unwrap();
	let before = state.clone();
	assert_eq!(
		takeover(&mut state, range.0, generation, successor, u64::MAX),
		refused(DecisionRefusal::CounterOverflow)
	);
	assert_eq!(decisions(&state), decisions(&before));
	assert_eq!(state.leases, before.leases);
	let mut exhausted = state.clone();
	exhausted.decisions.as_mut().unwrap().next_generation = u64::MAX;
	let exhausted_before = exhausted.clone();
	assert_eq!(
		takeover(&mut exhausted, range.0, generation, successor, LAPSED),
		refused(DecisionRefusal::CounterOverflow)
	);
	assert_eq!(decisions(&exhausted), decisions(&exhausted_before));

	takeover(&mut state, range.0, generation, successor, LAPSED);
	let bytes = postcard::to_stdvec(decisions(&state)).unwrap();
	let mut decoded: DecisionState = postcard::from_bytes(&bytes).unwrap();
	// Wendelon ADR 0113: as the snapshot reader does, the derived index is rebuilt.
	decoded.restore_table_operations().unwrap();
	assert_eq!(&decoded, decisions(&state));
	decoded.validate().unwrap();
	let lineage = decoded.lineages[&range.0];
	assert!(lineage.first_grant_pending && lineage.floor_ms > 0 && lineage.protected_until_ms > 0);
}

#[test]
fn a_tenant_with_fewer_replicas_than_data_members_is_refused_while_authority_is_off_its_primary() {
	let (mut state, _claim, _rsv, range, holder) = sealed_with_a_live_lease();
	let check = crate::crypto::test_root_check();
	state.root_check = Some(check.clone());
	let create = |tenant: u64, replication_factor: u32| {
		WendelonRequest::CreateTenant(crate::consensus::CreateTenant {
			tenant_id: TenantId::new(tenant),
			quota_bytes: 1 << 20,
			replication_factor,
			tenant_keyset: {
				let id = crate::schema::TenantKekId::try_from(7).unwrap();
				let mut keyset = crate::crypto::TenantKeyset::default();
				keyset.active_tenant_kek_id = Some(id);
				keyset.keys.insert(
					id,
					crate::crypto::WrappedTenantKek {
						wrapped_key: vec![1; 40],
					},
				);
				keyset
			},
			under_root: check.id(),
		})
	};
	// Every lineage is on its primary: the factor is not judged here.
	assert!(!any_lineage_off_primary(&state));
	assert_eq!(
		apply_next(&mut state, create(100, 2)),
		WendelonResponse::Success
	);
	state.tenants.remove(&TenantId::new(100));
	state.tenant_keysets.remove(&TenantId::new(100));

	let generation = decisions(&state).lineages[&range.0].generation;
	let successor = (1..=3).find(|id| *id != holder).unwrap();
	assert!(matches!(
		takeover(&mut state, range.0, generation, successor, LAPSED),
		DecisionResult::TookOver { .. }
	));
	assert!(any_lineage_off_primary(&state));
	let before = state.clone();
	for factor in [1, 2] {
		assert!(
			matches!(
				apply_next(&mut state, create(101, factor)),
				WendelonResponse::Error(_)
			),
			"{factor}"
		);
		assert_eq!(state.tenants, before.tenants);
		assert_eq!(state.tenant_keysets, before.tenant_keysets);
	}
	// At or above the data member count the replicas are every data member.
	for (tenant, factor) in [(102, 3), (103, 5)] {
		assert_eq!(
			apply_next(&mut state, create(tenant, factor)),
			WendelonResponse::Success,
			"{factor}"
		);
	}
}

// ============================================================
//  ADR 0736: THE BACKUP HOLD
// ============================================================

fn begin_hold(state: &mut SmState, id: u128) -> DecisionResult {
	decide(state, DecisionCommand::BeginBackupHold { id })
}

fn release_hold(state: &mut SmState, id: u128, entered_at: u64) -> DecisionResult {
	decide(state, DecisionCommand::ReleaseBackupHold { id, entered_at })
}

fn token(result: DecisionResult) -> (u128, u64) {
	match result {
		DecisionResult::HoldEntered { id, entered_at } => (id, entered_at),
		other => panic!("not a hold token: {other:?}"),
	}
}

/// Begin returns a token; a retry with the same id returns the SAME token; another id is
/// refused naming the current hold. Release needs the exact token: an earlier hold's delayed
/// release, with the same id but its own entry index, cannot clear a later hold.
#[test]
fn a_hold_is_entered_once_and_released_only_by_its_exact_token() {
	let mut state = base(3);
	let first = token(begin_hold(&mut state, 7));
	assert_eq!(decisions(&state).backup_hold(), Some(first));
	assert_eq!(
		token(begin_hold(&mut state, 7)),
		first,
		"a retry keeps the token"
	);
	assert_eq!(
		begin_hold(&mut state, 8),
		refused(DecisionRefusal::HoldConflict)
	);
	assert_eq!(
		release_hold(&mut state, 8, first.1),
		refused(DecisionRefusal::HoldMismatch)
	);
	assert_eq!(
		release_hold(&mut state, 7, first.1 + 1),
		refused(DecisionRefusal::HoldMismatch)
	);
	assert_eq!(decisions(&state).backup_hold(), Some(first));
	assert_eq!(
		release_hold(&mut state, first.0, first.1),
		DecisionResult::HoldReleased
	);
	assert_eq!(decisions(&state).backup_hold(), None);
	// The same id held again later is a different hold.
	let second = token(begin_hold(&mut state, 7));
	assert_ne!(second.1, first.1);
	assert_eq!(
		release_hold(&mut state, first.0, first.1),
		refused(DecisionRefusal::HoldMismatch),
		"the earlier hold's delayed release does not clear the later one"
	);
	assert_eq!(decisions(&state).backup_hold(), Some(second));
	// With nothing held a release is a mismatch too, and changes nothing.
	assert_eq!(
		release_hold(&mut state, second.0, second.1),
		DecisionResult::HoldReleased
	);
	assert_eq!(
		release_hold(&mut state, second.0, second.1),
		refused(DecisionRefusal::HoldMismatch)
	);
}

/// While held: a recorded parent still answers as recorded; a retired identity still answers
/// OutcomeUnavailable; only an eligible new reservation is refused BackupHold, and an
/// admitted parent still finalizes to its actual result.
#[test]
fn a_hold_refuses_only_new_eligible_reservations() {
	let mut state = base(3);
	grant_all(&mut state);
	let admitted = claim(key(T0, 1), children(&state, 1), 0);
	let DecisionResult::Reserved { reservation } = reserve(&mut state, &admitted, T0) else {
		panic!("reserved");
	};
	let held = token(begin_hold(&mut state, 1));
	// Replay of the admitted parent: its recorded answer.
	assert_eq!(
		reserve(&mut state, &admitted, T0),
		DecisionResult::Reserved { reservation }
	);
	// A new eligible parent: refused, nothing recorded.
	let new = claim(key(T0 + 10, 2), children(&state, 1), 0);
	assert_eq!(
		reserve(&mut state, &new, T0 + 10),
		refused(DecisionRefusal::BackupHold)
	);
	assert_eq!(
		answer(decisions(&state), &new.key, &new.request_digest),
		Answer::New
	);
	// The admitted parent reaches its actual terminal result under the hold.
	assert!(matches!(
		finalize(&mut state, &admitted, quorum_acks(&admitted, reservation)),
		DecisionResult::Terminal {
			status: ParentStatus::Accepted { .. },
			..
		}
	));
	// A retired identity keeps its own answer; the hold does not relabel it.
	let later = T0 + DECISION_EXPIRY_MS + 1_000;
	let cutoff = later - DECISION_EXPIRY_MS;
	let old = claim(key(cutoff, 3), children(&state, 1), 0);
	assert_eq!(
		reserve(&mut state, &old, later),
		refused(DecisionRefusal::OutcomeUnavailable)
	);
	// After release the new parent is admitted.
	assert_eq!(
		release_hold(&mut state, held.0, held.1),
		DecisionResult::HoldReleased
	);
	let after = claim(key(later, 4), children(&state, 1), 0);
	assert!(matches!(
		reserve(&mut state, &after, later),
		DecisionResult::Reserved { .. }
	));
}

/// While held, the logical mutations this mode admits are refused at apply and change
/// nothing; after release the same command applies.
#[test]
fn a_hold_refuses_tenant_and_key_mutations_at_apply() {
	let mut state = base(3);
	let held = token(begin_hold(&mut state, 9));
	let tenant = crate::schema::TenantId::ZERO;
	let quota_before = state.tenants.get(&tenant).map(|record| record.quota_bytes);
	for request in [
		WendelonRequest::UpdateTenantQuota(crate::consensus::UpdateTenantQuota {
			tenant_id: tenant,
			quota_bytes: 1,
		}),
		WendelonRequest::SuspendTenant(tenant),
		WendelonRequest::ResumeTenant(tenant),
	] {
		assert_eq!(
			apply_next(&mut state, request.clone()),
			WendelonResponse::Error(crate::consensus::raft_storage::BACKUP_HOLD_REFUSAL.into()),
			"{request:?}"
		);
	}
	assert_eq!(
		state.tenants.get(&tenant).map(|record| record.quota_bytes),
		quota_before
	);
	assert_eq!(
		release_hold(&mut state, held.0, held.1),
		DecisionResult::HoldReleased
	);
	assert_ne!(
		apply_next(&mut state, WendelonRequest::SuspendTenant(tenant)),
		WendelonResponse::Error(crate::consensus::raft_storage::BACKUP_HOLD_REFUSAL.into())
	);
}

// ============================================================
//  BACKUP ROSTER (ADR 0736)
// ============================================================

/// A state with the fixture table in the catalog, as a table create leaves it.
fn with_fixture_table(state: &mut SmState) {
	state
		.table_schemas
		.upsert(TENANT, fixture_table())
		.expect("upsert");
	state
		.table_lifecycle
		.ensure_inherit_cluster(TENANT, TABLE, hts(0));
}

fn roster(
	state: &SmState,
	token: (u128, u64),
) -> Result<crate::logical_backup::roster::BackupRoster, crate::logical_backup::roster::RosterRefusal>
{
	crate::logical_backup::roster::roster_at(state, token)
}

/// No roster without the exact hold; a reserved parent is waited for (its
/// finalize is admitted under the hold); once settled, the roster names every
/// tenant setting and the latest table descriptor with its exact policy, at the
/// applied index.
#[test]
fn the_roster_is_given_only_under_the_exact_hold_and_at_a_settled_cut() {
	use crate::logical_backup::roster::RosterRefusal;
	let (mut state, claim, reservation) = reserved_parent(1);
	with_fixture_table(&mut state);
	assert_eq!(
		roster(&state, (9, 1)),
		Err(RosterRefusal::HoldMismatch { current: None })
	);
	let held = token(begin_hold(&mut state, 9));
	assert_eq!(
		roster(&state, (9, held.1 + 1)),
		Err(RosterRefusal::HoldMismatch {
			current: Some(held)
		})
	);
	let answer = roster(&state, held);
	assert!(
		matches!(answer, Err(RosterRefusal::Unsettled { .. })),
		"{answer:?}"
	);
	let acks = quorum_acks(&claim, reservation);
	assert!(matches!(
		finalize(&mut state, &claim, acks),
		DecisionResult::Terminal {
			status: ParentStatus::Accepted { .. },
			..
		}
	));
	let roster = roster(&state, held).expect("settled");
	assert_eq!(roster.hold, held);
	assert_eq!(
		roster.cut,
		state.last_applied_log_id.map_or(0, |id| id.index)
	);
	assert_eq!(
		roster.tenants,
		vec![crate::logical_backup::roster::RosterTenant {
			tenant_id: TENANT,
			quota_bytes: u64::MAX,
			replication_factor: 3,
		}]
	);
	assert_eq!(roster.tables.len(), 1);
	assert_eq!(roster.tables[0].descriptor, fixture_table());
	assert_eq!(
		roster.tables[0].retention,
		crate::schema::TableRetentionPolicy::InheritCluster
	);
	assert!(roster.logical_types.is_empty() && roster.indexes.is_empty());
}

/// Each shape the restore cannot reproduce, or the hold keeps from settling,
/// refuses the roster by name; an index still building is waited for.
#[test]
fn each_unsupported_shape_refuses_the_roster_by_name() {
	use crate::logical_backup::roster::RosterRefusal;
	use crate::schema::{IndexObjectId, RetentionWindowMillis, TableRetentionPolicy};
	let settled = || {
		let mut state = base(3);
		grant_all(&mut state);
		with_fixture_table(&mut state);
		let held = token(begin_hold(&mut state, 9));
		assert!(roster(&state, held).is_ok());
		(state, held)
	};

	// A KV parent, whatever its status.
	let mut state = base(3);
	grant_all(&mut state);
	let one = point_child(&state, position_of(1));
	let kv = Claim {
		first_row_position: one.range_start,
		..claim(
			ParentKey {
				namespace: ParentNamespace::Kv,
				..key(T0, 1)
			},
			vec![one],
			0,
		)
	};
	assert!(matches!(
		reserve(&mut state, &kv, T0),
		DecisionResult::Reserved { .. }
	));
	let held = token(begin_hold(&mut state, 9));
	assert_eq!(roster(&state, held), Err(RosterRefusal::KvDataPresent));

	// A tenant not live: its resume is refused under the hold.
	let (mut state, held) = settled();
	state.tenants.get_mut(&TENANT).unwrap().state = TenantLifecycleState::Suspended;
	assert!(matches!(
		roster(&state, held),
		Err(RosterRefusal::LifecycleNotSettled { .. })
	));

	// A retention floor; a policy no route reproduces.
	let (mut state, held) = settled();
	let window = RetentionWindowMillis::try_new(crate::schema::MIN_RETENTION_WINDOW_MS).unwrap();
	for (policy, expected) in [
		(
			TableRetentionPolicy::FixedWindow(window),
			Err(RosterRefusal::RetentionActive {
				tenant_id: TENANT,
				table_object_id: TABLE,
			}),
		),
		(
			TableRetentionPolicy::Disabled,
			Err(RosterRefusal::SettingNotReproducible {
				what: "table retention policy Some(Disabled)".into(),
			}),
		),
	] {
		let generation = state.table_lifecycle.current_generation(TENANT, TABLE);
		state
			.table_lifecycle
			.set_next_generation(TENANT, TABLE, generation, policy, hts(0))
			.unwrap();
		assert_eq!(roster(&state, held), expected);
	}

	// A table of a tenant that is not on the roster.
	let (mut state, held) = settled();
	let stray = {
		use arrow::datatypes::{DataType, Field, Schema};
		crate::schema::TableSchemaDescriptor::new_with_layout(
			crate::schema::TableObjectId::new([0x1f; 16]),
			1,
			"fixture",
			"stray",
			&Schema::new(vec![Field::new("id", DataType::Int64, false)]),
			crate::schema::RowIdentityPolicy::PrimaryKey(
				crate::schema::PrimaryKeyDescriptor::single("id"),
			),
			crate::schema::LocalityPolicy::IdentityProjection,
			crate::schema::RecordLayoutPolicy::SingleRow,
			crate::schema::SchemaEvolutionPolicy::AdditiveOnly,
		)
		.unwrap()
	};
	state
		.table_schemas
		.upsert(crate::schema::TenantId(99), stray)
		.unwrap();
	assert!(matches!(
		roster(&state, held),
		Err(RosterRefusal::LifecycleNotSettled { .. })
	));

	// An index still building is waited for; once ready it is on the roster.
	let (mut state, held) = settled();
	let index = crate::schema::SecondaryIndexDescriptor::for_table_column(
		IndexObjectId::new([0x2f; 16]),
		"by_v",
		&fixture_table(),
		"v",
	)
	.unwrap();
	state
		.secondary_indexes
		.create(TENANT, index.clone(), &state.table_schemas.clone())
		.unwrap();
	assert!(matches!(
		roster(&state, held),
		Err(RosterRefusal::Unsettled { .. })
	));
	for voter in 1..=3 {
		state
			.secondary_indexes
			.mark_backfill_complete(
				TENANT,
				index.index_object_id,
				node(voter),
				&[1, 2, 3].into_iter().collect(),
			)
			.unwrap();
	}
	assert_eq!(roster(&state, held).unwrap().indexes, vec![(TENANT, index)]);
}

// ============================================================
//  ADR 0104 (wendelon): RAFT INSTANCES, GRANTS AND REPLACEMENT
// ============================================================

use crate::consensus::decisions::instances::{
	ALLOCATED_BASE, GrantKind, ReplacementPhase, slot_of,
};

const TOKEN: u128 = 0x0104;

fn grant_join(state: &mut SmState, birth: u8, slot: u32) -> DecisionResult {
	decide(
		state,
		DecisionCommand::GrantInstance {
			birth: [birth; 16],
			slot,
			kind: GrantKind::Join,
		},
	)
}

fn grant_replacement(state: &mut SmState, birth: u8, slot: u32, token: u128) -> DecisionResult {
	decide(
		state,
		DecisionCommand::GrantInstance {
			birth: [birth; 16],
			slot,
			kind: GrantKind::Replacement { token },
		},
	)
}

fn refusal_of(result: DecisionResult) -> DecisionRefusal {
	match result {
		DecisionResult::Refused(refusal) => refusal,
		other => panic!("expected a refusal, got {other:?}"),
	}
}

/// A state whose slot 3 is being replaced by `ALLOCATED_BASE`, granted to
/// birth 7.
pub(crate) fn reserved() -> SmState {
	let mut state = base(3);
	assert_eq!(
		decide(
			&mut state,
			DecisionCommand::BeginReplacement {
				token: TOKEN,
				slot: 3
			}
		),
		DecisionResult::ReplacementRecorded {
			slot: 3,
			r_old: 3,
			phase: ReplacementPhase::Begun,
		}
	);
	assert_eq!(
		grant_replacement(&mut state, 7, 3, TOKEN),
		DecisionResult::InstanceGranted {
			instance: ALLOCATED_BASE
		}
	);
	state
}

#[test]
fn a_join_grant_is_serialized_by_birth_and_intent() {
	let mut state = base(3);
	// A slot no instance serves, for the first birth that asks.
	assert_eq!(
		grant_join(&mut state, 1, 4),
		DecisionResult::InstanceGranted { instance: 4 }
	);
	// The same store asking again for the same thing gets the same answer.
	assert_eq!(
		grant_join(&mut state, 1, 4),
		DecisionResult::InstanceGranted { instance: 4 }
	);
	// A concurrent fresh store for the same slot is refused.
	assert_eq!(
		refusal_of(grant_join(&mut state, 2, 4)),
		DecisionRefusal::SlotGranted
	);
	// The same birth with another intent is not a grant for it.
	assert_eq!(
		refusal_of(grant_join(&mut state, 1, 5)),
		DecisionRefusal::GrantIntentMismatch
	);
	// A served slot, and slots outside the ParentDecisions range.
	assert_eq!(
		refusal_of(grant_join(&mut state, 3, 1)),
		DecisionRefusal::SlotServed
	);
	for slot in [0, ALLOCATED_BASE, u32::MAX] {
		assert_eq!(
			refusal_of(grant_join(&mut state, 4, slot)),
			DecisionRefusal::SlotOutOfRange,
			"{slot}"
		);
	}
}

#[test]
fn a_replacement_identity_is_never_handed_to_another_birth() {
	let mut state = reserved();
	// The same birth retries (lost response, crash before its identity write).
	assert_eq!(
		grant_replacement(&mut state, 7, 3, TOKEN),
		DecisionResult::InstanceGranted {
			instance: ALLOCATED_BASE
		}
	);
	// The store's Raft artifacts are lost: the blank store is born again, and
	// the token and data directory alone do not take the identity back.
	assert_eq!(
		refusal_of(grant_replacement(&mut state, 8, 3, TOKEN)),
		DecisionRefusal::GrantedToAnotherBirth
	);
	assert_eq!(
		refusal_of(grant_replacement(&mut state, 7, 2, TOKEN)),
		DecisionRefusal::GrantIntentMismatch
	);
	assert_eq!(
		refusal_of(grant_replacement(&mut state, 9, 3, TOKEN + 1)),
		DecisionRefusal::UnknownReplacement
	);
	// No second replacement while one is open, and no ordinary join of its slot.
	assert_eq!(
		refusal_of(decide(
			&mut state,
			DecisionCommand::BeginReplacement {
				token: TOKEN + 1,
				slot: 2
			}
		)),
		DecisionRefusal::ReplacementOpen
	);
	assert_eq!(
		refusal_of(grant_join(&mut state, 9, 3)),
		DecisionRefusal::ReplacementOpen
	);
	// The reserved instance maps to its slot; another allocated id maps nowhere.
	assert_eq!(slot_of(&state, ALLOCATED_BASE), Ok(3));
	assert!(slot_of(&state, ALLOCATED_BASE + 1).is_err());
}

#[test]
fn begin_names_exactly_one_voter_of_a_uniform_membership() {
	let mut state = base(3);
	assert_eq!(
		refusal_of(decide(
			&mut state,
			DecisionCommand::BeginReplacement {
				token: TOKEN,
				slot: 9
			}
		)),
		DecisionRefusal::SlotNotVoter
	);
	state.last_membership = membership_of(&[&[1, 2, 3], &[1, 2]], &[]);
	assert_eq!(
		refusal_of(decide(
			&mut state,
			DecisionCommand::BeginReplacement {
				token: TOKEN,
				slot: 3
			}
		)),
		DecisionRefusal::MembershipNotUniform
	);
	state.last_membership = membership_of(&[&[1, 2, 3]], &[4]);
	assert_eq!(
		refusal_of(decide(
			&mut state,
			DecisionCommand::BeginReplacement {
				token: TOKEN,
				slot: 3
			}
		)),
		DecisionRefusal::MembershipNotUniform
	);
}

#[test]
fn placement_compares_slots_and_tolerates_only_the_reserved_learner() {
	let mut state = reserved();
	let sealed = formed_placement(&base(3)).unwrap();
	// The reserved learner on slot 3 leaves the placement as it was.
	state.last_membership = membership_of(&[&[1, 2, 3]], &[ALLOCATED_BASE]);
	assert_eq!(formed_placement(&state).as_ref(), Some(&sealed));
	// Any other learner is a change in progress.
	state.last_membership = membership_of(&[&[1, 2, 3]], &[4]);
	assert_eq!(formed_placement(&state), None);
	// The joint entry keeps its refusal.
	state.last_membership = membership_of(&[&[1, 2, 3], &[1, 2, ALLOCATED_BASE]], &[]);
	assert_eq!(formed_placement(&state), None);
	// After the swap the voters' slots are the peers again.
	state.last_membership = formed_membership(&[1, 2, ALLOCATED_BASE]);
	assert_eq!(formed_placement(&state).as_ref(), Some(&sealed));
	// Two instances of one slot never vote together.
	state.last_membership = formed_membership(&[1, 2, 3, ALLOCATED_BASE]);
	assert_eq!(formed_placement(&state), None);
}

#[test]
fn done_is_derived_from_the_applied_target_and_retires_the_old_instance() {
	let mut state = reserved();
	let complete =
		|state: &mut SmState| decide(state, DecisionCommand::CompleteReplacement { token: TOKEN });
	// Learner, joint, and a target that still keeps the old instance as a node.
	for membership in [
		membership_of(&[&[1, 2, 3]], &[ALLOCATED_BASE]),
		membership_of(&[&[1, 2, 3], &[1, 2, ALLOCATED_BASE]], &[]),
		membership_of(&[&[1, 2, ALLOCATED_BASE]], &[3]),
	] {
		state.last_membership = membership;
		assert_eq!(
			refusal_of(complete(&mut state)),
			DecisionRefusal::ReplacementIncomplete
		);
	}
	state.last_membership = formed_membership(&[1, 2, ALLOCATED_BASE]);
	let done = DecisionResult::ReplacementCompleted {
		r_new: ALLOCATED_BASE,
	};
	assert_eq!(complete(&mut state), done);
	// Replays after Done answer the Done record; nothing starts again.
	assert_eq!(complete(&mut state), done);
	assert_eq!(
		decide(
			&mut state,
			DecisionCommand::BeginReplacement {
				token: TOKEN,
				slot: 3
			}
		),
		DecisionResult::ReplacementRecorded {
			slot: 3,
			r_old: 3,
			phase: ReplacementPhase::Done {
				r_new: ALLOCATED_BASE
			},
		}
	);
	assert_eq!(
		grant_replacement(&mut state, 7, 3, TOKEN),
		DecisionResult::InstanceGranted {
			instance: ALLOCATED_BASE
		}
	);
	assert_eq!(
		refusal_of(grant_replacement(&mut state, 8, 3, TOKEN)),
		DecisionRefusal::GrantedToAnotherBirth
	);
	// The retired instance never returns, even through an ordinary join.
	assert!(decisions(&state).instances().is_retired(3));
	assert_eq!(
		refusal_of(grant_join(&mut state, 9, 3)),
		DecisionRefusal::InstanceRetired
	);
	decisions(&state).validate().unwrap();
}

#[test]
fn the_instance_space_is_exhausted_after_its_last_id_and_never_wraps() {
	let mut state = base(3);
	crate::consensus::decisions::instances::exhaust_allocator_for_test(
		state.decisions.as_mut().unwrap(),
		u32::MAX,
	);
	decide(
		&mut state,
		DecisionCommand::BeginReplacement {
			token: TOKEN,
			slot: 3,
		},
	);
	assert_eq!(
		grant_replacement(&mut state, 7, 3, TOKEN),
		DecisionResult::InstanceGranted { instance: u32::MAX }
	);
	state.last_membership = formed_membership(&[1, 2, u32::MAX]);
	decide(
		&mut state,
		DecisionCommand::CompleteReplacement { token: TOKEN },
	);
	decide(
		&mut state,
		DecisionCommand::BeginReplacement {
			token: TOKEN + 1,
			slot: 2,
		},
	);
	assert_eq!(
		refusal_of(grant_replacement(&mut state, 8, 2, TOKEN + 1)),
		DecisionRefusal::InstanceSpaceExhausted
	);
}

#[test]
fn today_keeps_its_whole_node_id_domain() {
	let state = SmState::default();
	assert_eq!(slot_of(&state, ALLOCATED_BASE | 5), Ok(ALLOCATED_BASE | 5));
	assert_eq!(slot_of(&state, u32::MAX), Ok(u32::MAX));
}

/// Through the reducer, whose decision state is held apart from the state
/// machine while it applies: a write is admitted under the sealed placement
/// while the reserved learner catches up and after the swap, and refused only
/// during the joint entry.
#[test]
fn writes_stay_admitted_through_a_replacement_but_its_joint_entry() {
	let (mut state, _, _) = reserved_parent(1);
	// The replacement of slot 3, reserved for `ALLOCATED_BASE`.
	decide(
		&mut state,
		DecisionCommand::BeginReplacement {
			token: TOKEN,
			slot: 3,
		},
	);
	assert_eq!(
		grant_replacement(&mut state, 7, 3, TOKEN),
		DecisionResult::InstanceGranted {
			instance: ALLOCATED_BASE
		}
	);
	let write = |state: &mut SmState, sequence_no: u64| {
		let claim = claim(key(T0, sequence_no), children(state, 2), 0);
		reserve(state, &claim, T0)
	};
	state.last_membership = membership_of(&[&[1, 2, 3]], &[ALLOCATED_BASE]);
	assert!(
		matches!(write(&mut state, 2), DecisionResult::Reserved { .. }),
		"admitted while the learner catches up"
	);
	state.last_membership = membership_of(&[&[1, 2, 3], &[1, 2, ALLOCATED_BASE]], &[]);
	assert_eq!(
		refusal_of(write(&mut state, 3)),
		DecisionRefusal::MembershipSealed,
		"the joint entry keeps its refusal"
	);
	state.last_membership = formed_membership(&[1, 2, ALLOCATED_BASE]);
	assert!(
		matches!(write(&mut state, 4), DecisionResult::Reserved { .. }),
		"admitted after the swap"
	);
}

// ============================================================
//  ADR 0105 (wendelon): A COMPLETED REPLACEMENT ADMITS ITS NEW STORE
// ============================================================

use crate::consensus::decisions::instances::Promotion;
use crate::consensus::{PromoteIncarnation, SlabRefusal};

const BIRTH: u8 = 7;

fn store_j() -> StoreIncarnation {
	StoreIncarnation::from_raw(0x0105_0000_0001)
}

/// `state` with slot 3's replacement, under `token`, by the next allocated
/// instance granted to `birth`, carried to Done: the swap is the applied
/// membership and the old instance is retired.
fn complete(state: &mut SmState, token: u128, birth: u8) -> u32 {
	assert!(matches!(
		decide(state, DecisionCommand::BeginReplacement { token, slot: 3 }),
		DecisionResult::ReplacementRecorded {
			phase: ReplacementPhase::Begun,
			..
		}
	));
	let DecisionResult::InstanceGranted { instance } = grant_replacement(state, birth, 3, token)
	else {
		panic!("grant");
	};
	let survivors: Vec<u32> = state
		.last_membership
		.membership()
		.voter_ids()
		.filter(|voter| slot_of(state, *voter) != Ok(3))
		.chain([instance])
		.collect();
	state.last_membership = formed_membership(&survivors);
	assert_eq!(
		decide(state, DecisionCommand::CompleteReplacement { token }),
		DecisionResult::ReplacementCompleted { r_new: instance }
	);
	instance
}

fn done() -> SmState {
	let mut state = base(3);
	assert_eq!(complete(&mut state, TOKEN, BIRTH), ALLOCATED_BASE);
	state
}

fn promotion(
	sender: u32,
	token: u128,
	r_new: u32,
	birth: u8,
	expected: StoreIncarnation,
	incarnation: StoreIncarnation,
) -> WendelonRequest {
	WendelonRequest::PromoteIncarnation(PromoteIncarnation {
		node: sender,
		token,
		r_new,
		birth: [birth; 16],
		expected,
		incarnation,
	})
}

fn promote(state: &mut SmState) -> WendelonResponse {
	apply_next(
		state,
		promotion(3, TOKEN, ALLOCATED_BASE, BIRTH, inc(3), store_j()),
	)
}

fn refused_unchanged(state: &mut SmState, request: WendelonRequest, refusal: SlabRefusal) {
	let before = state.clone();
	assert_eq!(
		apply_next(state, request),
		WendelonResponse::Refused(refusal)
	);
	assert_eq!(state.incarnations, before.incarnations);
	assert_eq!(state.decisions, before.decisions);
}

#[test]
fn a_promotion_needs_a_done_replacement_bound_to_its_grant() {
	// Reserved, not Done.
	let mut state = reserved();
	refused_unchanged(
		&mut state,
		promotion(3, TOKEN, ALLOCATED_BASE, BIRTH, inc(3), store_j()),
		SlabRefusal::ReplacementNotDone,
	);
	// No such replacement.
	let mut state = done();
	refused_unchanged(
		&mut state,
		promotion(3, TOKEN + 1, ALLOCATED_BASE, BIRTH, inc(3), store_j()),
		SlabRefusal::ReplacementNotDone,
	);
	// Another birth, another instance, another slot's sender.
	for request in [
		promotion(3, TOKEN, ALLOCATED_BASE, BIRTH + 1, inc(3), store_j()),
		promotion(3, TOKEN, ALLOCATED_BASE + 1, BIRTH, inc(3), store_j()),
		promotion(2, TOKEN, ALLOCATED_BASE, BIRTH, inc(2), store_j()),
	] {
		refused_unchanged(&mut state, request, SlabRefusal::PromotionNotAuthorized);
	}
	assert_eq!(promote(&mut state), WendelonResponse::Success);
}

#[test]
fn a_promotion_is_recorded_once_and_its_exact_retry_changes_nothing() {
	let mut state = done();
	assert_eq!(promote(&mut state), WendelonResponse::Success);
	let registry = &state.incarnations[&3];
	assert_eq!(registry.current, Some(store_j()));
	assert!(registry.retired.contains(&inc(3)));
	assert!(registry.accepted.contains(&(store_j(), Some(inc(3)))));
	assert_eq!(
		decisions(&state)
			.instances()
			.replacement(TOKEN)
			.unwrap()
			.promoted,
		Some(Promotion {
			expected: inc(3),
			incarnation: store_j(),
		})
	);
	let before = state.clone();
	assert_eq!(promote(&mut state), WendelonResponse::Success);
	assert_eq!(state.incarnations, before.incarnations);
	assert_eq!(state.decisions, before.decisions);
	// Another tuple cannot reuse the authorization.
	let other = StoreIncarnation::from_raw(0x0105_0000_0002);
	for request in [
		promotion(3, TOKEN, ALLOCATED_BASE, BIRTH, inc(3), other),
		promotion(3, TOKEN, ALLOCATED_BASE, BIRTH, store_j(), other),
	] {
		refused_unchanged(&mut state, request, SlabRefusal::PromotionConflict);
	}

	// A later replacement supersedes J; the first promotion's exact retry is
	// its historical answer and moves nothing back.
	let r_next = complete(&mut state, TOKEN + 1, BIRTH + 1);
	assert_eq!(
		apply_next(
			&mut state,
			promotion(3, TOKEN + 1, r_next, BIRTH + 1, store_j(), other)
		),
		WendelonResponse::Success
	);
	let before = state.clone();
	assert_eq!(promote(&mut state), WendelonResponse::Success);
	assert_eq!(state.incarnations, before.incarnations);
	assert_eq!(state.incarnations[&3].current, Some(other));
}

#[test]
fn a_promotion_needs_the_current_incarnation_and_a_new_one() {
	let mut state = done();
	refused_unchanged(
		&mut state,
		promotion(3, TOKEN, ALLOCATED_BASE, BIRTH, inc(9), store_j()),
		SlabRefusal::StaleSupersede {
			node: 3,
			current: Some(inc(3)),
		},
	);
	refused_unchanged(
		&mut state,
		promotion(3, TOKEN, ALLOCATED_BASE, BIRTH, inc(3), inc(3)),
		SlabRefusal::RetiredIncarnation {
			node: 3,
			incarnation: inc(3),
		},
	);
}

/// P1: a lineage still held at the old store is moved by takeover first; the
/// promotion never drops it.
#[test]
fn a_promotion_waits_while_the_old_store_holds_a_lineage() {
	let mut state = base(3);
	grant_all(&mut state);
	complete(&mut state, TOKEN, BIRTH);
	let held = decisions(&state)
		.lineages
		.values()
		.filter(|lineage| lineage.holder.id == 3)
		.count() as u32;
	assert!(held > 0);
	refused_unchanged(
		&mut state,
		promotion(3, TOKEN, ALLOCATED_BASE, BIRTH, inc(3), store_j()),
		SlabRefusal::LineagesStillHeld { count: held },
	);
}

#[test]
fn the_ordinary_registration_rules_are_unchanged_by_a_replacement() {
	let mut state = done();
	for supersedes in [None, Some(inc(3))] {
		refused_unchanged(
			&mut state,
			WendelonRequest::RegisterIncarnation(crate::consensus::RegisterIncarnation {
				node: 3,
				incarnation: store_j(),
				supersedes,
			}),
			SlabRefusal::StaleSupersede {
				node: 3,
				current: Some(inc(3)),
			},
		);
	}
}

/// D3: from Done until activation the replaced slot takes no lease, takeover
/// or coordination; the survivors' authority and the resolution of earlier
/// work are unaffected.
#[test]
fn a_replaced_slot_takes_no_lease_takeover_or_coordination() {
	let (mut state, earlier, reservation) = reserved_parent(1);
	complete(&mut state, TOKEN, BIRTH);
	let mut theirs = None;
	for (holder, range) in universe() {
		let response = grant(&mut state, lease(holder, range, 0, 2_000));
		if holder == 3 {
			assert_eq!(response, WendelonResponse::LeaseDenied, "renewal by slot 3");
		} else {
			assert_eq!(response, WendelonResponse::Success, "renewal by {holder}");
			theirs = Some((holder, range));
		}
	}
	let (holder, (start, _)) = theirs.unwrap();
	let generation = decisions(&state).lineages[&start].generation;
	let before = state.clone();
	assert_eq!(
		takeover(&mut state, start, generation, 3, 1_000_000),
		refused(DecisionRefusal::SlotBeingReplaced)
	);
	assert_eq!(state.decisions, before.decisions);
	assert_ne!(holder, 3);

	let children = children(&state, 3);
	let coordinator = children
		.iter()
		.position(|child| child.holder.node == 3)
		.expect("slot 3 holds a child") as u16;
	let ours = claim(key(T0, 9), children, coordinator);
	assert_eq!(
		reserve(&mut state, &ours, T0),
		refused(DecisionRefusal::SlotBeingReplaced)
	);
	// Earlier work still resolves.
	assert_ne!(
		abort(&mut state, &earlier, reservation),
		refused(DecisionRefusal::SlotBeingReplaced)
	);
}

#[test]
fn the_old_stores_history_still_verifies_and_the_new_store_binds() {
	let mut state = done();
	let body = TakeOverBody {
		range_start: 0,
		generation: 1,
		successor: signer(3),
	};
	let old = sign_for_test(body, signer(3));
	assert_eq!(promote(&mut state), WendelonResponse::Success);
	assert!(evidence::verify(decisions(&state), &old).is_ok());
	let key = decision_verifying_key_bytes(dev_decision_signing_secret(3));
	assert_eq!(
		bind(
			&mut state,
			Signer {
				node: 3,
				incarnation: store_j(),
			},
			key
		),
		DecisionResult::SignerBound
	);
	assert!(evidence::verify(decisions(&state), &old).is_ok());
}

/// R1: a Done token that never promoted loses its first promotion once its
/// instance stops serving the slot: while a later replacement is open against
/// it, once that swap's uniform membership excludes it (before Done retires
/// it), after that Done, and after that replacement's own promotion. Nothing
/// changes; an already recorded promotion's exact retry stays its historical
/// answer (`a_promotion_is_recorded_once_and_its_exact_retry_changes_nothing`).
#[test]
fn a_superseded_replacement_cannot_make_its_first_promotion() {
	let mut state = done();
	let stale = promotion(3, TOKEN, ALLOCATED_BASE, BIRTH, inc(3), store_j());
	let next = TOKEN + 1;
	// A later replacement of the same slot is open against the first instance.
	assert!(matches!(
		decide(
			&mut state,
			DecisionCommand::BeginReplacement {
				token: next,
				slot: 3
			}
		),
		DecisionResult::ReplacementRecorded {
			r_old: ALLOCATED_BASE,
			phase: ReplacementPhase::Begun,
			..
		}
	));
	refused_unchanged(&mut state, stale.clone(), SlabRefusal::PromotionStale);
	let DecisionResult::InstanceGranted { instance: r_next } =
		grant_replacement(&mut state, BIRTH + 1, 3, next)
	else {
		panic!("grant");
	};
	refused_unchanged(&mut state, stale.clone(), SlabRefusal::PromotionStale);
	// The joint swap still names it, but the replacement against it is open.
	state.last_membership = membership_of(&[&[1, 2, ALLOCATED_BASE], &[1, 2, r_next]], &[]);
	refused_unchanged(&mut state, stale.clone(), SlabRefusal::PromotionStale);
	// The uniform swap excludes it before Done retires it.
	state.last_membership = formed_membership(&[1, 2, r_next]);
	refused_unchanged(&mut state, stale.clone(), SlabRefusal::PromotionStale);
	assert_eq!(
		decide(
			&mut state,
			DecisionCommand::CompleteReplacement { token: next }
		),
		DecisionResult::ReplacementCompleted { r_new: r_next }
	);
	refused_unchanged(&mut state, stale.clone(), SlabRefusal::PromotionStale);
	let other = StoreIncarnation::from_raw(0x0105_0000_0002);
	assert_eq!(
		apply_next(
			&mut state,
			promotion(3, next, r_next, BIRTH + 1, inc(3), other)
		),
		WendelonResponse::Success
	);
	refused_unchanged(&mut state, stale.clone(), SlabRefusal::PromotionStale);
	assert_eq!(state.incarnations[&3].current, Some(other));
	assert_eq!(
		decisions(&state)
			.instances()
			.replacement(TOKEN)
			.unwrap()
			.promoted,
		None
	);
}

// ============================================================
//  ADR 0106 (wendelon): A PROMOTED REPLACEMENT'S RECOVERY RECORD
// ============================================================

use crate::consensus::decisions::ActivationBody;
use crate::consensus::decisions::RecoveryBody;
use crate::consensus::decisions::instances::{Activation, Recovery};
use crate::consensus::decisions::{IndexReceiptBody, IndexReceiptPhase, IndexTransition};
use crate::schema::SecondaryIndexState;

/// `done()` with slot 3's store J promoted and bound to slot 3's key.
fn promoted() -> SmState {
	let mut state = done();
	assert_eq!(promote(&mut state), WendelonResponse::Success);
	let key = decision_verifying_key_bytes(dev_decision_signing_secret(3));
	assert_eq!(
		bind(
			&mut state,
			Signer {
				node: 3,
				incarnation: store_j(),
			},
			key
		),
		DecisionResult::SignerBound
	);
	state
}

fn recovery_body(gid: u64, target_digest: Option<[u8; 32]>, k: u64) -> RecoveryBody {
	RecoveryBody {
		token: TOKEN,
		slot: 3,
		r_new: ALLOCATED_BASE,
		birth: [BIRTH; 16],
		store: store_j(),
		gid,
		target_digest,
		k,
	}
}

fn prove(state: &mut SmState, body: RecoveryBody) -> DecisionResult {
	let signer = Signer {
		node: 3,
		incarnation: body.store,
	};
	decide(
		state,
		DecisionCommand::RecoveryProved {
			proof: sign_for_test(body, signer),
		},
	)
}

fn certify(state: &mut SmState, gid: u64, digest: [u8; 32]) {
	state.decisions.as_mut().unwrap().generations.current = Some(generations::Certificate {
		gid,
		target_digest: digest,
		t: 1,
		predecessor_gid: gid - 1,
		chunks: crate::storage::index::certified_base::chunk_sums(std::iter::empty()),
		committed_at: 1,
	});
}

fn unchanged(state: &mut SmState, body: RecoveryBody, refusal: DecisionRefusal) {
	let before = state.decisions.clone();
	assert_eq!(prove(state, body), refused(refusal));
	assert_eq!(state.decisions, before);
}

#[test]
fn a_recovery_is_recorded_once_for_the_current_promoted_store() {
	// Done but not promoted: no key is bound to the store, so its statement
	// verifies against nothing and changes nothing.
	let mut state = done();
	let before = state.decisions.clone();
	assert!(matches!(
		prove(&mut state, recovery_body(0, None, 1)),
		DecisionResult::Refused(DecisionRefusal::Unverifiable)
	));
	assert_eq!(state.decisions, before);
	let mut state = promoted();
	// Another birth, instance or store: not this replacement.
	for body in [
		RecoveryBody {
			birth: [BIRTH + 1; 16],
			..recovery_body(0, None, 1)
		},
		RecoveryBody {
			r_new: ALLOCATED_BASE + 1,
			..recovery_body(0, None, 1)
		},
	] {
		unchanged(&mut state, body, DecisionRefusal::RecoveryNotAuthorized);
	}
	// Genesis: no certificate, no digest.
	let k = peek_index() - 1;
	assert_eq!(
		prove(&mut state, recovery_body(0, None, k)),
		DecisionResult::RecoveryRecorded {
			token: TOKEN,
			gid: 0,
			k,
		}
	);
	assert_eq!(
		decisions(&state)
			.instances()
			.replacement(TOKEN)
			.unwrap()
			.recovered,
		Some(Recovery {
			gid: 0,
			target_digest: None,
			k,
		})
	);
	// The exact retry is its historical answer; another statement conflicts.
	let before = state.decisions.clone();
	assert_eq!(
		prove(&mut state, recovery_body(0, None, k)),
		DecisionResult::RecoveryRecorded {
			token: TOKEN,
			gid: 0,
			k,
		}
	);
	assert_eq!(state.decisions, before);
	unchanged(
		&mut state,
		recovery_body(0, None, k - 1),
		DecisionRefusal::RecoveryConflict,
	);
}

#[test]
fn a_recovery_names_the_current_certificate_and_a_cut_below_its_entry() {
	let mut state = promoted();
	certify(&mut state, 5, [0x05; 32]);
	let k = peek_index() - 1;
	for body in [
		recovery_body(0, None, k),
		recovery_body(5, Some([0x06; 32]), k),
		recovery_body(4, Some([0x05; 32]), k),
		recovery_body(5, None, k),
	] {
		unchanged(&mut state, body, DecisionRefusal::RecoveryStale);
	}
	unchanged(
		&mut state,
		recovery_body(5, Some([0x05; 32]), peek_index()),
		DecisionRefusal::RecoveryCutAhead,
	);
	let k = peek_index() - 1;
	assert_eq!(
		prove(&mut state, recovery_body(5, Some([0x05; 32]), k)),
		DecisionResult::RecoveryRecorded {
			token: TOKEN,
			gid: 5,
			k,
		}
	);
}

#[test]
fn a_recovery_is_signed_by_the_promoted_store_and_refused_once_superseded() {
	let mut state = promoted();
	let k = peek_index() - 1;
	// Signed as the old store: its key is bound to (3, I), not to this body.
	let forged = sign_for_test(recovery_body(0, None, k), signer(3));
	let before = state.decisions.clone();
	assert!(matches!(
		decide(
			&mut state,
			DecisionCommand::RecoveryProved { proof: forged }
		),
		DecisionResult::Refused(_)
	));
	assert_eq!(state.decisions, before);
	// A later replacement is opened against this store's instance.
	assert!(matches!(
		decide(
			&mut state,
			DecisionCommand::BeginReplacement {
				token: TOKEN + 1,
				slot: 3
			}
		),
		DecisionResult::ReplacementRecorded {
			r_old: ALLOCATED_BASE,
			..
		}
	));
	let k = peek_index() - 1;
	unchanged(
		&mut state,
		recovery_body(0, None, k),
		DecisionRefusal::RecoveryNotAuthorized,
	);
}

/// ADR 0108: `promoted()` with its recovery recorded at genesis, cut `k`.
fn recovered() -> (SmState, u64) {
	let mut state = promoted();
	let k = peek_index() - 1;
	assert!(matches!(
		prove(&mut state, recovery_body(0, None, k)),
		DecisionResult::RecoveryRecorded { .. }
	));
	(state, k)
}

fn activation_body(k: u64, through: u64, certified: u64) -> ActivationBody {
	ActivationBody {
		token: TOKEN,
		slot: 3,
		r_new: ALLOCATED_BASE,
		birth: [BIRTH; 16],
		store: store_j(),
		recovered_gid: 0,
		k,
		through,
		certified,
	}
}

fn activate(state: &mut SmState, body: ActivationBody) -> DecisionResult {
	let signer = Signer {
		node: 3,
		incarnation: body.store,
	};
	decide(
		state,
		DecisionCommand::ActivateReplacement {
			proof: sign_for_test(body, signer),
		},
	)
}

fn activation_unchanged(state: &mut SmState, body: ActivationBody, refusal: DecisionRefusal) {
	let before = state.decisions.clone();
	assert_eq!(activate(state, body), refused(refusal));
	assert_eq!(state.decisions, before);
}

/// ADR 0108 control 1: an activation needs the recorded recovery, its exact
/// replacement, a cut from the recovery cut to below its entry and a
/// certificate from the recovery's to the current one; it is recorded once and
/// ends the slot's preparation.
#[test]
fn an_activation_is_recorded_once_for_the_current_recovered_store() {
	// Promoted but not recovered.
	let mut state = promoted();
	let through = peek_index() - 1;
	activation_unchanged(
		&mut state,
		activation_body(through, through, 0),
		DecisionRefusal::ActivationNotAuthorized,
	);
	let (mut state, k) = recovered();
	assert!(slot_preparing(&state, 3));
	let through = peek_index() - 1;
	for body in [
		ActivationBody {
			birth: [BIRTH + 1; 16],
			..activation_body(k, through, 0)
		},
		ActivationBody {
			r_new: ALLOCATED_BASE + 1,
			..activation_body(k, through, 0)
		},
		// Not the recorded recovery.
		ActivationBody {
			k: k - 1,
			..activation_body(k, through, 0)
		},
		ActivationBody {
			recovered_gid: 1,
			..activation_body(k, through, 0)
		},
	] {
		activation_unchanged(&mut state, body, DecisionRefusal::ActivationNotAuthorized);
	}
	// The cut: from the recovery cut, below this entry.
	activation_unchanged(
		&mut state,
		ActivationBody {
			through: k - 1,
			..activation_body(k, through, 0)
		},
		DecisionRefusal::ActivationCutOutOfRange,
	);
	activation_unchanged(
		&mut state,
		activation_body(k, peek_index(), 0),
		DecisionRefusal::ActivationCutOutOfRange,
	);
	// The certificate: from the recovery's (genesis) to the current one.
	activation_unchanged(
		&mut state,
		activation_body(k, peek_index() - 1, 1),
		DecisionRefusal::ActivationCertificateOutOfRange,
	);
	certify(&mut state, 2, [0x02; 32]);
	activation_unchanged(
		&mut state,
		activation_body(k, peek_index() - 1, 3),
		DecisionRefusal::ActivationCertificateOutOfRange,
	);
	let through = peek_index() - 1;
	assert_eq!(
		activate(&mut state, activation_body(k, through, 2)),
		DecisionResult::ActivationRecorded {
			token: TOKEN,
			through,
			certified: 2,
		}
	);
	assert_eq!(
		decisions(&state)
			.instances()
			.replacement(TOKEN)
			.unwrap()
			.activated,
		Some(Activation {
			incarnation: store_j(),
			through,
			certified: 2,
		})
	);
	assert!(
		!slot_preparing(&state, 3),
		"activation ends the preparation"
	);
	// The exact retry is its historical answer, even past the cut bounds;
	// another activation conflicts.
	let before = state.decisions.clone();
	assert_eq!(
		activate(&mut state, activation_body(k, through, 2)),
		DecisionResult::ActivationRecorded {
			token: TOKEN,
			through,
			certified: 2,
		}
	);
	assert_eq!(state.decisions, before);
	activation_unchanged(
		&mut state,
		activation_body(k, through, 0),
		DecisionRefusal::ActivationConflict,
	);
}

/// ADR 0108 control 5: a lease request carries its holder's store. The replaced
/// slot takes none while preparing; after activation the replacement's store is
/// granted and renews, and the old store, carrying its own, is denied both a
/// first grant and a renewal of the replacement's lineage.
#[test]
fn a_replaced_slots_lease_carries_its_store_and_waits_for_its_activation() {
	let (mut state, k) = recovered();
	let range = universe()
		.into_iter()
		.find(|(holder, _)| *holder == 3)
		.unwrap()
		.1;
	let ask = |state: &mut SmState, incarnation: StoreIncarnation, expiry: u64| {
		let expected_generation = decisions(state)
			.lineages
			.get(&range.0)
			.map(|lineage| lineage.generation);
		let grants = vec![crate::consensus::LeaseBatchGrant {
			lease: lease(3, range, 0, expiry),
			expected_generation,
			holder_incarnation: incarnation,
		}];
		granted(apply_next(
			state,
			WendelonRequest::AcquireLeases(crate::consensus::LeaseBatch::new(0, grants).unwrap()),
		))
	};
	assert_eq!(
		ask(&mut state, store_j(), 2_000),
		WendelonResponse::LeaseDenied,
		"preparing"
	);
	let through = peek_index() - 1;
	assert!(matches!(
		activate(&mut state, activation_body(k, through, 0)),
		DecisionResult::ActivationRecorded { .. }
	));
	assert_eq!(
		ask(&mut state, inc(3), 2_000),
		WendelonResponse::LeaseDenied,
		"the old store's first grant"
	);
	assert_eq!(ask(&mut state, store_j(), 2_000), WendelonResponse::Success);
	assert_eq!(decisions(&state).lineages[&range.0].incarnation, store_j());
	assert_eq!(
		ask(&mut state, inc(3), 3_000),
		WendelonResponse::LeaseDenied,
		"the old store's renewal of the replacement's lineage"
	);
	assert_eq!(ask(&mut state, store_j(), 3_000), WendelonResponse::Success);
}

fn index_receipt(
	state: &mut SmState,
	index: crate::schema::IndexObjectId,
	phase: IndexReceiptPhase,
	signer: Signer,
) -> DecisionResult {
	decide(
		state,
		DecisionCommand::IndexReceipt {
			proof: sign_for_test(
				IndexReceiptBody {
					tenant: crate::schema::TenantId::new(7),
					index,
					phase,
					signer,
				},
				signer,
			),
		},
	)
}

fn index_state(
	state: &SmState,
	index: crate::schema::IndexObjectId,
) -> Option<SecondaryIndexState> {
	state
		.secondary_indexes
		.get(crate::schema::TenantId::new(7), index)
		.map(|row| row.state)
}

/// ADR 0108 control 9: a store's index receipt is bound to its current store
/// and to its phase. Readiness needs every slot's current store; a backfill
/// receipt never counts toward the drop, and only every current store's drop
/// receipt removes a Dropping index, with its receipts.
#[test]
fn index_receipts_are_bound_to_the_current_store_and_to_their_phase() {
	use IndexReceiptPhase::*;
	let tenant = crate::schema::TenantId::new(7);
	let index = crate::schema::IndexObjectId::new([0x61; 16]);
	let (mut state, k) = recovered();
	let through = peek_index() - 1;
	assert!(matches!(
		activate(&mut state, activation_body(k, through, 0)),
		DecisionResult::ActivationRecorded { .. }
	));
	// An absent index takes no receipt.
	assert_eq!(
		index_receipt(&mut state, index, Backfilled, signer(1)),
		refused(DecisionRefusal::IndexReceiptUnknownIndex)
	);
	state
		.secondary_indexes
		.insert_for_test(tenant, index, SecondaryIndexState::Building);
	let j = Signer {
		node: 3,
		incarnation: store_j(),
	};
	// The old store of slot 3, signing as itself, is not current.
	let before = state.decisions.clone();
	assert_eq!(
		index_receipt(&mut state, index, Backfilled, signer(3)),
		refused(DecisionRefusal::IndexReceiptNotCurrent)
	);
	assert_eq!(state.decisions, before);
	// A drop receipt for a Building index is the wrong phase.
	assert_eq!(
		index_receipt(&mut state, index, DropCollected, signer(1)),
		refused(DecisionRefusal::IndexReceiptWrongPhase)
	);
	for store in [signer(1), signer(2)] {
		assert_eq!(
			index_receipt(&mut state, index, Backfilled, store),
			DecisionResult::IndexReceiptRecorded {
				tenant,
				index,
				transition: None
			}
		);
	}
	assert_eq!(
		index_state(&state, index),
		Some(SecondaryIndexState::Building),
		"readiness waits for slot 3's current store"
	);
	assert_eq!(
		index_receipt(&mut state, index, Backfilled, j),
		DecisionResult::IndexReceiptRecorded {
			tenant,
			index,
			transition: Some(IndexTransition::Ready)
		}
	);
	assert_eq!(index_state(&state, index), Some(SecondaryIndexState::Ready));
	assert!(decisions(&state).holds_index_receipt(tenant, index, Backfilled, j));
	// Every current store held a backfill receipt; Dropping keeps none of them.
	state
		.secondary_indexes
		.mark_dropping(tenant, index)
		.unwrap();
	state
		.decisions
		.as_mut()
		.unwrap()
		.index_entered_dropping(tenant, index);
	assert!(!decisions(&state).holds_index_receipt(tenant, index, Backfilled, j));
	// A late backfill receipt is the wrong phase and never counts for the drop.
	assert_eq!(
		index_receipt(&mut state, index, Backfilled, signer(1)),
		refused(DecisionRefusal::IndexReceiptWrongPhase)
	);
	for store in [signer(1), signer(2)] {
		assert_eq!(
			index_receipt(&mut state, index, DropCollected, store),
			DecisionResult::IndexReceiptRecorded {
				tenant,
				index,
				transition: None
			}
		);
	}
	assert_eq!(
		index_state(&state, index),
		Some(SecondaryIndexState::Dropping),
		"the drop waits for slot 3's current store"
	);
	assert_eq!(
		index_receipt(&mut state, index, DropCollected, j),
		DecisionResult::IndexReceiptRecorded {
			tenant,
			index,
			transition: Some(IndexTransition::Removed)
		}
	);
	assert_eq!(index_state(&state, index), None);
	assert!(!decisions(&state).holds_index_receipt(tenant, index, DropCollected, j));
	assert_eq!(
		index_receipt(&mut state, index, DropCollected, j),
		refused(DecisionRefusal::IndexReceiptUnknownIndex)
	);
}

/// DC-257: an applied state in which slot 3's replacement instance has its CURRENT start
/// authority (recovered, then activated), with that authority's identity, birth and store,
/// for the storage-level handoff regression. Built only through the reducers.
pub(crate) fn activated_replacement_for_test() -> (
	SmState,
	crate::consensus::decisions::instances::RaftIdentity,
	crate::consensus::decisions::instances::Birth,
	StoreIncarnation,
) {
	use crate::consensus::decisions::instances::{Origin, RaftIdentity, Role};
	let (mut state, k) = recovered();
	let through = peek_index() - 1;
	assert!(matches!(
		activate(&mut state, activation_body(k, through, 0)),
		DecisionResult::ActivationRecorded { .. }
	));
	let identity = RaftIdentity {
		instance: ALLOCATED_BASE,
		origin: Origin::Replacement,
		role: Role::Full,
	};
	(state, identity, [BIRTH; 16], store_j())
}

/// ADR 0108 control 3 (D3): a replacement's store starts Full only under its
/// CURRENT activation: not before it, not for another birth or store, and not
/// once a later replacement opened against its instance.
#[test]
fn a_replacement_starts_full_only_under_its_current_activation() {
	use crate::consensus::decisions::instances::{Origin, RaftIdentity, Role};
	let identity = RaftIdentity {
		instance: ALLOCATED_BASE,
		origin: Origin::Replacement,
		role: Role::Full,
	};
	let birth = [BIRTH; 16];
	let (mut state, k) = recovered();
	let refused = |state: &SmState, birth, store| {
		replacement_start_authority(state, identity, birth, store)
			.expect_err("not current")
			.starts_with("ReplacementNotCurrent")
	};
	assert!(
		refused(&state, birth, store_j()),
		"recovered, not activated"
	);
	let through = peek_index() - 1;
	assert!(matches!(
		activate(&mut state, activation_body(k, through, 0)),
		DecisionResult::ActivationRecorded { .. }
	));
	replacement_start_authority(&state, identity, birth, store_j()).unwrap();
	assert!(refused(&state, [BIRTH + 1; 16], store_j()), "another birth");
	assert!(refused(&state, birth, inc(3)), "another store");
	assert!(
		replacement_start_authority(
			&state,
			RaftIdentity {
				instance: ALLOCATED_BASE + 1,
				..identity
			},
			birth,
			store_j()
		)
		.is_err(),
		"another instance"
	);
	// A later replacement opened against this instance supersedes it.
	assert!(matches!(
		decide(
			&mut state,
			DecisionCommand::BeginReplacement {
				token: TOKEN + 1,
				slot: 3
			}
		),
		DecisionResult::ReplacementRecorded {
			r_old: ALLOCATED_BASE,
			..
		}
	));
	assert!(refused(&state, birth, store_j()), "superseded");
}

/// ADR 0108 control 1 (Codex 01a1e41e R1): the recorded activation's answer is
/// bound to the recovery it names. A signed replay that changes only its cut
/// `k` or only its recovery gid is refused with state unchanged; the exact
/// retry keeps its historical answer after the replacement is superseded.
#[test]
fn an_activation_retry_names_its_recorded_recovery_exactly() {
	let (mut state, k) = recovered();
	let through = peek_index() - 1;
	assert!(matches!(
		activate(&mut state, activation_body(k, through, 0)),
		DecisionResult::ActivationRecorded { .. }
	));
	for body in [
		ActivationBody {
			k: k - 1,
			..activation_body(k, through, 0)
		},
		ActivationBody {
			recovered_gid: 1,
			..activation_body(k, through, 0)
		},
	] {
		activation_unchanged(&mut state, body, DecisionRefusal::ActivationNotAuthorized);
	}
	assert!(matches!(
		decide(
			&mut state,
			DecisionCommand::BeginReplacement {
				token: TOKEN + 1,
				slot: 3
			}
		),
		DecisionResult::ReplacementRecorded { .. }
	));
	let before = state.decisions.clone();
	assert_eq!(
		activate(&mut state, activation_body(k, through, 0)),
		DecisionResult::ActivationRecorded {
			token: TOKEN,
			through,
			certified: 0,
		},
		"the exact retry keeps its historical answer"
	);
	assert_eq!(state.decisions, before);
}

/// ADR 0108 control 9 (Codex 01a1e41e R2): a voter of the applied membership
/// that maps to no slot makes completeness unprovable: every receipt is refused
/// before it is recorded, and no transition happens on the known voters alone.
#[test]
fn an_index_receipt_is_refused_while_a_voter_maps_to_no_slot() {
	use IndexReceiptPhase::*;
	let tenant = crate::schema::TenantId::new(7);
	let index = crate::schema::IndexObjectId::new([0x65; 16]);
	let (mut state, k) = recovered();
	let through = peek_index() - 1;
	assert!(matches!(
		activate(&mut state, activation_body(k, through, 0)),
		DecisionResult::ActivationRecorded { .. }
	));
	state
		.secondary_indexes
		.insert_for_test(tenant, index, SecondaryIndexState::Building);
	state.last_membership = formed_membership(&[1, 2, ALLOCATED_BASE, ALLOCATED_BASE + 7]);
	let j = Signer {
		node: 3,
		incarnation: store_j(),
	};
	for store in [signer(1), signer(2), j] {
		let before = state.decisions.clone();
		assert_eq!(
			index_receipt(&mut state, index, Backfilled, store),
			refused(DecisionRefusal::IndexReceiptMembershipUnknown)
		);
		assert_eq!(state.decisions, before);
	}
	assert_eq!(
		index_state(&state, index),
		Some(SecondaryIndexState::Building)
	);
}

/// ADR 0108 control 1: an activation signed as the old store verifies against
/// nothing it names; once the replacement is superseded none is recorded.
#[test]
fn an_activation_is_signed_by_the_promoted_store_and_refused_once_superseded() {
	let (mut state, k) = recovered();
	let through = peek_index() - 1;
	let forged = sign_for_test(activation_body(k, through, 0), signer(3));
	let before = state.decisions.clone();
	assert!(matches!(
		decide(
			&mut state,
			DecisionCommand::ActivateReplacement { proof: forged }
		),
		DecisionResult::Refused(_)
	));
	assert_eq!(state.decisions, before);
	assert!(matches!(
		decide(
			&mut state,
			DecisionCommand::BeginReplacement {
				token: TOKEN + 1,
				slot: 3
			}
		),
		DecisionResult::ReplacementRecorded {
			r_old: ALLOCATED_BASE,
			..
		}
	));
	let through = peek_index() - 1;
	activation_unchanged(
		&mut state,
		activation_body(k, through, 0),
		DecisionRefusal::ActivationNotAuthorized,
	);
}

/// R2: the ancestry of the current store at slot 3 after I -> J1 -> J2 holds
/// J2, J1 and I (a direct-link rule would miss I), and nothing else.
#[test]
fn a_stores_recorded_ancestry_follows_every_promotion_back() {
	let mut state = base(3);
	let (j1, j2, other) = (
		StoreIncarnation::from_raw(0x0106_0001),
		StoreIncarnation::from_raw(0x0106_0002),
		StoreIncarnation::from_raw(0x0106_0003),
	);
	state.incarnations.insert(
		3,
		IncarnationRegistry {
			current: Some(j2),
			retired: [inc(3), j1].into_iter().collect(),
			accepted: [(j1, Some(inc(3))), (j2, Some(j1))].into_iter().collect(),
		},
	);
	use crate::consensus::decisions::in_recorded_ancestry;
	for ancestor in [j2, j1, inc(3)] {
		assert!(
			in_recorded_ancestry(&state, 3, j2, ancestor),
			"{ancestor:?}"
		);
	}
	assert!(!in_recorded_ancestry(&state, 3, j2, other));
	assert!(
		!in_recorded_ancestry(&state, 2, j2, inc(3)),
		"another slot's chain"
	);
	assert!(!in_recorded_ancestry(&state, 3, j1, j2), "never forward");
}

/// ADR 0106 (wendelon): the delta after a recorded cut K is selected by the
/// decision index. A parent reserved at or below K and accepted above it is
/// owed after K (a selection by reservation would skip it) and not through K.
#[test]
fn a_parent_reserved_before_the_cut_and_accepted_after_it_is_owed_as_delta() {
	use crate::node::replacement_recovery::owed_children;
	let mut state = base(3);
	grant_all(&mut state);
	let early = claim(key(T0, 1), children(&state, 2), 0);
	let DecisionResult::Reserved {
		reservation: rsv_early,
	} = reserve(&mut state, &early, T0)
	else {
		panic!("reserve");
	};
	let later = claim(key(T0, 2), children(&state, 2), 0);
	let DecisionResult::Reserved {
		reservation: rsv_later,
	} = reserve(&mut state, &later, T0)
	else {
		panic!("reserve");
	};
	finalize(&mut state, &later, quorum_acks(&later, rsv_later));
	let k = decisions(&state).bodies[&rsv_later]
		.status
		.dec()
		.expect("accepted");
	finalize(&mut state, &early, quorum_acks(&early, rsv_early));
	let dec_early = decisions(&state).bodies[&rsv_early]
		.status
		.dec()
		.expect("accepted");
	assert!(
		rsv_early <= k && dec_early > k,
		"reserved before K, accepted after"
	);
	let slot = early.children[0].plan[0].node;
	let after: Vec<(u64, u64)> = owed_children(&state, slot, k, u64::MAX)
		.iter()
		.map(|child| (child.rsv, child.dec))
		.collect();
	assert!(after.iter().all(|(rsv, _)| *rsv == rsv_early), "{after:?}");
	assert!(
		!after.is_empty(),
		"the late-accepted parent is owed after K"
	);
	let through: Vec<u64> = owed_children(&state, slot, 0, k)
		.iter()
		.map(|child| child.rsv)
		.collect();
	assert!(through.contains(&rsv_later) && !through.contains(&rsv_early));
}

/// ADR 0106 (wendelon): a recorded recovery is not a generation report. With
/// slot 3's promoted store a current member, a target commits only once that
/// store reports itself; its recorded RecoveryProved, and the survivors'
/// reports, do not stand in for it (taking part is ADR 0107).
#[test]
fn a_recorded_recovery_does_not_report_for_its_store() {
	let mut state = promoted();
	let k = peek_index() - 1;
	assert!(matches!(
		prove(&mut state, recovery_body(0, None, k)),
		DecisionResult::RecoveryRecorded { .. }
	));
	let t = peek_index() - 1;
	assert_eq!(
		propose(&mut state, 1, t, 0),
		DecisionResult::TargetRecorded { gid: 1 }
	);
	let members = decisions(&state)
		.generations
		.pending
		.clone()
		.unwrap()
		.target
		.members;
	let j = Signer {
		node: 3,
		incarnation: store_j(),
	};
	assert!(members.contains(&j), "the promoted store is a member");
	for member in members.iter().filter(|member| **member != j) {
		let report = report_of(&state, *member, 3);
		assert!(matches!(
			decide(&mut state, report),
			DecisionResult::ReportRecorded { .. }
		));
	}
	assert_eq!(generations::ready_commit(&state), None);
	assert_eq!(
		decide(&mut state, DecisionCommand::CommitGeneration { gid: 1 }),
		refused(DecisionRefusal::BarrierIncomplete)
	);
	let own = report_of(&state, j, 3);
	assert!(matches!(
		decide(&mut state, own),
		DecisionResult::ReportRecorded { .. }
	));
	assert!(matches!(
		decide(&mut state, DecisionCommand::CommitGeneration { gid: 1 }),
		DecisionResult::GenerationCommitted { .. }
	));
}

// ============================================================
//  Wendelon ADR 0112: one delete statement, one parent
// ============================================================

/// Row bytes `n` whose positions lie in `count` distinct lineages, in identity
/// order (`row(n)` is `[n; 32]`, so increasing `n` is canonical).
fn rows_in_distinct_lineages(state: &SmState, count: usize) -> Vec<u8> {
	let mut seen = std::collections::BTreeSet::new();
	let mut found = Vec::new();
	for n in 1..=u8::MAX {
		let lineage = point_child(state, position_of(n)).lineage.range_start;
		if seen.insert(lineage) {
			found.push(n);
			if found.len() == count {
				return found;
			}
		}
	}
	panic!("the fixture has fewer than {count} lineages");
}

fn deleted(winner_ms: u64) -> DeleteOutcome {
	DeleteOutcome::Deleted {
		winner_rsv: 1,
		winner_hlc: hts(winner_ms),
	}
}

fn already(winner_ms: u64) -> DeleteOutcome {
	DeleteOutcome::AlreadyDeleted {
		winner_rsv: 1,
		winner_hlc: hts(winner_ms),
	}
}

/// A delete statement of rows `ns` with `outcomes`: one point child per matched
/// row (each in its own lineage), ordered by position, the lowest coordinating.
fn statement_claim(
	state: &SmState,
	sequence_no: u64,
	ns: &[u8],
	outcomes: Vec<DeleteOutcome>,
) -> Claim {
	let request = DeleteRequest {
		table: TABLE,
		schema_generation: 1,
		rows: ns.iter().map(|n| row(*n)).collect(),
	};
	let mut matched: Vec<u64> = ns
		.iter()
		.zip(&outcomes)
		.filter(|(_, outcome)| matches!(outcome, DeleteOutcome::Deleted { .. }))
		.map(|(n, _)| position_of(*n))
		.collect();
	matched.sort_unstable();
	let children: Vec<ChildClaim> = matched.iter().map(|p| point_child(state, *p)).collect();
	Claim {
		key: key(T0, sequence_no),
		request_digest: delete_request_digest(TENANT, &request),
		commit_hlc: hts(T0),
		coordinator: 0,
		first_row_position: matched.first().copied().unwrap_or(0),
		children,
		operation: Operation::Delete {
			request,
			selection: DeleteSelection {
				cut: peek_index() - 1,
				outcomes,
			},
		},
	}
}

fn set_rows(claim: &mut Claim, rows: Vec<Identity>) {
	if let Operation::Delete { request, .. } = &mut claim.operation {
		request.rows = rows;
		claim.request_digest = delete_request_digest(TENANT, request);
	}
}

fn set_outcomes(claim: &mut Claim, outcomes: Vec<DeleteOutcome>) {
	if let Operation::Delete { selection, .. } = &mut claim.operation {
		selection.outcomes = outcomes;
	}
}

#[test]
fn a_delete_statement_is_one_parent_whose_children_are_exactly_its_matched_rows() {
	let mut state = base(3);
	grant_all(&mut state);
	let ns = rows_in_distinct_lineages(&state, 3);
	let before = decisions(&state).clone();
	let outcomes = vec![deleted(T0 - 3), already(T0 - 2), deleted(T0 - 1)];
	let good_claim = statement_claim(&state, 1, &ns, outcomes.clone());
	let good = || good_claim.clone();

	let mut reversed = good();
	set_rows(&mut reversed, ns.iter().rev().map(|n| row(*n)).collect());
	let mut duplicate = good();
	set_rows(&mut duplicate, vec![row(ns[0]), row(ns[0]), row(ns[2])]);
	let mut empty = good();
	set_rows(&mut empty, Vec::new());
	set_outcomes(&mut empty, Vec::new());
	let mut too_many = good();
	set_rows(
		&mut too_many,
		(0..=MAX_DELETE_ROWS as u16)
			.map(|i| {
				let mut bytes = [0u8; 32];
				bytes[..2].copy_from_slice(&i.to_be_bytes());
				Identity::new(bytes)
			})
			.collect(),
	);
	set_outcomes(
		&mut too_many,
		vec![DeleteOutcome::NotFound; MAX_DELETE_ROWS + 1],
	);
	let mut misaligned = good();
	set_outcomes(&mut misaligned, outcomes[..2].to_vec());
	let mut uncovered = good();
	uncovered.children.pop();
	let mut unmatched_child = good();
	set_outcomes(
		&mut unmatched_child,
		vec![
			deleted(T0 - 3),
			DeleteOutcome::NotFound,
			DeleteOutcome::NotFound,
		],
	);
	let mut wrong_count = good();
	wrong_count.children[0].rows = 2;
	let mut wide = good();
	wide.children[0].range_end += 1;
	let mut not_lowest = good();
	not_lowest.first_row_position = not_lowest.children[1].range_start;
	let mut other_coordinator = good();
	other_coordinator.coordinator = 1;
	let mut below_a_winner = good();
	set_outcomes(
		&mut below_a_winner,
		vec![deleted(T0 - 3), already(T0 - 2), deleted(T0)],
	);
	let mut nothing_matched = good();
	set_outcomes(&mut nothing_matched, vec![DeleteOutcome::NotFound; 3]);
	for (label, claim) in [
		("rows not in identity order", reversed),
		("a duplicate row", duplicate),
		("no row", empty),
		("more than 512 rows", too_many),
		("outcomes not aligned", misaligned),
		("a matched row in no child", uncovered),
		("a child with no matched row", unmatched_child),
		("a child's row count", wrong_count),
		("a child wider than its rows", wide),
		("first row not the lowest matched", not_lowest),
		("the coordinator not the lowest child", other_coordinator),
		("commit not above every winner", below_a_winner),
		("a reserve that publishes nothing", nothing_matched),
	] {
		assert_eq!(
			reserve(&mut state, &claim, T0),
			refused(DecisionRefusal::MalformedClaim),
			"{label}"
		);
	}
	assert_eq!(decisions(&state), &before);
	assert!(matches!(
		reserve(&mut state, &good(), T0),
		DecisionResult::Reserved { .. }
	));
	// The same set under the same key is the same record; another set conflicts.
	let mut other = good();
	set_rows(&mut other, vec![row(ns[0]), row(ns[2])]);
	set_outcomes(&mut other, vec![deleted(T0 - 3), deleted(T0 - 1)]);
	assert_eq!(
		reserve(&mut state, &other, T0),
		refused(DecisionRefusal::Conflict)
	);
	decisions(&state).validate().unwrap();
}

#[test]
fn a_delete_statement_noop_is_signed_at_its_lowest_requested_position() {
	let mut state = base(3);
	grant_all(&mut state);
	let ns = rows_in_distinct_lineages(&state, 3);
	let lowest = ns.iter().copied().min_by_key(|n| position_of(*n)).unwrap();
	let body = |state: &SmState| {
		let request = DeleteRequest {
			table: TABLE,
			schema_generation: 1,
			rows: ns.iter().map(|n| row(*n)).collect(),
		};
		let child = point_child(state, position_of(lowest));
		NoopBody {
			key: key(T0, 1),
			request_digest: delete_request_digest(TENANT, &request),
			request,
			selection: DeleteSelection {
				cut: peek_index() - 1,
				outcomes: vec![
					DeleteOutcome::NotFound,
					already(T0 - 1),
					DeleteOutcome::NotFound,
				],
			},
			position: position_of(lowest),
			coordinator: child.holder,
			lineage: child.lineage,
		}
	};
	let before = decisions(&state).clone();
	let highest = ns.iter().copied().max_by_key(|n| position_of(*n)).unwrap();
	let mut not_lowest = body(&state);
	let child = point_child(&state, position_of(highest));
	not_lowest.position = position_of(highest);
	not_lowest.coordinator = child.holder;
	not_lowest.lineage = child.lineage;
	let mut matched = body(&state);
	matched.selection.outcomes[1] = deleted(T0 - 1);
	let mut misaligned = body(&state);
	misaligned.selection.outcomes.pop();
	let mut reversed = body(&state);
	reversed.request.rows.reverse();
	reversed.request_digest = delete_request_digest(TENANT, &reversed.request);
	for (label, noop) in [
		("signed at another position", not_lowest),
		("a noop that deleted", matched),
		("outcomes not aligned", misaligned),
		("rows not in identity order", reversed),
	] {
		assert_eq!(
			record_noop(&mut state, noop),
			refused(DecisionRefusal::MalformedClaim),
			"{label}"
		);
	}
	assert_eq!(decisions(&state), &before);
	let good = body(&state);
	assert!(matches!(
		record_noop(&mut state, good),
		DecisionResult::Terminal {
			status: ParentStatus::Noop { .. },
			..
		}
	));
	decisions(&state).validate().unwrap();
}

/// The largest statement the bounds admit encodes inside the claim bound: 512
/// rows with the widest outcomes, 64 children with 16-member plans and the
/// widest fields; and the largest noop likewise.
#[test]
fn the_largest_delete_statement_fits_the_claim_bound() {
	let wide_hlc = HybridTimestamp {
		physical_ms: u64::MAX,
		logical: u32::MAX,
		node_id: u32::MAX,
	};
	let wide_signer = Signer {
		node: u32::MAX,
		incarnation: StoreIncarnation::from_raw(u128::MAX),
	};
	let rows: Vec<Identity> = (0..MAX_DELETE_ROWS as u16)
		.map(|i| {
			let mut bytes = [0xff; 32];
			bytes[..2].copy_from_slice(&i.to_be_bytes());
			Identity::new(bytes)
		})
		.collect();
	let request = DeleteRequest {
		table: TABLE,
		schema_generation: u64::MAX,
		rows,
	};
	let widest = DeleteOutcome::AlreadyDeleted {
		winner_rsv: u64::MAX,
		winner_hlc: wide_hlc,
	};
	let selection = DeleteSelection {
		cut: u64::MAX,
		outcomes: vec![widest; MAX_DELETE_ROWS],
	};
	let child = ChildClaim {
		range_start: u64::MAX,
		range_end: u64::MAX,
		lineage: LineageRef {
			range_start: u64::MAX,
			generation: u64::MAX,
		},
		holder: wide_signer,
		plan: vec![wide_signer; MAX_PLAN_MEMBERS],
		rows: u32::MAX,
		content: [0xff; 32],
	};
	let claim = Claim {
		key: ParentKey {
			tenant: TENANT,
			namespace: ParentNamespace::Table(TABLE),
			client_uuid: [0xff; 16],
			sequence_no: u64::MAX,
		},
		request_digest: [0xff; 32],
		commit_hlc: wide_hlc,
		coordinator: u16::MAX,
		first_row_position: u64::MAX,
		children: vec![child; MAX_CHILDREN],
		operation: Operation::Delete {
			request: request.clone(),
			selection: selection.clone(),
		},
	};
	let claim_bytes = postcard::to_stdvec(&claim).unwrap().len() as u64;
	assert!(claim_bytes <= MAX_CLAIM_BYTES, "claim {claim_bytes}");
	let noop = NoopBody {
		key: claim.key,
		request_digest: [0xff; 32],
		request,
		selection,
		position: u64::MAX,
		coordinator: wide_signer,
		lineage: claim.children[0].lineage,
	};
	let noop_bytes = postcard::to_stdvec(&noop).unwrap().len() as u64;
	assert!(noop_bytes <= MAX_CLAIM_BYTES, "noop {noop_bytes}");
}

/// Wendelon ADR 0112: both delete sequences are bounded at DECODE, so a long
/// statement is refused before it is materialized, whatever the frame allows: the
/// legal boundary (512) decodes and one more is refused, for the rows and for the
/// outcomes, alone and inside a whole claim and noop. 513 NotFound outcomes are
/// small on the wire; they still never decode.
#[test]
fn the_delete_sequences_are_bounded_at_decode() {
	let rows = |n: usize| -> Vec<Identity> {
		(0..n as u16)
			.map(|i| {
				let mut bytes = [0u8; 32];
				bytes[..2].copy_from_slice(&i.to_be_bytes());
				Identity::new(bytes)
			})
			.collect()
	};
	let request = |n: usize| DeleteRequest {
		table: TABLE,
		schema_generation: 1,
		rows: rows(n),
	};
	let selection = |n: usize| DeleteSelection {
		cut: 7,
		outcomes: vec![DeleteOutcome::NotFound; n],
	};
	for (n, decodes) in [(MAX_DELETE_ROWS, true), (MAX_DELETE_ROWS + 1, false)] {
		let bytes = postcard::to_stdvec(&request(n)).unwrap();
		assert_eq!(
			postcard::from_bytes::<DeleteRequest>(&bytes).is_ok(),
			decodes,
			"rows {n}"
		);
		let bytes = postcard::to_stdvec(&selection(n)).unwrap();
		assert_eq!(
			postcard::from_bytes::<DeleteSelection>(&bytes).is_ok(),
			decodes,
			"outcomes {n}"
		);
		if !decodes {
			assert!(
				bytes.len() < 1024,
				"513 NotFound outcomes are small on the wire: {}",
				bytes.len()
			);
		}
		// Inside the bodies a decision carries.
		let state = {
			let mut state = base(3);
			grant_all(&mut state);
			state
		};
		let mut noop = noop_body(&state, 1, 1);
		noop.request = request(n);
		noop.selection = selection(n);
		let bytes = postcard::to_stdvec(&noop).unwrap();
		assert_eq!(
			postcard::from_bytes::<NoopBody>(&bytes).is_ok(),
			decodes,
			"noop {n}"
		);
		let mut claim = delete_claim(&state, 1, 1, T0 - 1);
		claim.operation = Operation::Delete {
			request: request(n),
			selection: selection(n),
		};
		let bytes = postcard::to_stdvec(&claim).unwrap();
		assert_eq!(
			postcard::from_bytes::<Claim>(&bytes).is_ok(),
			decodes,
			"claim {n}"
		);
	}
	// Mixed lengths decode (alignment is the reducer's check, not the decoder's).
	let mut mixed = selection(MAX_DELETE_ROWS);
	mixed.outcomes.truncate(3);
	let bytes = postcard::to_stdvec(&mixed).unwrap();
	assert!(postcard::from_bytes::<DeleteSelection>(&bytes).is_ok());
}

// ============================================================
//  Wendelon ADR 0113: tenant-wide table operation identities
// ============================================================

fn on_table(key: ParentKey, table: u8) -> ParentKey {
	ParentKey {
		namespace: ParentNamespace::Table(crate::schema::TableObjectId::new([table; 16])),
		..key
	}
}

fn table_resolution(state: &SmState, key: &ParentKey) -> TableResolution {
	resolve_table_operation(
		decisions(state),
		key.tenant,
		key.client_uuid,
		key.sequence_no,
	)
}

/// One table operation identity names one table, tenant-wide: a second table under
/// it conflicts at Reserve and at RecordNoop, in either order, with no record and
/// the first table's receipt unchanged. The same table keeps its digest conflict
/// and its replay. Another tenant, and the KV namespace, are independent.
#[test]
fn a_table_operation_identity_names_one_table_per_tenant() {
	let mut state = base(3);
	grant_all(&mut state);
	let a = key(T0, 1);
	let first = claim(a, children(&state, 1), 0);
	let DecisionResult::Reserved { reservation } = reserve(&mut state, &first, T0) else {
		panic!("the first table reserves");
	};
	let before = decisions(&state).clone();
	// Another table, a Reserve: conflict, nothing recorded.
	let other = Claim {
		key: on_table(a, 0x42),
		..first.clone()
	};
	assert_eq!(
		reserve(&mut state, &other, T0),
		refused(DecisionRefusal::Conflict)
	);
	// Another table, a RecordNoop: conflict too.
	let mut noop = noop_body(&state, 1, 1);
	noop.key = on_table(a, 0x42);
	assert_eq!(
		record_noop(&mut state, noop),
		refused(DecisionRefusal::Conflict)
	);
	assert_eq!(decisions(&state), &before, "neither offer changed anything");
	assert!(matches!(
		table_resolution(&state, &a),
		TableResolution::Reserved { delete: false }
	));
	// The same table: replay answers the record; another digest conflicts.
	assert_eq!(
		reserve(&mut state, &first, T0),
		DecisionResult::Reserved { reservation }
	);
	let changed = Claim {
		request_digest: [8; 32],
		..first.clone()
	};
	assert_eq!(
		reserve(&mut state, &changed, T0),
		refused(DecisionRefusal::Conflict)
	);
	// Another tenant with the same identity on the other table is independent.
	let elsewhere = ParentKey {
		tenant: TenantId::new(TENANT.get() + 1),
		..on_table(a, 0x42)
	};
	assert_eq!(
		resolve_table_operation(
			decisions(&state),
			elsewhere.tenant,
			a.client_uuid,
			a.sequence_no
		),
		TableResolution::Unknown
	);
	// The KV namespace never answers a table resolve, and never conflicts with one.
	let kv_key = ParentKey {
		namespace: ParentNamespace::Kv,
		..a
	};
	assert_eq!(lookup(decisions(&state), &kv_key, &[9; 32]), Ok(None));
	decisions(&state).validate().unwrap();

	// The other order: a noop on table B first, then a Reserve on table A.
	let mut state = base(3);
	grant_all(&mut state);
	let noop = noop_body(&state, 2, 1);
	let noop_key = noop.key;
	assert!(matches!(
		record_noop(&mut state, noop),
		DecisionResult::Terminal { .. }
	));
	let later = Claim {
		key: on_table(noop_key, 0x43),
		..claim(noop_key, children(&state, 1), 0)
	};
	assert_eq!(
		reserve(&mut state, &later, T0),
		refused(DecisionRefusal::Conflict)
	);
	assert!(matches!(
		table_resolution(&state, &noop_key),
		TableResolution::Noop {
			counts: [0, 0, 1],
			..
		}
	));
	decisions(&state).validate().unwrap();
}

/// The resolve reads key presence first, then the replicated cutoff: Reserved
/// stays pending, Accepted carries its receipt, Refused had no effect, and an
/// identity with no retained key is UNKNOWN above the cutoff, EXPIRED at or below
/// it (also once its key retired with its body still retained), and invalid when
/// not a UUIDv7.
#[test]
fn the_table_resolve_reads_key_presence_then_the_cutoff() {
	let mut state = base(3);
	grant_all(&mut state);
	let accepted = claim(key(T0, 1), children(&state, 1), 0);
	let DecisionResult::Reserved { reservation } = reserve(&mut state, &accepted, T0) else {
		panic!("reserve");
	};
	assert!(matches!(
		table_resolution(&state, &accepted.key),
		TableResolution::Reserved { delete: false }
	));
	assert!(matches!(
		finalize(&mut state, &accepted, quorum_acks(&accepted, reservation)),
		DecisionResult::Terminal { .. }
	));
	let TableResolution::Accepted {
		dec,
		commit_hlc,
		delete_counts: None,
	} = table_resolution(&state, &accepted.key)
	else {
		panic!("accepted");
	};
	assert!(dec > reservation);
	assert_eq!(commit_hlc, accepted.commit_hlc);

	let refused_claim = claim(key(T0, 2), children(&state, 1), 0);
	let DecisionResult::Reserved { reservation } = reserve(&mut state, &refused_claim, T0) else {
		panic!("reserve");
	};
	assert!(matches!(
		abort(&mut state, &refused_claim, reservation),
		DecisionResult::Terminal { .. }
	));
	assert_eq!(
		table_resolution(&state, &refused_claim.key),
		TableResolution::Refused { delete: false }
	);

	let fresh = key(T0, 3);
	assert_eq!(table_resolution(&state, &fresh), TableResolution::Unknown);
	let old = key(1_000, 1);
	assert_eq!(table_resolution(&state, &old), TableResolution::Expired);
	let invalid = ParentKey {
		client_uuid: [0; 16],
		..fresh
	};
	assert_eq!(
		table_resolution(&state, &invalid),
		TableResolution::InvalidIdentity
	);

	// The floor retires the terminal keys, and their index entries with them;
	// the accepted body stays until a generation, yet the identity is EXPIRED.
	let bodies = decisions(&state).bodies.len();
	assert!(matches!(
		decide(
			&mut state,
			DecisionCommand::AdvanceFloor {
				floor_ms: T0 + 1,
				stamp_ms: T0 + DECISION_EXPIRY_MS + 10,
			},
		),
		DecisionResult::FloorAdvanced { .. }
	));
	assert_eq!(decisions(&state).bodies.len(), bodies, "bodies retained");
	assert_eq!(
		table_resolution(&state, &accepted.key),
		TableResolution::Expired
	);
	assert_eq!(
		decisions(&state).table_of_operation(accepted.key.operation()),
		None,
		"the index followed the key"
	);
	decisions(&state).validate().unwrap();
}

/// The index is derived, never serialized: rebuilt on decode and checked, and a
/// state holding one identity under two tables refuses.
#[test]
fn the_table_operation_index_is_rebuilt_on_decode_and_fails_closed() {
	let mut state = base(3);
	grant_all(&mut state);
	let first = claim(key(T0, 1), children(&state, 1), 0);
	assert!(matches!(
		reserve(&mut state, &first, T0),
		DecisionResult::Reserved { .. }
	));
	let bytes = postcard::to_stdvec(decisions(&state)).unwrap();
	let mut decoded: DecisionState = postcard::from_bytes(&bytes).unwrap();
	assert!(
		decoded.validate().is_err(),
		"an undecoded index does not match the keys"
	);
	decoded.restore_table_operations().unwrap();
	decoded.validate().unwrap();
	assert_eq!(&decoded, decisions(&state));
	// A tampered state with the same identity under a second table refuses.
	let entry = decoded.keys[&first.key];
	decoded.keys.insert(on_table(first.key, 0x42), entry);
	assert!(decoded.restore_table_operations().is_err());
	assert!(decoded.validate().is_err());
}
