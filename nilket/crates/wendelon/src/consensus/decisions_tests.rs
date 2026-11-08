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

fn grant(state: &mut SmState, lease: LeaseDescriptor) -> WendelonResponse {
	apply_next(state, WendelonRequest::AcquireLease(lease))
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
		run(
			&mut scratch,
			&mut entries,
			WendelonRequest::AcquireLease(lease(holder, range, 0, 1_000)),
		);
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
	run(
		&mut scratch,
		&mut entries,
		WendelonRequest::AcquireLease(lease(holder, range, 500, 2_000)),
	);
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
		(2, 2, 1)
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
	let holder = segment.replicas[0].id;
	let (range_start, lineage) = decisions(state)
		.lineages
		.range(..=segment.start)
		.next_back()
		.map(|(start, lineage)| (*start, *lineage))
		.unwrap();
	assert!(segment.end <= lineage.range_end && lineage.holder.id == holder);
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
	for segment in ring(3).replica_segments() {
		if out.len() == n {
			break;
		}
		if out
			.iter()
			.any(|child| child.holder.node == segment.replicas[0].id)
		{
			continue;
		}
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
	// Continuous and not shorter: the same generation.
	assert_eq!(
		grant(&mut state, lease(holder, range, 500, 2_000)),
		WendelonResponse::Success
	);
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

#[test]
fn a_release_ends_its_lineage_and_an_absent_or_mismatched_release_changes_nothing() {
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
	assert_eq!(
		apply_next(&mut state, release(other, EPOCH)),
		WendelonResponse::LeaseDenied
	);
	assert_eq!(decisions(&state), decisions(&before));
	assert_eq!(
		apply_next(&mut state, release(holder, EPOCH)),
		WendelonResponse::Success
	);
	assert!(decisions(&state).lineages.is_empty());
	let generation = decisions(&state).next_generation;
	for _ in 0..3 {
		assert_eq!(
			apply_next(&mut state, release(holder, EPOCH)),
			WendelonResponse::Success
		);
	}
	assert_eq!(
		decisions(&state).next_generation,
		generation,
		"no generation is manufactured"
	);
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
			let release = WendelonRequest::ReleaseLease(ReleaseLeaseRequestMeta {
				range_start: range.0,
				range_end: range.1,
				epoch: EPOCH,
				holder: node(*holder),
			});
			assert_eq!(apply_next(&mut state, release), WendelonResponse::Success);
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
	apply_next(
		&mut state,
		WendelonRequest::ReleaseLease(ReleaseLeaseRequestMeta {
			range_start: child.lineage.range_start,
			range_end: lineage.range_end,
			epoch: EPOCH,
			holder: node(child.holder.node),
		}),
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
	apply_next(
		&mut state,
		WendelonRequest::ReleaseLease(ReleaseLeaseRequestMeta {
			range_start: child.lineage.range_start,
			range_end: end,
			epoch: EPOCH,
			holder: node(child.holder.node),
		}),
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
		row: row(n),
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
				outcome: DeleteOutcome::Deleted {
					winner_rsv: 1,
					winner_hlc: hts(winner_ms),
				},
			},
		},
	}
}

fn noop_body(state: &SmState, sequence_no: u64, n: u8) -> NoopBody {
	let request = delete_request(n);
	let child = point_child(state, position_of(n));
	NoopBody {
		key: key(T0, sequence_no),
		request_digest: delete_request_digest(TENANT, &request),
		request,
		selection: DeleteSelection {
			cut: peek_index() - 1,
			outcome: DeleteOutcome::NotFound,
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
		selection.outcome = DeleteOutcome::NotFound;
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
	deleted.selection.outcome = DeleteOutcome::Deleted {
		winner_rsv: 1,
		winner_hlc: hts(T0 - 1),
	};
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
			incarnation: inc(1)
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
