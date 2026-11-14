//! ADR 0699 S3b: manifests and parent locks.

use super::*;
use crate::integration::compute::SchemaGeneration;
use crate::schema::{
	MultiRowLayoutDescriptor, PrimaryKeyDescriptor, RecordLayoutPolicy, RowIdentityPolicy,
	SchemaEvolutionPolicy, TableSpreadLocalityDescriptor,
};
use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};

fn descriptor(identity: RowIdentityPolicy, generation: u64) -> TableSchemaDescriptor {
	TableSchemaDescriptor::new_with_layout(
		TableObjectId::new([4; 16]),
		SchemaGeneration::new(generation).get(),
		"analytics",
		"events",
		&Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("v", DataType::Utf8, true),
		]),
		identity,
		crate::schema::LocalityPolicy::TableSpreadProjection(TableSpreadLocalityDescriptor {
			spread_factor: 1,
			prefix_bits: 16,
		}),
		RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::new(4, 8, 1024 * 1024).unwrap()),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.unwrap()
}

fn pk() -> RowIdentityPolicy {
	RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id"))
}

fn batch(descriptor: &TableSchemaDescriptor, rows: &[(i64, &str)]) -> RecordBatch {
	RecordBatch::try_new(
		Arc::new(descriptor.arrow_schema().unwrap()),
		vec![
			Arc::new(Int64Array::from(
				rows.iter().map(|r| r.0).collect::<Vec<_>>(),
			)),
			Arc::new(StringArray::from(
				rows.iter().map(|r| r.1).collect::<Vec<_>>(),
			)),
		],
	)
	.unwrap()
}

fn key() -> crate::consensus::decisions::ParentKey {
	crate::consensus::decisions::ParentKey {
		tenant: TenantId::new(7),
		namespace: crate::consensus::decisions::ParentNamespace::Table(TableObjectId::new([4; 16])),
		client_uuid: [1; 16],
		sequence_no: 3,
	}
}

const HLC: HybridTimestamp = HybridTimestamp {
	physical_ms: 1_800_000_000_000,
	logical: 4,
	node_id: 2,
};

fn manifest_of(
	descriptor: &TableSchemaDescriptor,
	rows: &[(i64, &str)],
) -> Result<Vec<OwnedVersion>, ManifestRefusal> {
	let batch = batch(descriptor, rows);
	let meta = child_request_meta(
		&key(),
		descriptor.table_object_id,
		descriptor.schema_generation,
		batch.num_rows(),
		HLC,
	)
	.unwrap();
	child_manifest(&meta, &batch, descriptor, HLC)
}

#[test]
fn a_manifest_names_each_row_once_at_the_commit_hlc_in_canonical_order() {
	let descriptor = descriptor(pk(), 1);
	let forward = manifest_of(&descriptor, &[(1, "a"), (2, "b"), (3, "c")]).unwrap();
	let shuffled = manifest_of(&descriptor, &[(3, "c"), (1, "a"), (2, "b")]).unwrap();
	assert_eq!(
		forward, shuffled,
		"the manifest does not depend on row order"
	);
	assert_eq!(forward.len(), 3);
	assert!(
		forward
			.iter()
			.all(|version| version.hlc == HLC && version.table == descriptor.table_object_id)
	);
	assert!(crate::storage::index::parent_intents::manifest_is_canonical(&forward));
	assert_eq!(manifest_content(&forward), manifest_content(&shuffled));
}

#[test]
fn the_row_digest_binds_content_and_schema_generation() {
	let first = manifest_of(&descriptor(pk(), 1), &[(1, "a")]).unwrap();
	let other_value = manifest_of(&descriptor(pk(), 1), &[(1, "z")]).unwrap();
	let other_generation = manifest_of(&descriptor(pk(), 2), &[(1, "a")]).unwrap();
	assert_eq!(first[0].key_identity, other_value[0].key_identity);
	assert_ne!(first[0].row_digest, other_value[0].row_digest);
	assert_ne!(first[0].row_digest, other_generation[0].row_digest);
	assert_ne!(manifest_content(&first), manifest_content(&other_value));
}

#[test]
fn a_repeated_key_or_an_unsupported_table_is_refused() {
	let descriptor = descriptor(pk(), 1);
	assert!(manifest_of(&descriptor, &[(1, "a"), (1, "b")]).is_err());
	for identity in [
		RowIdentityPolicy::AppendOnly,
		RowIdentityPolicy::AppendOnlyContentHash,
	] {
		let mut unsupported = descriptor.clone();
		unsupported.row_identity_policy = identity;
		assert_eq!(
			manifest_of(&unsupported, &[(1, "a")]),
			Err(ManifestRefusal::UnsupportedTable)
		);
	}
}

#[tokio::test]
async fn one_rsv_maps_to_one_stripe_and_a_spawned_holder_outlives_its_caller() {
	let locks = Arc::new(ParentLocks::new());
	assert!(Arc::ptr_eq(
		locks.stripe(5),
		locks.stripe(5 + PARENT_LOCK_STRIPES as u64)
	));
	assert!(!Arc::ptr_eq(locks.stripe(5), locks.stripe(6)));

	// Work spawned under the guard keeps it after the request that started it
	// is dropped.
	let (started_tx, started_rx) = tokio::sync::oneshot::channel();
	let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
	let caller = {
		let locks = locks.clone();
		tokio::spawn(async move {
			let guard = locks.lock(9).await;
			let work = tokio::spawn(async move {
				let _guard = guard;
				let _ = started_tx.send(());
				let _ = release_rx.await;
			});
			work.await.unwrap();
		})
	};
	started_rx.await.unwrap();
	caller.abort();
	let _ = caller.await;
	assert!(
		locks.stripe(9).try_lock().is_err(),
		"the cancelled caller's work still holds the parent lock"
	);
	release_tx.send(()).unwrap();
	let _guard = tokio::time::timeout(std::time::Duration::from_secs(5), locks.lock(9))
		.await
		.expect("released once the work finishes");
}

// ============================================================
//  PLAN AND PERSIST (S3b controls)
// ============================================================

use crate::consensus::decisions::tests as fixture;
use crate::consensus::decisions::{
	ChildClaim, Claim, DecisionResult, LineageRef, ParentKey, ParentNamespace, ParentStatus, Signer,
};
use crate::storage::index::parent_intents::{IntentRefusal, ParentIntentState};

const TENANT: TenantId = TenantId(7);

/// One reserved single-child table parent whose rows share one ring position.
struct Reserved {
	state: SmState,
	claim: Claim,
	rsv: u64,
	batch: RecordBatch,
	meta: ChildWriteMeta,
	holder: Signer,
	descriptor: TableSchemaDescriptor,
}

fn reserved(rows: &[(i64, &str)], sequence_no: u64) -> Reserved {
	let mut state = fixture::base(3);
	fixture::grant_all(&mut state);
	let descriptor = descriptor(pk(), 1);
	state
		.table_schemas
		.upsert(TENANT, descriptor.clone())
		.unwrap();
	let table = descriptor.table_object_id;
	let key = ParentKey {
		tenant: TENANT,
		namespace: ParentNamespace::Table(table),
		client_uuid: fixture::uuid_v7(fixture::T0, 1),
		sequence_no,
	};
	let batch = batch(&descriptor, rows);
	let commit_hlc = HybridTimestamp {
		physical_ms: fixture::T0,
		logical: 0,
		node_id: 1,
	};
	let request = child_request_meta(
		&key,
		table,
		descriptor.schema_generation,
		batch.num_rows(),
		commit_hlc,
	)
	.unwrap();
	let admission = validate_table_write_request_batch(
		&request,
		&batch,
		&descriptor,
		writer_of(commit_hlc),
		commit_hlc,
	)
	.unwrap();
	let bifurcated =
		bifurcate_table_write_with_admission_and_row_group_encoding_and_chunk_layout_config(
			&request,
			&batch,
			&descriptor,
			&admission,
			commit_hlc,
			&Default::default(),
			&Default::default(),
			&Default::default(),
		)
		.unwrap();
	let positions: std::collections::BTreeSet<u64> =
		bifurcated.children.iter().map(record_position).collect();
	assert_eq!(
		positions.len(),
		1,
		"a spread-factor-1 table places every row together"
	);
	let position = *positions.first().unwrap();
	let ring = crate::network::ring::RingState::build(
		&crate::network::ring::membership_from_epoch_config(&fixture::epoch_config(fixture::EPOCH)),
		3,
	);
	let segment = ring
		.replica_segments()
		.into_iter()
		.find(|segment| segment.start <= position && position <= segment.end)
		.unwrap();
	let holder = fixture::signer(segment.replicas[0].id);
	let decisions = state.decisions.as_ref().unwrap();
	let (range_start, lineage) = decisions
		.lineages()
		.range(..=position)
		.next_back()
		.map(|(start, lineage)| (*start, *lineage))
		.unwrap();
	let (_, manifest) = child_versions(&request, &batch, &descriptor, commit_hlc).unwrap();
	let child = ChildClaim {
		range_start: position,
		range_end: position,
		lineage: LineageRef {
			range_start,
			generation: lineage.generation,
		},
		holder,
		plan: segment
			.replicas
			.iter()
			.map(|n| fixture::signer(n.id))
			.collect(),
		rows: manifest.len() as u32,
		content: manifest_content(&manifest),
	};
	let claim = fixture::claim(key, vec![child], 0);
	assert_eq!(claim.commit_hlc, commit_hlc);
	let DecisionResult::Reserved { reservation } =
		fixture::reserve(&mut state, &claim, fixture::T0)
	else {
		panic!("reserve");
	};
	let meta = ChildWriteMeta {
		rsv: reservation,
		ordinal: 0,
		claim_digest: crate::consensus::decisions::claim_digest(&claim),
		schema_generation: descriptor.schema_generation,
	};
	Reserved {
		state,
		claim,
		rsv: reservation,
		batch,
		meta,
		holder,
		descriptor,
	}
}

fn plan(
	r: &Reserved,
	local: Signer,
	sender: u32,
	role: ChildRole,
) -> Result<ChildPlan, ChildRefusal> {
	plan_child(
		&r.state,
		local,
		sender,
		role,
		&r.meta,
		r.batch.clone(),
		|_, _| true,
	)
}

#[test]
fn a_child_is_planned_only_from_replicated_state_and_its_own_rows() {
	let r = reserved(&[(1, "a"), (2, "b")], 1);
	let holder = r.holder;
	let replica = r.claim.children[0]
		.plan
		.iter()
		.copied()
		.find(|member| *member != holder)
		.unwrap();
	let ok = plan(&r, holder, holder.node, ChildRole::Holder).unwrap();
	assert_eq!(ok.intent.manifest.len(), 2);
	assert_eq!(ok.ack.acker, holder);
	plan(&r, replica, holder.node, ChildRole::Replica).unwrap();

	assert_eq!(
		plan(&r, holder, replica.node, ChildRole::Holder).err(),
		Some(ChildRefusal::WrongSender),
		"only the coordinator may send to the holder"
	);
	assert_eq!(
		plan(&r, replica, replica.node, ChildRole::Replica).err(),
		Some(ChildRefusal::WrongSender),
		"only the holder may send to a replica"
	);
	let stranger = Signer {
		node: replica.node,
		incarnation: crate::consensus::StoreIncarnation::from_raw(77),
	};
	assert_eq!(
		plan(&r, stranger, holder.node, ChildRole::Replica).err(),
		Some(ChildRefusal::NotInPlan)
	);
	assert_eq!(
		plan_child(
			&r.state,
			holder,
			holder.node,
			ChildRole::Holder,
			&r.meta,
			r.batch.clone(),
			|_, _| false
		)
		.err(),
		Some(ChildRefusal::LeaseMissing)
	);
	let mut wrong = r.meta.clone();
	wrong.claim_digest = [0; 32];
	assert_eq!(
		plan_child(
			&r.state,
			holder,
			holder.node,
			ChildRole::Holder,
			&wrong,
			r.batch.clone(),
			|_, _| true
		)
		.err(),
		Some(ChildRefusal::ClaimMismatch)
	);
	let mut unknown = r.meta.clone();
	unknown.rsv += 1_000;
	assert_eq!(
		plan_child(
			&r.state,
			holder,
			holder.node,
			ChildRole::Holder,
			&unknown,
			r.batch.clone(),
			|_, _| true
		)
		.err(),
		Some(ChildRefusal::UnknownReservation)
	);
	let mut generation = r.meta.clone();
	generation.schema_generation = 9;
	assert_eq!(
		plan_child(
			&r.state,
			holder,
			holder.node,
			ChildRole::Holder,
			&generation,
			r.batch.clone(),
			|_, _| true
		)
		.err(),
		Some(ChildRefusal::SchemaUnknown)
	);
	// Other rows than the claim committed to.
	let other = batch(&r.descriptor, &[(1, "a"), (2, "CHANGED")]);
	assert_eq!(
		plan_child(
			&r.state,
			holder,
			holder.node,
			ChildRole::Holder,
			&r.meta,
			other,
			|_, _| true
		)
		.err(),
		Some(ChildRefusal::ContentMismatch)
	);
	// A closed lineage at the holder.
	let mut closed = r.state.clone();
	fixture::apply_next(
		&mut closed,
		crate::consensus::WendelonRequest::AdvanceEpoch(crate::consensus::AdvanceEpochRecord {
			from: fixture::epoch_config(fixture::EPOCH),
			to: fixture::epoch_config(fixture::EPOCH + 1),
		}),
	);
	assert_eq!(
		plan_child(
			&closed,
			holder,
			holder.node,
			ChildRole::Holder,
			&r.meta,
			r.batch.clone(),
			|_, _| true
		)
		.err(),
		Some(ChildRefusal::LineageClosed)
	);
}

fn slab_config(
	dir: std::path::PathBuf,
	crypto: Arc<crate::crypto::CryptoManager>,
) -> crate::storage::manager::SlabManagerConfig {
	crate::storage::manager::SlabManagerConfig {
		tier_history_budget_bytes: crate::storage::DEFAULT_TIER_HISTORY_BUDGET_BYTES,
		dir,
		max_slab_bytes: 1 << 30,
		io: crate::storage::io_uring::spawn_io_thread(64).unwrap(),
		l2: None,
		rehydration: crate::storage::manager::RehydrationPolicy::default(),
		l2_column_fetch: Default::default(),
		row_group_writer: Default::default(),
		l2_row_group_fetch: Default::default(),
		encoding_writer: Default::default(),
		l2_chunk_layout: Default::default(),
		l2_encoding_fetch: Default::default(),
		encoding_compute: Default::default(),
		group_commit: crate::storage::GroupCommitConfig::default(),
		crypto,
	}
}

fn test_crypto() -> Arc<crate::crypto::CryptoManager> {
	let crypto = Arc::new(crate::crypto::CryptoManager::new_dev());
	crypto.install_local_only_for_tests(TENANT).unwrap();
	crypto
}

async fn manager() -> (crate::test_support::TestDir, Arc<SlabManager>) {
	let (dir, slab, _) = manager_with_crypto().await;
	(dir, slab)
}

async fn manager_with_crypto() -> (
	crate::test_support::TestDir,
	Arc<SlabManager>,
	Arc<crate::crypto::CryptoManager>,
) {
	let scratch = crate::test_support::TestDir::new("wendelon-child-writes");
	let crypto = test_crypto();
	let manager =
		SlabManager::initialize_new_store(slab_config(scratch.to_path_buf(), crypto.clone()))
			.await
			.unwrap();
	(scratch, manager, crypto)
}

/// A real reopen of the same store: slab scan, index rebuild, intents read back.
async fn reopen(
	dir: &crate::test_support::TestDir,
	slab: Arc<SlabManager>,
	crypto: Arc<crate::crypto::CryptoManager>,
) -> Arc<SlabManager> {
	drop(slab);
	SlabManager::open(slab_config(dir.to_path_buf(), crypto))
		.await
		.unwrap()
}

fn authority(r: &Reserved, role: ChildRole) -> ChildAuthority {
	authority_with_leases(r, role, Arc::new(std::sync::atomic::AtomicBool::new(true)))
}

fn authority_with_leases(
	r: &Reserved,
	role: ChildRole,
	lease_live: Arc<std::sync::atomic::AtomicBool>,
) -> ChildAuthority {
	let signer = match role {
		ChildRole::Holder => r.holder,
		ChildRole::Replica | ChildRole::Repair => r.claim.children[0]
			.plan
			.iter()
			.copied()
			.find(|member| *member != r.holder)
			.unwrap(),
	};
	ChildAuthority {
		signer: Arc::new(DecisionSigner {
			key: fixture::test_key(signer.node),
			signer,
		}),
		role,
		leases: Some(Arc::new(move || {
			let live = lease_live.load(std::sync::atomic::Ordering::SeqCst);
			Box::pin(async move {
				if live {
					vec![(0, u64::MAX, fixture::EPOCH)]
				} else {
					Vec::new()
				}
			})
		})),
		before_sign: None,
	}
}

async fn machine(state: &SmState) -> WendelonStateMachine {
	let machine = WendelonStateMachine::new();
	machine
		.seed_complete_snapshot_state_for_test(state.clone())
		.await;
	machine
}

async fn copies(slab: &SlabManager, plan: &ChildPlan) -> Vec<usize> {
	slab.persistent_index()
		.unwrap()
		.owned_version_copies(TENANT, plan.intent.manifest.clone())
		.await
		.unwrap()
}

async fn intent_state(slab: &SlabManager, rsv: u64) -> Option<ParentIntentState> {
	slab.persistent_index()
		.unwrap()
		.parent_intent(rsv, 0)
		.await
		.unwrap()
		.map(|intent| intent.state)
}

#[tokio::test]
async fn a_reserved_child_completes_once_and_a_retry_adds_no_version() {
	let r = reserved(&[(1, "a"), (2, "b"), (3, "c")], 1);
	let (_dir, slab) = manager().await;
	let machine = machine(&r.state).await;
	let locks = Arc::new(ParentLocks::new());
	let planned = plan(&r, r.holder, r.holder.node, ChildRole::Holder).unwrap();
	persist_child(
		slab.clone(),
		machine.clone(),
		locks.clone(),
		planned.clone(),
		authority(&r, ChildRole::Holder),
	)
	.await
	.unwrap();
	assert_eq!(
		intent_state(&slab, r.rsv).await,
		Some(ParentIntentState::Complete)
	);
	assert_eq!(copies(&slab, &planned).await, vec![1, 1, 1]);
	// A lost ack is retried: verified again, nothing appended.
	persist_child(
		slab.clone(),
		machine,
		locks,
		planned.clone(),
		authority(&r, ChildRole::Holder),
	)
	.await
	.unwrap();
	assert_eq!(copies(&slab, &planned).await, vec![1, 1, 1]);
}

#[tokio::test]
async fn a_partial_append_is_finished_with_only_the_missing_rows() {
	let r = reserved(&[(1, "a"), (2, "b"), (3, "c")], 1);
	let (_dir, slab) = manager().await;
	let machine = machine(&r.state).await;
	let locks = Arc::new(ParentLocks::new());
	let planned = plan(&r, r.holder, r.holder.node, ChildRole::Holder).unwrap();
	// A crash after the intent and one row: record the intent, append row 0 only.
	let index = slab.persistent_index().unwrap();
	index
		.begin_parent_intent(planned.intent.clone())
		.await
		.unwrap()
		.unwrap();
	let first = arrow::compute::take_record_batch(
		&planned.batch,
		&arrow::array::UInt32Array::from(vec![0u32]),
	)
	.unwrap();
	let bifurcated = bifurcate_child(&slab, &planned, &first).unwrap();
	append_table_children_durable(
		&slab,
		TENANT,
		planned.ack_hlc(),
		&to_replica_children(&bifurcated),
		&planned.secondary_indexes,
	)
	.await
	.unwrap();
	let partial = copies(&slab, &planned).await;
	assert_eq!(partial.iter().filter(|c| **c == 1).count(), 1);
	persist_child(
		slab.clone(),
		machine,
		locks,
		planned.clone(),
		authority(&r, ChildRole::Holder),
	)
	.await
	.unwrap();
	assert_eq!(
		copies(&slab, &planned).await,
		vec![1, 1, 1],
		"no duplicate copies"
	);
	assert_eq!(
		intent_state(&slab, r.rsv).await,
		Some(ParentIntentState::Complete)
	);
}

#[tokio::test]
async fn a_recorded_refusal_stops_the_writer_and_never_recreates_an_intent() {
	let r = reserved(&[(1, "a"), (2, "b")], 1);
	let (_dir, slab) = manager().await;
	let locks = Arc::new(ParentLocks::new());
	let planned = plan(&r, r.holder, r.holder.node, ChildRole::Holder).unwrap();
	// Partial: intent recorded, no rows.
	slab.persistent_index()
		.unwrap()
		.begin_parent_intent(planned.intent.clone())
		.await
		.unwrap()
		.unwrap();
	let mut refused = r.state.clone();
	let result = fixture::decide(&mut refused, fixture::abort_command(&r.claim, r.rsv));
	assert!(matches!(
		result,
		DecisionResult::Terminal {
			status: ParentStatus::Refused { .. },
			..
		}
	));
	let machine = machine(&refused).await;
	assert_eq!(
		persist_child(
			slab.clone(),
			machine.clone(),
			locks.clone(),
			planned.clone(),
			authority(&r, ChildRole::Holder)
		)
		.await,
		Err(ChildRefusal::ParentRefused)
	);
	assert_eq!(
		intent_state(&slab, r.rsv).await,
		Some(ParentIntentState::Refused)
	);
	assert_eq!(
		copies(&slab, &planned).await,
		vec![0, 0],
		"nothing was appended"
	);
	// The delayed writer arrives again: refused by state, still nothing appended.
	assert_eq!(
		persist_child(
			slab.clone(),
			machine.clone(),
			locks.clone(),
			planned.clone(),
			authority(&r, ChildRole::Holder)
		)
		.await,
		Err(ChildRefusal::ParentRefused)
	);
	assert_eq!(copies(&slab, &planned).await, vec![0, 0]);

	// A store with no intent at all, after the refusal, creates none.
	let (_dir2, other) = manager().await;
	assert_eq!(
		persist_child(
			other.clone(),
			machine,
			locks,
			planned,
			authority(&r, ChildRole::Holder)
		)
		.await,
		Err(ChildRefusal::ParentRefused)
	);
	assert_eq!(intent_state(&other, r.rsv).await, None);
}

#[tokio::test]
async fn a_writer_waiting_on_the_lock_rechecks_the_decision_inside_it() {
	let r = reserved(&[(1, "a")], 1);
	let (_dir, slab) = manager().await;
	let locks = Arc::new(ParentLocks::new());
	let planned = plan(&r, r.holder, r.holder.node, ChildRole::Holder).unwrap();
	let machine = machine(&r.state).await;
	// Hold the parent's lock, start the writer, then record Refused.
	let held = locks.lock(r.rsv).await;
	let writer = tokio::spawn(persist_child(
		slab.clone(),
		machine.clone(),
		locks.clone(),
		planned.clone(),
		authority(&r, ChildRole::Holder),
	));
	tokio::time::sleep(std::time::Duration::from_millis(50)).await;
	assert!(!writer.is_finished());
	let mut refused = r.state.clone();
	fixture::decide(&mut refused, fixture::abort_command(&r.claim, r.rsv));
	machine.seed_complete_snapshot_state_for_test(refused).await;
	drop(held);
	assert_eq!(writer.await.unwrap(), Err(ChildRefusal::ParentRefused));
	assert_eq!(copies(&slab, &planned).await, vec![0]);
	assert_eq!(intent_state(&slab, r.rsv).await, None);
}

#[tokio::test]
async fn ownership_is_by_version_and_another_parent_cannot_adopt_it() {
	let r = reserved(&[(1, "a")], 1);
	let (_dir, slab) = manager().await;
	let locks = Arc::new(ParentLocks::new());
	let planned = plan(&r, r.holder, r.holder.node, ChildRole::Holder).unwrap();
	// Another reservation claiming the same version first, with the same bytes.
	let mut foreign = planned.intent.clone();
	foreign.rsv = r.rsv + 500;
	slab.persistent_index()
		.unwrap()
		.begin_parent_intent(foreign)
		.await
		.unwrap()
		.unwrap();
	assert_eq!(
		persist_child(
			slab.clone(),
			machine(&r.state).await,
			locks,
			planned.clone(),
			authority(&r, ChildRole::Holder)
		)
		.await,
		Err(ChildRefusal::Intent(IntentRefusal::ForeignOwner))
	);
	assert_eq!(copies(&slab, &planned).await, vec![0]);
}

#[tokio::test]
async fn accepted_without_a_complete_intent_is_not_acknowledged_and_the_sweep_marks_refusals() {
	let r = reserved(&[(1, "a")], 1);
	let (_dir, slab) = manager().await;
	let locks = Arc::new(ParentLocks::new());
	let planned = plan(&r, r.holder, r.holder.node, ChildRole::Holder).unwrap();
	let mut accepted = r.state.clone();
	let acks = fixture::quorum_acks(&r.claim, r.rsv);
	assert!(matches!(
		fixture::finalize(&mut accepted, &r.claim, acks),
		DecisionResult::Terminal {
			status: ParentStatus::Accepted { .. },
			..
		}
	));
	assert_eq!(
		persist_child(
			slab.clone(),
			machine(&accepted).await,
			locks.clone(),
			planned.clone(),
			authority(&r, ChildRole::Holder)
		)
		.await,
		Err(ChildRefusal::ParentAccepted)
	);

	// The sweep: an intent whose parent is later recorded Refused is marked; one
	// whose decision is still Reserved is left alone.
	let (_dir2, other) = manager().await;
	let reserved_machine = machine(&r.state).await;
	persist_child(
		other.clone(),
		reserved_machine.clone(),
		locks.clone(),
		planned.clone(),
		authority(&r, ChildRole::Holder),
	)
	.await
	.unwrap();
	assert_eq!(
		refuse_recorded_intents(&other, &reserved_machine, &locks)
			.await
			.unwrap(),
		0
	);
	let mut refused = r.state.clone();
	fixture::decide(&mut refused, fixture::abort_command(&r.claim, r.rsv));
	assert_eq!(
		refuse_recorded_intents(&other, &machine(&refused).await, &locks)
			.await
			.unwrap(),
		1
	);
	assert_eq!(
		intent_state(&other, r.rsv).await,
		Some(ParentIntentState::Refused)
	);
}

#[tokio::test]
async fn the_ack_is_signed_by_the_store_and_verifies_against_the_registry() {
	let r = reserved(&[(1, "a")], 1);
	let (_dir, slab) = manager().await;
	let planned = plan(&r, r.holder, r.holder.node, ChildRole::Holder).unwrap();
	let signed = persist_child(
		slab,
		machine(&r.state).await,
		Arc::new(ParentLocks::new()),
		planned.clone(),
		authority(&r, ChildRole::Holder),
	)
	.await
	.unwrap();
	let decisions = r.state.decisions.as_ref().unwrap();
	assert!(crate::consensus::decisions::evidence::verify(decisions, &signed).is_ok());
	assert_eq!(signed.body, planned.ack);
	assert_eq!(signed.body.reservation, r.rsv);
	assert_eq!(signed.body.claim_digest, r.meta.claim_digest);
	assert_eq!(signed.body.content, r.claim.children[0].content);
}

#[test]
fn a_child_frame_is_bounded_before_it_is_decoded() {
	let r = reserved(&[(1, "a")], 1);
	let meta = postcard::to_stdvec(&r.meta).unwrap();
	let rows = crate::storage::multirow::encode_batch(&r.batch).unwrap();
	let (decoded, batch) = decode_child_frame(&meta, Some(&rows)).unwrap();
	assert_eq!(decoded, r.meta);
	assert_eq!(batch.num_rows(), 1);
	assert!(decode_child_frame(&vec![0u8; MAX_CHILD_META_BYTES + 1], Some(&rows)).is_err());
	assert!(decode_child_frame(&meta, None).is_err());
	assert!(decode_child_frame(&meta, Some(&[0xFF; 16])).is_err());
}

// ============================================================
//  CODEX-5: verification of stored rows, and signing under the lock
// ============================================================

#[tokio::test]
async fn a_complete_intent_missing_a_version_is_never_acknowledged() {
	let r = reserved(&[(1, "a"), (2, "b")], 1);
	let (_dir, slab) = manager().await;
	let planned = plan(&r, r.holder, r.holder.node, ChildRole::Holder).unwrap();
	let index = slab.persistent_index().unwrap();
	index
		.begin_parent_intent(planned.intent.clone())
		.await
		.unwrap()
		.unwrap();
	index
		.set_parent_intent_state(
			r.rsv,
			0,
			&[ParentIntentState::Intent],
			ParentIntentState::Complete,
		)
		.await
		.unwrap()
		.unwrap();
	let result = persist_child(
		slab.clone(),
		machine(&r.state).await,
		Arc::new(ParentLocks::new()),
		planned,
		authority(&r, ChildRole::Holder),
	)
	.await;
	assert!(
		matches!(result, Err(ChildRefusal::Unverified(_))),
		"{result:?}"
	);
}

#[tokio::test]
async fn a_stored_version_with_other_bytes_never_becomes_complete() {
	let r = reserved(&[(1, "a")], 1);
	let (_dir, slab) = manager().await;
	let planned = plan(&r, r.holder, r.holder.node, ChildRole::Holder).unwrap();
	slab.persistent_index()
		.unwrap()
		.begin_parent_intent(planned.intent.clone())
		.await
		.unwrap()
		.unwrap();
	// The same key at the same HLC, other content, stored before the retry.
	let other = batch(&r.descriptor, &[(1, "OTHER")]);
	let bifurcated = bifurcate_child(&slab, &planned, &other).unwrap();
	append_table_children_durable(
		&slab,
		TENANT,
		planned.ack_hlc(),
		&to_replica_children(&bifurcated),
		&planned.secondary_indexes,
	)
	.await
	.unwrap();
	let result = persist_child(
		slab.clone(),
		machine(&r.state).await,
		Arc::new(ParentLocks::new()),
		planned,
		authority(&r, ChildRole::Holder),
	)
	.await;
	assert!(
		matches!(result, Err(ChildRefusal::Unverified(_))),
		"{result:?}"
	);
	assert_eq!(
		intent_state(&slab, r.rsv).await,
		Some(ParentIntentState::Intent)
	);
}

#[tokio::test]
async fn an_inconsistent_owner_entry_refuses_the_acknowledgement() {
	let r = reserved(&[(1, "a")], 1);
	let (_dir, slab) = manager().await;
	let locks = Arc::new(ParentLocks::new());
	let planned = plan(&r, r.holder, r.holder.node, ChildRole::Holder).unwrap();
	let machine = machine(&r.state).await;
	persist_child(
		slab.clone(),
		machine.clone(),
		locks.clone(),
		planned.clone(),
		authority(&r, ChildRole::Holder),
	)
	.await
	.unwrap();
	slab.persistent_index()
		.unwrap()
		.set_version_owner_for_test(TENANT, planned.intent.manifest[0], (r.rsv + 7, 0))
		.await
		.unwrap();
	let retry = persist_child(
		slab,
		machine,
		locks,
		planned,
		authority(&r, ChildRole::Holder),
	)
	.await;
	assert!(
		matches!(retry, Err(ChildRefusal::Unverified(_))),
		"{retry:?}"
	);
}

#[tokio::test]
async fn a_partial_append_survives_a_real_reopen_and_finishes_only_what_is_missing() {
	let r = reserved(&[(1, "a"), (2, "b"), (3, "c")], 1);
	let (dir, slab, crypto) = manager_with_crypto().await;
	let planned = plan(&r, r.holder, r.holder.node, ChildRole::Holder).unwrap();
	slab.persistent_index()
		.unwrap()
		.begin_parent_intent(planned.intent.clone())
		.await
		.unwrap()
		.unwrap();
	let first = arrow::compute::take_record_batch(
		&planned.batch,
		&arrow::array::UInt32Array::from(vec![0u32]),
	)
	.unwrap();
	let bifurcated = bifurcate_child(&slab, &planned, &first).unwrap();
	append_table_children_durable(
		&slab,
		TENANT,
		planned.ack_hlc(),
		&to_replica_children(&bifurcated),
		&planned.secondary_indexes,
	)
	.await
	.unwrap();
	let slab = reopen(&dir, slab, crypto).await;
	assert_eq!(
		intent_state(&slab, r.rsv).await,
		Some(ParentIntentState::Intent)
	);
	persist_child(
		slab.clone(),
		machine(&r.state).await,
		Arc::new(ParentLocks::new()),
		planned.clone(),
		authority(&r, ChildRole::Holder),
	)
	.await
	.unwrap();
	for version in &planned.intent.manifest {
		let copies = slab
			.stored_rows_at_version(
				TENANT,
				Identity::new(version.key_identity),
				version.hlc,
				None,
			)
			.await
			.unwrap();
		assert_eq!(copies.len(), 1, "exactly one copy of each version");
	}
	assert_eq!(
		intent_state(&slab, r.rsv).await,
		Some(ParentIntentState::Complete)
	);
}

#[tokio::test]
async fn a_refusal_seen_before_signing_produces_no_acknowledgement() {
	let r = reserved(&[(1, "a")], 1);
	let (_dir, slab) = manager().await;
	let locks = Arc::new(ParentLocks::new());
	let planned = plan(&r, r.holder, r.holder.node, ChildRole::Holder).unwrap();
	let machine = machine(&r.state).await;
	let reached = Arc::new(tokio::sync::Notify::new());
	let release = Arc::new(tokio::sync::Notify::new());
	let mut paused = authority(&r, ChildRole::Holder);
	paused.before_sign = Some((reached.clone(), release.clone()));
	let writer = tokio::spawn(persist_child(
		slab.clone(),
		machine.clone(),
		locks.clone(),
		planned.clone(),
		paused,
	));
	reached.notified().await;
	// Persisted and verified; Refused is recorded and cleanup is attempted.
	let mut refused = r.state.clone();
	fixture::decide(&mut refused, fixture::abort_command(&r.claim, r.rsv));
	machine.seed_complete_snapshot_state_for_test(refused).await;
	let sweep = {
		let (slab, machine, locks) = (slab.clone(), machine.clone(), locks.clone());
		tokio::spawn(async move { refuse_recorded_intents(&slab, &machine, &locks).await })
	};
	tokio::time::sleep(std::time::Duration::from_millis(50)).await;
	assert!(!sweep.is_finished(), "cleanup waits for the parent's lock");
	release.notify_one();
	assert_eq!(writer.await.unwrap(), Err(ChildRefusal::ParentRefused));
	sweep.await.unwrap().unwrap();
	assert_eq!(
		intent_state(&slab, r.rsv).await,
		Some(ParentIntentState::Refused)
	);
}

#[tokio::test]
async fn a_holder_lease_that_expires_while_waiting_for_the_lock_stops_the_write() {
	let r = reserved(&[(1, "a")], 1);
	let (_dir, slab) = manager().await;
	let locks = Arc::new(ParentLocks::new());
	let planned = plan(&r, r.holder, r.holder.node, ChildRole::Holder).unwrap();
	let live = Arc::new(std::sync::atomic::AtomicBool::new(true));
	let held = locks.lock(r.rsv).await;
	let writer = tokio::spawn(persist_child(
		slab.clone(),
		machine(&r.state).await,
		locks.clone(),
		planned.clone(),
		authority_with_leases(&r, ChildRole::Holder, live.clone()),
	));
	tokio::time::sleep(std::time::Duration::from_millis(50)).await;
	live.store(false, std::sync::atomic::Ordering::SeqCst);
	drop(held);
	assert_eq!(writer.await.unwrap(), Err(ChildRefusal::LeaseMissing));
	assert_eq!(intent_state(&slab, r.rsv).await, None);
	assert_eq!(copies(&slab, &planned).await, vec![0]);
}

/// ADR 0700: the mode opens exactly the surfaces it serves, in production; no
/// test-only opening exists.
#[test]
fn the_mode_opens_exactly_the_surfaces_it_serves() {
	let peer = DecisionPeer::default();
	for msg_type in [
		MsgType::TABLE_WRITE_REQ,
		MsgType::TABLE_CHILD_WRITE_REQ,
		MsgType::TABLE_CHILD_REPLICATE_REQ,
		MsgType::TABLE_CHILD_FETCH_REQ,
		MsgType::TABLE_QUERY_REQ,
		MsgType::WRITE_REQ,
		MsgType::READ_REQ,
		MsgType::TABLE_DELETE_REQ,
	] {
		assert!(peer.opens(msg_type.0), "served: {msg_type:?}");
	}
	for msg_type in [
		MsgType::QUERY_REQ,
		MsgType::AGG_REQ,
		MsgType::BULK_WRITE_REQ,
		MsgType::STREAM_FIN_REQ,
		MsgType::COUNTER_ADD_REQ,
		MsgType::CDC_SUBSCRIBE_REQ,
	] {
		assert!(!peer.opens(msg_type.0), "refused: {msg_type:?}");
	}
}

/// DC-207 (acceptance 3): a child RPC's one deadline covers the whole attempt.
/// On a simulated network, against peers outside any cluster:
/// - a peer that opens the stream but never reads it holds the sender in its
///   transfer (the rows exceed the stream's buffer);
/// - a peer that reads the whole request but never answers holds it waiting
///   for the reply;
/// - a dial held in its connect stage holds it before any stream exists.
///
/// Each attempt ends `TimedOut`, no earlier than its kind's budget measured
/// from before the call, and within a margin. Afterwards the same cached
/// connection (no new dial) serves an answered request, and a cancelled dial
/// was never cached: the next send dials again and is answered.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_child_rpc_deadline_covers_the_dial_the_transfer_and_the_reply() {
	use crate::network::core::{HrftFrame, read_frame};
	use crate::network::server::{HrftService, RequestContext};
	use crate::network::transport::{
		CapabilityResolver, MembershipEpoch, NodeCapabilitySet, NodeRoleSet, PeerCapabilities,
		TransportProvider, TransportStream, TransportTier, TransportTierSet,
	};
	use crate::simulator::network::{
		InjectedSimulatedTransportFault, SimulatedTransportEventKind,
		SimulatedTransportFaultAction, SimulatedTransportFaultOp, SimulatedTransportNetwork,
		SimulatedTransportProvider,
	};
	use std::sync::atomic::{AtomicUsize, Ordering};
	/// Scheduling margin over a budget.
	const MARGIN: std::time::Duration = std::time::Duration::from_secs(5);
	const LOCAL: u32 = 1;
	const SILENT_UNREAD: u32 = 2;
	const SILENT_READ: u32 = 3;
	const HELD_DIAL: u32 = 4;
	fn node(id: u32) -> NodeId {
		NodeId { id, _reserved: 0 }
	}
	fn address(id: u32) -> String {
		format!("loopback://child-rpc-peer-{id}")
	}
	struct Resolver;
	#[async_trait::async_trait]
	impl CapabilityResolver for Resolver {
		async fn resolve(&self, node: NodeId) -> std::io::Result<PeerCapabilities> {
			Ok(PeerCapabilities {
				address: address(node.id),
				tiers: TransportTierSet::empty().with(TransportTier::QuicIoUring),
				roles: NodeRoleSet::empty(),
				capabilities: NodeCapabilitySet::empty(),
			})
		}
	}
	/// Opens every stream; reads the request only if `reads`; never answers.
	struct Silent {
		reads: bool,
		opened: AtomicUsize,
		read: AtomicUsize,
	}
	#[async_trait::async_trait]
	impl HrftService for Silent {
		async fn handle_frame(
			&self,
			_context: RequestContext,
			_frame: HrftFrame,
		) -> std::io::Result<Option<HrftFrame>> {
			std::future::pending().await
		}

		async fn handle_stream(
			&self,
			mut stream: Box<dyn TransportStream>,
			pool: Arc<BufPool>,
			_context: RequestContext,
		) -> std::io::Result<()> {
			self.opened.fetch_add(1, Ordering::SeqCst);
			if self.reads {
				read_frame(&mut *stream, &pool).await?;
				self.read.fetch_add(1, Ordering::SeqCst);
			}
			std::future::pending().await
		}
	}
	/// Answers every child frame with a refusal of its own kind.
	struct Answering(Arc<BufPool>);
	#[async_trait::async_trait]
	impl HrftService for Answering {
		async fn handle_frame(
			&self,
			_context: RequestContext,
			frame: HrftFrame,
		) -> std::io::Result<Option<HrftFrame>> {
			let msg_type = if frame.header.msg_type == MsgType::TABLE_CHILD_WRITE_REQ.0 {
				MsgType::TABLE_CHILD_WRITE_RESP
			} else {
				MsgType::TABLE_CHILD_REPLICATE_RESP
			};
			let bytes =
				postcard::to_stdvec(&ChildWriteResponse::Refused("answered".into())).unwrap();
			let mut meta = self.0.acquire();
			meta.as_mut_vec().extend_from_slice(&bytes);
			Ok(Some(HrftFrame {
				header: FrameHeader {
					magic: HRFT_MAGIC,
					version: 1,
					msg_type: msg_type.0,
					flags: 0,
					stream_id: frame.header.stream_id,
					meta_len: meta.len() as u32,
					arrow_len: 0,
					trailing_len: 0,
				},
				meta,
				arrow_payload: None,
				trailing: None,
			}))
		}
	}
	/// One send, timed from before the call, under an outer bound so a
	/// missing deadline fails here.
	async fn timed(
		send: impl std::future::Future<Output = std::io::Result<ChildWriteResponse>>,
		rpc: ChildRpc,
	) -> (
		Option<std::io::Result<ChildWriteResponse>>,
		std::time::Duration,
	) {
		let started = tokio::time::Instant::now();
		let outcome = tokio::time::timeout(rpc.budget() + MARGIN, send).await.ok();
		(outcome, started.elapsed())
	}
	fn timed_out(
		what: &str,
		rpc: ChildRpc,
		(outcome, elapsed): &(
			Option<std::io::Result<ChildWriteResponse>>,
			std::time::Duration,
		),
	) {
		assert!(
			matches!(outcome, Some(Err(error)) if error.kind() == ErrorKind::TimedOut),
			"{what}: after {elapsed:?} (budget {:?}, margin {MARGIN:?}): {outcome:?}",
			rpc.budget()
		);
		assert!(
			*elapsed >= rpc.budget(),
			"{what}: ended at {elapsed:?}, before its budget"
		);
	}
	fn connects(network: &SimulatedTransportNetwork, to: u32) -> usize {
		network
			.events()
			.iter()
			.filter(|event| {
				event.source == Some(node(LOCAL))
					&& event.target == Some(node(to))
					&& event.kind == SimulatedTransportEventKind::Connect
			})
			.count()
	}

	let network = Arc::new(SimulatedTransportNetwork::default());
	let pool = BufPool::new(64, 1 << 20);
	let provider: Arc<dyn TransportProvider> = Arc::new(SimulatedTransportProvider::new(
		node(LOCAL),
		network.clone(),
		pool.clone(),
		TransportTier::QuicIoUring,
	));
	let (_epoch, epoch_rx) = tokio::sync::watch::channel(Arc::new(MembershipEpoch::empty()));
	let factory = TransportProviderFactory::new(
		TransportTierSet::empty().with(TransportTier::QuicIoUring),
		[(TransportTier::QuicIoUring, provider)]
			.into_iter()
			.collect(),
		Arc::new(Resolver),
		epoch_rx,
	);
	let meta = postcard::to_stdvec(&ChildWriteMeta {
		rsv: 1,
		ordinal: 0,
		claim_digest: [0; 32],
		schema_generation: 1,
	})
	.unwrap();
	let send = |to: u32, rpc: ChildRpc, rows: usize| {
		send_child_frame(
			factory.clone(),
			pool.clone(),
			node(to),
			rpc,
			meta.clone(),
			Arc::new(vec![0; rows]),
		)
	};

	// The transfer: 1 MiB of rows to a peer that never reads its stream.
	let unread = Arc::new(Silent {
		reads: false,
		opened: AtomicUsize::new(0),
		read: AtomicUsize::new(0),
	});
	network.publish(node(SILENT_UNREAD), &address(SILENT_UNREAD), unread.clone());
	let transfer = timed(
		send(SILENT_UNREAD, ChildRpc::Replicate, 1 << 20),
		ChildRpc::Replicate,
	)
	.await;

	// The reply: a peer that reads the whole request and never answers.
	let silent = Arc::new(Silent {
		reads: true,
		opened: AtomicUsize::new(0),
		read: AtomicUsize::new(0),
	});
	network.publish(node(SILENT_READ), &address(SILENT_READ), silent.clone());
	let reply = timed(
		send(SILENT_READ, ChildRpc::Replicate, 1024),
		ChildRpc::Replicate,
	)
	.await;
	// The same peer answering again, on the cached connection.
	network.publish(
		node(SILENT_READ),
		&address(SILENT_READ),
		Arc::new(Answering(pool.clone())),
	);
	let answered = timed(
		send(SILENT_READ, ChildRpc::Replicate, 1024),
		ChildRpc::Replicate,
	)
	.await;

	// The dial: the first connect to this peer is held.
	network.publish(
		node(HELD_DIAL),
		&address(HELD_DIAL),
		Arc::new(Answering(pool.clone())),
	);
	network.inject(
		InjectedSimulatedTransportFault::on(
			SimulatedTransportFaultOp::Connect,
			SimulatedTransportFaultAction::Delay(std::time::Duration::from_secs(3600)),
		)
		.from(node(LOCAL))
		.to(node(HELD_DIAL)),
	);
	let dial = timed(send(HELD_DIAL, ChildRpc::Write, 1024), ChildRpc::Write).await;
	let dials_held = connects(&network, HELD_DIAL);
	let redialed = timed(send(HELD_DIAL, ChildRpc::Write, 1024), ChildRpc::Write).await;

	timed_out("a peer that never reads", ChildRpc::Replicate, &transfer);
	assert_eq!(
		unread.opened.load(Ordering::SeqCst),
		1,
		"the stream was opened"
	);
	timed_out("a peer that never answers", ChildRpc::Replicate, &reply);
	assert_eq!(
		silent.read.load(Ordering::SeqCst),
		1,
		"the request was read whole"
	);
	assert!(
		matches!(&answered.0, Some(Ok(ChildWriteResponse::Refused(why))) if why == "answered"),
		"the same peer answering again: {answered:?}"
	);
	assert_eq!(
		connects(&network, SILENT_READ),
		1,
		"the timed-out attempt left its connection cached and in use"
	);
	timed_out("a held dial", ChildRpc::Write, &dial);
	assert_eq!(dials_held, 1, "the held dial was attempted");
	assert!(
		matches!(&redialed.0, Some(Ok(ChildWriteResponse::Refused(why))) if why == "answered"),
		"the next send to the held dial's peer: {redialed:?}"
	);
	assert_eq!(
		connects(&network, HELD_DIAL),
		2,
		"a cancelled dial is never cached: the next send dialed again"
	);
}
