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
		lease_evidence: None,
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
		admitted(&slab, planned.clone()).await,
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
		admitted(&slab, planned.clone()).await,
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
	let bifurcated = bifurcate_child(&slab, &planned, &first, &[0]).unwrap();
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
		admitted(&slab, planned.clone()).await,
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
			admitted(&slab, planned.clone()).await,
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
			admitted(&slab, planned.clone()).await,
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
			admitted(&slab, planned).await,
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
		admitted(&slab, planned.clone()).await,
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
			admitted(&slab, planned.clone()).await,
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
			admitted(&slab, planned.clone()).await,
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
		admitted(&slab, planned.clone()).await,
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
		slab.clone(),
		machine(&r.state).await,
		Arc::new(ParentLocks::new()),
		admitted(&slab, planned.clone()).await,
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
		admitted(&slab, planned).await,
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
	let bifurcated = bifurcate_child(&slab, &planned, &other, &[0]).unwrap();
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
		admitted(&slab, planned).await,
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
		admitted(&slab, planned.clone()).await,
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
		slab.clone(),
		machine,
		locks,
		admitted(&slab, planned).await,
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
	let bifurcated = bifurcate_child(&slab, &planned, &first, &[0]).unwrap();
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
		admitted(&slab, planned.clone()).await,
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
		admitted(&slab, planned.clone()).await,
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
		admitted(&slab, planned.clone()).await,
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
		MsgType::MUTATION_OUTCOME_REQ,
		MsgType::MUTATION_OUTCOME_PIN_REQ,
		MsgType::TABLE_QUERY_CANCEL_REQ,
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
					version: crate::consensus::decision_mode::RAFT_FRAME_VERSION_PARENT_DECISIONS,
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

/// ADR 0108 D5: a plan admitted as production admits one, under the store's
/// index admission guard.
async fn admitted(slab: &SlabManager, plan: ChildPlan) -> AdmittedPlan {
	AdmittedPlan::for_test(slab, plan).await
}

/// ADR 0108 D5 (DC-250): an index on the fixture table's `v` column.
fn id_index(r: &Reserved) -> crate::schema::SecondaryIndexDescriptor {
	let table = r
		.state
		.table_schemas
		.latest_by_id(TENANT, TableObjectId::new([4; 16]))
		.unwrap();
	crate::schema::SecondaryIndexDescriptor::for_table_column(
		crate::schema::IndexObjectId::new([0x62; 16]),
		"events_v_idx",
		table,
		"v",
	)
	.unwrap()
}

/// ADR 0108 D5 (DC-250): the store's index answer for `v = value`: the rows
/// found, or the lookup's refusal.
async fn index_lookup(
	slab: &SlabManager,
	index: &crate::schema::SecondaryIndexDescriptor,
	value: &str,
) -> Result<usize, String> {
	let encoded_key = crate::storage::secondary_index::encode_secondary_index_key_from_array(
		&StringArray::from(vec![value]),
		0,
		index.key.arrow_type_tag,
	)
	.unwrap()
	.unwrap();
	let request = crate::storage::TableLookupByIndexRequest::from_descriptor(
		TENANT,
		index,
		crate::storage::IndexQuery::PointEqual { encoded_key },
		HybridTimestamp {
			physical_ms: fixture::T0 + 3_600_000,
			logical: 0,
			node_id: 1,
		},
	);
	slab.table_lookup_by_index(&request, index, crate::schema::SecondaryIndexState::Ready)
		.await
		.map(|report| report.rows.len())
		.map_err(|error| error.to_string())
}

/// ADR 0108 D5 (DC-250), window 1: a child planned while no index exists, and
/// the index's declaration cut asked for before the child is appended. The
/// answer for its row is that row or a refusal, never a covered slab that omits
/// it. (On the code before D5 the cut passed and the slab was covered by an
/// empty segment: dc250-red2.)
#[tokio::test]
async fn a_child_planned_before_an_index_is_not_lost_to_its_cut() {
	let r = reserved(&[(1, "a")], 1);
	let (_dir, slab) = manager().await;
	let machine = machine(&r.state).await;
	let locks = Arc::new(ParentLocks::new());
	let index = id_index(&r);
	let table = r.descriptor.clone();
	// The plan is admitted while no index exists: it carries no descriptor.
	let planned = AdmittedPlan::admit(&slab, &machine, |state| {
		plan_child(
			state,
			r.holder,
			r.holder.node,
			ChildRole::Holder,
			&r.meta,
			r.batch.clone(),
			|_, _| true,
		)
	})
	.await
	.unwrap();
	assert!(planned.plan().secondary_indexes.is_empty());
	let mut indexed = r.state.clone();
	indexed
		.secondary_indexes
		.create(TENANT, index.clone(), &indexed.table_schemas.clone())
		.unwrap();
	machine.seed_complete_snapshot_state_for_test(indexed).await;
	let cut_slab = slab.clone();
	let cut_index = index.clone();
	let cut = tokio::spawn(async move {
		cut_slab
			.establish_secondary_index_declaration_cut(
				TENANT,
				&cut_index,
				std::slice::from_ref(&cut_index),
				4096,
			)
			.await
	});
	tokio::time::sleep(std::time::Duration::from_millis(200)).await;
	assert!(
		!cut.is_finished(),
		"the cut passed a plan whose descriptors were read before it"
	);
	persist_child(
		slab.clone(),
		machine.clone(),
		locks.clone(),
		planned,
		authority(&r, ChildRole::Holder),
	)
	.await
	.unwrap();
	cut.await.unwrap().unwrap();
	// The slab rotates and the buffer the cut proved seals; backfill heals
	// every slab without a segment, as the lifecycle would.
	slab.force_seal_active_for_test().await;
	slab.seal_secondary_index_active_buffer_if_source_changed(
		TENANT,
		&index,
		slab.active_slab_id().await,
	)
	.await
	.unwrap();
	slab.run_secondary_index_backfill_once(TENANT, &table, &index, 8, 4096)
		.await
		.unwrap();
	let answer = index_lookup(&slab, &index, "a").await;
	eprintln!(
		"dc250 window 1: segments {:?}, answer {answer:?}",
		slab.list_secondary_index_segments(
			TENANT,
			index.table_object_id,
			index.index_object_id,
			index.logical_schema_fingerprint
		)
		.iter()
		.map(|info| (info.source_slab_id.get(), info.entry_count))
		.collect::<Vec<_>>()
	);
	assert!(
		matches!(answer, Ok(1) | Err(_)),
		"the index answered a covered slab without the stored row: {answer:?}"
	);
}

/// ADR 0108 D5 (DC-250), window 2: a child whose plan names the index, between
/// its durable append and its index buffer append when the index's declaration
/// cut comes. The slab it wrote to also holds an earlier row with no index
/// entry. Neither row may be lost behind a segment that counts the slab covered.
/// (On the code before D5 the cut passed, and a one-entry segment covered the
/// slab without the earlier row: dc250-red3.)
#[tokio::test]
async fn a_cut_between_a_childs_append_and_its_index_entry_loses_no_row() {
	let r = reserved(&[(1, "a")], 1);
	let (_dir, slab) = manager().await;
	let index = id_index(&r);
	let table = r.descriptor.clone();
	let mut indexed = r.state.clone();
	indexed
		.secondary_indexes
		.create(TENANT, index.clone(), &indexed.table_schemas.clone())
		.unwrap();
	let machine = machine(&indexed).await;
	let locks = Arc::new(ParentLocks::new());
	// An earlier row of the table in the same slab, stored with no index entry.
	ingest_recovered_rows(
		&slab,
		TENANT,
		&table,
		&batch(&table, &[(9, "z")]),
		HybridTimestamp {
			physical_ms: fixture::T0 - 1,
			logical: 0,
			node_id: 1,
		},
		&[],
	)
	.await
	.unwrap();
	let planned = AdmittedPlan::admit(&slab, &machine, |state| {
		plan_child(
			state,
			r.holder,
			r.holder.node,
			ChildRole::Holder,
			&r.meta,
			r.batch.clone(),
			|_, _| true,
		)
	})
	.await
	.unwrap();
	assert_eq!(planned.plan().secondary_indexes, vec![index.clone()]);
	let reached = Arc::new(tokio::sync::Notify::new());
	let release = Arc::new(tokio::sync::Notify::new());
	crate::network::data_server::PAUSE_BEFORE_INDEX_BUFFER
		.lock()
		.unwrap()
		.insert(
			slab.data_dir().to_path_buf(),
			(reached.clone(), release.clone()),
		);
	let writer = tokio::spawn(persist_child(
		slab.clone(),
		machine.clone(),
		locks.clone(),
		planned,
		authority(&r, ChildRole::Holder),
	));
	reached.notified().await;
	let cut_slab = slab.clone();
	let cut_index = index.clone();
	let cut = tokio::spawn(async move {
		cut_slab
			.establish_secondary_index_declaration_cut(
				TENANT,
				&cut_index,
				std::slice::from_ref(&cut_index),
				4096,
			)
			.await
	});
	tokio::time::sleep(std::time::Duration::from_millis(200)).await;
	let cut_passed_the_append = cut.is_finished();
	assert!(
		!cut_passed_the_append,
		"the cut passed a child between its append and its index entry"
	);
	release.notify_one();
	writer.await.unwrap().unwrap();
	cut.await.unwrap().unwrap();
	slab.force_seal_active_for_test().await;
	slab.seal_secondary_index_active_buffer_if_source_changed(
		TENANT,
		&index,
		slab.active_slab_id().await,
	)
	.await
	.unwrap();
	slab.run_secondary_index_backfill_once(TENANT, &table, &index, 8, 4096)
		.await
		.unwrap();
	let earlier = index_lookup(&slab, &index, "z").await;
	let child = index_lookup(&slab, &index, "a").await;
	eprintln!(
		"dc250 window 2: cut passed the paused append {cut_passed_the_append}, segments {:?}, z {earlier:?}, a {child:?}",
		slab.list_secondary_index_segments(
			TENANT,
			index.table_object_id,
			index.index_object_id,
			index.logical_schema_fingerprint
		)
		.iter()
		.map(|info| (info.source_slab_id.get(), info.entry_count))
		.collect::<Vec<_>>()
	);
	for (row, answer) in [("z", &earlier), ("a", &child)] {
		assert!(
			matches!(answer, Ok(1) | Err(_)),
			"the index answered a covered slab without row {row}: {answer:?}"
		);
	}
}

/// ADR 0108 D5 (DC-250): the admission guard lives in the spawned persistence
/// work. A caller cancelled after that work started does not let a cut in
/// before the work's index entries are written.
#[tokio::test]
async fn a_cancelled_child_write_keeps_the_cut_out_until_its_work_ends() {
	let r = reserved(&[(1, "a")], 1);
	let (_dir, slab) = manager().await;
	let index = id_index(&r);
	let table = r.descriptor.clone();
	let mut indexed = r.state.clone();
	indexed
		.secondary_indexes
		.create(TENANT, index.clone(), &indexed.table_schemas.clone())
		.unwrap();
	let machine = machine(&indexed).await;
	let locks = Arc::new(ParentLocks::new());
	ingest_recovered_rows(
		&slab,
		TENANT,
		&table,
		&batch(&table, &[(9, "z")]),
		HybridTimestamp {
			physical_ms: fixture::T0 - 1,
			logical: 0,
			node_id: 1,
		},
		&[],
	)
	.await
	.unwrap();
	let planned = AdmittedPlan::admit(&slab, &machine, |state| {
		plan_child(
			state,
			r.holder,
			r.holder.node,
			ChildRole::Holder,
			&r.meta,
			r.batch.clone(),
			|_, _| true,
		)
	})
	.await
	.unwrap();
	let reached = Arc::new(tokio::sync::Notify::new());
	let release = Arc::new(tokio::sync::Notify::new());
	crate::network::data_server::PAUSE_BEFORE_INDEX_BUFFER
		.lock()
		.unwrap()
		.insert(
			slab.data_dir().to_path_buf(),
			(reached.clone(), release.clone()),
		);
	let caller = tokio::spawn(persist_child(
		slab.clone(),
		machine.clone(),
		locks.clone(),
		planned,
		authority(&r, ChildRole::Holder),
	));
	reached.notified().await;
	caller.abort();
	assert!(caller.await.unwrap_err().is_cancelled());
	let cut_slab = slab.clone();
	let cut_index = index.clone();
	let cut = tokio::spawn(async move {
		cut_slab
			.establish_secondary_index_declaration_cut(
				TENANT,
				&cut_index,
				std::slice::from_ref(&cut_index),
				4096,
			)
			.await
	});
	tokio::time::sleep(std::time::Duration::from_millis(200)).await;
	assert!(
		!cut.is_finished(),
		"a cut passed spawned work whose caller was cancelled"
	);
	release.notify_one();
	tokio::time::timeout(std::time::Duration::from_secs(10), cut)
		.await
		.expect("the cut follows the spawned work")
		.unwrap()
		.unwrap();
	slab.force_seal_active_for_test().await;
	slab.seal_secondary_index_active_buffer_if_source_changed(
		TENANT,
		&index,
		slab.active_slab_id().await,
	)
	.await
	.unwrap();
	slab.run_secondary_index_backfill_once(TENANT, &table, &index, 8, 4096)
		.await
		.unwrap();
	for row in ["z", "a"] {
		assert_eq!(
			index_lookup(&slab, &index, row).await,
			Ok(1),
			"row {row} after the cancelled write and the cut"
		);
	}
}

/// Wendelon ADR 0119: `reserved` for a named AppendOnly table, its claim built
/// from the manifest a holder computes from the rows' ordinals `0..n`.
fn reserved_append_only(rows: &[(i64, &str)], sequence_no: u64) -> Reserved {
	let mut state = fixture::base(3);
	fixture::grant_all(&mut state);
	let descriptor = descriptor(RowIdentityPolicy::AppendOnly, 1);
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
	let ordinals: Vec<u32> = (0..rows.len() as u32).collect();
	let (in_order, manifest) = child_versions_keyed(
		&request,
		&batch,
		&descriptor,
		commit_hlc,
		RowKeys::Ordinals(&ordinals),
	)
	.unwrap();
	let position = crate::consensus::decisions::delete_row_position(
		TENANT,
		&descriptor,
		crate::schema::Identity::new(in_order[0].key_identity),
	)
	.unwrap();
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

fn plan_keyed(r: &Reserved, given: GivenKeys) -> Result<ChildPlan, ChildRefusal> {
	plan_child_keyed(
		&r.state,
		r.holder,
		r.holder.node,
		ChildRole::Holder,
		&r.meta,
		r.batch.clone(),
		given,
		|_, _| true,
	)
}

fn section(bytes: Vec<u8>) -> GivenKeys {
	GivenKeys::Section(Arc::new(bytes))
}

fn decodes() -> usize {
	KEY_SECTION_DECODES.with(|count| count.get())
}

/// Wendelon ADR 0119 D4/D5, cross-child ordinal misuse at the receiver: only
/// the write's own ordinals plan. A missing key section, a repeated ordinal, an
/// ordinal at or past N, a forged association (ordinals swapped over distinct
/// rows) and too few ordinals are refused before anything is planned. A section
/// of the wrong kind at a valid length (identities on a write; any section for a
/// PrimaryKey child) is refused before any key vector is built (Codex 01a1f9ca).
#[test]
fn an_append_only_child_plans_only_with_its_own_ordinals() {
	let r = reserved_append_only(&[(1, "a"), (2, "b"), (3, "c")], 1);
	let before = decodes();
	plan_keyed(&r, section(encode_ordinals(&[0, 1, 2]))).expect("its own ordinals");
	assert_eq!(decodes(), before + 1, "the probe counts a decoded section");
	let refused = |given: GivenKeys, what: &str| {
		assert!(
			matches!(
				plan_keyed(&r, given),
				Err(ChildRefusal::Rows(_) | ChildRefusal::ContentMismatch)
			),
			"{what}"
		);
	};
	refused(GivenKeys::None, "no ordinals");
	refused(section(encode_ordinals(&[0, 0, 2])), "a repeated ordinal");
	refused(section(encode_ordinals(&[0, 1, 3])), "an ordinal at N");
	refused(
		section(encode_ordinals(&[1, 0, 2])),
		"ordinals swapped over distinct rows",
	);
	refused(section(encode_ordinals(&[0, 1])), "too few ordinals");

	// The wrong kind of section, at a valid length for it: no vector is built.
	let before = decodes();
	let identities = vec![crate::schema::Identity::new([7; 32]); 3];
	refused(
		section(encode_identities(&identities)),
		"identities on a write",
	);
	let pk = reserved(&[(1, "a")], 2);
	assert!(
		matches!(
			plan_child_keyed(
				&pk.state,
				pk.holder,
				pk.holder.node,
				ChildRole::Holder,
				&pk.meta,
				pk.batch.clone(),
				section(encode_ordinals(&[0])),
				|_, _| true,
			),
			Err(ChildRefusal::Rows(_))
		),
		"a valid-length ordinal section for a PrimaryKey child"
	);
	assert_eq!(decodes(), before, "refused before any key vector was built");

	assert!(
		decode_ordinals(&[0, 0, 0, 1, 0, 0], 2).is_err(),
		"a short section"
	);
	assert!(decode_ordinals(&[0; 12], 2).is_err(), "a long section");
	assert_eq!(
		decode_ordinals(&encode_ordinals(&[9, 3]), 2).unwrap(),
		vec![9, 3]
	);
}

/// Wendelon ADR 0119 D5 (Codex 01a1f9ca): the repair answer's bound holds for
/// every flat representation. The write side is `take` of the child's rows from
/// the request (as `child_sections_fit` encodes it); the answer is the server's
/// construction, a concatenation of individually stored rows, here each a
/// one-row slice of a larger stored batch, some stored with a validity buffer
/// and no null. Row counts straddle the bitmap and alignment boundaries; values
/// vary in length and nulls are scattered; the decimals include Decimal32 and
/// Decimal64 (Codex 01a1f9f6). Value and offset buffers are equal
/// byte lengths, and the encoded answer never exceeds `repair_rows_bound`.
#[test]
fn a_repair_answer_is_bounded_by_its_write_for_every_flat_column() {
	use arrow::array::{
		ArrayRef, BooleanArray, Decimal128Array, FixedSizeBinaryArray, Int64Array,
		LargeStringArray, StringArray, UInt32Array,
	};
	use arrow::datatypes::{DataType, Field, Schema};
	let mut seed = 0x2545_f491_4f6c_dd1du64;
	let mut next = move || {
		seed ^= seed << 13;
		seed ^= seed >> 7;
		seed ^= seed << 17;
		seed
	};
	let schema = Arc::new(Schema::new(vec![
		Field::new("i", DataType::Int64, true),
		Field::new("s", DataType::Utf8, true),
		Field::new("l", DataType::LargeUtf8, true),
		Field::new("b", DataType::Boolean, true),
		Field::new("f", DataType::FixedSizeBinary(8), true),
		Field::new("d", DataType::Decimal128(20, 3), true),
		Field::new("d32", DataType::Decimal32(9, 2), true),
		Field::new("d64", DataType::Decimal64(18, 4), true),
	]));
	for fields in schema.fields() {
		assert!(repair_bounded_column(fields.data_type()));
	}
	assert!(!repair_bounded_column(&DataType::Dictionary(
		Box::new(DataType::Int32),
		Box::new(DataType::Utf8)
	)));
	assert!(!repair_bounded_column(&DataType::Utf8View));
	let column = |n: usize, nulls: bool, next: &mut dyn FnMut() -> u64| -> Vec<ArrayRef> {
		let null = |next: &mut dyn FnMut() -> u64| nulls && next() % 5 == 0;
		let text = |next: &mut dyn FnMut() -> u64| {
			let len = (next() % 40) as usize;
			"x".repeat(len)
		};
		let mut i = Vec::new();
		let mut s = Vec::new();
		let mut l = Vec::new();
		let mut b = Vec::new();
		let mut f = Vec::new();
		let mut d = Vec::new();
		let mut d32 = Vec::new();
		let mut d64 = Vec::new();
		for _ in 0..n {
			i.push((!null(next)).then(|| next() as i64));
			s.push((!null(next)).then(|| text(next)));
			l.push((!null(next)).then(|| text(next)));
			b.push((!null(next)).then(|| next() % 2 == 0));
			f.push((!null(next)).then(|| next().to_be_bytes()));
			d.push((!null(next)).then(|| (next() % 1_000_000) as i128));
			d32.push((!null(next)).then(|| (next() % 1_000_000) as i32));
			d64.push((!null(next)).then(|| (next() % 1_000_000_000) as i64));
		}
		vec![
			Arc::new(Int64Array::from(i)),
			Arc::new(StringArray::from(s)),
			Arc::new(LargeStringArray::from(l)),
			Arc::new(BooleanArray::from(b)),
			Arc::new(
				FixedSizeBinaryArray::try_from_sparse_iter_with_size(f.into_iter(), 8).unwrap(),
			),
			Arc::new(
				Decimal128Array::from(d)
					.with_precision_and_scale(20, 3)
					.unwrap(),
			),
			Arc::new(
				arrow::array::Decimal32Array::from(d32)
					.with_precision_and_scale(9, 2)
					.unwrap(),
			),
			Arc::new(
				arrow::array::Decimal64Array::from(d64)
					.with_precision_and_scale(18, 4)
					.unwrap(),
			),
		]
	};
	for n in [1usize, 7, 8, 9, 63, 64, 65, 200, 513] {
		for nulls in [false, true] {
			let request =
				RecordBatch::try_new(schema.clone(), column(n + 5, nulls, &mut next)).unwrap();
			// The child's rows, in the request's order, minus a few.
			let mut picked: Vec<u32> = (0..(n + 5) as u32).collect();
			for _ in 0..5 {
				let at = (next() as usize) % picked.len();
				picked.remove(at);
			}
			let write =
				arrow::compute::take_record_batch(&request, &UInt32Array::from(picked.clone()))
					.unwrap();
			// The server's construction: each stored row a one-row slice of a larger
			// stored batch (half of them with a validity buffer and no null).
			let stored: Vec<RecordBatch> = picked
				.iter()
				.map(|row| {
					let around = if next() % 2 == 0 {
						let mut with_nulls = column(3, true, &mut next);
						for (at, array) in with_nulls.iter_mut().enumerate() {
							*array = arrow::compute::concat(&[
								array.slice(0, 1).as_ref(),
								request.column(at).slice(*row as usize, 1).as_ref(),
								array.slice(2, 1).as_ref(),
							])
							.unwrap();
						}
						RecordBatch::try_new(schema.clone(), with_nulls).unwrap()
					} else {
						request.slice(*row as usize, 1)
					};
					let at = if around.num_rows() == 3 { 1 } else { 0 };
					around.slice(at, 1)
				})
				.collect();
			let answer = arrow::compute::concat_batches(&schema, &stored).unwrap();
			assert_eq!(answer.num_rows(), write.num_rows());
			// The bytes each column uses (a slice may keep a wider buffer it does not
			// use; the IPC writer sends only the used range): equal, column by column.
			for (at, (got, want)) in answer.columns().iter().zip(write.columns()).enumerate() {
				let used = |array: &ArrayRef| {
					let compact =
						arrow::compute::concat(&[array.as_ref(), array.slice(0, 0).as_ref()])
							.unwrap();
					let data = compact.to_data();
					data.buffers()
						.iter()
						.map(|buffer| buffer.len())
						.collect::<Vec<_>>()
				};
				assert_eq!(
					used(got),
					used(want),
					"column {at} ({}), n {n}, nulls {nulls}",
					schema.field(at).data_type()
				);
			}
			let write_bytes = crate::storage::multirow::encode_batch(&write)
				.unwrap()
				.len();
			let answer_bytes = crate::storage::multirow::encode_batch(&answer)
				.unwrap()
				.len();
			let bound = repair_rows_bound(write_bytes, write.num_columns(), n).unwrap();
			assert!(
				answer_bytes <= bound,
				"n {n}, nulls {nulls}: answer {answer_bytes} > bound {bound} (write {write_bytes})"
			);
		}
	}
}

/// Wendelon ADR 0119 D5 (Codex 01a1f9ca): a named AppendOnly table with a
/// column that is not flat (here a dictionary) is refused at admission, before
/// any identity; a flat one is admitted.
#[test]
fn an_append_only_table_with_a_dictionary_column_is_refused_before_its_identities() {
	use arrow::datatypes::{DataType, Field, Schema};
	let dictionary = TableSchemaDescriptor::new_with_layout(
		TableObjectId::new([5; 16]),
		1,
		"analytics",
		"events_dict",
		&Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new(
				"v",
				DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
				true,
			),
		]),
		RowIdentityPolicy::AppendOnly,
		crate::schema::LocalityPolicy::TableSpreadProjection(TableSpreadLocalityDescriptor {
			spread_factor: 1,
			prefix_bits: 16,
		}),
		RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::new(4, 8, 1024 * 1024).unwrap()),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.unwrap();
	assert!(!parent_decisions_append_only_schema(&dictionary));
	assert!(parent_decisions_append_only_schema(&descriptor(
		RowIdentityPolicy::AppendOnly,
		1
	)));
	let batch = RecordBatch::try_new(
		Arc::new(dictionary.arrow_schema().unwrap()),
		vec![
			Arc::new(arrow::array::Int64Array::from(vec![1])),
			Arc::new(
				vec!["a"]
					.into_iter()
					.collect::<arrow::array::DictionaryArray<arrow::datatypes::Int32Type>>(),
			),
		],
	)
	.unwrap();
	let key = ParentKey {
		tenant: TENANT,
		namespace: ParentNamespace::Table(dictionary.table_object_id),
		client_uuid: fixture::uuid_v7(fixture::T0, 1),
		sequence_no: 1,
	};
	let hlc = HybridTimestamp {
		physical_ms: fixture::T0,
		logical: 0,
		node_id: 1,
	};
	let request = child_request_meta(&key, dictionary.table_object_id, 1, 1, hlc).unwrap();
	assert!(matches!(
		keyed_admission(&request, &batch, &dictionary, hlc, RowKeys::Ordinals(&[0])),
		Err(ManifestRefusal::UnsupportedTable)
	));
}

/// A named AppendOnly table with an `id` column and `field`; `None` when the
/// descriptor itself is refused.
fn append_only_with(field: arrow::datatypes::Field) -> Option<TableSchemaDescriptor> {
	use arrow::datatypes::{DataType, Field, Schema};
	TableSchemaDescriptor::new_with_layout(
		TableObjectId::new([6; 16]),
		1,
		"analytics",
		"events_types",
		&Schema::new(vec![Field::new("id", DataType::Int64, false), field]),
		RowIdentityPolicy::AppendOnly,
		crate::schema::LocalityPolicy::TableSpreadProjection(TableSpreadLocalityDescriptor {
			spread_factor: 1,
			prefix_bits: 16,
		}),
		RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::new(4, 8, 1024 * 1024).unwrap()),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.ok()
}

/// `keyed_admission` of one row, its `v` cell `column`, as a ParentDecisions
/// AppendOnly write's ordinal 0.
fn admit_one(
	descriptor: &TableSchemaDescriptor,
	column: arrow::array::ArrayRef,
) -> Result<crate::schema::TableWriteAdmission, ManifestRefusal> {
	let batch = RecordBatch::try_new(
		Arc::new(descriptor.arrow_schema().unwrap()),
		vec![Arc::new(arrow::array::Int64Array::from(vec![1])), column],
	)
	.unwrap();
	let key = ParentKey {
		tenant: TENANT,
		namespace: ParentNamespace::Table(descriptor.table_object_id),
		client_uuid: fixture::uuid_v7(fixture::T0, 1),
		sequence_no: 1,
	};
	let hlc = HybridTimestamp {
		physical_ms: fixture::T0,
		logical: 0,
		node_id: 1,
	};
	let request = child_request_meta(&key, descriptor.table_object_id, 1, 1, hlc).unwrap();
	keyed_admission(&request, &batch, descriptor, hlc, RowKeys::Ordinals(&[0]))
}

/// Wendelon ADR 0119 D5 (Codex 01a1fa16): a named PD AppendOnly table admits
/// exactly the ADR's declared column types, refused at admission before any
/// identity. A nullable `FixedSizeBinary` column whose payload is all null (the
/// canonical row encoding skips a null cell before its type check) is refused
/// `UnsupportedTable`, as is every other excluded type with an all-null cell;
/// each supported type, nullable and null, is admitted.
#[test]
fn a_parent_decisions_append_only_table_admits_only_its_declared_column_types() {
	use arrow::datatypes::{DataType, Field, IntervalUnit, TimeUnit, UnionFields, UnionMode};
	let refused = |data_type: DataType| {
		let field = Field::new("v", data_type.clone(), true);
		match append_only_with(field) {
			// The descriptor itself refuses this type: nothing reaches admission.
			None => eprintln!("{data_type}: the descriptor is refused"),
			Some(descriptor) => {
				let cell = arrow::array::new_null_array(&data_type, 1);
				let admitted = admit_one(&descriptor, cell);
				assert!(
					matches!(admitted, Err(ManifestRefusal::UnsupportedTable)),
					"{data_type}: an all-null cell is refused before any identity: {admitted:?}"
				);
			}
		}
	};
	// The finding: a nullable FixedSizeBinary, its payload all null.
	refused(DataType::FixedSizeBinary(8));
	refused(DataType::Null);
	refused(DataType::Float16);
	refused(DataType::Dictionary(
		Box::new(DataType::Int32),
		Box::new(DataType::Utf8),
	));
	refused(DataType::Utf8View);
	refused(DataType::BinaryView);
	refused(DataType::List(Arc::new(Field::new(
		"item",
		DataType::Int64,
		true,
	))));
	refused(DataType::LargeList(Arc::new(Field::new(
		"item",
		DataType::Int64,
		true,
	))));
	refused(DataType::Struct(
		vec![Field::new("a", DataType::Int64, true)].into(),
	));
	refused(DataType::Union(
		UnionFields::try_new(vec![0], vec![Field::new("a", DataType::Int64, true)]).unwrap(),
		UnionMode::Sparse,
	));

	// Every declared type, nullable and null, is admitted.
	for data_type in [
		DataType::Boolean,
		DataType::Int8,
		DataType::Int16,
		DataType::Int32,
		DataType::Int64,
		DataType::UInt8,
		DataType::UInt16,
		DataType::UInt32,
		DataType::UInt64,
		DataType::Float32,
		DataType::Float64,
		DataType::Decimal32(9, 2),
		DataType::Decimal64(18, 4),
		DataType::Decimal128(20, 3),
		DataType::Decimal256(40, 3),
		DataType::Date32,
		DataType::Date64,
		DataType::Time32(TimeUnit::Second),
		DataType::Time32(TimeUnit::Millisecond),
		DataType::Time64(TimeUnit::Microsecond),
		DataType::Time64(TimeUnit::Nanosecond),
		DataType::Timestamp(TimeUnit::Microsecond, None),
		DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
		DataType::Duration(TimeUnit::Millisecond),
		DataType::Interval(IntervalUnit::YearMonth),
		DataType::Interval(IntervalUnit::DayTime),
		DataType::Interval(IntervalUnit::MonthDayNano),
		DataType::Utf8,
		DataType::LargeUtf8,
		DataType::Binary,
		DataType::LargeBinary,
	] {
		let descriptor = append_only_with(Field::new("v", data_type.clone(), true))
			.unwrap_or_else(|| panic!("{data_type}: a supported descriptor"));
		let admitted = admit_one(&descriptor, arrow::array::new_null_array(&data_type, 1));
		assert!(admitted.is_ok(), "{data_type}: admitted: {admitted:?}");
	}
}

// ============================================================
//  DC-287 (Codex 01a20985): a holder's own reads under the store-wide retirement fence
// ============================================================

/// A local holder mechanism (`reserved_append_only` plans three members), not an RF1 cluster run.
/// One append-only child of two rows (one physical child frame), retried with the same parent lock
/// registry, child identity and store. The fence is the real store-wide retirement fence, raised
/// for a slab other than the rows' (the store's active slab), and only once a hook is witnessed.
/// Every refusal produces no acknowledgement; a final acknowledgement is witnessed to need no
/// append (the store's L1 bytes are unchanged, and every append is refused while it runs).
mod holder_retirement_fence {
	use super::*;
	use crate::schema::SlabId;

	const FENCE: u64 = 0x7700;

	struct Holder {
		_dir: crate::test_support::TestDir,
		r: Reserved,
		slab: Arc<SlabManager>,
		machine: WendelonStateMachine,
		locks: Arc<ParentLocks>,
	}

	async fn holder(sequence_no: u64) -> Arc<Holder> {
		let r = reserved_append_only(&[(1, "a"), (2, "b")], sequence_no);
		let (dir, slab) = manager().await;
		let machine = machine(&r.state).await;
		Arc::new(Holder {
			_dir: dir,
			r,
			slab,
			machine,
			locks: Arc::new(ParentLocks::new()),
		})
	}

	/// One attempt of the same child: its plan admitted afresh, persisted under the same locks.
	async fn attempt(
		h: Arc<Holder>,
	) -> Result<
		crate::consensus::decisions::Signed<crate::consensus::decisions::ChildAckBody>,
		ChildRefusal,
	> {
		let planned = AdmittedPlan::admit(&h.slab, &h.machine, |state| {
			plan_child_keyed(
				state,
				h.r.holder,
				h.r.holder.node,
				ChildRole::Holder,
				&h.r.meta,
				h.r.batch.clone(),
				section(encode_ordinals(&[0, 1])),
				|_, _| true,
			)
		})
		.await
		.unwrap();
		persist_child(
			h.slab.clone(),
			h.machine.clone(),
			h.locks.clone(),
			planned,
			authority(&h.r, ChildRole::Holder),
		)
		.await
	}

	/// Arms the one-shot hold after this store's next durable child frame (before verification).
	fn hold_after_append(
		slab: &SlabManager,
	) -> (Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>) {
		let (reached, release) = (
			Arc::new(tokio::sync::Notify::new()),
			Arc::new(tokio::sync::Notify::new()),
		);
		crate::network::data_server::PAUSE_BEFORE_INDEX_BUFFER
			.lock()
			.unwrap()
			.insert(
				slab.data_dir().to_path_buf(),
				(reached.clone(), release.clone()),
			);
		(reached, release)
	}

	/// The refusal's variant and its detail text.
	fn detail(
		result: &Result<
			crate::consensus::decisions::Signed<crate::consensus::decisions::ChildAckBody>,
			ChildRefusal,
		>,
	) -> (&'static str, String) {
		match result {
			Ok(_) => ("acknowledged", String::new()),
			Err(ChildRefusal::Unverified(text)) => ("Unverified", text.clone()),
			Err(ChildRefusal::Storage(text)) => ("Storage", text.clone()),
			Err(other) => ("other", format!("{other:?}")),
		}
	}

	async fn bounded<T>(what: &str, f: impl std::future::Future<Output = T>) -> T {
		tokio::time::timeout(std::time::Duration::from_secs(30), f)
			.await
			.unwrap_or_else(|_| panic!("{what}: timed out"))
	}

	/// A: the stable comparator (the same hold, no fence) acknowledges after one append; and the
	/// append witness is shown to catch a child append (a fresh child is refused while it is armed).
	#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
	async fn a_stable_comparator_acknowledges_and_the_append_witness_catches_a_child_append() {
		let h = holder(1).await;
		let before = h.slab.local_l1_bytes().await;
		let (reached, release) = hold_after_append(&h.slab);
		let write = tokio::spawn(attempt(h.clone()));
		bounded("the hold after the append", reached.notified()).await;
		let appended = h.slab.local_l1_bytes().await;
		release.notify_one();
		let result = bounded("the comparator", write).await.unwrap();
		eprintln!(
			"DC287 holder control A: {:?}; L1 {before} -> {appended}; intent {:?}",
			detail(&result),
			intent_state(&h.slab, h.r.rsv).await
		);
		assert!(result.is_ok(), "{:?}", detail(&result));
		assert!(appended > before, "one child frame was appended");
		assert_eq!(h.slab.local_l1_bytes().await, appended, "and nothing more");

		let fresh = holder(2).await;
		let before = fresh.slab.local_l1_bytes().await;
		fresh.slab.refuse_append_after_for_test(0);
		let refused = bounded("the witness control", attempt(fresh.clone())).await;
		fresh.slab.clear_append_refusal_for_test();
		eprintln!("DC287 holder control A witness: {:?}", detail(&refused));
		assert_eq!(
			detail(&refused).0,
			"Storage",
			"the armed witness refuses a child append"
		);
		assert_eq!(fresh.slab.local_l1_bytes().await, before);
	}

	/// B: the fence raised after the durable child frame refuses verification (Unverified); the same
	/// child retried with the fence up meets it at the pre-append scan (Storage, the same text), with
	/// no append; once cleared, the same child acknowledges from its stored rows with no new append.
	#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
	async fn a_fence_after_the_append_refuses_verification_then_the_scan_and_clears_without_a_new_append()
	 {
		let h = holder(1).await;
		let fence = SlabId::new(FENCE);
		assert_ne!(
			h.slab.active_slab_id().await,
			fence,
			"a slab other than the rows'"
		);
		let before = h.slab.local_l1_bytes().await;
		let (reached, release) = hold_after_append(&h.slab);
		let write = tokio::spawn(attempt(h.clone()));
		// The witness: the hook fired after the one durable child frame.
		bounded("the hold after the append", reached.notified()).await;
		let appended = h.slab.local_l1_bytes().await;
		assert!(appended > before, "the child frame is durable");
		h.slab.raise_retirement_fence_for_test(fence);
		release.notify_one();
		let first = bounded("attempt 1", write).await.unwrap();
		let first_intent = intent_state(&h.slab, h.r.rsv).await;
		eprintln!(
			"DC287 holder control B attempt 1: {:?}; intent {first_intent:?}",
			detail(&first)
		);

		let second = bounded("attempt 2", attempt(h.clone())).await;
		let second_intent = intent_state(&h.slab, h.r.rsv).await;
		let after_second = h.slab.local_l1_bytes().await;
		eprintln!(
			"DC287 holder control B attempt 2: {:?}; intent {second_intent:?}; L1 {appended} -> {after_second}",
			detail(&second)
		);

		h.slab.clear_retirement_fence_for_test(fence);
		h.slab.refuse_append_after_for_test(0);
		let third = bounded("attempt 3", attempt(h.clone())).await;
		h.slab.clear_append_refusal_for_test();
		let third_intent = intent_state(&h.slab, h.r.rsv).await;
		eprintln!(
			"DC287 holder control B attempt 3 (fence cleared): {:?}; intent {third_intent:?}; L1 {}",
			detail(&third),
			h.slab.local_l1_bytes().await
		);

		let (kind, text) = detail(&first);
		assert_eq!(kind, "Unverified", "attempt 1: {text}");
		assert!(text.contains("fenced"), "attempt 1: {text}");
		assert_eq!(first_intent, Some(ParentIntentState::Intent));
		let (kind2, text2) = detail(&second);
		assert_eq!(kind2, "Storage", "attempt 2: {text2}");
		assert_eq!(text2, text, "the same fenced read error at the scan");
		assert_eq!(after_second, appended, "attempt 2 appended nothing");
		assert!(third.is_ok(), "attempt 3: {:?}", detail(&third));
		assert_eq!(
			h.slab.local_l1_bytes().await,
			appended,
			"attempt 3 appended nothing"
		);
		assert_eq!(third_intent, Some(ParentIntentState::Complete));
	}

	/// C: the fence raised after the begin and before the pre-append scan refuses fresh rows at the
	/// scan (Storage), before any append.
	#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
	async fn a_fence_before_the_scan_refuses_fresh_rows_at_the_scan_before_any_append() {
		let h = holder(1).await;
		let fence = SlabId::new(FENCE);
		let before = h.slab.local_l1_bytes().await;
		let probe = persist_pause::arm_after_begin(&h.slab, h.r.descriptor.table_object_id);
		let write = tokio::spawn(attempt(h.clone()));
		bounded("the hold after the begin", probe.reached.notified()).await;
		h.slab.raise_retirement_fence_for_test(fence);
		probe.release.notify_one();
		let result = bounded("the attempt", write).await.unwrap();
		h.slab.clear_retirement_fence_for_test(fence);
		eprintln!(
			"DC287 holder control C: {:?}; intent {:?}",
			detail(&result),
			intent_state(&h.slab, h.r.rsv).await
		);
		let (kind, text) = detail(&result);
		assert_eq!(kind, "Storage", "{text}");
		assert!(text.contains("fenced"), "{text}");
		assert_eq!(
			h.slab.local_l1_bytes().await,
			before,
			"nothing was appended"
		);
	}

	/// D: with the fence clear, a version made absent after its durable frame and before
	/// verification reads as not stored (an empty answer), distinct from a fenced read.
	#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
	async fn an_absent_version_at_verification_reads_as_not_stored_not_fenced() {
		let h = holder(1).await;
		let versions = plan_keyed(&h.r, section(encode_ordinals(&[0, 1])))
			.unwrap()
			.row_versions;
		let gone = versions[0];
		let (reached, release) = hold_after_append(&h.slab);
		let write = tokio::spawn(attempt(h.clone()));
		bounded("the hold after the append", reached.notified()).await;
		h.slab.forget_version_for_test(
			TENANT,
			crate::schema::Identity::new(gone.key_identity),
			gone.hlc,
		);
		release.notify_one();
		let result = bounded("the attempt", write).await.unwrap();
		eprintln!(
			"DC287 holder control D: {:?}; intent {:?}",
			detail(&result),
			intent_state(&h.slab, h.r.rsv).await
		);
		let (kind, text) = detail(&result);
		assert_eq!(kind, "Unverified", "{text}");
		assert_eq!(text, "a manifest version is not stored");
	}

	// ============================================================
	//  Wendelon ADR 0126: the holder's own reads wait out a logged retirement fence
	// ============================================================

	use crate::storage::manager::tier_authority::{
		HolderAdmission, RETIRING_LOG_INDEXED, RETIRING_SNAPSHOT, RETIRING_UNINDEXED,
		RetirementWait,
	};

	/// The same child as [`attempt`], with the coordinated attempt's wait.
	async fn attempt_waiting(
		h: Arc<Holder>,
		wait: RetirementWait,
	) -> Result<
		crate::consensus::decisions::Signed<crate::consensus::decisions::ChildAckBody>,
		ChildRefusal,
	> {
		let planned = AdmittedPlan::admit(&h.slab, &h.machine, |state| {
			plan_child_keyed(
				state,
				h.r.holder,
				h.r.holder.node,
				ChildRole::Holder,
				&h.r.meta,
				h.r.batch.clone(),
				section(encode_ordinals(&[0, 1])),
				|_, _| true,
			)
		})
		.await
		.unwrap();
		persist_child_waiting(
			h.slab.clone(),
			h.machine.clone(),
			h.locks.clone(),
			planned,
			authority(&h.r, ChildRole::Holder),
			wait,
		)
		.await
	}

	fn wait_for(duration: std::time::Duration) -> RetirementWait {
		RetirementWait::until(tokio::time::Instant::now() + duration)
	}

	/// What the holder admission answers: admitted, or the refusal's text and eligibility.
	fn admission(slab: &SlabManager) -> Result<(), (String, bool)> {
		match slab.holder_location_admission() {
			HolderAdmission::Admitted => Ok(()),
			HolderAdmission::Refused { error, eligible } => Err((error.to_string(), eligible)),
		}
	}

	/// D1: the class follows today's map transitions exactly, with no map scan by its readers.
	#[tokio::test]
	async fn the_retirement_class_follows_the_existing_map_transitions() {
		let (_dir, slab) = manager().await;
		let class = || slab.retirement_class_for_test();
		let (a, b, c, d, e) = (
			SlabId::new(0x7701),
			SlabId::new(0x7702),
			SlabId::new(0x7703),
			SlabId::new(0x7704),
			SlabId::new(0x7705),
		);
		assert_eq!(class(), 0);
		// An indexed mark advances only to a newer index; a stale clear removes nothing.
		slab.mark_retirement_for_test(b, Some(3));
		slab.mark_retirement_for_test(b, Some(2));
		slab.clear_retirement_for_test(b, Some(2));
		assert_eq!(
			class(),
			RETIRING_LOG_INDEXED,
			"an older mark and a stale clear"
		);
		slab.mark_retirement_for_test(b, Some(7));
		slab.clear_retirement_for_test(b, Some(3));
		assert_eq!(class(), RETIRING_LOG_INDEXED, "the entry advanced to 7");
		slab.clear_retirement_for_test(b, Some(7));
		assert_eq!(class(), 0, "the matching clear");
		// A None-only mark is unindexed; an incoming Some leaves an occupied None.
		slab.mark_retirement_for_test(a, None);
		slab.mark_retirement_for_test(a, Some(5));
		slab.clear_retirement_for_test(a, Some(5));
		assert_eq!(
			class(),
			RETIRING_UNINDEXED,
			"None stays None; the clear is stale"
		);
		// Some -> None replaces, and a stale clear of the old index leaves the fence.
		slab.mark_retirement_for_test(c, Some(4));
		assert_eq!(class(), RETIRING_UNINDEXED | RETIRING_LOG_INDEXED);
		slab.mark_retirement_for_test(c, None);
		slab.clear_retirement_for_test(c, Some(4));
		assert_eq!(
			class(),
			RETIRING_UNINDEXED,
			"the replaced entry survives the stale clear"
		);
		// A snapshot retirement adds its class; clearing it removes only it.
		let (_dir2, slab2) = manager().await;
		slab2.mark_retirement_for_test(d, Some(1));
		slab2.mark_snapshot_retirement_for_test(e, 9);
		assert_eq!(
			slab2.retirement_class_for_test(),
			RETIRING_LOG_INDEXED | RETIRING_SNAPSHOT
		);
		slab2.clear_snapshot_retirement_for_test(e, 9);
		assert_eq!(slab2.retirement_class_for_test(), RETIRING_LOG_INDEXED);
		slab2.clear_retirement_for_test(d, Some(1));
		assert_eq!(slab2.retirement_class_for_test(), 0);
	}

	/// D1: a handoff that marks the next retirement before clearing the prior one keeps the fence
	/// standing throughout, and one load never reads it clear.
	#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
	async fn a_mark_before_clear_handoff_never_reads_clear() {
		let (_dir, slab) = manager().await;
		slab.mark_retirement_for_test(SlabId::new(1), Some(1));
		use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
		let done = Arc::new(AtomicBool::new(false));
		let reads = Arc::new(AtomicU64::new(0));
		let reader = {
			let (slab, done, reads) = (slab.clone(), done.clone(), reads.clone());
			std::thread::spawn(move || {
				while !done.load(Ordering::Acquire) {
					assert_ne!(
						slab.retirement_class_for_test(),
						0,
						"read clear mid-handoff"
					);
					reads.fetch_add(1, Ordering::Release);
				}
			})
		};
		// The handoffs run until they have overlapped the reader's loop (a scheduled reader is
		// the premise, not a given under load), bounded in wall time.
		let started = std::time::Instant::now();
		let mut i = 1u64;
		while i < 2_000 || reads.load(Ordering::Acquire) < 2_000 {
			assert!(
				started.elapsed() < std::time::Duration::from_secs(30),
				"premise: the reader overlapped the handoffs ({} reads, {i} handoffs)",
				reads.load(Ordering::Acquire)
			);
			slab.mark_retirement_for_test(SlabId::new(i + 1), Some(i + 1));
			slab.clear_retirement_for_test(SlabId::new(i), Some(i));
			i += 1;
		}
		done.store(true, Ordering::Release);
		reader
			.join()
			.expect("the reader never read the fence clear");
		assert_eq!(slab.retirement_class_for_test(), RETIRING_LOG_INDEXED);
		slab.clear_retirement_for_test(SlabId::new(i), Some(i));
		assert_eq!(slab.retirement_class_for_test(), 0);
	}

	/// D2: the admission is unchanged, and only a sole log-indexed retirement at the fallback is
	/// eligible; every refusal is today's error.
	#[tokio::test]
	async fn only_a_sole_log_indexed_retirement_is_eligible_and_every_error_is_todays() {
		let fenced = "stale location in slab 0: fenced".to_owned();
		let (_dir, slab) = manager().await;
		assert_eq!(admission(&slab), Ok(()));
		slab.mark_retirement_for_test(SlabId::new(FENCE), Some(1));
		assert_eq!(admission(&slab), Err((fenced.clone(), true)));
		assert_eq!(
			slab.require_tier_authority_ready()
				.map_err(|e| e.to_string()),
			Err(fenced.clone()),
			"the same error on the unchanged path"
		);
		slab.mark_snapshot_retirement_for_test(SlabId::new(FENCE + 1), 3);
		assert_eq!(admission(&slab), Err((fenced.clone(), false)), "mixed");
		let (_dir2, unindexed) = manager().await;
		unindexed.mark_retirement_for_test(SlabId::new(FENCE), None);
		assert_eq!(
			admission(&unindexed),
			Err((fenced.clone(), false)),
			"unindexed"
		);
		// Unresolved-op alone admits on both paths; with a log-indexed retirement it keeps its
		// error and priority and is not eligible.
		let (_dir3, op) = manager().await;
		op.fence_unresolved_op_for_test();
		assert_eq!(admission(&op), Ok(()));
		assert!(op.require_tier_authority_ready().is_ok());
		op.mark_retirement_for_test(SlabId::new(FENCE), Some(1));
		assert_eq!(
			admission(&op),
			Err(("UnresolvedOpIntent".to_owned(), false))
		);
		assert_eq!(
			op.require_tier_authority_ready().map_err(|e| e.to_string()),
			Err("UnresolvedOpIntent".to_owned())
		);
	}

	/// R1: a ready store's holder reads, at the scan and at verification, register no wake.
	#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
	async fn ready_holder_reads_make_no_wake_registration() {
		let h = holder(1).await;
		let wait = wait_for(std::time::Duration::from_secs(10));
		let result = bounded("the write", attempt_waiting(h.clone(), wait)).await;
		assert!(result.is_ok(), "{:?}", detail(&result));
		assert_eq!(
			intent_state(&h.slab, h.r.rsv).await,
			Some(ParentIntentState::Complete),
			"the scan and the verification ran"
		);
		let version = plan_keyed(&h.r, section(encode_ordinals(&[0, 1])))
			.unwrap()
			.row_versions[0];
		let rows = h
			.slab
			.stored_rows_at_version_for_holder(
				TENANT,
				crate::schema::Identity::new(version.key_identity),
				version.hlc,
				wait,
			)
			.await
			.unwrap();
		assert_eq!(rows.len(), 1);
		assert_eq!(h.slab.holder_retirement_registrations_for_test(), 0);
		assert_eq!(h.slab.holder_retirement_waits_for_test(), 0);
	}

	/// R1: a clear that lands between the first (unregistered) eligible refusal and the first
	/// arm, its wake lost, is still seen: the armed recheck admits, and no wait begins.
	#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
	async fn a_clear_between_the_first_refusal_and_the_armed_recheck_still_progresses() {
		let h = holder(1).await;
		assert!(bounded("the write", attempt(h.clone())).await.is_ok());
		let version = plan_keyed(&h.r, section(encode_ordinals(&[0, 1])))
			.unwrap()
			.row_versions[0];
		let fence = SlabId::new(FENCE);
		h.slab.raise_retirement_fence_for_test(fence);
		let (reached, release) = h.slab.hold_holder_first_refusal_for_test();
		let read = {
			let slab = h.slab.clone();
			tokio::spawn(async move {
				slab.stored_rows_at_version_for_holder(
					TENANT,
					crate::schema::Identity::new(version.key_identity),
					version.hlc,
					wait_for(std::time::Duration::from_secs(10)),
				)
				.await
			})
		};
		bounded("the first eligible refusal", reached.notified()).await;
		assert_eq!(h.slab.holder_retirement_registrations_for_test(), 0);
		h.slab.clear_retirement_fence_for_test(fence);
		release.notify_one();
		let rows = bounded("the read", read).await.unwrap().unwrap();
		assert_eq!(rows.len(), 1);
		assert_eq!(
			h.slab.holder_retirement_registrations_for_test(),
			1,
			"one arm"
		);
		assert_eq!(
			h.slab.holder_retirement_waits_for_test(),
			0,
			"no wait began"
		);
	}

	/// D3: an eligible holder read waits (witnessed), and reads once the fence clears.
	#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
	async fn an_eligible_holder_read_waits_then_reads_once_the_fence_clears() {
		let h = holder(1).await;
		assert!(bounded("the write", attempt(h.clone())).await.is_ok());
		let version = plan_keyed(&h.r, section(encode_ordinals(&[0, 1])))
			.unwrap()
			.row_versions[0];
		let fence = SlabId::new(FENCE);
		h.slab.raise_retirement_fence_for_test(fence);
		let read = {
			let slab = h.slab.clone();
			tokio::spawn(async move {
				slab.stored_rows_at_version_for_holder(
					TENANT,
					crate::schema::Identity::new(version.key_identity),
					version.hlc,
					wait_for(std::time::Duration::from_secs(10)),
				)
				.await
			})
		};
		bounded(
			"the eligible wait",
			h.slab.holder_retirement_wait_began_for_test(1),
		)
		.await;
		assert!(!read.is_finished(), "it waits while the fence stands");
		h.slab.clear_retirement_fence_for_test(fence);
		let rows = bounded("the read", read).await.unwrap().unwrap();
		assert_eq!(rows.len(), 1, "the stored copy, read after the clear");
	}

	/// D3: a refusal that is not eligible is answered at once, with no wait, whatever the budget.
	#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
	async fn an_ineligible_refusal_is_answered_at_once() {
		let h = holder(1).await;
		assert!(bounded("the write", attempt(h.clone())).await.is_ok());
		let version = plan_keyed(&h.r, section(encode_ordinals(&[0, 1])))
			.unwrap()
			.row_versions[0];
		h.slab.mark_retirement_for_test(SlabId::new(FENCE), None);
		let started = std::time::Instant::now();
		let read = h
			.slab
			.stored_rows_at_version_for_holder(
				TENANT,
				crate::schema::Identity::new(version.key_identity),
				version.hlc,
				wait_for(std::time::Duration::from_secs(10)),
			)
			.await;
		assert_eq!(
			read.map(|rows| rows.len()).map_err(|e| e.to_string()),
			Err("stale location in slab 0: fenced".to_owned())
		);
		assert!(started.elapsed() < std::time::Duration::from_secs(5));
		assert_eq!(
			h.slab.holder_retirement_waits_for_test(),
			0,
			"no wait began"
		);
	}

	/// D3: a fence that outlives the wait gives today's refusal at the wait's end; a close during
	/// a wait ends it promptly.
	#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
	async fn a_persistent_fence_refuses_at_the_end_and_a_close_ends_the_wait() {
		let h = holder(1).await;
		assert!(bounded("the write", attempt(h.clone())).await.is_ok());
		let version = plan_keyed(&h.r, section(encode_ordinals(&[0, 1])))
			.unwrap()
			.row_versions[0];
		let key = crate::schema::Identity::new(version.key_identity);
		h.slab.raise_retirement_fence_for_test(SlabId::new(FENCE));
		let budget = std::time::Duration::from_millis(300);
		let started = std::time::Instant::now();
		let read = h
			.slab
			.stored_rows_at_version_for_holder(TENANT, key, version.hlc, wait_for(budget))
			.await;
		let elapsed = started.elapsed();
		assert_eq!(
			read.map(|rows| rows.len()).map_err(|e| e.to_string()),
			Err("stale location in slab 0: fenced".to_owned())
		);
		assert!(elapsed >= budget, "it waited the budget: {elapsed:?}");
		assert!(h.slab.holder_retirement_waits_for_test() > 0);
		// A close while waiting.
		let read = {
			let slab = h.slab.clone();
			tokio::spawn(async move {
				slab.stored_rows_at_version_for_holder(
					TENANT,
					key,
					version.hlc,
					wait_for(std::time::Duration::from_secs(10)),
				)
				.await
			})
		};
		let before = h.slab.holder_retirement_waits_for_test();
		bounded(
			"the eligible wait",
			h.slab.holder_retirement_wait_began_for_test(before + 1),
		)
		.await;
		let closed = std::time::Instant::now();
		h.slab.stop_tier_authority_for_test();
		let answer = bounded("the closed read", read).await.unwrap();
		assert!(answer.is_err(), "a closed authority refuses");
		assert!(
			closed.elapsed() < std::time::Duration::from_secs(2),
			"promptly, not at the wait's end"
		);
	}

	/// D3 through the persist path: a self-clearing fence after the durable frame (verification),
	/// with the append witness armed: the FIRST attempt acknowledges, with no re-append.
	#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
	async fn a_self_clearing_fence_at_verification_lets_the_first_attempt_acknowledge() {
		let h = holder(1).await;
		let fence = SlabId::new(FENCE);
		let before = h.slab.local_l1_bytes().await;
		let (reached, release) = hold_after_append(&h.slab);
		let write = tokio::spawn(attempt_waiting(
			h.clone(),
			wait_for(std::time::Duration::from_secs(10)),
		));
		bounded("the hold after the append", reached.notified()).await;
		let appended = h.slab.local_l1_bytes().await;
		assert!(appended > before);
		h.slab.raise_retirement_fence_for_test(fence);
		h.slab.refuse_append_after_for_test(0);
		release.notify_one();
		bounded(
			"the eligible wait",
			h.slab.holder_retirement_wait_began_for_test(1),
		)
		.await;
		h.slab.clear_retirement_fence_for_test(fence);
		let result = bounded("the attempt", write).await.unwrap();
		h.slab.clear_append_refusal_for_test();
		eprintln!(
			"ADR0126 verification: {:?}; intent {:?}",
			detail(&result),
			intent_state(&h.slab, h.r.rsv).await
		);
		assert!(result.is_ok(), "{:?}", detail(&result));
		assert_eq!(
			intent_state(&h.slab, h.r.rsv).await,
			Some(ParentIntentState::Complete)
		);
		assert_eq!(h.slab.local_l1_bytes().await, appended, "no re-append");
	}

	/// D3 through the persist path: a resumed child (its frame durable from a refused attempt)
	/// meets the fence at its scan, waits, and acknowledges with no new append.
	#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
	async fn a_resumed_child_waits_at_its_scan_and_acknowledges_without_a_new_append() {
		let h = holder(1).await;
		let fence = SlabId::new(FENCE);
		let (reached, release) = hold_after_append(&h.slab);
		let first = tokio::spawn(attempt(h.clone()));
		bounded("the hold after the append", reached.notified()).await;
		let appended = h.slab.local_l1_bytes().await;
		h.slab.raise_retirement_fence_for_test(fence);
		release.notify_one();
		let first = bounded("attempt 1 (no wait)", first).await.unwrap();
		assert_eq!(detail(&first).0, "Unverified");
		h.slab.refuse_append_after_for_test(0);
		let resumed = tokio::spawn(attempt_waiting(
			h.clone(),
			wait_for(std::time::Duration::from_secs(10)),
		));
		bounded(
			"the eligible wait",
			h.slab.holder_retirement_wait_began_for_test(1),
		)
		.await;
		h.slab.clear_retirement_fence_for_test(fence);
		let resumed = bounded("the resumed attempt", resumed).await.unwrap();
		h.slab.clear_append_refusal_for_test();
		eprintln!(
			"ADR0126 scan: {:?}; intent {:?}",
			detail(&resumed),
			intent_state(&h.slab, h.r.rsv).await
		);
		assert!(resumed.is_ok(), "{:?}", detail(&resumed));
		assert_eq!(
			intent_state(&h.slab, h.r.rsv).await,
			Some(ParentIntentState::Complete)
		);
		assert_eq!(h.slab.local_l1_bytes().await, appended, "no new append");
	}

	/// D3: with a wait, a missing version still reads as not stored, at once.
	#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
	async fn with_a_wait_a_missing_version_still_reads_as_not_stored() {
		let h = holder(1).await;
		let gone = plan_keyed(&h.r, section(encode_ordinals(&[0, 1])))
			.unwrap()
			.row_versions[0];
		let (reached, release) = hold_after_append(&h.slab);
		let write = tokio::spawn(attempt_waiting(
			h.clone(),
			wait_for(std::time::Duration::from_secs(10)),
		));
		bounded("the hold after the append", reached.notified()).await;
		h.slab.forget_version_for_test(
			TENANT,
			crate::schema::Identity::new(gone.key_identity),
			gone.hlc,
		);
		release.notify_one();
		let result = bounded("the attempt", write).await.unwrap();
		assert_eq!(
			detail(&result),
			("Unverified", "a manifest version is not stored".to_owned())
		);
		assert_eq!(h.slab.holder_retirement_waits_for_test(), 0);
	}
}

#[path = "verifier_semantics/tests.rs"]
mod verifier_semantics;

#[path = "verifier_charge/tests.rs"]
mod verifier_charge;
