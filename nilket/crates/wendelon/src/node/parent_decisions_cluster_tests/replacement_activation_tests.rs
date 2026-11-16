//! ADR 0108 (wendelon): an activated replacement becomes a full serving member.
//! The recovered ControlOnly replacement records its activation from one view,
//! hands over (every part closed, the authority checked on its durably
//! recovered state, the role rewritten, the owner closed), and its next start
//! (the supervised restart its process requests) is Full on the same directory,
//! where it serves, restarts, certifies and may lead.

use super::*;
use crate::node::replacement_preparation::{PANIC_AFTER_ACTIVATION, PAUSE_BEFORE_ACTIVATION};
use crate::node::replacement_recovery::ACTIVATION_ENABLED;

/// The activation applied at the leader for `TOKEN`, if any.
async fn activation_record(
	cluster: &Cluster,
) -> Option<crate::consensus::decisions::instances::Activation> {
	let leader = cluster.leader().await;
	cluster
		.node(leader)
		.state_machine
		.read_state(|state| {
			state
				.decisions
				.as_ref()?
				.instances()
				.replacement(TOKEN)?
				.activated
		})
		.await
}

async fn until_activated(control: &ControlOnlyNode) {
	let mut activated = control.activated();
	tokio::time::timeout(BOUND * 2, activated.wait_for(|activated| *activated))
		.await
		.unwrap_or_else(|_| {
			panic!(
				"never activated; preparation ended {}",
				control.preparation_ended()
			)
		})
		.unwrap();
}

/// The role on the replacement's own vote page.
fn page_role(cfg: &NodeBootstrapConfig) -> Role {
	let (_, vote_path, _) = crate::node::control_artifact_paths_for_test(&cfg.data_dir);
	crate::consensus::raft_storage::peek_identity(&vote_path)
		.unwrap()
		.expect("an identity")
		.role
}

/// Waits until the Full replacement's readiness names its instance and store
/// and reports no degradation.
async fn until_ready_as(cluster: &Cluster, slot: u32, store: crate::consensus::StoreIncarnation) {
	let deadline = tokio::time::Instant::now() + BOUND;
	loop {
		let snapshot = crate::metrics::observability::ReadinessReporter::snapshot(
			&cluster.node(slot).readiness_reporter(),
		)
		.await;
		assert_eq!(snapshot.raft_instance, Some(ALLOCATED_BASE));
		assert_eq!(
			snapshot.store_incarnation,
			Some(format!("{:032x}", store.get()))
		);
		if snapshot.degraded_reason.is_none() && snapshot.leader_id.is_some() {
			return;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"never ready: {:?}",
			snapshot.degraded_reason
		);
		tokio::time::sleep(TICK).await;
	}
}

fn rows(entries: &[(i64, &str)]) -> Expected {
	entries
		.iter()
		.map(|(id, name)| (*id, (*name).to_owned()))
		.collect()
}

/// ADR 0108 controls 1 (live), 3 (H2), 6 (a later row replicates), 10 and the
/// handoff: the recovered replacement activates at a cut no lower than its
/// recovery's, at the current certificate, which ends its preparation; it hands
/// over, its page says Full, and its Full start (unclean, so fully validated)
/// names its instance and store. It serves exact rows, takes a later row,
/// restarts Full, reports so a generation commits, and becomes the leader under
/// which the cluster commits.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn an_activated_replacement_hands_over_and_serves_as_a_full_member() {
	let (
		Replaced {
			mut cluster,
			descriptor,
			lost,
			cfg,
			now,
		},
		control,
	) = replaced_after_gid_one(false).await;
	ACTIVATION_ENABLED
		.lock()
		.unwrap()
		.insert(cfg.data_dir.clone());
	let recorded = until_recorded(&cluster, &control).await;
	until_activated(&control).await;
	let activation = activation_record(&cluster).await.expect("an activation");
	let store = control
		.recovery_storage()
		.await
		.unwrap()
		.owner_ref()
		.unwrap()
		.incarnation();
	assert_eq!(activation.incarnation, store);
	assert!(activation.through >= recorded.k);
	assert_eq!(
		Some(activation.certified),
		current_certificate(&cluster)
			.await
			.map(|certificate| certificate.gid)
	);
	let leader = cluster.leader().await;
	assert!(
		!cluster
			.node(leader)
			.state_machine
			.read_state(move |state| crate::consensus::decisions::slot_preparing(state, lost))
			.await,
		"activation ends the preparation"
	);
	assert_eq!(page_role(&cfg), Role::ControlOnly);
	// The recovery storage is unbound in the product too: the handoff must not
	// need a binding (owner_ref's test-build fallback once hid that).
	assert!(
		!control
			.recovery_storage()
			.await
			.unwrap()
			.bound_to_control_node_for_test()
	);
	control.hand_over().await.unwrap();
	assert_eq!(page_role(&cfg), Role::Full);
	cluster.reopen(lost).await;
	until_ready_as(&cluster, lost, store).await;
	let mut expected = rows(&[(1, "a"), (2, "b"), (3, "c")]);
	assert_eq!(
		cluster.read_once_validated(lost, &descriptor).await,
		expected
	);
	let (id, name) = write_current(&cluster, &descriptor, now + 120_000, 100, "after").await;
	expected.insert(id, name);
	assert_eq!(
		cluster.read_once_validated(lost, &descriptor).await,
		expected
	);
	// A Full restart passes its start authority again.
	cluster.stop(lost, false).await;
	cluster.reopen(lost).await;
	until_ready_as(&cluster, lost, store).await;
	assert_eq!(
		cluster.read_once_validated(lost, &descriptor).await,
		expected
	);
	// Control 10: certification needs the Full replacement's report.
	let before = current_certificate(&cluster).await.map_or(0, |c| c.gid);
	jump_floor(&cluster, now + 200_000).await;
	until_active(&cluster, before + 1).await;
	// Leadership moves to the replacement's instance, and the cluster commits.
	cluster.node(lost).raft.trigger().elect().await.unwrap();
	let deadline = tokio::time::Instant::now() + BOUND;
	while cluster.node(lost).raft.current_leader().await != Some(ALLOCATED_BASE)
		|| cluster.leader().await != lost
	{
		assert!(
			tokio::time::Instant::now() < deadline,
			"the replacement never led"
		);
		tokio::time::sleep(TICK).await;
	}
	let (id, name) = write_current(&cluster, &descriptor, now + 240_000, 200, "led").await;
	expected.insert(id, name);
	assert_eq!(
		cluster.read_once_validated(lost, &descriptor).await,
		expected
	);
}

/// ADR 0108 controls 2, 3 (H1) and 6: a row accepted after the activation's
/// view (while the replacement still answers NotServing) is owed past the cut
/// and is read at the Full replacement. Stopped after its activation applied but
/// before any role rewrite, it restarts ControlOnly, finds its activation and
/// hands over then.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_row_after_the_activation_cut_is_served_and_a_stop_before_the_rewrite_resumes() {
	let (
		Replaced {
			mut cluster,
			descriptor,
			lost,
			cfg,
			now,
		},
		control,
	) = replaced_after_gid_one(false).await;
	ACTIVATION_ENABLED
		.lock()
		.unwrap()
		.insert(cfg.data_dir.clone());
	let reached = Arc::new(tokio::sync::Notify::new());
	let release = Arc::new(tokio::sync::Notify::new());
	PAUSE_BEFORE_ACTIVATION
		.lock()
		.unwrap()
		.insert(cfg.data_dir.clone(), (reached.clone(), release.clone()));
	until_recorded(&cluster, &control).await;
	tokio::time::timeout(BOUND, reached.notified())
		.await
		.expect("the activation was built");
	let (id, name) = write_current(&cluster, &descriptor, now + 120_000, 100, "owed").await;
	let leader = cluster.leader().await;
	let applied_after_row = cluster
		.node(leader)
		.state_machine
		.read_state(|state| state.last_applied_log_id.map_or(0, |id| id.index))
		.await;
	release.notify_one();
	until_activated(&control).await;
	let activation = activation_record(&cluster).await.expect("an activation");
	assert!(
		activation.through < applied_after_row,
		"the view was read before the row (through {}, row applied by {applied_after_row})",
		activation.through
	);
	// H1: stopped before the role rewrite, it restarts ControlOnly.
	control.shutdown().await.unwrap();
	assert_eq!(page_role(&cfg), Role::ControlOnly);
	let control = ControlOnlyNode::bootstrap_with_provider(cfg.clone(), provider(&cluster, lost))
		.await
		.unwrap();
	let store = control
		.recovery_storage()
		.await
		.unwrap()
		.owner_ref()
		.unwrap()
		.incarnation();
	until_activated(&control).await;
	control.hand_over().await.unwrap();
	cluster.reopen(lost).await;
	until_ready_as(&cluster, lost, store).await;
	let mut expected = rows(&[(1, "a"), (2, "b"), (3, "c")]);
	expected.insert(id, name);
	assert_eq!(
		cluster.read_once_validated(lost, &descriptor).await,
		expected
	);
}

/// ADR 0108 control 3 (H3 at the handoff): an activated replacement superseded
/// before its handoff refuses it on its recovered state and keeps its page.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_superseded_replacement_refuses_its_handoff_and_keeps_its_page() {
	let (
		Replaced {
			cluster, lost, cfg, ..
		},
		control,
	) = replaced_after_gid_one(false).await;
	ACTIVATION_ENABLED
		.lock()
		.unwrap()
		.insert(cfg.data_dir.clone());
	until_recorded(&cluster, &control).await;
	until_activated(&control).await;
	assert!(matches!(
		decide(
			&cluster,
			DecisionCommand::BeginReplacement {
				token: TOKEN + 1,
				slot: lost,
			}
		)
		.await,
		DecisionResult::ReplacementRecorded {
			r_old: ALLOCATED_BASE,
			..
		}
	));
	until_caught_up(&cluster, &control, "the later replacement").await;
	let refused = control.hand_over().await.unwrap_err();
	assert!(
		refused.to_string().contains("ReplacementNotCurrent"),
		"{refused}"
	);
	assert_eq!(page_role(&cfg), Role::ControlOnly);
}

fn index_envelope(
	descriptor: &crate::schema::SecondaryIndexDescriptor,
	mutation: crate::integration::compute::SchemaMutationKind,
	payload: Vec<u8>,
	generation: u64,
) -> crate::consensus::WendelonRequest {
	use crate::integration::compute::{
		COMPUTE_STORAGE_CONTRACT_VERSION, SchemaGeneration, SchemaIntentId, SchemaIntentOrigin,
		SchemaObjectKind, SchemaObjectRef, SchemaUpdateEnvelope, encode_schema_update_envelope_v1,
	};
	crate::consensus::WendelonRequest::SchemaUpdate(
		encode_schema_update_envelope_v1(&SchemaUpdateEnvelope {
			contract_version: COMPUTE_STORAGE_CONTRACT_VERSION,
			origin: SchemaIntentOrigin::WendelonOperator,
			tenant_id: TenantId::ZERO,
			object: SchemaObjectRef {
				kind: SchemaObjectKind::Index,
				namespace: descriptor.namespace.clone(),
				name: descriptor.index_name.clone(),
			},
			expected_generation: SchemaGeneration::new(generation),
			requested_generation: SchemaGeneration::new(generation + 1),
			intent_id: SchemaIntentId::new([0x63 + generation as u8; 16]),
			mutation,
			payload,
		})
		.unwrap(),
	)
}

/// The stores (slot and current incarnation) of every member.
async fn current_stores(cluster: &Cluster) -> Vec<crate::consensus::decisions::Signer> {
	let mut stores = Vec::new();
	for node in NODES {
		stores.push(crate::consensus::decisions::Signer {
			node,
			incarnation: cluster.node(node).slab.owner_ref().unwrap().incarnation(),
		});
	}
	stores
}

/// ADR 0108 D4 control 9 (cluster): under ParentDecisions a store's index
/// lifecycle receipts are signed decision commands. Every store backfills under
/// its own cut and submits its backfill receipt; the index is Ready on every
/// current store's, with nothing in the shared slot-keyed set. A drop is
/// collected the same way, and the index is removed with its receipts.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_parent_decisions_index_is_ready_and_dropped_on_every_current_stores_receipt() {
	use crate::consensus::decisions::IndexReceiptPhase;
	use crate::integration::compute::SchemaMutationKind;
	let (cluster, _, descriptor) = ready().await;
	let now = std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.unwrap()
		.as_millis() as u64;
	write_at(&cluster, &descriptor, now, &[(1, "a"), (2, "b")]).await;
	let index = crate::schema::SecondaryIndexDescriptor::for_table_column(
		crate::schema::IndexObjectId::new([0x63; 16]),
		"events_v_idx",
		&descriptor,
		"v",
	)
	.unwrap();
	let id = index.index_object_id;
	let leader = cluster.leader().await;
	cluster
		.node(leader)
		.raft
		.client_write(index_envelope(
			&index,
			SchemaMutationKind::Create,
			crate::schema::encode_secondary_index_descriptor(&index).unwrap(),
			0,
		))
		.await
		.unwrap();
	cluster
		.until("the index is Ready", move |state| {
			state
				.secondary_indexes
				.get(TenantId::ZERO, id)
				.is_some_and(|row| row.state == crate::schema::SecondaryIndexState::Ready)
		})
		.await;
	let stores = current_stores(&cluster).await;
	let leader = cluster.leader().await;
	let (held, shared) = cluster
		.node(leader)
		.state_machine
		.read_state(move |state| {
			let decisions = state.decisions.as_ref().unwrap();
			(
				stores
					.iter()
					.filter(|store| {
						decisions.holds_index_receipt(
							TenantId::ZERO,
							id,
							IndexReceiptPhase::Backfilled,
							**store,
						)
					})
					.count(),
				state
					.secondary_indexes
					.get(TenantId::ZERO, id)
					.unwrap()
					.drop_gc_completed_by
					.len(),
			)
		})
		.await;
	assert_eq!((held, shared), (3, 0));
	cluster
		.node(leader)
		.raft
		.client_write(index_envelope(
			&index,
			SchemaMutationKind::Drop,
			crate::schema::encode_secondary_index_drop_request(
				&crate::schema::SecondaryIndexDropRequest {
					index_object_id: id,
				},
			)
			.unwrap(),
			1,
		))
		.await
		.unwrap();
	cluster
		.until("the dropped index is removed", move |state| {
			state.secondary_indexes.get(TenantId::ZERO, id).is_none()
		})
		.await;
	let stores = current_stores(&cluster).await;
	let leader = cluster.leader().await;
	assert!(
		cluster
			.node(leader)
			.state_machine
			.read_state(move |state| {
				let decisions = state.decisions.as_ref().unwrap();
				stores.iter().all(|store| {
					!decisions.holds_index_receipt(
						TenantId::ZERO,
						id,
						IndexReceiptPhase::Backfilled,
						*store,
					) && !decisions.holds_index_receipt(
						TenantId::ZERO,
						id,
						IndexReceiptPhase::DropCollected,
						*store,
					)
				})
			})
			.await,
		"the removed index keeps no receipt"
	);
}

/// ADR 0108 control 8: the Full replacement rebuilds its index from the rows it
/// recovered. An index declared after its handoff is Ready only on its store's
/// own signed receipt, and a lookup on its store finds a recovered row.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_full_replacement_indexes_its_recovered_rows_under_its_own_receipt() {
	use crate::consensus::decisions::IndexReceiptPhase;
	use crate::integration::compute::SchemaMutationKind;
	let (
		Replaced {
			mut cluster,
			descriptor,
			lost,
			cfg,
			..
		},
		control,
	) = replaced_after_gid_one(false).await;
	ACTIVATION_ENABLED
		.lock()
		.unwrap()
		.insert(cfg.data_dir.clone());
	until_recorded(&cluster, &control).await;
	until_activated(&control).await;
	let store = control
		.recovery_storage()
		.await
		.unwrap()
		.owner_ref()
		.unwrap()
		.incarnation();
	control.hand_over().await.unwrap();
	cluster.reopen(lost).await;
	until_ready_as(&cluster, lost, store).await;
	let index = crate::schema::SecondaryIndexDescriptor::for_table_column(
		crate::schema::IndexObjectId::new([0x64; 16]),
		"events_v_idx",
		&descriptor,
		"v",
	)
	.unwrap();
	let id = index.index_object_id;
	let leader = cluster.leader().await;
	cluster
		.node(leader)
		.raft
		.client_write(index_envelope(
			&index,
			SchemaMutationKind::Create,
			crate::schema::encode_secondary_index_descriptor(&index).unwrap(),
			0,
		))
		.await
		.unwrap();
	cluster
		.until("the index is Ready", move |state| {
			state
				.secondary_indexes
				.get(TenantId::ZERO, id)
				.is_some_and(|row| row.state == crate::schema::SecondaryIndexState::Ready)
		})
		.await;
	let replacement = crate::consensus::decisions::Signer {
		node: lost,
		incarnation: store,
	};
	assert!(
		cluster
			.node(lost)
			.state_machine
			.read_state(
				move |state| state.decisions.as_ref().unwrap().holds_index_receipt(
					TenantId::ZERO,
					id,
					IndexReceiptPhase::Backfilled,
					replacement,
				)
			)
			.await
	);
	let encoded_key = crate::storage::secondary_index::encode_secondary_index_key_from_array(
		&arrow::array::StringArray::from(vec!["a"]),
		0,
		index.key.arrow_type_tag,
	)
	.unwrap()
	.unwrap();
	let request = crate::storage::TableLookupByIndexRequest::from_descriptor(
		TenantId::ZERO,
		&index,
		crate::storage::IndexQuery::PointEqual { encoded_key },
		crate::schema::HybridTimestamp {
			physical_ms: u64::MAX >> 1,
			logical: 0,
			node_id: lost,
		},
	);
	let deadline = tokio::time::Instant::now() + BOUND;
	loop {
		match cluster
			.node(lost)
			.slab
			.table_lookup_by_index(&request, &index, crate::schema::SecondaryIndexState::Ready)
			.await
		{
			Ok(report) => {
				assert_eq!(report.rows.len(), 1, "the recovered row");
				break;
			}
			Err(refused) => assert!(
				tokio::time::Instant::now() < deadline,
				"the replacement's index never covered its rows: {refused}"
			),
		}
		tokio::time::sleep(TICK).await;
	}
}

/// ADR 0108 handoff control (Codex 01a1e41e R4): an unexpected failure of the
/// preparation task is no clean stop. The handoff returns it, and neither the
/// role rewrite nor a Full start follows.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_failed_preparation_task_ends_the_handoff_without_a_rewrite() {
	let (Replaced { cluster, cfg, .. }, control) = replaced_after_gid_one(false).await;
	ACTIVATION_ENABLED
		.lock()
		.unwrap()
		.insert(cfg.data_dir.clone());
	PANIC_AFTER_ACTIVATION
		.lock()
		.unwrap()
		.insert(cfg.data_dir.clone());
	until_recorded(&cluster, &control).await;
	until_activated(&control).await;
	let failed = control.hand_over().await.unwrap_err();
	assert!(
		failed.to_string().contains("the preparation task failed"),
		"{failed}"
	);
	assert_eq!(page_role(&cfg), Role::ControlOnly);
}
