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
		.persistent_index()
		.expect("the recovery storage's index")
		.store_incarnation();
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
		.persistent_index()
		.expect("the recovery storage's index")
		.store_incarnation();
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
		.persistent_index()
		.expect("the recovery storage's index")
		.store_incarnation();
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

/// DST stage 1 pilot (Codex 01a1e607, 01a1e616, 01a1e61e, 01a1e637): the activation handoff
/// above with the PRODUCTION decision expiry passed in real protocol, not by a floor shortcut.
/// One cluster's test time (one forward-only wall clock, one abort latch) reaches every
/// lifetime: the three Full nodes, the ControlOnly replacement (its preparation, recovery and
/// certification path) and its Full starts. The wall clock moves only through
/// `time_advance`'s restored steps (lease TTL, skew bound and E unchanged):
///
/// 1. rows 1 and 2, then wall time to their issue + E + 5 s: the floor passes them and
///    generation 1 certifies;
/// 2. row 3, the follower lost with its disk, a replacement begun, the ControlOnly instance
///    promoted and its recovery recorded at (G, K), its activation gate CLOSED;
/// 3. row 4 (the designated post-base write, its reservations recorded), then wall time to
///    its issue + E + 5 s with the two survivors Full and the replacement owner-less;
/// 4. still gated, a certificate past G whose observed target lists the replacement's
///    current slot and incarnation, whose replacement report was applied between target and
///    commit, and whose cut covers and erased row 4's reservations; no activation meanwhile;
/// 5. the gate opens: the replacement activates at that certificate or an observed
///    successor (the current one), as its recovered store, through a cut no lower than K;
/// 6. it hands over, starts Full on the same directory and clock, serves exactly rows 1–4,
///    and does again after a Full restart;
/// 7. every node shuts down Ok, and only then is the latch read.
///
/// ONE absolute real deadline, [`PILOT_DEADLINE`], judges the whole scenario, setup to the
/// last shutdown: every await is bounded by it and a success observed after it is refused.
/// After a failure, cleanup is bounded separately, inside the owner's remaining cap.
///
/// Coverage limits: the handoff is the in-process `hand_over` and a Full `reopen`, not a
/// daemon exiting 75 under a supervisor (the real-daemon and systemd records cover that, at
/// their own trees); writes enter through the in-process `write_at` with identities minted on
/// the shared wall clock, not through a public SDK client; the drift monitor gets no samples
/// in this mode, so its absence of an abort is no drift coverage. Ignored: it runs once,
/// under its launch ruling.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "DST stage 1 pilot: one execution under its launch ruling"]
async fn dst_pilot_an_activation_after_the_production_expiry_on_an_advanced_clock() {
	let real = std::time::Instant::now();
	let deadline = tokio::time::Instant::now() + PILOT_DEADLINE;
	let time = crate::node::test_time::TestTime::new();
	let mut cluster = None;
	let mut control = None;
	let mut records = (
		super::super::time_advance::Record::default(),
		super::super::time_advance::Record::default(),
	);
	// The owning unit's cgroup, captured during the execution (Codex 01a1e648).
	eprintln!(
		"DST pilot cgroup: {}",
		std::fs::read_to_string("/proc/self/cgroup")
			.unwrap_or_else(|error| format!("unreadable: {error}"))
	);
	let mut gate = None;
	let outcome = dst_pilot(
		&time,
		deadline,
		&mut cluster,
		&mut control,
		&mut gate,
		&mut records,
	)
	.await;
	eprintln!(
		"DST pilot: {:?} real; phase 1 {:?}; phase 2 {:?}",
		real.elapsed(),
		records.0,
		records.1
	);
	if let Err(why) = outcome {
		// Bounded cleanup after a failure, inside the owner's remaining cap; every outcome is
		// recorded. The test-owned gate entry is removed.
		if let Some(dir) = gate {
			ACTIVATION_ENABLED.lock().unwrap().remove(&dir);
			eprintln!("DST pilot cleanup: activation gate entry removed");
		}
		if let Some(control) = control {
			let outcome = tokio::time::timeout(PILOT_CLEANUP, control.shutdown()).await;
			eprintln!("DST pilot cleanup: the ControlOnly shutdown: {outcome:?}");
		}
		if let Some(cluster) = cluster {
			for node in cluster.nodes {
				let id = node.local_node.id;
				let outcome = tokio::time::timeout(PILOT_CLEANUP, node.shutdown()).await;
				eprintln!("DST pilot cleanup: node {id}'s shutdown: {outcome:?}");
			}
		}
		panic!(
			"DST pilot failed: {why}; latched {:?}",
			time.abort.latched()
		);
	}
	eprintln!(
		"DST pilot passed within its deadline: {:?} real, explicit wall {} ms in {} steps, wall span {} ms",
		real.elapsed(),
		records.0.wall_advanced_ms + records.1.wall_advanced_ms,
		records.0.steps + records.1.steps,
		records.0.wall_span_ms + records.1.wall_span_ms
	);
}

/// The pilot's one workflow deadline, setup to the last shutdown.
const PILOT_DEADLINE: std::time::Duration = std::time::Duration::from_secs(780);
/// Each cleanup shutdown's bound after a failure (the owner caps the whole run at 900 s).
const PILOT_CLEANUP: std::time::Duration = std::time::Duration::from_secs(20);

/// The reservations of the parent bodies in `leader`'s applied decision state.
async fn body_reservations(cluster: &Cluster, leader: u32) -> std::collections::BTreeSet<u64> {
	cluster
		.node(leader)
		.state_machine
		.read_state(|state| {
			state
				.decisions
				.as_ref()
				.map(|decisions| decisions.bodies().keys().copied().collect())
				.unwrap_or_default()
		})
		.await
}

fn check(holds: bool, what: impl FnOnce() -> String) -> Result<(), String> {
	if holds { Ok(()) } else { Err(what()) }
}

async fn dst_pilot(
	time: &crate::node::test_time::TestTime,
	deadline: tokio::time::Instant,
	cluster_slot: &mut Option<Cluster>,
	control_slot: &mut Option<ControlOnlyNode>,
	gate_slot: &mut Option<std::path::PathBuf>,
	records: &mut (
		super::super::time_advance::Record,
		super::super::time_advance::Record,
	),
) -> Result<(), String> {
	use super::super::time_advance::{self, advance_to, within};
	use crate::consensus::decisions::DECISION_EXPIRY_MS;
	let (cluster, leader, descriptor) = within(
		deadline,
		"cluster start",
		ready_with({
			let time = time.clone();
			Cluster::start_with(move |_, cfg| {
				cfg.test_time = Some(time.clone());
				None
			})
		}),
	)
	.await?;
	let cluster = cluster_slot.insert(cluster);
	let lost = *NODES[1..].iter().find(|id| **id != leader).unwrap();
	let issued = time.clock.now_ms();
	within(
		deadline,
		"rows 1 and 2",
		write_at(cluster, &descriptor, issued, &[(1, "a"), (2, "b")]),
	)
	.await?;
	let all: std::collections::BTreeSet<u32> = NODES.into_iter().collect();
	advance_to(
		cluster,
		time,
		&all,
		issued + DECISION_EXPIRY_MS + 5_000,
		deadline,
		&mut records.0,
	)
	.await
	.map_err(|why| format!("phase 1: {why}"))?;
	within(deadline, "generation 1", until_active(cluster, 1)).await?;
	let issued = time.clock.now_ms();
	within(
		deadline,
		"row 3",
		write_at(cluster, &descriptor, issued, &[(3, "c")]),
	)
	.await?;
	within(deadline, "the loss", cluster.stop(lost, true)).await?;
	let cfg = cluster.configs[&lost].clone();
	check(cfg.test_time.as_ref() == Some(time), || {
		"the replacement does not share the cluster's time".to_owned()
	})?;
	lose_disk(&cfg.data_dir);
	crate::crypto::test_support::enroll_root_artifact(
		&cluster.configs[&NODES[0]].data_dir,
		&cfg.data_dir,
	)
	.map_err(|error| format!("enrolment: {error}"))?;
	let begun = within(
		deadline,
		"BeginReplacement",
		decide(
			cluster,
			DecisionCommand::BeginReplacement {
				token: TOKEN,
				slot: lost,
			},
		),
	)
	.await?;
	check(
		matches!(begun, DecisionResult::ReplacementRecorded { .. }),
		|| format!("BeginReplacement: {begun:?}"),
	)?;
	crate::node::provision_replacement_token(&cfg.data_dir, TOKEN)
		.map_err(|error| format!("token: {error}"))?;
	let started = within(
		deadline,
		"the ControlOnly start",
		ControlOnlyNode::bootstrap_with_provider(cfg.clone(), provider(cluster, lost)),
	)
	.await?
	.map_err(|error| format!("the ControlOnly start: {error}"))?;
	let control = control_slot.insert(started);
	// The activation gate stays CLOSED through recovery and the second expiry (Codex
	// 01a1e648): a later generation must first certify with the replacement reporting.
	let recorded = within(
		deadline,
		"the recovery record",
		until_recorded(cluster, control),
	)
	.await?;
	let store = within(deadline, "the recovery storage", control.recovery_storage())
		.await?
		.map_err(|error| format!("the recovery storage: {error}"))?
		.persistent_index()
		.ok_or("the recovery storage has no index")?
		.store_incarnation();
	let replacement = crate::consensus::decisions::Signer {
		node: lost,
		incarnation: store,
	};
	// The designated post-base write: its reservations, from the leader's applied bodies.
	let leader = within(deadline, "the leader", cluster.leader()).await?;
	records.1.scanned_through = within(deadline, "the view", time_advance::view(cluster, leader))
		.await?
		.applied;
	let before = within(deadline, "the bodies", body_reservations(cluster, leader)).await?;
	let issued = time.clock.now_ms();
	within(
		deadline,
		"row 4",
		write_at(cluster, &descriptor, issued, &[(4, "d")]),
	)
	.await?;
	let after = within(deadline, "the bodies", body_reservations(cluster, leader)).await?;
	let designated: Vec<u64> = after.difference(&before).copied().collect();
	check(!designated.is_empty(), || {
		"row 4 reserved no body".to_owned()
	})?;
	let survivors: std::collections::BTreeSet<u32> =
		NODES.into_iter().filter(|id| *id != lost).collect();
	advance_to(
		cluster,
		time,
		&survivors,
		issued + DECISION_EXPIRY_MS + 5_000,
		deadline,
		&mut records.1,
	)
	.await
	.map_err(|why| format!("phase 2: {why}"))?;
	// Still gated: wait for a certificate past the recovery's base, scanning every applied
	// generation event, with no replacement activation recorded meanwhile.
	let certificate = loop {
		let leader = within(deadline, "the leader", cluster.leader()).await?;
		let applied = within(deadline, "the view", time_advance::view(cluster, leader))
			.await?
			.applied;
		time_advance::scan_generations(cluster, leader, &mut records.1, deadline, applied).await?;
		let activated = within(
			deadline,
			"the activation record",
			activation_record(cluster),
		)
		.await?;
		check(activated.is_none(), || {
			format!("a replacement activation while gated: {activated:?}")
		})?;
		let current = within(deadline, "the certificate", current_certificate(cluster)).await?;
		// The covering certificate with its exact commit scanned (Codex 01a1e655 R1); an
		// intermediate one, or a commit past the scan's mark, means keep polling, gated.
		let scanned: Vec<_> = records
			.1
			.generations
			.iter()
			.map(|(event, _, _)| event.clone())
			.collect();
		if time_advance::covering_certificate_ready(
			&scanned,
			current.as_ref(),
			recorded.gid,
			&designated,
		) {
			break current.expect("a ready certificate");
		}
		check(tokio::time::Instant::now() < deadline, || {
			format!(
				"no certificate past gid {} covering {designated:?} with its commit scanned before \
				 the deadline (current {current:?})",
				recorded.gid
			)
		})?;
		tokio::time::sleep(std::time::Duration::from_millis(20)).await;
	};
	let events: Vec<_> = records
		.1
		.generations
		.iter()
		.map(|(event, _, _)| event.clone())
		.collect();
	let (target_at, report_at, commit_at) =
		time_advance::certified_with(&events, &certificate, replacement, &designated)?;
	let erased = within(deadline, "the bodies", body_reservations(cluster, leader)).await?;
	check(designated.iter().all(|rsv| !erased.contains(rsv)), || {
		format!("designated reservations {designated:?} not erased by the commit")
	})?;
	let seen = records
		.1
		.generations
		.iter()
		.find(|(event, _, _)| {
			matches!(event, time_advance::GenerationEvent::Commit { gid, index }
				if *gid == certificate.gid && *index == certificate.committed_at)
		})
		.map(|(_, steps, at)| (*steps, *at));
	eprintln!(
		"DST pilot certificate: gid {} t {} committed_at {} (target {target_at}, replacement report {report_at}, commit {commit_at}); replacement {replacement:?}; designated {designated:?}; recovery G {} K {}; seen after phase-2 step {:?} (steps, real since its scan began)",
		certificate.gid, certificate.t, certificate.committed_at, recorded.gid, recorded.k, seen
	);
	// Now the gate opens; the activation must be at that certificate or an observed successor.
	*gate_slot = Some(cfg.data_dir.clone());
	ACTIVATION_ENABLED
		.lock()
		.unwrap()
		.insert(cfg.data_dir.clone());
	within(deadline, "the activation", until_activated(control)).await?;
	let activation = within(
		deadline,
		"the activation record",
		activation_record(cluster),
	)
	.await?
	.ok_or("no activation record")?;
	let current = within(deadline, "the certificate", current_certificate(cluster))
		.await?
		.map(|certificate| certificate.gid);
	check(activation.incarnation == store, || {
		format!(
			"activated {:?}, recovered {store:?}",
			activation.incarnation
		)
	})?;
	check(activation.through >= recorded.k, || {
		format!(
			"activation through {} below K {}",
			activation.through, recorded.k
		)
	})?;
	check(activation.certified >= certificate.gid, || {
		format!(
			"activation at gid {}, before the evidenced certificate {}",
			activation.certified, certificate.gid
		)
	})?;
	check(Some(activation.certified) == current, || {
		format!(
			"activation at {}, current {current:?}",
			activation.certified
		)
	})?;
	ACTIVATION_ENABLED.lock().unwrap().remove(&cfg.data_dir);
	*gate_slot = None;
	let control = control_slot.take().expect("the ControlOnly instance");
	within(deadline, "the handoff", control.hand_over())
		.await?
		.map_err(|error| format!("the handoff: {error}"))?;
	check(page_role(&cfg) == Role::Full, || {
		"the page is not Full".to_owned()
	})?;
	let expected = rows(&[(1, "a"), (2, "b"), (3, "c"), (4, "d")]);
	for start in ["the Full start", "the Full restart"] {
		if start == "the Full restart" {
			within(deadline, "the Full stop", cluster.stop(lost, false)).await?;
		}
		within(deadline, start, cluster.reopen(lost)).await?;
		within(deadline, start, until_ready_as(cluster, lost, store)).await?;
		let read = within(
			deadline,
			start,
			cluster.read_once_validated(lost, &descriptor),
		)
		.await?;
		check(read == expected, || format!("{start}: read {read:?}"))?;
	}
	let cluster = cluster_slot.take().expect("the cluster");
	for node in cluster.nodes {
		let id = node.local_node.id;
		within(deadline, &format!("node {id}'s shutdown"), node.shutdown())
			.await?
			.map_err(|error| format!("node {id}'s shutdown: {error}"))?;
	}
	let latched = time.abort.latched();
	check(latched.is_empty(), || {
		format!("the abort latched: {latched:?}")
	})?;
	// The whole judged scenario, observed within its one deadline.
	within(deadline, "the pilot", async {}).await
}
