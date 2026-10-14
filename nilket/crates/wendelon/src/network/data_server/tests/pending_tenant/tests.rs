//! B1b checks the indexed existing witness, not concurrent or failover ownership.
use crate::consensus::{TenantFinalization, WendelonRequest};
use crate::integration::compute::{
	COMPUTE_STORAGE_CONTRACT_VERSION, SchemaGeneration, SchemaIntentId, SchemaIntentOrigin,
	SchemaMutationKind, SchemaObjectKind, SchemaObjectRef, SchemaUpdateEnvelope,
	encode_schema_update_envelope_v1,
};
use crate::network::data_server::tests::{
	FixedReplicaRouter, append_only_table_descriptor, bootstrap_tempdir,
	content_hash_table_descriptor, hlc, shutdown_lease_node, simple_write_meta, single_kv_batch,
	typed_table_batch, typed_table_meta,
};
use crate::network::data_server::write_replication::WriteOutcome;
use crate::network::data_server::{
	CachedTableDeleteOutcome, CachedWriteOutcome, WendelonDataServer,
};
use crate::network::pending_finalizations::PendingFinalizationRecord;
use crate::network::traits::DataRing;
use crate::node::{NodeBootstrapConfig, WendelonNode};
use crate::schema::{
	ClientWriteId, DurabilityLevel, FinalizationMarker, Identity, MsgType, MutationCommitKnowledge,
	MutationOutcomeRequestMeta, MutationResolutionStatus, MutationStopReason, NodeId,
	PbcRejectCode, TableDeleteRowsRequestMeta, TableSchemaDescriptor, TenantId, WriteResponseMeta,
	WriteStatus, WriterNodeId, encode_table_schema_descriptor_v1,
	validate_table_write_request_batch,
};
use std::sync::Arc;
use std::time::Duration;

async fn fixture(
	tag: &str,
) -> (
	crate::test_support::TestDir,
	WendelonNode,
	WendelonDataServer,
) {
	let dir = bootstrap_tempdir(tag);
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		48,
		dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();
	let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
	while node.raft.current_leader().await != Some(node.local_node.id) {
		assert!(
			tokio::time::Instant::now() < deadline,
			"leader wait expired"
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
	// Abort and wait for cancellation before seeding; never mix maintenance into
	// the assertions that require A's witness to remain byte-for-byte unchanged.
	node.handles.pending_finalization_reconciler.abort();
	while !node.handles.pending_finalization_reconciler.is_finished() {
		assert!(
			tokio::time::Instant::now() < deadline,
			"reconciler cancellation expired"
		);
		tokio::task::yield_now().await;
	}
	for tenant_id in [TenantId::new(7), TenantId::new(8)] {
		node.create_tenant(tenant_id, u64::MAX, 1).await.unwrap();
		for descriptor in [
			append_only_table_descriptor(),
			content_hash_table_descriptor(),
		] {
			let envelope = SchemaUpdateEnvelope {
				contract_version: COMPUTE_STORAGE_CONTRACT_VERSION,
				origin: SchemaIntentOrigin::ComputeClient,
				tenant_id,
				object: SchemaObjectRef {
					kind: SchemaObjectKind::Table,
					namespace: descriptor.namespace.clone(),
					name: descriptor.table_name.clone(),
				},
				expected_generation: SchemaGeneration::ZERO,
				requested_generation: SchemaGeneration::new(descriptor.schema_generation),
				intent_id: SchemaIntentId::new(
					[tenant_id.get() as u8 ^ descriptor.table_object_id.as_bytes()[0]; 16],
				),
				mutation: SchemaMutationKind::Create,
				payload: encode_table_schema_descriptor_v1(&descriptor).unwrap(),
			};
			node.raft
				.client_write(WendelonRequest::SchemaUpdate(
					encode_schema_update_envelope_v1(&envelope).unwrap(),
				))
				.await
				.unwrap();
		}
	}
	node.data_server.sync_tenant_policy_cache_once().await;
	let server = node.data_server.as_ref().clone().with_lease_gate(false);
	(dir, node, server)
}

fn operation(sequence: u64) -> ClientWriteId {
	ClientWriteId {
		client_uuid: [0xB1; 16],
		sequence_no: sequence,
		idempotency_ttl_ms: 30_000,
		_pad: 0,
	}
}

async fn seed(
	server: &WendelonDataServer,
	tenant_id: TenantId,
	id: ClientWriteId,
) -> PendingFinalizationRecord {
	let record = PendingFinalizationRecord::new(
		id,
		true,
		TenantFinalization {
			tenant_id,
			key_identity: Identity::new([0xA4; 32]),
			charged_bytes: 777,
			record_count: 1,
			marker: FinalizationMarker {
				log_index: 991,
				term: 9,
				epoch: 1,
				commit_hlc: hlc(998, 7, 91),
				payload_hash: [0xFA; 32],
				e_safe_index: 991,
				durability: DurabilityLevel::QUORUM,
				_pad: [0; 7],
			},
		},
	);
	server
		.pending_finalizations
		.upsert(record.clone())
		.await
		.unwrap();
	record
}

fn assert_refusal(
	server: &WendelonDataServer,
	response: WriteResponseMeta,
	id: ClientWriteId,
	marker: Option<FinalizationMarker>,
) {
	assert_eq!(response.status, WriteStatus::AUTHORIZATION_FAILED);
	assert_eq!(
		response.mutation_commit_knowledge(),
		MutationCommitKnowledge::KNOWN_NOT_COMMITTED
	);
	assert_eq!(response.mutation_stop_reason(), MutationStopReason::NONE);
	assert_eq!(&response._pad1[..2], &[2, 0]);
	assert_eq!(response.log_index, 0);
	assert_eq!(response.term, 0);
	assert_eq!(response.commit_hlc, hlc(0, 0, 0));
	assert_eq!(response.echoed_write_id, id);
	assert_eq!(response.epoch, server.admission_snapshot().current_epoch);
	assert_eq!(response.leader_hint, server.local_node);
	assert_eq!(response.has_finalization, 0);
	assert!(marker.is_none());
	let frame = server.build_write_response_frame(MsgType::WRITE_RESP, 1, response, marker);
	let decoded: WriteResponseMeta = *bytemuck::from_bytes(&frame.meta);
	assert_eq!(
		(
			decoded.status.0,
			decoded.mutation_commit_knowledge().0,
			decoded.mutation_stop_reason().0
		),
		(12, 2, 0)
	);
	assert!(frame.trailing.is_none());
}

async fn assert_unresolved(server: &WendelonDataServer, tenant_id: TenantId, id: ClientWriteId) {
	let response = server
		.resolve_mutation_outcome(MutationOutcomeRequestMeta {
			operation_id: id,
			tenant_id,
		})
		.await
		.unwrap();
	assert_eq!(response.resolution, MutationResolutionStatus::UNKNOWN);
	assert_eq!(
		(
			response.write.status.0,
			response.write.mutation_commit_knowledge().0,
			response.write.mutation_stop_reason().0
		),
		(23, 3, 0)
	);
	assert_eq!(
		[
			response.deleted_count,
			response.already_deleted_count,
			response.not_found_count
		],
		[0; 3]
	);
	assert_eq!(response.write.commit_hlc, hlc(0, 0, 0));
	assert_eq!(response.write.has_finalization, 0);
}

fn delete_meta(
	tenant_id: TenantId,
	descriptor: &TableSchemaDescriptor,
	id: ClientWriteId,
) -> TableDeleteRowsRequestMeta {
	let mut meta: TableDeleteRowsRequestMeta = bytemuck::Zeroable::zeroed();
	meta.parent_write_id = id;
	meta.tenant_id = tenant_id;
	meta.table_object_id = descriptor.table_object_id;
	meta.schema_generation = descriptor.schema_generation;
	meta.client_hlc = hlc(123, 4, 48);
	meta.expected_epoch = 1;
	meta.row_count = 1;
	meta.durability = DurabilityLevel::QUORUM;
	meta
}

#[tokio::test]
async fn existing_foreign_pending_refuses_all_fresh_paths_before_persistence_both_orders() {
	let (_dir, node, server) = fixture("b1b-foreign-pending-matrix").await;
	for (order, (owner, requester)) in [(7, 8), (8, 7)].into_iter().enumerate() {
		let owner = TenantId::new(owner);
		let requester = TenantId::new(requester);
		for (case, kind) in [
			"raw",
			"table",
			"content-noop",
			"delete",
			"delete-noop",
			"write-expiry",
			"delete-expiry",
		]
		.into_iter()
		.enumerate()
		{
			let id = operation(100 + order as u64 * 20 + case as u64);
			let descriptor = if kind == "content-noop" {
				content_hash_table_descriptor()
			} else {
				append_only_table_descriptor()
			};
			let batch = typed_table_batch(vec![1000 + order as i64 * 20 + case as i64]);
			let mut meta = typed_table_meta(&descriptor, 1);
			meta.tenant_id = requester;
			meta.parent_write_id = id;
			let mut row_key = Identity::new([0xEE; 32]);
			if kind == "content-noop" || kind == "delete" {
				let mut preparation = meta;
				preparation.parent_write_id = operation(1000 + id.sequence_no);
				let (prepared, _) = server
					.table_write(preparation, batch.clone())
					.await
					.unwrap();
				assert_eq!(prepared.write.status, WriteStatus::OK);
				row_key = validate_table_write_request_batch(
					&preparation,
					&batch,
					&descriptor,
					WriterNodeId::from_node_id(node.local_node),
					prepared.write.commit_hlc,
				)
				.unwrap()
				.key_identities[0];
			}
			let record = seed(&server, owner, id).await;
			let before_bytes = node.slab.local_l1_bytes().await;
			let before_write = server.idempotency.len();
			let before_delete = server.table_delete_idempotency.len();
			let available = server.mutation_work_slots.available_permits();
			let held = if kind.ends_with("expiry") {
				Some(
					server
						.mutation_work_slots
						.clone()
						.acquire_many_owned(available as u32)
						.await
						.unwrap(),
				)
			} else {
				None
			};
			if kind == "raw" {
				let mut raw = simple_write_meta(1);
				raw.tenant_id = requester;
				raw.key_identity = Identity::blake3_tenant(requester, b"k");
				raw.write_id = id;
				let (response, marker) = server.write(raw, single_kv_batch()).await.unwrap();
				assert_refusal(&server, response, id, marker);
			} else if kind.starts_with("delete") {
				let mut delete = delete_meta(requester, &descriptor, id);
				delete.set_attempts_remaining(1);
				if kind == "delete-expiry" {
					delete.deadline_remaining_millis = 1;
				}
				let (response, marker) = server.table_delete(delete, vec![row_key]).await.unwrap();
				assert_eq!(
					[
						response.deleted_count,
						response.already_deleted_count,
						response.not_found_count
					],
					[0; 3]
				);
				assert_refusal(&server, response.write, id, marker);
				let final_response = server
					.finalize_table_delete_attempt(delete, response)
					.await
					.unwrap();
				assert_eq!(
					bytemuck::bytes_of(&final_response),
					bytemuck::bytes_of(&response)
				);
				assert_eq!(delete.attempts_remaining(), 1);
			} else {
				meta.set_attempts_remaining(1);
				if kind == "write-expiry" {
					meta.deadline_remaining_millis = 1;
				}
				let (response, marker) = server.table_write(meta, batch.clone()).await.unwrap();
				assert_refusal(&server, response.write, id, marker);
				let final_response = server
					.finalize_table_write_attempt(meta, response)
					.await
					.unwrap();
				assert_eq!(
					bytemuck::bytes_of(&final_response),
					bytemuck::bytes_of(&response)
				);
				assert_eq!(meta.attempts_remaining(), 1);
			}
			drop(held);
			assert_eq!(server.mutation_work_slots.available_permits(), available);
			assert_eq!(node.slab.local_l1_bytes().await, before_bytes);
			assert_eq!(server.idempotency.len(), before_write);
			assert_eq!(server.table_delete_idempotency.len(), before_delete);
			assert!(
				!server
					.idempotency
					.contains_key(&WendelonDataServer::idempotency_key_for(requester, id))
			);
			assert!(
				!server
					.table_delete_idempotency
					.contains_key(&WendelonDataServer::idempotency_key_for(requester, id))
			);
			assert_unresolved(&server, requester, id).await;
			assert_eq!(
				server.pending_finalizations.get(record.id).await.unwrap(),
				Some(record.clone())
			);
			server
				.pending_finalizations
				.remove(record.id)
				.await
				.unwrap();
			// Prove these are actual no-op candidates, not merely labels on invalid requests.
			if kind == "content-noop" {
				let (response, _) = server.table_write(meta, batch).await.unwrap();
				assert_eq!(response.write.status, WriteStatus::OK);
				assert_eq!(node.slab.local_l1_bytes().await, before_bytes);
			} else if kind == "delete-noop" {
				let (response, _) = server
					.table_delete(delete_meta(requester, &descriptor, id), vec![row_key])
					.await
					.unwrap();
				assert_eq!(response.write.status, WriteStatus::OK);
				assert_eq!(
					[
						response.deleted_count,
						response.already_deleted_count,
						response.not_found_count
					],
					[0, 0, 1]
				);
				assert_eq!(node.slab.local_l1_bytes().await, before_bytes);
			}
			println!(
				"PENDING_TENANT_PIN order={order} kind={kind} refusal=12/2/0 resolution=23/3/0 new_state=false"
			);
		}
	}
	shutdown_lease_node(node).await;
}

#[tokio::test]
async fn polluted_pending_cache_refuses_before_redirect_and_preserves_seeded_entry() {
	let (_dir, node, server) = fixture("b1b-polluted-pending-cache").await;
	for (order, (owner, requester)) in [(7, 8), (8, 7)].into_iter().enumerate() {
		let id = operation(200 + order as u64);
		let record = seed(&server, TenantId::new(owner), id).await;
		let requester = TenantId::new(requester);
		let outcome = server.recoverable_pending_outcome_for_record(&record);
		let WriteOutcome::FinalizationRecoverablePending {
			response: seeded_response,
			..
		} = outcome
		else {
			panic!("expected pending seed")
		};
		let key = WendelonDataServer::idempotency_key_for(requester, id);
		server
			.idempotency
			.insert(key, CachedWriteOutcome::new(outcome));
		let before = server.idempotency.len();
		let before_delete = server.table_delete_idempotency.len();
		let before_bytes = node.slab.local_l1_bytes().await;
		let mut request_id = id;
		request_id.idempotency_ttl_ms += 1;
		request_id._pad = u32::MAX;
		let mut nonleader = server.clone();
		nonleader.local_node = NodeId {
			id: 99,
			_reserved: 0,
		};
		let mut raw = simple_write_meta(1);
		raw.tenant_id = requester;
		raw.key_identity = Identity::blake3_tenant(requester, b"k");
		raw.write_id = request_id;
		let (response, marker) = nonleader.write(raw, single_kv_batch()).await.unwrap();
		assert_refusal(&nonleader, response, request_id, marker);
		let mut table = typed_table_meta(&append_only_table_descriptor(), 1);
		table.tenant_id = requester;
		table.parent_write_id = request_id;
		let (response, marker) = nonleader
			.table_write(table, typed_table_batch(vec![1]))
			.await
			.unwrap();
		assert_refusal(&nonleader, response.write, request_id, marker);
		let retained = server.idempotency.get(&key).unwrap();
		match retained.outcome() {
			WriteOutcome::FinalizationRecoverablePending {
				response,
				pending_finalization_id,
			} => {
				assert_eq!(pending_finalization_id, record.id);
				assert_eq!(response.commit_hlc, record.finalization.marker.commit_hlc);
				assert_eq!(response.status, WriteStatus::FINALIZATION_PENDING);
				assert_eq!(
					bytemuck::bytes_of(&response),
					bytemuck::bytes_of(&seeded_response)
				);
			}
			other => panic!("seeded cache changed: {other:?}"),
		}
		drop(retained);
		assert_eq!(server.idempotency.len(), before);
		assert_eq!(server.table_delete_idempotency.len(), before_delete);
		assert_eq!(node.slab.local_l1_bytes().await, before_bytes);
		assert_eq!(
			server.pending_finalizations.get(record.id).await.unwrap(),
			Some(record.clone())
		);
		assert_unresolved(&server, requester, id).await;
		server.idempotency.remove(&key);
		server
			.pending_finalizations
			.remove(record.id)
			.await
			.unwrap();
	}
	shutdown_lease_node(node).await;
}

#[tokio::test]
async fn foreign_pending_first_error_order_preserves_each_request_path() {
	let (_dir, node, server) = fixture("b1b-pending-first-error").await;
	for (order, (owner, requester)) in [(7, 8), (8, 7)].into_iter().enumerate() {
		let id = operation(300 + order as u64);
		let requester = TenantId::new(requester);
		let record = seed(&server, TenantId::new(owner), id).await;
		let mut raw = simple_write_meta(1);
		raw.write_id = id;
		raw.tenant_id = requester;
		raw.key_identity = Identity::blake3_tenant(requester, b"k");
		let mut table = typed_table_meta(&append_only_table_descriptor(), 1);
		table.parent_write_id = id;
		table.tenant_id = requester;
		let delete = delete_meta(requester, &append_only_table_descriptor(), id);
		// Raw keeps degraded and decode failures ahead of the new lower-helper gate.
		*server.local_persistence_distrust_reason.write().unwrap() = Some("test-fence".into());
		assert_eq!(
			server.write(raw, single_kv_batch()).await.unwrap().0.status,
			WriteStatus::INTERNAL_ERROR
		);
		// Table/delete foreign refusal is earlier than their degraded fresh work.
		assert_refusal(
			&server,
			server
				.table_write(table, typed_table_batch(vec![1]))
				.await
				.unwrap()
				.0
				.write,
			id,
			None,
		);
		assert_refusal(
			&server,
			server
				.table_delete(delete, vec![Identity::new([1; 32])])
				.await
				.unwrap()
				.0
				.write,
			id,
			None,
		);
		*server.local_persistence_distrust_reason.write().unwrap() = None;
		assert!(server.write(raw, typed_table_batch(vec![1])).await.is_err());
		let mut redirect = server.clone().with_lease_gate(true);
		redirect.router = Arc::new(FixedReplicaRouter::new(vec![NodeId {
			id: 99,
			_reserved: 0,
		}]));
		assert_eq!(
			redirect
				.write(raw, single_kv_batch())
				.await
				.unwrap()
				.0
				.status,
			WriteStatus::NOT_LEADER
		);
		assert_refusal(
			&redirect,
			redirect
				.table_write(table, typed_table_batch(vec![1]))
				.await
				.unwrap()
				.0
				.write,
			id,
			None,
		);
		let mut delete_nonleader = server.clone();
		delete_nonleader.local_node = NodeId {
			id: 99,
			_reserved: 0,
		};
		assert_eq!(
			delete_nonleader
				.table_delete(delete, vec![Identity::new([1; 32])])
				.await
				.unwrap()
				.0
				.write
				.status,
			WriteStatus::NOT_LEADER
		);
		// Proxy/epoch/validation remain ahead of tenant replay and refusal.
		let mut proxy = raw;
		proxy.proxy_hop = 2;
		assert_eq!(
			server
				.write(proxy, single_kv_batch())
				.await
				.unwrap()
				.0
				.status,
			WriteStatus::PROXY_LIMIT
		);
		let mut stale = table;
		stale.expected_epoch = 0;
		let mut stale_server = server.clone();
		let mut stale_snapshot = (*server.admission_snapshot()).clone();
		stale_snapshot.current_epoch = 1;
		let (stale_snapshot_tx, stale_snapshot_rx) =
			tokio::sync::watch::channel(Arc::new(stale_snapshot));
		stale_server.admission_snapshot_rx = stale_snapshot_rx;
		assert!(stale.expected_epoch < stale_server.admission_snapshot().current_epoch);
		assert_eq!(
			stale_server
				.table_write(stale, typed_table_batch(vec![1]))
				.await
				.unwrap()
				.0
				.write
				.status,
			WriteStatus::EPOCH_STALE
		);
		drop(stale_snapshot_tx);
		assert_eq!(
			server
				.table_delete(delete, vec![])
				.await
				.unwrap()
				.0
				.write
				.status,
			WriteStatus::INTERNAL_ERROR
		);
		// Tenant-owned warm results win over a foreign indexed witness.
		let mut owned = server.foreign_refusal(id).0;
		owned.status = WriteStatus::OK;
		owned.commit_hlc = hlc(55, 0, 48);
		owned = owned
			.with_mutation_outcome(MutationCommitKnowledge::COMMITTED, MutationStopReason::NONE);
		let key = WendelonDataServer::idempotency_key_for(requester, id);
		server.idempotency.insert(
			key,
			CachedWriteOutcome::new(WriteOutcome::NoopCommitted { response: owned }),
		);
		assert_eq!(
			server.write(raw, single_kv_batch()).await.unwrap().0.status,
			WriteStatus::DUPLICATE
		);
		assert_eq!(
			server
				.table_write(table, typed_table_batch(vec![1]))
				.await
				.unwrap()
				.0
				.write
				.commit_hlc,
			owned.commit_hlc
		);
		server.idempotency.remove(&key);
		let cached_delete = crate::network::data_server::table_delete_rows_response(
			owned,
			4,
			3,
			2,
			PbcRejectCode::NONE,
		);
		server.table_delete_idempotency.insert(
			key,
			CachedTableDeleteOutcome {
				response: cached_delete,
				trailing: None,
			},
		);
		assert_eq!(
			server
				.table_delete(delete, vec![Identity::new([1; 32])])
				.await
				.unwrap()
				.0
				.deleted_count,
			4
		);
		server.table_delete_idempotency.remove(&key);
		// Table/delete A3 also wins, without creating any warm entries.
		server
			.record_mutation_outcome(id, requester, owned, None, Some([4, 3, 2]))
			.await
			.unwrap();
		assert_eq!(
			server
				.table_write(table, typed_table_batch(vec![1]))
				.await
				.unwrap()
				.0
				.write
				.status,
			WriteStatus::DUPLICATE
		);
		assert_eq!(
			server
				.table_delete(delete, vec![Identity::new([1; 32])])
				.await
				.unwrap()
				.0
				.deleted_count,
			4
		);
		assert_eq!(
			server.pending_finalizations.get(record.id).await.unwrap(),
			Some(record.clone())
		);
		server
			.pending_finalizations
			.remove(record.id)
			.await
			.unwrap();
	}
	shutdown_lease_node(node).await;
}

#[tokio::test]
async fn same_tenant_pending_replay_keeps_original_retryable_outcome() {
	let (_dir, node, server) = fixture("b1b-same-tenant-pending").await;
	node.raft.shutdown().await.unwrap();
	for tenant_id in [TenantId::new(7), TenantId::new(8)] {
		let id = operation(400 + tenant_id.get());
		let record = seed(&server, tenant_id, id).await;
		assert!(
			server
				.foreign_pending(tenant_id, id)
				.await
				.unwrap()
				.is_none()
		);
		let outcome = server.recoverable_pending_outcome_for_record(&record);
		let (response, marker) = server
			.replay_write_outcome(tenant_id, id, outcome)
			.await
			.unwrap();
		assert_eq!(response.status, WriteStatus::FINALIZATION_PENDING);
		assert_eq!(response.commit_hlc, record.finalization.marker.commit_hlc);
		assert!(marker.is_none());
		assert_eq!(
			server.pending_finalizations.get(record.id).await.unwrap(),
			Some(record.clone())
		);
		server
			.pending_finalizations
			.remove(record.id)
			.await
			.unwrap();
	}
	shutdown_lease_node(node).await;
}
