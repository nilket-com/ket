//! Tenant-scoped pending identities; corrupt cache entries remain invariant failures.
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
	ClientWriteId, DurabilityLevel, FinalizationMarker, Identity, MutationCommitKnowledge,
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

fn assert_own_response(response: WriteResponseMeta, id: ClientWriteId, expired: bool) {
	assert_eq!(response.echoed_write_id, id);
	assert_eq!(
		response.status,
		if expired {
			WriteStatus::WRITE_DEADLINE_EXCEEDED
		} else {
			WriteStatus::OK
		}
	);
	assert_eq!(
		response.mutation_commit_knowledge(),
		if expired {
			MutationCommitKnowledge::KNOWN_NOT_COMMITTED
		} else {
			MutationCommitKnowledge::COMMITTED
		}
	);
	assert_eq!(
		response.mutation_stop_reason(),
		if expired {
			MutationStopReason::DEADLINE
		} else {
			MutationStopReason::NONE
		}
	);
	assert_ne!(response.commit_hlc, hlc(998, 7, 91));
}

fn assert_internal(error: std::io::Error) {
	assert_eq!(error.kind(), std::io::ErrorKind::Other);
	assert_eq!(
		error.to_string(),
		"pending replay tenant invariant violated"
	);
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
async fn other_tenant_pending_does_not_suppress_fresh_paths_both_orders() {
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
				assert_own_response(response, id, false);
				assert!(marker.is_none() || marker != Some(record.finalization.marker));
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
					if kind == "delete" {
						[1, 0, 0]
					} else if kind == "delete-noop" {
						[0, 0, 1]
					} else {
						[0; 3]
					}
				);
				assert_own_response(response.write, id, kind == "delete-expiry");
				assert!(marker.is_none() || marker != Some(record.finalization.marker));
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
				assert_own_response(response.write, id, kind == "write-expiry");
				assert!(marker.is_none() || marker != Some(record.finalization.marker));
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
			if kind.ends_with("expiry") || kind == "content-noop" || kind == "delete-noop" {
				assert_eq!(node.slab.local_l1_bytes().await, before_bytes);
			}
			// A's durable bytes, pending resolution and own cache namespace are unchanged.
			assert!(
				!server
					.idempotency
					.contains_key(&WendelonDataServer::idempotency_key_for(owner, id))
			);
			assert!(
				!server
					.table_delete_idempotency
					.contains_key(&WendelonDataServer::idempotency_key_for(owner, id))
			);
			let a = server
				.resolve_mutation_outcome(MutationOutcomeRequestMeta {
					operation_id: id,
					tenant_id: owner,
				})
				.await
				.unwrap();
			assert_eq!(a.write.commit_hlc, record.finalization.marker.commit_hlc);
			let b = server
				.resolve_mutation_outcome(MutationOutcomeRequestMeta {
					operation_id: id,
					tenant_id: requester,
				})
				.await
				.unwrap();
			assert_ne!(b.write.commit_hlc, a.write.commit_hlc);

			assert_eq!(
				server.pending_finalizations.get(record.id).await.unwrap(),
				Some(record.clone())
			);
			server
				.pending_finalizations
				.remove(record.id)
				.await
				.unwrap();
			println!(
				"PENDING_TENANT_PIN order={order} kind={kind} independent=true foreign_state_unchanged=true"
			);
		}
	}
	shutdown_lease_node(node).await;
}

#[tokio::test]
async fn polluted_pending_cache_is_internal_before_redirect_and_preserves_seeded_entry() {
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
		assert_internal(nonleader.write(raw, single_kv_batch()).await.unwrap_err());
		let mut table = typed_table_meta(&append_only_table_descriptor(), 1);
		table.tenant_id = requester;
		table.parent_write_id = request_id;
		assert_internal(
			nonleader
				.table_write(table, typed_table_batch(vec![1]))
				.await
				.unwrap_err(),
		);
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
async fn other_tenant_pending_preserves_ordinary_first_error_order() {
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
		// Raw keeps its ordinary degraded/decode ordering with an unrelated tenant record.
		*server.local_persistence_distrust_reason.write().unwrap() = Some("test-fence".into());
		assert_eq!(
			server.write(raw, single_kv_batch()).await.unwrap().0.status,
			WriteStatus::INTERNAL_ERROR
		);
		// Other tenant's record cannot override degraded fresh-work failures.
		assert_eq!(
			server
				.table_write(table, typed_table_batch(vec![1]))
				.await
				.unwrap()
				.0
				.write
				.status,
			WriteStatus::INTERNAL_ERROR
		);
		assert_eq!(
			server
				.table_delete(delete, vec![Identity::new([1; 32])])
				.await
				.unwrap()
				.0
				.write
				.status,
			WriteStatus::INTERNAL_ERROR
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
		assert_eq!(
			redirect
				.table_write(table, typed_table_batch(vec![1]))
				.await
				.unwrap()
				.0
				.write
				.status,
			WriteStatus::NOT_LEADER
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
		// Proxy/epoch/validation retain their own-operation precedence.
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
		// Tenant-owned warm results are independent of another tenant's witness.
		let mut owned =
			crate::network::data_server::error_response(&raw, WriteStatus::OK, server.local_node);
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
		assert_eq!(
			server
				.pending_finalizations
				.find_by_write_id(tenant_id, id)
				.await
				.unwrap(),
			Some(record.clone())
		);
		let outcome = server.recoverable_pending_outcome_for_record(&record);
		let (response, marker) = server
			.replay_write_outcome(tenant_id, outcome)
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

#[tokio::test]
async fn pending_restart_and_resolution_readers_are_independent_both_orders() {
	let (_dir, node, server) = fixture("tenant-pending-readers").await;
	for reverse in [false, true] {
		for table in [false, true] {
			let id = operation(2000 + u64::from(reverse) * 2 + u64::from(table));
			let a = seed(&server, TenantId::new(7), id).await;
			let mut finalization = a.finalization.clone();
			finalization.tenant_id = TenantId::new(8);
			finalization.marker.commit_hlc = hlc(1999, 8, 92);
			let b = PendingFinalizationRecord::new(id, true, finalization);
			let ordered = if reverse { [&b, &a] } else { [&a, &b] };
			for row in ordered {
				server
					.pending_finalizations
					.upsert_with_delete_counts(
						row.clone(),
						if row.finalization.tenant_id == TenantId::new(7) {
							Some([7, 1, 0])
						} else {
							Some([8, 2, 0])
						},
					)
					.await
					.unwrap();
			}
			for (row, counts) in [(&a, [7, 1, 0]), (&b, [8, 2, 0])] {
				let resolution = server
					.resolve_mutation_outcome(MutationOutcomeRequestMeta {
						operation_id: id,
						tenant_id: row.finalization.tenant_id,
					})
					.await
					.unwrap();
				assert_eq!(resolution.resolution, MutationResolutionStatus::PRESENT);
				assert_eq!(resolution.write.status, WriteStatus::FINALIZATION_PENDING);
				assert_eq!(
					resolution.write.commit_hlc,
					row.finalization.marker.commit_hlc
				);
				assert_eq!(
					[
						resolution.deleted_count,
						resolution.already_deleted_count,
						resolution.not_found_count
					],
					counts
				);
			}
			// A point lookup for B must select B despite A's competing canonical24.
			let response = if table {
				let mut meta = typed_table_meta(&append_only_table_descriptor(), 1);
				meta.tenant_id = TenantId::new(8);
				meta.parent_write_id = id;
				server
					.table_write(meta, typed_table_batch(vec![1]))
					.await
					.unwrap()
					.0
					.write
			} else {
				let mut meta = simple_write_meta(1);
				meta.tenant_id = TenantId::new(8);
				meta.key_identity = Identity::blake3_tenant(meta.tenant_id, b"k");
				meta.write_id = id;
				server.write(meta, single_kv_batch()).await.unwrap().0
			};
			assert_eq!(response.status, WriteStatus::DUPLICATE);
			assert_eq!(response.commit_hlc, b.finalization.marker.commit_hlc);
			assert_eq!(
				server.pending_finalizations.get(a.id).await.unwrap(),
				Some(a.clone())
			);
			assert_eq!(
				server
					.pending_finalizations
					.delete_counts(a.id)
					.await
					.unwrap(),
				Some([7, 1, 0])
			);
			assert!(
				!server
					.idempotency
					.contains_key(&WendelonDataServer::idempotency_key_for(
						TenantId::new(7),
						id
					))
			);
			let a_response = if table {
				let mut meta = typed_table_meta(&append_only_table_descriptor(), 1);
				meta.tenant_id = TenantId::new(7);
				meta.parent_write_id = id;
				server
					.table_write(meta, typed_table_batch(vec![1]))
					.await
					.unwrap()
					.0
					.write
			} else {
				let mut meta = simple_write_meta(1);
				meta.tenant_id = TenantId::new(7);
				meta.key_identity = Identity::blake3_tenant(meta.tenant_id, b"k");
				meta.write_id = id;
				server.write(meta, single_kv_batch()).await.unwrap().0
			};
			assert_eq!(a_response.status, WriteStatus::DUPLICATE);
			assert_eq!(a_response.commit_hlc, a.finalization.marker.commit_hlc);
			server.pending_finalizations.remove(a.id).await.unwrap();
		}
	}
	shutdown_lease_node(node).await;
}

#[tokio::test]
async fn other_tenant_terminal_outcome_does_not_suppress_own_fresh_write() {
	let (_dir, node, server) = fixture("tenant-terminal-independence").await;
	for (owner, requester) in [(7, 8), (8, 7)] {
		let id = operation(3000 + owner);
		let mut meta = simple_write_meta(1);
		meta.write_id = id;
		meta.tenant_id = TenantId::new(owner);
		let mut a =
			crate::network::data_server::error_response(&meta, WriteStatus::OK, server.local_node);
		a.commit_hlc = hlc(888, 7, 91);
		a = a.with_mutation_outcome(MutationCommitKnowledge::COMMITTED, MutationStopReason::NONE);
		server
			.record_mutation_outcome(id, TenantId::new(owner), a, None, Some([9, 8, 7]))
			.await
			.unwrap();
		let key = WendelonDataServer::idempotency_key_for(TenantId::new(owner), id);
		server.idempotency.insert(
			key,
			CachedWriteOutcome::new(WriteOutcome::NoopCommitted { response: a }),
		);
		meta.tenant_id = TenantId::new(requester);
		meta.key_identity = Identity::blake3_tenant(meta.tenant_id, b"k");
		let (b, _) = server.write(meta, single_kv_batch()).await.unwrap();
		assert_own_response(b, id, false);
		assert_ne!(b.commit_hlc, a.commit_hlc);
		let unchanged = server
			.resolve_mutation_outcome(MutationOutcomeRequestMeta {
				operation_id: id,
				tenant_id: TenantId::new(owner),
			})
			.await
			.unwrap();
		assert_eq!(bytemuck::bytes_of(&unchanged.write), bytemuck::bytes_of(&a));
		assert_eq!(
			[
				unchanged.deleted_count,
				unchanged.already_deleted_count,
				unchanged.not_found_count
			],
			[9, 8, 7]
		);
		match server.idempotency.get(&key).unwrap().outcome() {
			WriteOutcome::NoopCommitted { response } => {
				assert_eq!(bytemuck::bytes_of(&response), bytemuck::bytes_of(&a))
			}
			other => panic!("owner cache changed: {other:?}"),
		}
	}
	shutdown_lease_node(node).await;
}
