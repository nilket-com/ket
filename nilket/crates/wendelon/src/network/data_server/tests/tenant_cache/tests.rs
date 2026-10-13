//! B1a: sequential warm-cache isolation; pending-finalization ownership is excluded.
use crate::consensus::WendelonRequest;
use crate::integration::compute::{
	COMPUTE_STORAGE_CONTRACT_VERSION, SchemaGeneration, SchemaIntentId, SchemaIntentOrigin,
	SchemaMutationKind, SchemaObjectKind, SchemaObjectRef, SchemaUpdateEnvelope,
	encode_schema_update_envelope_v1,
};
use crate::network::data_server::WendelonDataServer;
use crate::network::data_server::tests::{
	append_only_table_descriptor, bootstrap_tempdir, hlc, shutdown_lease_node, typed_table_batch,
	typed_table_meta,
};
use crate::node::{NodeBootstrapConfig, WendelonNode};
use crate::schema::{
	ClientWriteId, DurabilityLevel, MutationCommitKnowledge, TableDeleteRowsRequestMeta, TenantId,
	WriteStatus, WriterNodeId, encode_table_schema_descriptor_v1,
	validate_table_write_request_batch,
};
use std::time::Duration;

#[tokio::test]
async fn warm_mutation_caches_isolate_tenants_and_ignore_same_tenant_ttl_padding() {
	let dir = bootstrap_tempdir("tenant-scoped-warm-mutation-cache");
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
	node.data_server
		.refresh_leases_once(&node.ring, &*node.clock, 60_000)
		.await;
	let tenants = [TenantId::new(7), TenantId::new(8)];
	let descriptor = append_only_table_descriptor();
	for tenant_id in tenants {
		node.create_tenant(tenant_id, u64::MAX, 1).await.unwrap();
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
			intent_id: SchemaIntentId::new([tenant_id.get() as u8; 16]),
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
	node.data_server.sync_tenant_policy_cache_once().await;
	for (order, ordered) in [tenants, [tenants[1], tenants[0]]].into_iter().enumerate() {
		let write_before = node.data_server.idempotency.len();
		let delete_before = node.data_server.table_delete_idempotency.len();
		let mut written = Vec::new();
		for (position, tenant_id) in ordered.into_iter().enumerate() {
			let batch = typed_table_batch(vec![100 + order as i64]);
			let mut meta = typed_table_meta(&descriptor, 1);
			meta.tenant_id = tenant_id;
			meta.parent_write_id.sequence_no = 629100 + order as u64;
			let (response, marker) = node
				.data_server
				.table_write(meta, batch.clone())
				.await
				.unwrap();
			assert_eq!(response.write.status, WriteStatus::OK);
			assert_eq!(
				response.write.mutation_commit_knowledge(),
				MutationCommitKnowledge::COMMITTED
			);
			assert_eq!(
				node.data_server.idempotency.len(),
				write_before + position + 1
			);
			let mut drifted = meta;
			drifted.parent_write_id.idempotency_ttl_ms += 1;
			drifted.parent_write_id._pad = u32::MAX;
			let (retry, retry_marker) = node
				.data_server
				.table_write(drifted, batch.clone())
				.await
				.unwrap();
			assert_eq!(retry.write.status, WriteStatus::DUPLICATE);
			assert_eq!(retry.write.commit_hlc, response.write.commit_hlc);
			assert_eq!(retry_marker, marker);
			assert_eq!(
				node.data_server.idempotency.len(),
				write_before + position + 1
			);
			assert!(node.data_server.idempotency.contains_key(
				&WendelonDataServer::idempotency_key_for(tenant_id, meta.parent_write_id)
			));
			let admission = validate_table_write_request_batch(
				&meta,
				&batch,
				&descriptor,
				WriterNodeId::from_node_id(node.local_node),
				response.write.commit_hlc,
			)
			.unwrap();
			written.push((
				tenant_id,
				admission.key_identities[0],
				response.write.commit_hlc,
			));
		}
		assert_ne!(
			written[0].2, written[1].2,
			"each tenant has its own committed write"
		);
		for (position, (tenant_id, row_key, _)) in written.into_iter().enumerate() {
			let mut meta: TableDeleteRowsRequestMeta = bytemuck::Zeroable::zeroed();
			meta.parent_write_id = ClientWriteId {
				client_uuid: [0x62; 16],
				sequence_no: order as u64,
				idempotency_ttl_ms: 30_000,
				_pad: 0,
			};
			meta.tenant_id = tenant_id;
			meta.table_object_id = descriptor.table_object_id;
			meta.schema_generation = descriptor.schema_generation;
			meta.client_hlc = hlc(10, 0, 48);
			meta.expected_epoch = 1;
			meta.row_count = 1;
			meta.durability = DurabilityLevel::QUORUM;
			let (response, marker) = node
				.data_server
				.table_delete(meta, vec![row_key])
				.await
				.unwrap();
			assert_eq!(response.write.status, WriteStatus::OK);
			assert_eq!(
				[
					response.deleted_count,
					response.already_deleted_count,
					response.not_found_count
				],
				[1, 0, 0]
			);
			let retained_writes = node.data_server.idempotency.len();
			assert_eq!(retained_writes, write_before + 2 + position + 1);
			assert_eq!(
				node.data_server.table_delete_idempotency.len(),
				delete_before + position + 1
			);
			meta.parent_write_id.idempotency_ttl_ms += 1;
			meta.parent_write_id._pad = u32::MAX;
			let (retry, retry_marker) = node
				.data_server
				.table_delete(meta, vec![row_key])
				.await
				.unwrap();
			assert_eq!(retry.write.commit_hlc, response.write.commit_hlc);
			assert_eq!(
				[
					retry.deleted_count,
					retry.already_deleted_count,
					retry.not_found_count
				],
				[1, 0, 0]
			);
			assert_eq!(retry_marker, marker);
			assert_eq!(node.data_server.idempotency.len(), retained_writes);
			assert_eq!(
				node.data_server.table_delete_idempotency.len(),
				delete_before + position + 1
			);
			assert!(node.data_server.table_delete_idempotency.contains_key(
				&WendelonDataServer::idempotency_key_for(tenant_id, meta.parent_write_id)
			));
		}
		println!(
			"TENANT_CACHE_PIN order={order} write_entries={} delete_entries={} ttl_pad_replay_adds_entries=false",
			node.data_server.idempotency.len(),
			node.data_server.table_delete_idempotency.len()
		);
	}
	shutdown_lease_node(node).await;
}
