#[path = "support/control_ring.rs"]
mod support;

use arrow::datatypes::{DataType, Field, Schema};
use wendelon::consensus::{SetTableRetentionPolicy, WendelonRequest, WendelonResponse};
use wendelon::integration::compute::{
	COMPUTE_STORAGE_CONTRACT_VERSION, SchemaGeneration, SchemaIntentId, SchemaIntentOrigin,
	SchemaMutationKindV1, SchemaObjectKind, SchemaObjectRefV1, SchemaUpdateEnvelopeV1,
	encode_schema_update_envelope_v1,
};
use wendelon::schema::{
	HybridTimestamp, LocalityPolicy, MultiRowLayoutDescriptor, PolicyGeneration,
	PrimaryKeyDescriptorV1, RecordLayoutPolicy, RetentionRejectCode, RetentionWindowMillis,
	RowIdentityPolicy, SchemaEvolutionPolicyV1, TableObjectId, TableRetentionPolicy,
	TableSchemaDescriptor, TenantId, encode_table_schema_descriptor_v1,
};

fn table_schema() -> Schema {
	Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("name", DataType::LargeUtf8, true),
	])
}

fn append_only_descriptor(id_byte: u8, table_name: &str) -> TableSchemaDescriptor {
	TableSchemaDescriptor::new_with_layout(
		TableObjectId::new([id_byte; 16]),
		1,
		"analytics",
		table_name,
		&table_schema(),
		RowIdentityPolicy::AppendOnly,
		LocalityPolicy::default_table_spread(),
		RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::new(4, 8, 1024 * 1024).unwrap()),
		SchemaEvolutionPolicyV1::AdditiveOnly,
	)
	.unwrap()
}

fn pk_descriptor(id_byte: u8, table_name: &str) -> TableSchemaDescriptor {
	TableSchemaDescriptor::new(
		TableObjectId::new([id_byte; 16]),
		1,
		"analytics",
		table_name,
		&table_schema(),
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("id")),
		LocalityPolicy::default_table_spread(),
		SchemaEvolutionPolicyV1::AdditiveOnly,
	)
	.unwrap()
}

fn schema_update(descriptor: &TableSchemaDescriptor, intent_byte: u8) -> Vec<u8> {
	let payload = encode_table_schema_descriptor_v1(descriptor).unwrap();
	let envelope = SchemaUpdateEnvelopeV1 {
		contract_version: COMPUTE_STORAGE_CONTRACT_VERSION,
		origin: SchemaIntentOrigin::ComputeClient,
		tenant_id: TenantId::ZERO,
		object: SchemaObjectRefV1 {
			kind: SchemaObjectKind::Table,
			namespace: descriptor.namespace.clone(),
			name: descriptor.table_name.clone(),
		},
		expected_generation: SchemaGeneration::ZERO,
		requested_generation: SchemaGeneration::new(descriptor.schema_generation),
		intent_id: SchemaIntentId::new([intent_byte; 16]),
		mutation: SchemaMutationKindV1::Create,
		payload,
	};
	encode_schema_update_envelope_v1(&envelope).unwrap()
}

fn hlc(physical_ms: u64) -> HybridTimestamp {
	HybridTimestamp {
		physical_ms,
		logical: 0,
		node_id: 10,
	}
}

#[tokio::test]
async fn set_table_retention_policy_commits_through_control_ring() {
	let node = support::bootstrap_node("retention-append", 61).await;
	support::wait_for_local_leader(&node).await;

	let descriptor = append_only_descriptor(0x61, "retention_append");
	assert_eq!(
		node.raft
			.client_write(WendelonRequest::SchemaUpdate(schema_update(
				&descriptor,
				0x61
			)))
			.await
			.unwrap()
			.data,
		WendelonResponse::Success
	);

	let policy = TableRetentionPolicy::FixedWindow(RetentionWindowMillis::try_new(90_000).unwrap());
	assert_eq!(
		node.raft
			.client_write(WendelonRequest::SetTableRetentionPolicy(
				SetTableRetentionPolicy {
					tenant_id: TenantId::ZERO,
					table_object_id: descriptor.table_object_id,
					expected_policy_generation: PolicyGeneration::ZERO,
					new_policy: policy,
					updated_at_hlc: hlc(90),
				}
			))
			.await
			.unwrap()
			.data,
		WendelonResponse::Success
	);

	let snapshot = node.state_machine.state_snapshot().await;
	let lifecycle = snapshot
		.table_lifecycle
		.get(TenantId::ZERO, descriptor.table_object_id)
		.expect("create should initialize table lifecycle");
	assert_eq!(lifecycle.policy_generation, PolicyGeneration::new(1));
	assert_eq!(lifecycle.retention, policy);
	assert_eq!(lifecycle.updated_at_hlc, hlc(90));
	let catalog_descriptor = snapshot
		.table_schemas
		.get(
			TenantId::ZERO,
			descriptor.table_object_id,
			descriptor.schema_generation,
		)
		.expect("retention updates must not rewrite table descriptor");
	assert_eq!(
		catalog_descriptor.logical_schema_fingerprint,
		descriptor.logical_schema_fingerprint
	);
	assert_eq!(
		catalog_descriptor.physical_descriptor_fingerprint,
		descriptor.physical_descriptor_fingerprint
	);

	support::abort_node(&node);
}

#[tokio::test]
async fn pk_keyed_retention_override_rejects_through_control_ring() {
	let node = support::bootstrap_node("retention-pk", 62).await;
	support::wait_for_local_leader(&node).await;

	let descriptor = pk_descriptor(0x62, "retention_pk");
	assert_eq!(
		node.raft
			.client_write(WendelonRequest::SchemaUpdate(schema_update(
				&descriptor,
				0x62
			)))
			.await
			.unwrap()
			.data,
		WendelonResponse::Success
	);

	let response = node
		.raft
		.client_write(WendelonRequest::SetTableRetentionPolicy(
			SetTableRetentionPolicy {
				tenant_id: TenantId::ZERO,
				table_object_id: descriptor.table_object_id,
				expected_policy_generation: PolicyGeneration::ZERO,
				new_policy: TableRetentionPolicy::Disabled,
				updated_at_hlc: hlc(91),
			},
		))
		.await
		.unwrap();
	let WendelonResponse::Error(error) = response.data else {
		panic!("expected PK-keyed retention override rejection");
	};
	assert!(error.contains(RetentionRejectCode::RETENTION_REJECTED_PK_DISALLOWED.name()));

	let snapshot = node.state_machine.state_snapshot().await;
	let lifecycle = snapshot
		.table_lifecycle
		.get(TenantId::ZERO, descriptor.table_object_id)
		.expect("create should initialize table lifecycle");
	assert_eq!(lifecycle.policy_generation, PolicyGeneration::ZERO);
	assert_eq!(lifecycle.retention, TableRetentionPolicy::InheritCluster);
	assert_eq!(
		lifecycle.updated_at_hlc,
		HybridTimestamp {
			physical_ms: 0,
			logical: 0,
			node_id: 0
		}
	);

	support::abort_node(&node);
}

#[tokio::test]
async fn stale_policy_generation_rejects_generically_through_control_ring() {
	let node = support::bootstrap_node("retention-stale", 63).await;
	support::wait_for_local_leader(&node).await;

	let descriptor = append_only_descriptor(0x63, "retention_stale");
	assert_eq!(
		node.raft
			.client_write(WendelonRequest::SchemaUpdate(schema_update(
				&descriptor,
				0x63
			)))
			.await
			.unwrap()
			.data,
		WendelonResponse::Success
	);

	let first = SetTableRetentionPolicy {
		tenant_id: TenantId::ZERO,
		table_object_id: descriptor.table_object_id,
		expected_policy_generation: PolicyGeneration::ZERO,
		new_policy: TableRetentionPolicy::FixedWindow(
			RetentionWindowMillis::try_new(90_000).unwrap(),
		),
		updated_at_hlc: hlc(92),
	};
	assert_eq!(
		node.raft
			.client_write(WendelonRequest::SetTableRetentionPolicy(first))
			.await
			.unwrap()
			.data,
		WendelonResponse::Success
	);

	let stale = SetTableRetentionPolicy {
		expected_policy_generation: PolicyGeneration::ZERO,
		new_policy: TableRetentionPolicy::Disabled,
		updated_at_hlc: hlc(93),
		..first
	};
	let response = node
		.raft
		.client_write(WendelonRequest::SetTableRetentionPolicy(stale))
		.await
		.unwrap();
	let WendelonResponse::Error(error) = response.data else {
		panic!("expected stale-generation rejection");
	};
	assert!(error.contains("stale policy_generation"));
	assert!(!error.contains("RETENTION_REJECTED"));

	let snapshot = node.state_machine.state_snapshot().await;
	let lifecycle = snapshot
		.table_lifecycle
		.get(TenantId::ZERO, descriptor.table_object_id)
		.expect("create should initialize table lifecycle");
	assert_eq!(lifecycle.policy_generation, PolicyGeneration::new(1));
	assert_eq!(lifecycle.retention, first.new_policy);
	assert_eq!(lifecycle.updated_at_hlc, first.updated_at_hlc);

	support::abort_node(&node);
}
