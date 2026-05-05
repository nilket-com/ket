use bytemuck::Zeroable;
use wendelon::network::cdc::CdcSubscriptionRegistry;
use wendelon::schema::{
	CdcCursor, CdcSubscribeBound, CdcSubscribeRequestMeta, HybridTimestamp, Identity,
	RecordRowIndex, StateDigest, TableObjectId, TenantId, default_kv_table_descriptor_v1,
};

fn hlc(ms: u64) -> HybridTimestamp {
	HybridTimestamp {
		physical_ms: ms,
		logical: 0,
		node_id: 1,
	}
}

#[test]
fn cdc_subscribe_open_meta_decodes_bound_and_resume_digest() {
	let descriptor = default_kv_table_descriptor_v1().unwrap();
	let mut meta = CdcSubscribeRequestMeta::zeroed();
	meta.tenant_id = TenantId::new(7);
	meta.table_object_id = TableObjectId::new([3; 16]);
	meta.start_cursor = CdcCursor::new(hlc(5), Identity::new([5; 32]), RecordRowIndex::ZERO);
	meta.bound_raw = CdcSubscribeBound::SnapshotAtOpen.as_raw();
	meta.logical_schema_fingerprint = *descriptor.logical_schema_fingerprint.as_bytes();
	assert_eq!(meta.bound().unwrap(), CdcSubscribeBound::SnapshotAtOpen);
	assert_eq!(
		meta.decoded_logical_schema_fingerprint(),
		descriptor.logical_schema_fingerprint
	);
	assert_eq!(meta.resume_digest(), None);

	meta.resume_digest_present = 1;
	meta.resume_visible_state_digest = StateDigest::new([7; 32]);
	assert_eq!(meta.resume_digest(), Some(StateDigest::new([7; 32])));
}

#[test]
fn cdc_subscribe_open_registry_starts_at_requested_cursor() {
	let tenant_id = TenantId::new(7);
	let table_object_id = TableObjectId::new([3; 16]);
	let start_cursor = CdcCursor::new(hlc(9), Identity::new([9; 32]), RecordRowIndex::ZERO);
	let registry = CdcSubscriptionRegistry::new();
	let (handle, _recv) = registry.register(
		tenant_id,
		table_object_id,
		start_cursor,
		CdcSubscribeBound::Live,
		default_kv_table_descriptor_v1()
			.unwrap()
			.logical_schema_fingerprint,
	);

	assert_eq!(handle.cursor_high_water.load(), start_cursor);
	assert_eq!(handle.bound, CdcSubscribeBound::Live);
	assert_eq!(registry.active_count(tenant_id, table_object_id), 1);
}

#[test]
fn cdc_subscribe_open_cancel_unregisters_registry_entry() {
	let tenant_id = TenantId::new(7);
	let table_object_id = TableObjectId::new([3; 16]);
	let registry = CdcSubscriptionRegistry::new();
	let (handle, _recv) = registry.register(
		tenant_id,
		table_object_id,
		CdcCursor::MIN,
		CdcSubscribeBound::Live,
		default_kv_table_descriptor_v1()
			.unwrap()
			.logical_schema_fingerprint,
	);

	assert!(registry.unregister(
		tenant_id,
		table_object_id,
		handle.subscription_id,
		wendelon::metrics::CdcSubscriptionMetricResult::Cancelled,
	));
	assert_eq!(registry.active_count(tenant_id, table_object_id), 0);
	assert!(!registry.unregister(
		tenant_id,
		table_object_id,
		handle.subscription_id,
		wendelon::metrics::CdcSubscriptionMetricResult::Cancelled,
	));
}
