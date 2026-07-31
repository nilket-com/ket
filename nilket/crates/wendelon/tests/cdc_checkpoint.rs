use wendelon::schema::{
	CdcCheckpointMeta, CdcCursor, CdcDigestDomain, CdcSubscriptionId, Identity, StateDigest,
	TableObjectId, TenantId, cdc_visible_rows_digest,
};

#[test]
fn cdc_checkpoint_digest_is_visible_state_order_independent() {
	let tenant_id = TenantId::new(7);
	let table_object_id = TableObjectId::new([3; 16]);
	let first = Identity::new([1; 32]);
	let second = Identity::new([2; 32]);

	let left = cdc_visible_rows_digest(tenant_id, table_object_id, [first, second]);
	let right = cdc_visible_rows_digest(tenant_id, table_object_id, [second, first]);
	assert_eq!(left, right);

	let meta = CdcCheckpointMeta {
		subscription_id: CdcSubscriptionId::new(9),
		cursor: CdcCursor::MAX,
		visible_state_digest: left,
		digest_domain_raw: CdcDigestDomain::TableRowVisible.as_raw(),
		_pad: [0; 4],
	};
	assert_eq!(
		meta.digest_domain().unwrap(),
		CdcDigestDomain::TableRowVisible
	);
	assert_ne!(meta.visible_state_digest, StateDigest::ZERO);
}
