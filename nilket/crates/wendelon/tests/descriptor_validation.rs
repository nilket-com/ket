use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use wendelon::schema::{
	LocalityPolicy, MultiRowLayoutDescriptor, PolicyGeneration, PrimaryKeyDescriptor,
	RecordLayoutPolicy, RetentionRejectCode, RetentionWindowMillis, RowIdentityPolicy,
	STORAGE_ROW_HLC_COLUMN, STORAGE_ROW_KEY_ID_COLUMN, STORAGE_ROW_ROUTING_LOCALITY_COLUMN,
	STORAGE_ROW_TABLE_OBJECT_ID_COLUMN, STORAGE_ROW_TOMBSTONE_COLUMN, SchemaEvolutionPolicy,
	TableObjectId, TableRetentionPolicy, TableSchemaCatalog, TableSchemaDescriptor, TenantId,
};

fn base_schema() -> Schema {
	Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new(
			"event_time",
			DataType::Timestamp(TimeUnit::Millisecond, None),
			false,
		),
		Field::new("payload", DataType::LargeUtf8, true),
	])
}

fn multirow() -> RecordLayoutPolicy {
	RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::new(4, 8, 1024 * 1024).unwrap())
}

fn pk_descriptor(id_byte: u8, generation: u64, name: &str) -> TableSchemaDescriptor {
	TableSchemaDescriptor::new(
		TableObjectId::new([id_byte; 16]),
		generation,
		"analytics",
		name,
		&base_schema(),
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
		LocalityPolicy::default_table_spread(),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.unwrap()
}

#[test]
fn pk_descriptor_validates_and_supports_row_tombstones() {
	let descriptor = pk_descriptor(0x50, 1, "pk_events");

	assert!(descriptor.primary_key_descriptor().is_some());
	assert!(descriptor.supports_row_tombstones());
	assert!(!descriptor.is_append_only());
	assert!(descriptor.validate().is_ok());
}

#[test]
fn append_only_and_content_hash_descriptors_require_multirow_layout() {
	let append_single = TableSchemaDescriptor::new(
		TableObjectId::new([0x51; 16]),
		1,
		"analytics",
		"append_single",
		&base_schema(),
		RowIdentityPolicy::AppendOnly,
		LocalityPolicy::default_table_spread(),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.unwrap_err();
	assert!(
		append_single
			.to_string()
			.contains("append-only tables require RecordLayoutPolicy::MultiRow")
	);

	let append_multi = TableSchemaDescriptor::new_with_layout(
		TableObjectId::new([0x52; 16]),
		1,
		"analytics",
		"append_multi",
		&base_schema(),
		RowIdentityPolicy::AppendOnly,
		LocalityPolicy::default_table_spread(),
		multirow(),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.unwrap();
	assert!(append_multi.is_append_only());
	assert!(append_multi.supports_row_tombstones());

	let content_hash_single = TableSchemaDescriptor::new(
		TableObjectId::new([0x53; 16]),
		1,
		"analytics",
		"content_hash_single",
		&base_schema(),
		RowIdentityPolicy::AppendOnlyContentHash,
		LocalityPolicy::default_table_spread(),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.unwrap_err();
	assert!(
		content_hash_single
			.to_string()
			.contains("AppendOnlyContentHash tables require RecordLayoutPolicy::MultiRow")
	);
}

#[test]
fn descriptor_validation_rejects_invalid_primary_keys() {
	let nullable_pk = Schema::new(vec![Field::new("id", DataType::Int64, true)]);
	let nullable = TableSchemaDescriptor::new(
		TableObjectId::new([0x54; 16]),
		1,
		"analytics",
		"nullable_pk",
		&nullable_pk,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
		LocalityPolicy::default_table_spread(),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.unwrap_err();
	assert!(nullable.to_string().contains("must be non-nullable"));

	let unsupported_pk = Schema::new(vec![Field::new("id", DataType::Float64, false)]);
	let unsupported = TableSchemaDescriptor::new(
		TableObjectId::new([0x55; 16]),
		1,
		"analytics",
		"unsupported_pk",
		&unsupported_pk,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
		LocalityPolicy::default_table_spread(),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.unwrap_err();
	assert!(unsupported.to_string().contains("unsupported V1 type"));

	let missing = TableSchemaDescriptor::new(
		TableObjectId::new([0x56; 16]),
		1,
		"analytics",
		"missing_pk",
		&base_schema(),
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("missing")),
		LocalityPolicy::default_table_spread(),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.unwrap_err();
	assert!(
		missing
			.to_string()
			.contains("is not present in Arrow schema")
	);
}

#[test]
fn descriptor_validation_reserves_storage_synthetic_columns() {
	for reserved in [
		STORAGE_ROW_HLC_COLUMN,
		STORAGE_ROW_KEY_ID_COLUMN,
		STORAGE_ROW_ROUTING_LOCALITY_COLUMN,
		STORAGE_ROW_TABLE_OBJECT_ID_COLUMN,
		STORAGE_ROW_TOMBSTONE_COLUMN,
	] {
		let schema = Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new(reserved, DataType::UInt64, false),
		]);
		let err = TableSchemaDescriptor::new(
			TableObjectId::new([0x57; 16]),
			1,
			"analytics",
			"reserved_column",
			&schema,
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
			LocalityPolicy::default_table_spread(),
			SchemaEvolutionPolicy::AdditiveOnly,
		)
		.unwrap_err();
		assert!(
			err.to_string().contains("storage-only synthetic column"),
			"reserved column {reserved} should be rejected: {err}"
		);
	}
}

#[test]
fn schema_catalog_rejects_name_and_object_identity_conflicts() {
	let tenant = TenantId::new(7);
	let first = pk_descriptor(0x58, 1, "cataloged");
	let newer_generation = pk_descriptor(0x58, 2, "cataloged");
	let conflicting_object = pk_descriptor(0x59, 1, "cataloged");
	let conflicting_name = pk_descriptor(0x58, 3, "renamed");

	let mut catalog = TableSchemaCatalog::default();
	catalog.upsert(tenant, first.clone()).unwrap();
	catalog.upsert(tenant, newer_generation.clone()).unwrap();
	assert_eq!(
		catalog.latest_by_id(tenant, first.table_object_id),
		Some(&newer_generation)
	);
	assert_eq!(
		catalog.latest_by_name(tenant, "analytics", "cataloged"),
		Some(&newer_generation)
	);

	let err = catalog.upsert(tenant, conflicting_object).unwrap_err();
	assert!(
		err.to_string()
			.contains("name is already bound to a different table_object_id")
	);

	let err = catalog.upsert(tenant, conflicting_name).unwrap_err();
	assert!(
		err.to_string()
			.contains("table_object_id is already bound to a different table name")
	);
}

#[test]
fn pk_retention_override_disposition_remains_rejected_for_v1() {
	assert!(!TableRetentionPolicy::InheritCluster.is_table_override_disallowed_for_pk());
	assert!(TableRetentionPolicy::Disabled.is_table_override_disallowed_for_pk());
	assert!(
		TableRetentionPolicy::FixedWindow(RetentionWindowMillis::try_new(60_000).unwrap())
			.is_table_override_disallowed_for_pk()
	);
	assert_eq!(
		RetentionRejectCode::RETENTION_REJECTED_PK_DISALLOWED.name(),
		"RETENTION_REJECTED_PK_DISALLOWED"
	);
	assert_eq!(PolicyGeneration::ZERO.next(), PolicyGeneration::new(1));
}
