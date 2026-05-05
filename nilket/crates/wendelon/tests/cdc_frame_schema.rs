use arrow::array::{Array, BooleanArray, Int64Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use wendelon::schema::{
	CDC_ROW_INDEX_COLUMN, CDC_SUPERSEDES_HLC_COLUMN, HybridTimestamp, Identity, Locality,
	RecordRowIndex, STORAGE_ROW_HLC_COLUMN, STORAGE_ROW_KEY_ID_COLUMN,
	STORAGE_ROW_ROUTING_LOCALITY_COLUMN, STORAGE_ROW_TABLE_OBJECT_ID_COLUMN,
	STORAGE_ROW_TOMBSTONE_COLUMN, TableObjectId, cdc_event_batch, cdc_event_schema,
	cdc_tombstone_event_batch, default_kv_table_descriptor_v1,
};

#[test]
fn cdc_frame_schema_event_schema_prepends_row_axis_columns() {
	let source = Schema::new(vec![
		Field::new("account_id", DataType::UInt64, false),
		Field::new("amount", DataType::Int64, true),
	]);
	let fingerprint = default_kv_table_descriptor_v1()
		.unwrap()
		.logical_schema_fingerprint;
	let schema = cdc_event_schema(fingerprint, &source);
	let names: Vec<_> = schema.fields().iter().map(|field| field.name()).collect();

	assert_eq!(
		&names[..9],
		&[
			STORAGE_ROW_KEY_ID_COLUMN,
			STORAGE_ROW_HLC_COLUMN,
			STORAGE_ROW_TOMBSTONE_COLUMN,
			STORAGE_ROW_TABLE_OBJECT_ID_COLUMN,
			STORAGE_ROW_ROUTING_LOCALITY_COLUMN,
			CDC_ROW_INDEX_COLUMN,
			CDC_SUPERSEDES_HLC_COLUMN,
			"account_id",
			"amount",
		]
	);
	assert!(
		schema
			.metadata()
			.get("wendelon.cdc.logical_schema_fingerprint")
			.is_some()
	);
}

#[test]
fn cdc_frame_schema_event_schema_deduplicates_existing_synthetic_columns() {
	let source = Schema::new(vec![
		Field::new(STORAGE_ROW_HLC_COLUMN, DataType::FixedSizeBinary(16), false),
		Field::new("value", DataType::Utf8, true),
	]);
	let fingerprint = default_kv_table_descriptor_v1()
		.unwrap()
		.logical_schema_fingerprint;
	let schema = cdc_event_schema(fingerprint, &source);
	let hlc_count = schema
		.fields()
		.iter()
		.filter(|field| field.name() == STORAGE_ROW_HLC_COLUMN)
		.count();

	assert_eq!(hlc_count, 1);
	assert!(schema.field_with_name("value").is_ok());
}

#[test]
fn cdc_frame_schema_event_batch_materializes_row_axis_columns() {
	let source_schema = Arc::new(Schema::new(vec![
		Field::new("account_id", DataType::UInt64, false),
		Field::new("amount", DataType::Int64, false),
	]));
	let source = RecordBatch::try_new(
		source_schema,
		vec![
			Arc::new(UInt64Array::from(vec![42])),
			Arc::new(Int64Array::from(vec![-7])),
		],
	)
	.unwrap();
	let descriptor = default_kv_table_descriptor_v1().unwrap();
	let row_hlc = HybridTimestamp {
		physical_ms: 75,
		logical: 2,
		node_id: 1,
	};
	let row_key = Identity::new([9; 32]);
	let event = cdc_event_batch(
		descriptor.logical_schema_fingerprint,
		row_hlc,
		row_key,
		TableObjectId::new([3; 16]),
		Some(Locality::new(99)),
		RecordRowIndex::new(5),
		None,
		&source,
	)
	.unwrap();

	assert_eq!(event.num_rows(), 1);
	assert_eq!(event.schema().field(0).name(), STORAGE_ROW_KEY_ID_COLUMN);
	assert_eq!(event.schema().field(6).name(), CDC_SUPERSEDES_HLC_COLUMN);
	assert_eq!(event.schema().field(7).name(), "account_id");
	assert_eq!(event.schema().field(8).name(), "amount");
}

#[test]
fn cdc_frame_schema_tombstone_event_batch_sets_tombstone_and_null_user_columns() {
	let source_schema = Schema::new(vec![
		Field::new("account_id", DataType::UInt64, false),
		Field::new("amount", DataType::Int64, true),
	]);
	let descriptor = default_kv_table_descriptor_v1().unwrap();
	let event = cdc_tombstone_event_batch(
		descriptor.logical_schema_fingerprint,
		HybridTimestamp {
			physical_ms: 90,
			logical: 0,
			node_id: 1,
		},
		Identity::new([7; 32]),
		TableObjectId::new([3; 16]),
		Some(Locality::new(99)),
		RecordRowIndex::ZERO,
		None,
		&source_schema,
	)
	.unwrap();

	assert_eq!(event.num_rows(), 1);
	let tombstones = event
		.column_by_name(STORAGE_ROW_TOMBSTONE_COLUMN)
		.unwrap()
		.as_any()
		.downcast_ref::<BooleanArray>()
		.unwrap();
	assert!(tombstones.value(0));
	assert!(event.column_by_name("account_id").unwrap().is_null(0));
	assert!(event.column_by_name("amount").unwrap().is_null(0));
}
