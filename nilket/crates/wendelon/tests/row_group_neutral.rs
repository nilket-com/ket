use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use wendelon::schema::pbc::{OP_GT, OP_PROJECT, OP_PUSH_COL, OP_PUSH_INT, Program};
use wendelon::schema::{
	HybridTimestamp, Identity, Locality, LocalityPolicy, MultiRowLayoutDescriptor,
	PrimaryKeyDescriptorV1, RecordBodyByteLen, RecordBodyCompressionPolicy, RecordLayoutPolicy,
	RowCount, RowGroupRowCount, RowIdentityPolicy, SchemaEvolutionPolicyV1, TableObjectId,
};
use wendelon::storage::multirow::MultiRowSourceRow;
use wendelon::storage::{
	RowGroupWriterConfig, decode_batch, encode_multirow_from_survivors_with_row_group_config,
};

fn descriptor(schema: &Schema) -> wendelon::schema::TableSchemaDescriptor {
	wendelon::schema::TableSchemaDescriptor::new_with_layout(
		TableObjectId::new([0x56; 16]),
		1,
		"test",
		"row_group_neutral",
		schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("id")),
		LocalityPolicy::IdentityProjection,
		RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::new(4, 4, 8 * 1024 * 1024).unwrap()),
		SchemaEvolutionPolicyV1::AdditiveOnly,
	)
	.unwrap()
}

fn batch(row_count: usize) -> RecordBatch {
	let schema = Arc::new(Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("value", DataType::Int64, false),
	]));
	let ids = (0..row_count).map(|row| row as i64).collect::<Vec<_>>();
	let values = (0..row_count)
		.map(|row| (row as i64) * 10)
		.collect::<Vec<_>>();
	RecordBatch::try_new(
		schema,
		vec![
			Arc::new(Int64Array::from(ids)) as ArrayRef,
			Arc::new(Int64Array::from(values)) as ArrayRef,
		],
	)
	.unwrap()
}

fn source_rows(row_count: usize) -> Vec<MultiRowSourceRow> {
	(0..row_count)
		.map(|row| MultiRowSourceRow {
			source_row_index: row,
			key_identity: Identity::new([row as u8; 32]),
			routing_locality: Locality::new(7),
			row_hlc: HybridTimestamp {
				physical_ms: 1,
				logical: row as u32,
				node_id: 1,
			},
			is_tombstone: false,
			table_object_id: Some(TableObjectId::new([0x56; 16])),
		})
		.collect()
}

fn row_group_config(enabled: bool) -> RowGroupWriterConfig {
	RowGroupWriterConfig::point_read_with_overrides(
		RowGroupRowCount::new(4),
		RowGroupRowCount::new(4),
		RowCount::new(8),
		RecordBodyByteLen::new(1),
	)
	.with_enabled(enabled)
}

fn encoded_frame(row_group_enabled: bool) -> Vec<u8> {
	let batch = batch(10);
	let descriptor = descriptor(batch.schema().as_ref());
	encode_multirow_from_survivors_with_row_group_config(
		&batch,
		&source_rows(10),
		&descriptor,
		RecordBodyCompressionPolicy::Uncompressed,
		&row_group_config(row_group_enabled),
	)
	.unwrap()
	.frame
}

fn int_values(batch: &RecordBatch, column: usize) -> Vec<i64> {
	batch
		.column(column)
		.as_any()
		.downcast_ref::<Int64Array>()
		.unwrap()
		.values()
		.to_vec()
}

fn pbc_id_gt_project_value() -> Program {
	let mut bytecode = vec![OP_PUSH_COL];
	bytecode.extend_from_slice(&0_u32.to_le_bytes());
	bytecode.push(OP_PUSH_INT);
	bytecode.extend_from_slice(&5_i64.to_le_bytes());
	bytecode.push(OP_GT);
	bytecode.push(OP_PROJECT);
	bytecode.extend_from_slice(&1_u16.to_le_bytes());
	bytecode.extend_from_slice(&1_u32.to_le_bytes());
	Program::compile(&bytecode).unwrap()
}

#[test]
fn directory_bearing_record_decodes_as_the_same_whole_storage_batch() {
	let unpaged = decode_batch(&encoded_frame(false)).unwrap();
	let paged = decode_batch(&encoded_frame(true)).unwrap();

	assert_eq!(paged.num_rows(), unpaged.num_rows());
	assert_eq!(paged.num_columns(), unpaged.num_columns());
	assert_eq!(int_values(&paged, 0), int_values(&unpaged, 0));
	assert_eq!(int_values(&paged, 1), int_values(&unpaged, 1));
}

#[test]
fn pbc_over_whole_decoded_directory_record_matches_unpaged_record() {
	let program = pbc_id_gt_project_value();
	let unpaged = wendelon::schema::pbc::execute_program(
		&program,
		&decode_batch(&encoded_frame(false)).unwrap(),
	)
	.unwrap();
	let paged = wendelon::schema::pbc::execute_program(
		&program,
		&decode_batch(&encoded_frame(true)).unwrap(),
	)
	.unwrap();

	assert_eq!(int_values(&paged, 0), int_values(&unpaged, 0));
}
