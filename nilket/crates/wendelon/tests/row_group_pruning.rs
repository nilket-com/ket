use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use wendelon::schema::pbc::{
	OP_AND, OP_GT, OP_NEQ, OP_OR, OP_PUSH_COL, OP_PUSH_INT, OP_PUSH_STR, Program,
};
use wendelon::schema::{
	HybridTimestamp, Identity, Locality, LocalityPolicy, MultiRowLayoutDescriptor,
	PrimaryKeyDescriptorV1, RecordBodyByteLen, RecordBodyByteOffset, RecordBodyCompressionPolicy,
	RecordLayoutPolicy, RowCount, RowGroupRowCount, RowIdentityPolicy, SchemaEvolutionPolicyV1,
	SlabId, TableObjectId,
};
use wendelon::storage::multirow::MultiRowSourceRow;
use wendelon::storage::{
	L2RowGroupFetchConfig, PlaintextByteRange, RowGroupClassification, RowGroupWriterConfig,
	TieredRecordId, classify_pbc_for_row_group_pruning, classify_row_groups,
	encode_multirow_from_survivors_with_row_group_config, parse_record_buffer_directory,
};

fn schema() -> Schema {
	Schema::new(vec![
		Field::new("x", DataType::Int64, false),
		Field::new("name", DataType::Utf8, true),
	])
}

fn descriptor() -> wendelon::schema::TableSchemaDescriptor {
	wendelon::schema::TableSchemaDescriptor::new_with_layout(
		TableObjectId::new([0x57; 16]),
		1,
		"test",
		"row_group_pruning",
		&schema(),
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("x")),
		LocalityPolicy::IdentityProjection,
		RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::new(4, 4, 8 * 1024 * 1024).unwrap()),
		SchemaEvolutionPolicyV1::AdditiveOnly,
	)
	.unwrap()
}

fn fixture_directory() -> wendelon::storage::RecordPageDirectory {
	let batch = RecordBatch::try_new(
		Arc::new(schema()),
		vec![
			Arc::new(Int64Array::from(vec![1, 2, 3, 100, 101, 102, 103, 104])) as ArrayRef,
			Arc::new(StringArray::from(vec![
				Some("a"),
				Some("b"),
				Some("c"),
				Some("d"),
				Some("e"),
				Some("f"),
				Some("g"),
				Some("h"),
			])) as ArrayRef,
		],
	)
	.unwrap();
	let source_rows = (0..batch.num_rows())
		.map(|row| MultiRowSourceRow {
			source_row_index: row,
			key_identity: Identity::new([row as u8; 32]),
			routing_locality: Locality::new(1),
			row_hlc: HybridTimestamp {
				physical_ms: 1,
				logical: row as u32,
				node_id: 1,
			},
			is_tombstone: false,
			table_object_id: Some(TableObjectId::new([0x57; 16])),
		})
		.collect::<Vec<_>>();
	let writer_config = RowGroupWriterConfig::point_read_with_overrides(
		RowGroupRowCount::new(4),
		RowGroupRowCount::new(4),
		RowCount::new(8),
		RecordBodyByteLen::new(1),
	)
	.with_enabled(true);
	let descriptor = descriptor();
	let encoded = encode_multirow_from_survivors_with_row_group_config(
		&batch,
		&source_rows,
		&descriptor,
		RecordBodyCompressionPolicy::Uncompressed,
		&writer_config,
	)
	.unwrap();
	parse_record_buffer_directory(
		TieredRecordId {
			slab_id: SlabId::new(57),
			record_offset: RecordBodyByteOffset::ZERO,
		},
		PlaintextByteRange {
			start: 0,
			len: encoded.frame.len() as u64,
		},
		&encoded.frame,
		&descriptor,
	)
	.unwrap()
	.page_directory
	.unwrap()
}

fn push_col(bytecode: &mut Vec<u8>, column: u32) {
	bytecode.push(OP_PUSH_COL);
	bytecode.extend_from_slice(&column.to_le_bytes());
}

fn push_int(bytecode: &mut Vec<u8>, value: i64) {
	bytecode.push(OP_PUSH_INT);
	bytecode.extend_from_slice(&value.to_le_bytes());
}

fn push_str(bytecode: &mut Vec<u8>, value: &str) {
	bytecode.push(OP_PUSH_STR);
	bytecode.extend_from_slice(&(value.len() as u16).to_le_bytes());
	bytecode.extend_from_slice(value.as_bytes());
}

#[test]
fn range_predicate_skips_disjoint_row_groups() {
	let directory = fixture_directory();
	let mut bytecode = Vec::new();
	push_col(&mut bytecode, 0);
	push_int(&mut bytecode, 100);
	bytecode.push(OP_GT);
	let program = Program::compile(&bytecode).unwrap();
	let predicate = classify_pbc_for_row_group_pruning(&program, &schema());

	let classified = classify_row_groups(&directory, &predicate, &L2RowGroupFetchConfig::default());

	assert_eq!(classified[0].1, RowGroupClassification::Skip);
	assert_eq!(classified[1].1, RowGroupClassification::Keep);
}

#[test]
fn conjunction_skips_when_supported_branch_is_disjoint() {
	let directory = fixture_directory();
	let mut bytecode = Vec::new();
	push_col(&mut bytecode, 0);
	push_int(&mut bytecode, 100);
	bytecode.push(OP_GT);
	push_col(&mut bytecode, 1);
	push_str(&mut bytecode, "foo");
	bytecode.push(OP_NEQ);
	bytecode.push(OP_AND);
	let program = Program::compile(&bytecode).unwrap();
	let predicate = classify_pbc_for_row_group_pruning(&program, &schema());

	let classified = classify_row_groups(&directory, &predicate, &L2RowGroupFetchConfig::default());

	assert_eq!(classified[0].1, RowGroupClassification::Skip);
	assert_eq!(classified[1].1, RowGroupClassification::Keep);
}

#[test]
fn disjunction_skips_only_when_every_branch_is_disjoint() {
	let directory = fixture_directory();
	let mut bytecode = Vec::new();
	push_col(&mut bytecode, 0);
	push_int(&mut bytecode, 100);
	bytecode.push(OP_GT);
	push_col(&mut bytecode, 0);
	push_int(&mut bytecode, 0);
	bytecode.push(OP_GT);
	bytecode.push(OP_OR);
	let program = Program::compile(&bytecode).unwrap();
	let predicate = classify_pbc_for_row_group_pruning(&program, &schema());

	let classified = classify_row_groups(&directory, &predicate, &L2RowGroupFetchConfig::default());

	assert_eq!(classified[0].1, RowGroupClassification::Keep);
	assert_eq!(classified[1].1, RowGroupClassification::Keep);
}
