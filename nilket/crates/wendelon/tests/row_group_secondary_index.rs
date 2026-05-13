use arrow::datatypes::{DataType, Field, Schema};
use wendelon::schema::{
	LocalityPolicy, MultiRowLayoutDescriptor, PrimaryKeyDescriptorV1, RecordBodyByteLen,
	RecordBodyByteOffset, RecordLayoutPolicy, RecordRowIndex, RowCount, RowGroupIndex,
	RowGroupRowCount, RowIdentityPolicy, SchemaEvolutionPolicyV1, SlabId, TableObjectId,
};
use wendelon::storage::{
	RecordBodyByteRange, RecordPageDirectory, RowGroupDirectoryEntry, TieredRecordId,
	row_groups_for_record_rows,
};

fn directory() -> RecordPageDirectory {
	let descriptor = descriptor();
	RecordPageDirectory {
		record_id: TieredRecordId {
			slab_id: SlabId::new(52),
			record_offset: RecordBodyByteOffset::ZERO,
		},
		row_count: RowCount::new(10),
		row_groups: vec![
			RowGroupDirectoryEntry {
				row_group_index: RowGroupIndex::new(0),
				first_record_row_index: RecordRowIndex::new(0),
				row_count: RowGroupRowCount::new(4),
				metadata_range: range(0, 10),
				body_range: range(10, 100),
			},
			RowGroupDirectoryEntry {
				row_group_index: RowGroupIndex::new(1),
				first_record_row_index: RecordRowIndex::new(4),
				row_count: RowGroupRowCount::new(4),
				metadata_range: range(110, 10),
				body_range: range(120, 100),
			},
			RowGroupDirectoryEntry {
				row_group_index: RowGroupIndex::new(2),
				first_record_row_index: RecordRowIndex::new(8),
				row_count: RowGroupRowCount::new(2),
				metadata_range: range(220, 10),
				body_range: range(230, 50),
			},
		],
		buffer_pages: Vec::new(),
		row_group_stats: Vec::new(),
		logical_schema_fingerprint: descriptor.logical_schema_fingerprint,
	}
}

fn descriptor() -> wendelon::schema::TableSchemaDescriptor {
	let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
	wendelon::schema::TableSchemaDescriptor::new_with_layout(
		TableObjectId::new([0x52; 16]),
		1,
		"test",
		"row_group_secondary_index",
		&schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("id")),
		LocalityPolicy::IdentityProjection,
		RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::new(4, 4, 8 * 1024 * 1024).unwrap()),
		SchemaEvolutionPolicyV1::AdditiveOnly,
	)
	.unwrap()
}

fn range(start: u64, len: u64) -> RecordBodyByteRange {
	RecordBodyByteRange {
		start: RecordBodyByteOffset::new(start),
		len: RecordBodyByteLen::new(len),
	}
}

#[test]
fn secondary_index_hits_on_same_record_coalesce_to_row_group_set() {
	let groups = row_groups_for_record_rows(
		&directory(),
		&[
			RecordRowIndex::new(1),
			RecordRowIndex::new(3),
			RecordRowIndex::new(4),
			RecordRowIndex::new(9),
		],
	)
	.unwrap();

	assert_eq!(
		groups,
		vec![
			RowGroupIndex::new(0),
			RowGroupIndex::new(1),
			RowGroupIndex::new(2)
		]
	);
}

#[test]
fn secondary_index_hit_outside_directory_is_corruption() {
	let err = row_groups_for_record_rows(&directory(), &[RecordRowIndex::new(10)]).unwrap_err();

	assert!(err.to_string().contains("not covered"));
}
