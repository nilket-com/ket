use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use wendelon::schema::{
	HybridTimestamp, Identity, IpcColumnOrdinal, Locality, LocalityPolicy,
	MultiRowLayoutDescriptor, PrimaryKeyDescriptor, RecordBodyByteLen, RecordBodyByteOffset,
	RecordBodyCompressionPolicy, RecordLayoutPolicy, RecordRowIndex, RowCount, RowGroupIndex,
	RowGroupRowCount, RowIdentityPolicy, SchemaEvolutionPolicy, SlabId, TableObjectId,
};
use wendelon::storage::multirow::MultiRowSourceRow;
use wendelon::storage::{
	ColumnFetchRequirement, ColumnOrdinalSet, PlaintextByteRange, RecordBodyByteRange,
	RecordBufferDirectory, RowGroupWriterConfig, TieredRecordId,
	encode_multirow_from_survivors_with_row_group_config, parse_record_buffer_directory,
	row_group_phase_record_body_ranges, row_groups_for_record_rows,
	selected_row_group_record_body_ranges,
};

fn descriptor(schema: &Schema) -> wendelon::schema::TableSchemaDescriptor {
	wendelon::schema::TableSchemaDescriptor::new_with_layout(
		TableObjectId::new([0x56; 16]),
		1,
		"test",
		"row_group_fetch_plan",
		schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
		LocalityPolicy::IdentityProjection,
		RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::new(4, 4, 8 * 1024 * 1024).unwrap()),
		SchemaEvolutionPolicy::AdditiveOnly,
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

fn row_group_config() -> RowGroupWriterConfig {
	RowGroupWriterConfig::point_read_with_overrides(
		RowGroupRowCount::new(4),
		RowGroupRowCount::new(4),
		RowCount::new(8),
		RecordBodyByteLen::new(1),
	)
	.with_enabled(true)
}

fn parsed_directory() -> RecordBufferDirectory {
	let batch = batch(10);
	let descriptor = descriptor(batch.schema().as_ref());
	let encoded = encode_multirow_from_survivors_with_row_group_config(
		&batch,
		&source_rows(10),
		&descriptor,
		RecordBodyCompressionPolicy::Uncompressed,
		&row_group_config(),
	)
	.unwrap();
	parse_record_buffer_directory(
		TieredRecordId {
			slab_id: SlabId::new(56),
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
}

fn output_requirement(column: u32) -> ColumnFetchRequirement {
	ColumnFetchRequirement {
		output_columns: ColumnOrdinalSet::from_columns([column]),
		..ColumnFetchRequirement::default()
	}
}

fn predicate_requirement(column: u32) -> ColumnFetchRequirement {
	ColumnFetchRequirement {
		predicate_columns: ColumnOrdinalSet::from_columns([column]),
		..ColumnFetchRequirement::default()
	}
}

fn range_contains(outer: RecordBodyByteRange, inner: RecordBodyByteRange) -> bool {
	let outer_end = outer.end().unwrap();
	let inner_end = inner.end().unwrap();
	outer.start.get() <= inner.start.get() && inner_end <= outer_end
}

fn any_range_contains(ranges: &[RecordBodyByteRange], needle: RecordBodyByteRange) -> bool {
	ranges
		.iter()
		.copied()
		.any(|range| range_contains(range, needle))
}

#[test]
fn sparse_row_group_selection_fetches_only_selected_groups_projected_buffers() {
	let directory = parsed_directory();
	let page_directory = directory.page_directory.as_ref().unwrap();
	let selected = [RowGroupIndex::new(0), RowGroupIndex::new(2)];
	let ranges = selected_row_group_record_body_ranges(
		&directory,
		page_directory,
		&output_requirement(1),
		&selected,
	)
	.unwrap();

	for page in &page_directory.buffer_pages {
		if page.ipc_column_ordinal == IpcColumnOrdinal::new(1) && page.range.len.get() > 0 {
			let expected = selected.contains(&page.row_group_index);
			assert_eq!(
				any_range_contains(&ranges, page.range),
				expected,
				"unexpected range coverage for row group {}",
				page.row_group_index.get()
			);
		}
	}
}

#[test]
fn phase_c_ranges_exclude_buffers_already_fetched_by_phase_b() {
	let directory = parsed_directory();
	let page_directory = directory.page_directory.as_ref().unwrap();
	let selected = [RowGroupIndex::new(0)];
	let phase_b_requirement = predicate_requirement(0);
	let phase_c_requirement = {
		let mut requirement = output_requirement(1);
		requirement.predicate_columns = ColumnOrdinalSet::from_columns([0]);
		requirement
	};
	let (phase_b, phase_c) = row_group_phase_record_body_ranges(
		&directory,
		page_directory,
		&phase_b_requirement,
		&phase_c_requirement,
		&selected,
	)
	.unwrap();

	for page in &page_directory.buffer_pages {
		if page.row_group_index != selected[0] || page.range.len.get() == 0 {
			continue;
		}
		if page.ipc_column_ordinal == IpcColumnOrdinal::new(0) {
			assert!(any_range_contains(&phase_b, page.range));
			assert!(!any_range_contains(&phase_c, page.range));
		}
		if page.ipc_column_ordinal == IpcColumnOrdinal::new(1) {
			assert!(any_range_contains(&phase_c, page.range));
		}
	}
}

#[test]
fn indexed_record_rows_coalesce_to_unique_row_groups() {
	let directory = parsed_directory();
	let page_directory = directory.page_directory.as_ref().unwrap();
	let groups = row_groups_for_record_rows(
		page_directory,
		&[
			RecordRowIndex::new(0),
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
