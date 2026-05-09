use arrow::array::{Array, ArrayRef, BooleanArray, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use wendelon::schema::{
	LocalityPolicyV1, MultiRowLayoutDescriptor, PrimaryKeyDescriptorV1, RecordBodyByteLen,
	RecordBodyByteOffset, RecordLayoutPolicy, RecordRowIndex, RowCount, RowGroupIndex,
	RowGroupRowCount, RowIdentityPolicy, SchemaEvolutionPolicyV1, SlabId, TableObjectId,
};
use wendelon::storage::{
	ColumnFetchRequirement, ColumnOrdinalSet, PhaseBOutcome, ProjectedRecordBatch,
	RecordBodyByteRange, RecordPageDirectory, RowGroupDirectoryEntry, TieredRecordId,
	apply_phase_b_masks_to_projected_row_groups, project_record_batch_for_requirement,
	zip_predicate_and_projection_columns,
};

fn batch() -> RecordBatch {
	let schema = Arc::new(Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("value", DataType::Int64, false),
	]));
	RecordBatch::try_new(
		schema,
		vec![
			Arc::new(Int64Array::from(vec![1, 2, 3, 4])) as ArrayRef,
			Arc::new(Int64Array::from(vec![10, 20, 30, 40])) as ArrayRef,
		],
	)
	.unwrap()
}

fn descriptor(schema: &Schema) -> wendelon::schema::TableSchemaDescriptorV1 {
	wendelon::schema::TableSchemaDescriptorV1::new_with_layout(
		TableObjectId::new([0x56; 16]),
		1,
		"test",
		"row_group_materialization",
		schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("id")),
		LocalityPolicyV1::IdentityProjection,
		RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::new(4, 4, 8 * 1024 * 1024).unwrap()),
		SchemaEvolutionPolicyV1::AdditiveOnly,
	)
	.unwrap()
}

fn output_requirement() -> ColumnFetchRequirement {
	ColumnFetchRequirement {
		output_columns: ColumnOrdinalSet::from_columns([1]),
		..ColumnFetchRequirement::default()
	}
}

fn predicate_requirement() -> ColumnFetchRequirement {
	ColumnFetchRequirement {
		predicate_columns: ColumnOrdinalSet::from_columns([0]),
		..ColumnFetchRequirement::default()
	}
}

fn predicate_and_output_requirement() -> ColumnFetchRequirement {
	ColumnFetchRequirement {
		predicate_columns: ColumnOrdinalSet::from_columns([0]),
		output_columns: ColumnOrdinalSet::from_columns([0, 1]),
		..ColumnFetchRequirement::default()
	}
}

fn output_then_predicate_requirement() -> ColumnFetchRequirement {
	ColumnFetchRequirement {
		predicate_columns: ColumnOrdinalSet::from_columns([1]),
		output_columns: ColumnOrdinalSet::from_columns([0, 1]),
		..ColumnFetchRequirement::default()
	}
}

fn predicate_value_requirement() -> ColumnFetchRequirement {
	ColumnFetchRequirement {
		predicate_columns: ColumnOrdinalSet::from_columns([1]),
		..ColumnFetchRequirement::default()
	}
}

fn id_output_requirement() -> ColumnFetchRequirement {
	ColumnFetchRequirement {
		output_columns: ColumnOrdinalSet::from_columns([0]),
		..ColumnFetchRequirement::default()
	}
}

fn page_directory() -> RecordPageDirectory {
	let batch = batch();
	RecordPageDirectory {
		record_id: TieredRecordId {
			slab_id: SlabId::new(56),
			record_offset: RecordBodyByteOffset::ZERO,
		},
		row_count: RowCount::new(4),
		row_groups: vec![RowGroupDirectoryEntry {
			row_group_index: RowGroupIndex::new(0),
			first_record_row_index: RecordRowIndex::ZERO,
			row_count: RowGroupRowCount::new(4),
			metadata_range: RecordBodyByteRange {
				start: RecordBodyByteOffset::ZERO,
				len: RecordBodyByteLen::new(1),
			},
			body_range: RecordBodyByteRange {
				start: RecordBodyByteOffset::new(1),
				len: RecordBodyByteLen::new(1),
			},
		}],
		buffer_pages: Vec::new(),
		row_group_stats: Vec::new(),
		logical_schema_fingerprint: descriptor(batch.schema().as_ref()).logical_schema_fingerprint,
	}
}

fn projected_output() -> ProjectedRecordBatch {
	project_record_batch_for_requirement(&batch(), &output_requirement()).unwrap()
}

#[test]
fn predicate_projection_zip_prefers_predicate_side_for_overlapping_output() {
	let source = batch();
	let predicate =
		project_record_batch_for_requirement(&source, &predicate_requirement()).unwrap();
	let projection = project_record_batch_for_requirement(&source, &output_requirement()).unwrap();
	let zipped = zip_predicate_and_projection_columns(
		&predicate,
		&projection,
		&predicate_and_output_requirement(),
	)
	.unwrap();

	let ids = zipped
		.batch
		.column(zipped.physical_for(0).unwrap())
		.as_any()
		.downcast_ref::<Int64Array>()
		.unwrap();
	let values = zipped
		.batch
		.column(zipped.physical_for(1).unwrap())
		.as_any()
		.downcast_ref::<Int64Array>()
		.unwrap();
	assert_eq!(ids.values(), &[1, 2, 3, 4]);
	assert_eq!(values.values(), &[10, 20, 30, 40]);
}

#[test]
fn predicate_projection_zip_handles_projection_first_output_order() {
	let source = batch();
	let predicate =
		project_record_batch_for_requirement(&source, &predicate_value_requirement()).unwrap();
	let projection =
		project_record_batch_for_requirement(&source, &id_output_requirement()).unwrap();
	let zipped = zip_predicate_and_projection_columns(
		&predicate,
		&projection,
		&output_then_predicate_requirement(),
	)
	.unwrap();

	let ids = zipped
		.batch
		.column(zipped.physical_for(0).unwrap())
		.as_any()
		.downcast_ref::<Int64Array>()
		.unwrap();
	let values = zipped
		.batch
		.column(zipped.physical_for(1).unwrap())
		.as_any()
		.downcast_ref::<Int64Array>()
		.unwrap();
	assert_eq!(ids.values(), &[1, 2, 3, 4]);
	assert_eq!(values.values(), &[10, 20, 30, 40]);
	assert_eq!(zipped.batch.schema().field(0).name(), "id");
	assert_eq!(zipped.batch.schema().field(1).name(), "value");
}

#[test]
fn phase_b_mask_filters_projected_phase_c_batch_without_rerunning_pbc() {
	let group = RowGroupIndex::new(0);
	let mut phase_b = PhaseBOutcome::default();
	phase_b
		.survival_masks
		.insert(group, BooleanArray::from(vec![true, false, true, false]));

	let output = apply_phase_b_masks_to_projected_row_groups(
		vec![(group, projected_output())],
		&phase_b,
		&[group],
		&page_directory(),
	)
	.unwrap();

	let values = output
		.batch
		.column(0)
		.as_any()
		.downcast_ref::<Int64Array>()
		.unwrap();
	assert_eq!(values.values(), &[10, 30]);
}

#[test]
fn zero_phase_b_mask_preserves_projected_schema() {
	let group = RowGroupIndex::new(0);
	let mut phase_b = PhaseBOutcome::default();
	phase_b
		.survival_masks
		.insert(group, BooleanArray::from(vec![false, false, false, false]));

	let output = apply_phase_b_masks_to_projected_row_groups(
		vec![(group, projected_output())],
		&phase_b,
		&[group],
		&page_directory(),
	)
	.unwrap();

	assert_eq!(output.batch.num_rows(), 0);
	assert_eq!(output.batch.num_columns(), 1);
	assert_eq!(output.batch.schema().field(0).name(), "value");
}

#[test]
fn phase_b_mask_length_must_cover_full_row_group() {
	let group = RowGroupIndex::new(0);
	let mut phase_b = PhaseBOutcome::default();
	phase_b
		.survival_masks
		.insert(group, BooleanArray::from(vec![true, false]));

	let err = apply_phase_b_masks_to_projected_row_groups(
		vec![(group, projected_output())],
		&phase_b,
		&[group],
		&page_directory(),
	)
	.unwrap_err();

	assert!(err.to_string().contains("survival mask length"));
}
