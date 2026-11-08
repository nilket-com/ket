//! ADR 0699 S3c: coordinator unit controls.

use super::*;
use crate::consensus::decisions::tests as fixture;
use crate::integration::compute::SchemaGeneration;
use crate::schema::{
	MultiRowLayoutDescriptor, PrimaryKeyDescriptor, RecordLayoutPolicy, RowIdentityPolicy,
	SchemaEvolutionPolicy, TableSpreadLocalityDescriptor,
};
use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};

const TENANT: TenantId = TenantId(7);

fn descriptor(spread: bool) -> TableSchemaDescriptor {
	let (locality, layout) = if spread {
		(
			crate::schema::LocalityPolicy::IdentityProjection,
			RecordLayoutPolicy::SingleRow,
		)
	} else {
		(
			crate::schema::LocalityPolicy::TableSpreadProjection(TableSpreadLocalityDescriptor {
				spread_factor: 1,
				prefix_bits: 16,
			}),
			RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::new(4, 8, 1024 * 1024).unwrap()),
		)
	};
	TableSchemaDescriptor::new_with_layout(
		TableObjectId::new([5; 16]),
		SchemaGeneration::new(1).get(),
		"analytics",
		"events",
		&Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("v", DataType::Utf8, true),
		]),
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
		locality,
		layout,
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.unwrap()
}

fn batch(descriptor: &TableSchemaDescriptor, ids: impl Iterator<Item = i64>) -> RecordBatch {
	let ids: Vec<i64> = ids.collect();
	let values: Vec<String> = ids.iter().map(|id| format!("v{id}")).collect();
	RecordBatch::try_new(
		Arc::new(descriptor.arrow_schema().unwrap()),
		vec![
			Arc::new(Int64Array::from(ids)),
			Arc::new(StringArray::from(values)),
		],
	)
	.unwrap()
}

fn key(descriptor: &TableSchemaDescriptor) -> ParentKey {
	ParentKey {
		tenant: TENANT,
		namespace: ParentNamespace::Table(descriptor.table_object_id),
		client_uuid: fixture::uuid_v7(fixture::T0, 3),
		sequence_no: 1,
	}
}

const HLC: HybridTimestamp = HybridTimestamp {
	physical_ms: 1_800_000_000_000,
	logical: 0,
	node_id: 1,
};

fn positions(descriptor: &TableSchemaDescriptor, batch: &RecordBatch) -> Vec<u64> {
	let meta = child_request_meta(
		&key(descriptor),
		descriptor.table_object_id,
		descriptor.schema_generation,
		batch.num_rows(),
		HLC,
	)
	.unwrap();
	let admission = validate_table_write_request_batch(
		&meta,
		batch,
		descriptor,
		super::super::child_writes::writer_of(HLC),
		HLC,
	)
	.unwrap();
	admission
		.localities
		.iter()
		.map(|locality| row_position(descriptor, *locality).unwrap())
		.collect()
}

#[test]
fn a_rows_position_is_where_storage_places_its_record() {
	for spread in [false, true] {
		let descriptor = descriptor(spread);
		for id in 0..40 {
			let one = batch(&descriptor, std::iter::once(id));
			let meta = child_request_meta(
				&key(&descriptor),
				descriptor.table_object_id,
				descriptor.schema_generation,
				1,
				HLC,
			)
			.unwrap();
			let admission = validate_table_write_request_batch(
				&meta,
				&one,
				&descriptor,
				super::super::child_writes::writer_of(HLC),
				HLC,
			)
			.unwrap();
			let bifurcated =
				bifurcate_table_write_with_admission_and_row_group_encoding_and_chunk_layout_config(
					&meta,
					&one,
					&descriptor,
					&admission,
					HLC,
					&Default::default(),
					&Default::default(),
					&Default::default(),
				)
				.unwrap();
			assert_eq!(
				positions(&descriptor, &one),
				vec![super::super::child_writes::record_position(
					&bifurcated.children[0]
				)],
				"spread {spread} id {id}"
			);
		}
	}
}

#[test]
fn the_request_digest_ignores_order_and_time_but_not_content() {
	let descriptor = descriptor(true);
	let manifest_of = |ids: Vec<i64>, hlc: HybridTimestamp, value: &str| {
		let values: Vec<String> = ids.iter().map(|_| value.to_owned()).collect();
		let batch = RecordBatch::try_new(
			Arc::new(descriptor.arrow_schema().unwrap()),
			vec![
				Arc::new(Int64Array::from(ids.clone())),
				Arc::new(StringArray::from(values)),
			],
		)
		.unwrap();
		let meta = child_request_meta(
			&key(&descriptor),
			descriptor.table_object_id,
			descriptor.schema_generation,
			batch.num_rows(),
			hlc,
		)
		.unwrap();
		let (_, manifest) = child_versions(&meta, &batch, &descriptor, hlc).unwrap();
		request_digest(TENANT, &descriptor, &manifest)
	};
	let later = HybridTimestamp {
		physical_ms: HLC.physical_ms + 9,
		..HLC
	};
	let base = manifest_of(vec![1, 2, 3], HLC, "a");
	assert_eq!(base, manifest_of(vec![3, 1, 2], HLC, "a"));
	assert_eq!(base, manifest_of(vec![1, 2, 3], later, "a"));
	assert_ne!(base, manifest_of(vec![1, 2, 3], HLC, "b"));
	assert_ne!(base, manifest_of(vec![1, 2], HLC, "a"));
}

fn table_state(descriptor: &TableSchemaDescriptor) -> SmState {
	let mut state = fixture::base(3);
	fixture::grant_all(&mut state);
	state
		.table_schemas
		.upsert(TENANT, descriptor.clone())
		.unwrap();
	state
}

#[test]
fn a_non_holder_of_the_first_row_redirects_to_that_holder() {
	let descriptor = descriptor(false);
	let state = table_state(&descriptor);
	let rows = batch(&descriptor, 0..4);
	let positions = positions(&descriptor, &rows);
	let lineages = state.decisions.as_ref().unwrap().lineages();
	let holder = lineages
		.range(..=positions[0])
		.next_back()
		.unwrap()
		.1
		.holder;
	let stranger = fixture::signer(holder.id % 3 + 1);
	let result = build_claim(
		&state,
		stranger,
		key(&descriptor),
		[1; 32],
		&descriptor,
		&rows,
		&positions,
		HLC,
	);
	assert!(matches!(result, Err(Settled::Redirect(node)) if node == holder));
}

#[test]
fn a_built_claim_is_one_the_reducer_reserves_and_its_rows_reassemble() {
	let descriptor = descriptor(true);
	let mut state = table_state(&descriptor);
	let rows = batch(&descriptor, 0..60);
	let positions = positions(&descriptor, &rows);
	let lineages = state.decisions.as_ref().unwrap().lineages();
	let first = lineages.range(..=positions[0]).next_back().unwrap().1;
	let local = Signer {
		node: first.holder.id,
		incarnation: first.incarnation,
	};
	let key = key(&descriptor);
	let built = build_claim(
		&state,
		local,
		key,
		[1; 32],
		&descriptor,
		&rows,
		&positions,
		HybridTimestamp {
			physical_ms: fixture::T0,
			logical: 0,
			node_id: local.node,
		},
	)
	.ok()
	.unwrap();
	let claim = built.clone();
	let child_rows = rows_of_claim(&claim, &positions).unwrap();
	assert!(
		claim.children.len() > 1,
		"60 spread rows cross several segments"
	);
	assert_eq!(
		child_rows.iter().map(Vec::len).sum::<usize>(),
		60,
		"every row in exactly one child"
	);
	assert!(child_rows[usize::from(claim.coordinator)].contains(&0));
	assert_eq!(claim.children[usize::from(claim.coordinator)].holder, local);
	assert!(
		child_rows
			.iter()
			.zip(&claim.children)
			.all(|(rows, child)| rows.len() == child.rows as usize),
		"each child's rows are the ones it claims"
	);
	// The reducer's own checks accept it: placement, lineages, plans, content.
	let result = fixture::decide(
		&mut state,
		crate::consensus::decisions::DecisionCommand::Reserve {
			claim: fixture::sign_for_test(claim, local),
			stamp_ms: fixture::T0,
		},
	);
	assert!(
		matches!(result, DecisionResult::Reserved { .. }),
		"{result:?}"
	);
}
