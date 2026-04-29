use std::sync::Arc;

use arrow::array::{ArrayRef, LargeBinaryArray, TimestampMillisecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use wendelon::hash::state_digest::{
	DOMAIN_TABLE_ROW_PHYSICAL, DOMAIN_TABLE_ROW_VISIBLE, StateDigest, StateDigestAccumulator,
	StateDigestElement, accumulate_projected_scalars, projected_scalar_bytes, visible_row_element,
};
use wendelon::schema::{
	DurabilityLevel, HybridTimestamp, Identity, IntraBatchIndex, Locality, LocalityPolicyV1,
	MultiRowLayoutDescriptor, RecordBodyCompressionPolicy, RecordLayoutPolicy, RowIdentityPolicy,
	SchemaEvolutionPolicyV1, TableObjectId, TableSchemaDescriptorV1, TableWriteRequestMeta,
	TenantId, WriterNodeId, encode_table_write_row_count, synthesize_append_only_row_id,
};
use wendelon::storage::{
	MultiRowSourceRow, decode_batch, pack_record_batch_by_bucket,
	pack_tombstone_multirow_source_rows_with_policy, storage_rows_from_batch,
};

fn visible_preimage(
	tenant_id: TenantId,
	table_object_id: TableObjectId,
	row_key_id: Identity,
) -> [u8; 64] {
	let mut preimage = [0u8; 64];
	preimage[..8].copy_from_slice(&tenant_id.get().to_le_bytes());
	preimage[16..32].copy_from_slice(table_object_id.as_bytes());
	preimage[32..64].copy_from_slice(row_key_id.as_bytes());
	preimage
}

fn row_key_id(index: usize) -> Identity {
	let mut preimage = [0u8; 16];
	preimage[..8].copy_from_slice(&(index as u64).to_le_bytes());
	preimage[8..].copy_from_slice(&0x4600_0000_0000_0000_u64.to_le_bytes());
	Identity::blake3(&preimage)
}

fn row_key_ids(count: usize) -> Vec<Identity> {
	(0..count).map(row_key_id).collect()
}

fn digest_rows(tenant: TenantId, table: TableObjectId, rows: &[Identity]) -> StateDigest {
	let mut accumulator = StateDigestAccumulator::new();
	for row_key_id in rows {
		accumulator.add_element(visible_row_element(tenant, table, *row_key_id));
	}
	accumulator.finish()
}

fn digest_visible_delta(
	tenant: TenantId,
	table: TableObjectId,
	adds: &[Identity],
	subtracts: &[Identity],
) -> StateDigest {
	let mut accumulator = StateDigestAccumulator::new();
	for row_key_id in adds {
		accumulator.add_element(visible_row_element(tenant, table, *row_key_id));
	}
	for row_key_id in subtracts {
		accumulator.subtract_element(visible_row_element(tenant, table, *row_key_id));
	}
	accumulator.finish()
}

fn batched_digest_rows(tenant: TenantId, table: TableObjectId, rows: &[Identity]) -> StateDigest {
	accumulate_projected_scalars(rows.iter().map(|row_key_id| {
		let preimage = visible_preimage(tenant, table, *row_key_id);
		projected_scalar_bytes(DOMAIN_TABLE_ROW_VISIBLE, &preimage)
	}))
}

fn digest_packed_visible_rows(
	tenant: TenantId,
	table: TableObjectId,
	packed: &[wendelon::storage::PackedMultiRowRecord],
) -> std::io::Result<(StateDigest, Vec<Identity>, usize)> {
	let mut accumulator = StateDigestAccumulator::new();
	let mut row_key_ids = Vec::new();
	let mut total_rows = 0usize;
	for record in packed {
		let decoded = decode_batch(&record.frame_bytes)?;
		let storage_rows = storage_rows_from_batch(&decoded)?;
		total_rows += storage_rows.len();
		for row in storage_rows {
			assert_eq!(row.table_object_id, Some(table));
			accumulator.add_element(visible_row_element(tenant, table, row.key_identity));
			row_key_ids.push(row.key_identity);
		}
	}
	row_key_ids.sort();
	Ok((accumulator.finish(), row_key_ids, total_rows))
}

fn fixture_value(key: &str) -> &'static str {
	include_str!("fixtures/state_digest/vectors.txt")
		.lines()
		.filter_map(|line| line.split_once('='))
		.find(|(name, _)| *name == key)
		.map(|(_, value)| value)
		.unwrap_or_else(|| panic!("missing fixture key {key}"))
}

fn hex(bytes: &[u8]) -> String {
	bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[test]
fn visible_row_helper_isolates_table_and_tenant() {
	let row_key = Identity::new([0x11; 32]);
	let tenant_a = TenantId::new(7);
	let tenant_b = TenantId::new(8);
	let table_a = TableObjectId::new([0x46; 16]);
	let table_b = TableObjectId::new([0x47; 16]);

	assert_ne!(
		visible_row_element(tenant_a, table_a, row_key).digest(),
		visible_row_element(tenant_a, table_b, row_key).digest()
	);
	assert_ne!(
		visible_row_element(tenant_a, table_a, row_key).digest(),
		visible_row_element(tenant_b, table_a, row_key).digest()
	);
}

#[test]
fn visible_digest_is_independent_of_row_order() {
	let tenant = TenantId::new(46);
	let table = TableObjectId::new([0x46; 16]);
	let rows = row_key_ids(10_000);
	let mut reversed = rows.clone();
	reversed.reverse();
	let mut rotated = rows.clone();
	rotated.rotate_left(137);

	let expected = digest_rows(tenant, table, &rows);
	assert_eq!(expected, digest_rows(tenant, table, &reversed));
	assert_eq!(expected, digest_rows(tenant, table, &rotated));
}

#[test]
fn split_merge_and_subtract_preserve_visible_state() {
	let tenant = TenantId::new(46);
	let table = TableObjectId::new([0x46; 16]);
	let rows = row_key_ids(2_048);
	let split_at = 1_217;
	let (left, right) = rows.split_at(split_at);

	let full = digest_rows(tenant, table, &rows);
	let left_digest = digest_rows(tenant, table, left);
	let right_digest = digest_rows(tenant, table, right);

	let mut merged = StateDigestAccumulator::new();
	merged.merge_digest(left_digest).unwrap();
	merged.merge_digest(right_digest).unwrap();
	assert_eq!(full, merged.finish());

	let mut subtracted = StateDigestAccumulator::from_digest(full).unwrap();
	subtracted.subtract_digest(right_digest).unwrap();
	assert_eq!(left_digest, subtracted.finish());
}

#[test]
fn multiset_semantics_are_not_dedupe_semantics() {
	let tenant = TenantId::new(46);
	let table = TableObjectId::new([0x46; 16]);
	let row = row_key_id(99);
	let single = digest_rows(tenant, table, &[row]);
	let double = digest_rows(tenant, table, &[row, row]);

	assert_ne!(single, double);

	let mut subtracted = StateDigestAccumulator::from_digest(double).unwrap();
	subtracted.subtract_element(visible_row_element(tenant, table, row));
	assert_eq!(single, subtracted.finish());
}

#[test]
fn domain_isolation_holds_for_identical_preimages() {
	let tenant = TenantId::new(46);
	let table = TableObjectId::new([0x46; 16]);
	let row = row_key_id(1);
	let preimage = visible_preimage(tenant, table, row);
	let visible = StateDigestElement::project(DOMAIN_TABLE_ROW_VISIBLE, &preimage).digest();
	let physical = StateDigestElement::project(DOMAIN_TABLE_ROW_PHYSICAL, &preimage).digest();

	assert_ne!(visible, physical);
}

#[test]
fn batched_accumulation_matches_per_element_for_l2_shaped_batch() {
	let tenant = TenantId::new(46);
	let table = TableObjectId::new([0x46; 16]);
	let rows = row_key_ids(10_000);

	assert_eq!(
		digest_rows(tenant, table, &rows),
		batched_digest_rows(tenant, table, &rows)
	);
}

#[test]
fn golden_fixture_visible_row_vector_matches_helper() {
	let tenant = TenantId::new(0x0102_0304_0506_0708);
	let table = TableObjectId::new([0x46; 16]);
	let row = Identity::new([0xa5; 32]);
	let digest = visible_row_element(tenant, table, row).digest();

	assert_eq!(
		hex(digest.as_bytes()),
		fixture_value("single_visible_row_element")
	);
}

#[test]
fn append_only_row_key_ids_feed_visible_row_digest() {
	let tenant = TenantId::new(7);
	let table = TableObjectId::new([0x45; 16]);
	let writer = WriterNodeId::new([0xab; 16]);
	let row_hlc = HybridTimestamp {
		physical_ms: 1_700_000_000_777,
		logical: 42,
		node_id: 11,
	};
	let row_key_id =
		synthesize_append_only_row_id(tenant, table, writer, row_hlc, IntraBatchIndex::ZERO);
	let preimage = visible_preimage(tenant, table, row_key_id);

	assert_eq!(
		visible_row_element(tenant, table, row_key_id).digest(),
		StateDigestElement::project(DOMAIN_TABLE_ROW_VISIBLE, &preimage).digest()
	);
}

#[test]
fn identity_and_digest_preimages_are_isolated() {
	let tenant = TenantId::new(7);
	let table = TableObjectId::new([0x45; 16]);
	let writer = WriterNodeId::new([0xab; 16]);
	let row_hlc = HybridTimestamp {
		physical_ms: 1_700_000_000_777,
		logical: 42,
		node_id: 11,
	};
	let row_key_id =
		synthesize_append_only_row_id(tenant, table, writer, row_hlc, IntraBatchIndex::ZERO);
	let digest_preimage = visible_preimage(tenant, table, row_key_id);
	let digest_scalar = projected_scalar_bytes(DOMAIN_TABLE_ROW_VISIBLE, &digest_preimage);

	assert_ne!(digest_scalar, row_key_id.into_bytes());
}

#[test]
fn visible_digest_survives_multirow_repacking_boundaries() {
	let tenant = TenantId::new(46);
	let table = TableObjectId::new([0x46; 16]);
	let writer = WriterNodeId::new([0x46; 16]);
	let commit_hlc = HybridTimestamp {
		physical_ms: 1_700_000_046_000,
		logical: 3,
		node_id: 46,
	};
	let rows = 96usize;
	let batch = append_only_batch(rows);
	let descriptor_small = append_only_descriptor(
		table,
		MultiRowLayoutDescriptor::new(7, 64, 1024 * 1024).unwrap(),
	);
	let descriptor_large = append_only_descriptor(
		table,
		MultiRowLayoutDescriptor::new(31, 64, 1024 * 1024).unwrap(),
	);
	let meta = table_meta(tenant, &descriptor_small, rows);

	let packed_small =
		pack_record_batch_by_bucket(&meta, &descriptor_small, &batch, writer, commit_hlc)
			.expect("pack small multi-row records");
	let packed_large =
		pack_record_batch_by_bucket(&meta, &descriptor_large, &batch, writer, commit_hlc)
			.expect("pack large multi-row records");
	assert!(
		packed_small.len() > packed_large.len(),
		"test must exercise different multi-row grouping boundaries"
	);

	let (small_digest, small_keys, small_rows) =
		digest_packed_visible_rows(tenant, table, &packed_small).unwrap();
	let (large_digest, large_keys, large_rows) =
		digest_packed_visible_rows(tenant, table, &packed_large).unwrap();

	assert_eq!(small_rows, rows);
	assert_eq!(large_rows, rows);
	assert_eq!(small_keys, large_keys);
	assert_eq!(small_digest, large_digest);
}

#[test]
fn row_tombstone_subtracts_visible_row_digest() {
	let tenant = TenantId::new(47);
	let table = TableObjectId::new([0x47; 16]);
	let writer = WriterNodeId::new([0x47; 16]);
	let commit_hlc = HybridTimestamp {
		physical_ms: 1_700_000_047_000,
		logical: 1,
		node_id: 47,
	};
	let delete_hlc = HybridTimestamp {
		physical_ms: 1_700_000_047_100,
		logical: 0,
		node_id: 47,
	};
	let layout = MultiRowLayoutDescriptor::new(8, 8, 1024 * 1024).unwrap();
	let descriptor = append_only_descriptor(table, layout);
	let batch = append_only_batch(3);
	let meta = table_meta(tenant, &descriptor, batch.num_rows());
	let packed = pack_record_batch_by_bucket(&meta, &descriptor, &batch, writer, commit_hlc)
		.expect("pack data rows");
	let mut data_rows = Vec::new();
	for record in &packed {
		let decoded = decode_batch(&record.frame_bytes).unwrap();
		data_rows.extend(storage_rows_from_batch(&decoded).unwrap());
	}
	data_rows.sort_by_key(|row| row.key_identity);
	assert_eq!(data_rows.len(), 3);
	assert!(data_rows.iter().all(|row| !row.is_tombstone));

	let deleted = data_rows[1];
	let tombstones = pack_tombstone_multirow_source_rows_with_policy(
		layout,
		&[MultiRowSourceRow {
			source_row_index: 0,
			key_identity: deleted.key_identity,
			routing_locality: Locality::new(deleted.routing_locality.get()),
			row_hlc: delete_hlc,
			is_tombstone: true,
			table_object_id: Some(table),
		}],
		RecordBodyCompressionPolicy::Uncompressed,
	)
	.unwrap();
	let tombstone_batch = decode_batch(&tombstones[0].encoded.frame).unwrap();
	let tombstone_rows = storage_rows_from_batch(&tombstone_batch).unwrap();
	assert_eq!(tombstone_rows.len(), 1);
	assert!(tombstone_rows[0].is_tombstone);

	let mut actual = StateDigestAccumulator::new();
	for row in &data_rows {
		actual.add_element(visible_row_element(tenant, table, row.key_identity));
	}
	for row in &tombstone_rows {
		actual.subtract_element(visible_row_element(tenant, table, row.key_identity));
	}
	let survivors = vec![data_rows[0].key_identity, data_rows[2].key_identity];
	assert_eq!(actual.finish(), digest_rows(tenant, table, &survivors));
}

#[test]
fn row_tombstone_digest_is_order_independent_and_merge_equivalent() {
	let tenant = TenantId::new(47);
	let table = TableObjectId::new([0x47; 16]);
	let rows = row_key_ids(5);
	let tombstones = vec![rows[1], rows[3]];
	let expected = digest_rows(tenant, table, &[rows[0], rows[2], rows[4]]);

	assert_eq!(
		expected,
		digest_visible_delta(tenant, table, &rows, &tombstones)
	);

	let mut interleaved = StateDigestAccumulator::new();
	interleaved.subtract_element(visible_row_element(tenant, table, rows[3]));
	interleaved.add_element(visible_row_element(tenant, table, rows[0]));
	interleaved.add_element(visible_row_element(tenant, table, rows[1]));
	interleaved.subtract_element(visible_row_element(tenant, table, rows[1]));
	interleaved.add_element(visible_row_element(tenant, table, rows[2]));
	interleaved.add_element(visible_row_element(tenant, table, rows[3]));
	interleaved.add_element(visible_row_element(tenant, table, rows[4]));
	assert_eq!(expected, interleaved.finish());

	let slab_a = digest_visible_delta(tenant, table, &[rows[0], rows[1]], &[rows[1]]);
	let slab_b = digest_visible_delta(tenant, table, &[rows[2], rows[3], rows[4]], &[rows[3]]);
	let mut merged = StateDigestAccumulator::new();
	merged.merge_digest(slab_a).unwrap();
	merged.merge_digest(slab_b).unwrap();
	assert_eq!(expected, merged.finish());
}

#[test]
fn row_tombstone_pair_gc_and_retombstone_noop_preserve_digest() {
	let tenant = TenantId::new(47);
	let table = TableObjectId::new([0x47; 16]);
	let row = row_key_id(707);
	let element = visible_row_element(tenant, table, row);

	let mut pair = StateDigestAccumulator::new();
	pair.add_element(element);
	pair.subtract_element(element);
	assert_eq!(StateDigest::ZERO, pair.finish());
	assert_eq!(StateDigest::ZERO, StateDigestAccumulator::new().finish());

	let mut first_delete = StateDigestAccumulator::from_digest(digest_rows(tenant, table, &[row]))
		.expect("live row digest should decompress");
	first_delete.subtract_element(element);
	assert_eq!(StateDigest::ZERO, first_delete.finish());

	let mut duplicate_delete =
		StateDigestAccumulator::from_digest(digest_rows(tenant, table, &[row]))
			.expect("live row digest should decompress");
	duplicate_delete.subtract_element(element);
	duplicate_delete.subtract_element(element);
	assert_ne!(StateDigest::ZERO, duplicate_delete.finish());
}

fn append_only_descriptor(
	table_object_id: TableObjectId,
	layout: MultiRowLayoutDescriptor,
) -> TableSchemaDescriptorV1 {
	let schema = Schema::new(vec![
		Field::new(
			"event_time",
			DataType::Timestamp(TimeUnit::Millisecond, None),
			false,
		),
		Field::new("payload", DataType::LargeBinary, true),
	]);
	TableSchemaDescriptorV1::new_with_layout(
		table_object_id,
		1,
		"analytics",
		"events",
		&schema,
		RowIdentityPolicy::AppendOnly,
		LocalityPolicyV1::default_table_spread(),
		RecordLayoutPolicy::MultiRow(layout),
		SchemaEvolutionPolicyV1::AdditiveOnly,
	)
	.unwrap()
}

fn append_only_batch(rows: usize) -> RecordBatch {
	let times = (0..rows)
		.map(|row| 1_700_000_000_000_i64 + row as i64)
		.collect::<Vec<_>>();
	let payloads = (0..rows)
		.map(|row| {
			let mut payload = vec![0u8; 32];
			payload[..8].copy_from_slice(&(row as u64).to_le_bytes());
			payload
		})
		.collect::<Vec<_>>();
	let payload_refs = payloads
		.iter()
		.map(|payload| Some(payload.as_slice()))
		.collect::<Vec<_>>();
	RecordBatch::try_new(
		Arc::new(Schema::new(vec![
			Field::new(
				"event_time",
				DataType::Timestamp(TimeUnit::Millisecond, None),
				false,
			),
			Field::new("payload", DataType::LargeBinary, true),
		])),
		vec![
			Arc::new(TimestampMillisecondArray::from(times)) as ArrayRef,
			Arc::new(LargeBinaryArray::from(payload_refs)) as ArrayRef,
		],
	)
	.unwrap()
}

fn table_meta(
	tenant_id: TenantId,
	descriptor: &TableSchemaDescriptorV1,
	rows: usize,
) -> TableWriteRequestMeta {
	let mut meta: TableWriteRequestMeta = bytemuck::Zeroable::zeroed();
	meta.tenant_id = tenant_id;
	meta.table_object_id = descriptor.table_object_id;
	meta.schema_generation = descriptor.schema_generation;
	meta.row_count = encode_table_write_row_count(rows).unwrap();
	meta.durability = DurabilityLevel::QUORUM;
	meta.client_hlc = HybridTimestamp {
		physical_ms: 1_700_000_045_000,
		logical: 0,
		node_id: 46,
	};
	meta
}
