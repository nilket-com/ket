//! ADR 045 — append-only HLC / identity invariants.
//!
//! These integration tests exercise the end-to-end admission /
//! packing / encode / decode flow and assert the §4 invariant
//! that the HLC fed into the `__row_key_id__` BLAKE3 preimage is
//! the same HLC persisted in the row's `__row_hlc__` synthetic
//! column. The leader-merged commit HLC is computed once per
//! request and reused across both surfaces. Recovering the same
//! identity from the stored HLC is the load-bearing invariant
//! that lets the V1 preimage omit the canonical row payload hash.

use std::sync::Arc;

use arrow::array::{ArrayRef, LargeBinaryArray, TimestampMillisecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;

use wendelon::schema::table::{
	MultiRowLayoutDescriptor, TableSchemaDescriptorV1, TableWriteRequestMeta,
	encode_table_write_row_count, synthesize_append_only_row_id,
	validate_table_write_request_batch,
};
use wendelon::schema::{
	HybridTimestamp, IntraBatchIndex, LocalityPolicyV1, RecordLayoutPolicy, RowIdentityPolicy,
	SchemaEvolutionPolicyV1, TableObjectId, TenantId, WriterNodeId,
};
use wendelon::storage::multirow::{
	decode_batch, pack_record_batch_by_bucket, storage_row_table_object_id_at,
	storage_rows_from_batch, strip_storage_synthetic_columns,
};

fn append_only_descriptor() -> TableSchemaDescriptorV1 {
	let schema = Schema::new(vec![
		Field::new(
			"event_time",
			DataType::Timestamp(TimeUnit::Millisecond, None),
			false,
		),
		Field::new("payload", DataType::LargeBinary, true),
	]);
	TableSchemaDescriptorV1::new_with_layout(
		TableObjectId::new([0x45; 16]),
		1,
		"analytics",
		"events",
		&schema,
		RowIdentityPolicy::AppendOnly,
		LocalityPolicyV1::default_table_spread(),
		RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::default()),
		SchemaEvolutionPolicyV1::AdditiveOnly,
	)
	.unwrap()
}

fn append_only_batch(payloads: Vec<Option<&[u8]>>, times: Vec<i64>) -> RecordBatch {
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
			Arc::new(LargeBinaryArray::from(payloads)) as ArrayRef,
		],
	)
	.unwrap()
}

fn meta(descriptor: &TableSchemaDescriptorV1, rows: usize) -> TableWriteRequestMeta {
	let mut meta: TableWriteRequestMeta = bytemuck::Zeroable::zeroed();
	meta.tenant_id = TenantId::new(7);
	meta.table_object_id = descriptor.table_object_id;
	meta.schema_generation = descriptor.schema_generation;
	meta.row_count = encode_table_write_row_count(rows).unwrap();
	meta.client_hlc = HybridTimestamp {
		physical_ms: 10,
		logical: 0,
		node_id: 1,
	};
	meta
}

#[test]
fn append_only_stored_row_hlc_matches_identity_synthesis_hlc() {
	let descriptor = append_only_descriptor();
	let batch = append_only_batch(
		vec![Some(b"alpha"), Some(b"beta"), Some(b"alpha")],
		vec![10_000, 20_000, 30_000],
	);
	let meta = meta(&descriptor, batch.num_rows());
	// Stand in for the leader-merged commit HLC. Distinct from the
	// `meta.client_hlc` set above so that any path still feeding
	// admission `client_hlc` would synthesize a different identity.
	let commit_hlc = HybridTimestamp {
		physical_ms: 1_700_000_000_777,
		logical: 42,
		node_id: 11,
	};
	// Phase 8: WriterNodeId is bound to the admitting server's
	// authenticated NodeId, no longer to `meta.proxy_source`. The
	// test stamps a deterministic non-zero writer to mimic the
	// server.
	let writer = WriterNodeId::new([0xab; 16]);
	let packed = pack_record_batch_by_bucket(&meta, &descriptor, &batch, writer, commit_hlc)
		.expect("append-only multi-row pack");
	assert!(!packed.is_empty());
	let mut total_rows = 0usize;
	for record in &packed {
		let decoded = decode_batch(&record.frame_bytes).unwrap();
		let storage_rows = storage_rows_from_batch(&decoded).unwrap();
		for (idx_in_record, row) in storage_rows.iter().enumerate() {
			assert_eq!(
				row.row_hlc, commit_hlc,
				"stored __row_hlc__ must equal the leader-merged commit HLC"
			);
			// The encoded record may reorder rows by the ADR 042
			// deterministic sort — recover the original position from
			// `record.source_row_indices`.
			let source_idx = record.source_row_indices[idx_in_record];
			let intra_batch_index = IntraBatchIndex::try_new(source_idx as u32).unwrap();
			let expected = synthesize_append_only_row_id(
				meta.tenant_id,
				descriptor.table_object_id,
				writer,
				row.row_hlc,
				intra_batch_index,
			);
			assert_eq!(
				row.key_identity, expected,
				"stored __row_key_id__ must derive from stored __row_hlc__ \
				 (ADR 045 §4 invariant — see Phase 7 HLC-split fix)"
			);
			total_rows += 1;
		}
	}
	assert_eq!(total_rows, batch.num_rows());
}

#[test]
fn append_only_distinct_commit_hlcs_produce_distinct_identities_at_e2e() {
	// Two batches with byte-identical payloads, identical
	// `meta.client_hlc`, identical writer, but distinct
	// leader-merged commit HLCs MUST produce distinct identities.
	// This is the regression guard for the HLC-split correctness
	// bug fixed in Phase 7: admission used to consume
	// `meta.client_hlc` while storage persisted `commit_hlc`,
	// which (without payload hashing) would have collapsed
	// identical-payload retries from the same client into one
	// identity.
	let descriptor = append_only_descriptor();
	let batch_a = append_only_batch(vec![Some(b"x")], vec![10_000]);
	let batch_b = append_only_batch(vec![Some(b"x")], vec![10_000]);
	let meta_a = meta(&descriptor, batch_a.num_rows());
	let meta_b = meta(&descriptor, batch_b.num_rows());
	assert_eq!(meta_a.client_hlc, meta_b.client_hlc);
	assert_eq!(meta_a.proxy_source, meta_b.proxy_source);

	let commit_hlc_a = HybridTimestamp {
		physical_ms: 1_700_000_000_000,
		logical: 1,
		node_id: 1,
	};
	let commit_hlc_b = HybridTimestamp {
		physical_ms: 1_700_000_000_000,
		logical: 2,
		node_id: 1,
	};
	assert_ne!(commit_hlc_a, commit_hlc_b);

	let writer = WriterNodeId::new([0xab; 16]);
	let packed_a =
		pack_record_batch_by_bucket(&meta_a, &descriptor, &batch_a, writer, commit_hlc_a)
			.expect("pack a");
	let packed_b =
		pack_record_batch_by_bucket(&meta_b, &descriptor, &batch_b, writer, commit_hlc_b)
			.expect("pack b");

	let id_a = packed_a[0].key_identity;
	let id_b = packed_b[0].key_identity;
	assert_ne!(
		id_a, id_b,
		"distinct commit HLCs must produce distinct identities even when \
		 client_hlc, payload, writer, and intra_batch_index all match"
	);
}

#[test]
fn append_only_locality_is_stable_across_idempotency_ttl_changes() {
	// ADR 045 §4a / Phase 8 contract: the append-only locality
	// salt hashes only the canonical idempotency identity
	// (`client_uuid` + `sequence_no`). A retry of the same logical
	// write whose `idempotency_ttl_ms` (or `_pad`) differs but
	// whose canonical bytes are unchanged MUST route to the same
	// locality, otherwise idempotency convergence breaks (the
	// retry would land on a different leader and not see the
	// previous attempt's pending finalization).
	use wendelon::schema::ClientWriteId;
	let descriptor = append_only_descriptor();
	let batch = append_only_batch(vec![Some(b"x")], vec![10_000]);

	let mut meta_a = meta(&descriptor, batch.num_rows());
	let mut meta_b = meta_a;
	meta_a.parent_write_id = ClientWriteId {
		client_uuid: [0xAB; 16],
		sequence_no: 42,
		idempotency_ttl_ms: 5_000,
		_pad: 0,
	};
	meta_b.parent_write_id = ClientWriteId {
		client_uuid: [0xAB; 16],
		sequence_no: 42,
		idempotency_ttl_ms: 60_000,
		_pad: 0xDEAD_BEEF,
	};
	assert_ne!(
		meta_a.parent_write_id, meta_b.parent_write_id,
		"struct equality includes TTL / pad — that is the whole point of the test"
	);

	let admission_a = validate_table_write_request_batch(
		&meta_a,
		&batch,
		&descriptor,
		WriterNodeId::ZERO,
		meta_a.client_hlc,
	)
	.expect("admit a");
	let admission_b = validate_table_write_request_batch(
		&meta_b,
		&batch,
		&descriptor,
		WriterNodeId::ZERO,
		meta_b.client_hlc,
	)
	.expect("admit b");
	assert_eq!(
		admission_a.localities, admission_b.localities,
		"same canonical idempotency identity must route to the same locality \
		 even when TTL / pad differ"
	);
}

#[test]
fn append_only_client_preview_locality_matches_server_admission() {
	// ADR 045 §4a / Phase 8: the locality computed on the client
	// preview path (with `WriterNodeId::ZERO` and `client_hlc` as
	// the row HLC) must match the locality computed on the
	// server's authoritative admission path (with the server's
	// authenticated WriterNodeId and the leader-merged
	// `commit_hlc`). This is the regression guard for the routing
	// divergence the Phase 8 decoupling fixes — without it, the
	// lease gate would trip on AppendOnly writes whose client
	// preview routed to one range but whose server-side admission
	// computed a different range from the commit HLC's per-row
	// identity.
	let descriptor = append_only_descriptor();
	let batch = append_only_batch(
		vec![Some(b"alpha"), Some(b"beta"), Some(b"gamma")],
		vec![10_000, 20_000, 30_000],
	);
	let meta = meta(&descriptor, batch.num_rows());

	// Client preview: zero writer, client_hlc as row HLC.
	let preview = validate_table_write_request_batch(
		&meta,
		&batch,
		&descriptor,
		WriterNodeId::ZERO,
		meta.client_hlc,
	)
	.expect("client preview admission");

	// Server admission: server's authenticated writer, commit_hlc.
	let server_writer = WriterNodeId::new([0xab; 16]);
	let commit_hlc = HybridTimestamp {
		physical_ms: 1_700_000_000_777,
		logical: 42,
		node_id: 11,
	};
	let server =
		validate_table_write_request_batch(&meta, &batch, &descriptor, server_writer, commit_hlc)
			.expect("server admission");

	// All localities are equal across preview and server, and all
	// rows in a request share one locality.
	assert_eq!(preview.localities, server.localities);
	let first = server.localities[0];
	for locality in &server.localities {
		assert_eq!(*locality, first, "AppendOnly: one locality per request");
	}
	// Identities differ — preview used ZERO writer + client_hlc;
	// server used the authenticated writer + commit_hlc. Phase 7
	// already covers the identity-uniqueness invariant.
	assert_ne!(preview.key_identities, server.key_identities);
}

#[test]
fn append_only_storage_record_carries_per_row_table_object_id() {
	// ADR 045 §5 / Phase 8: AppendOnly multi-row records carry a
	// per-row `__row_table_object_id__` synthetic column with
	// the descriptor's table_object_id stamped on every row.
	// This is the load-bearing signal that Phase 8's matchers
	// use to isolate AppendOnly rows from cross-table
	// table-spread prefix collisions.
	let descriptor = append_only_descriptor();
	let batch = append_only_batch(
		vec![Some(b"a"), Some(b"b"), Some(b"c")],
		vec![10_000, 20_000, 30_000],
	);
	let meta = meta(&descriptor, batch.num_rows());
	let writer = WriterNodeId::new([0xab; 16]);
	let commit_hlc = HybridTimestamp {
		physical_ms: 1_700_000_000_777,
		logical: 7,
		node_id: 1,
	};
	let packed =
		pack_record_batch_by_bucket(&meta, &descriptor, &batch, writer, commit_hlc).expect("pack");
	assert!(!packed.is_empty());
	for record in &packed {
		let decoded = decode_batch(&record.frame_bytes).unwrap();
		// Every row of every record must carry the descriptor's
		// table_object_id.
		let storage_rows = storage_rows_from_batch(&decoded).unwrap();
		assert!(!storage_rows.is_empty());
		for row in &storage_rows {
			assert_eq!(
				row.table_object_id,
				Some(descriptor.table_object_id),
				"AppendOnly storage row must carry __row_table_object_id__ \
				 stamped from descriptor (ADR 045 §5 / Phase 8)"
			);
		}
		// The accessor by record_row_index must agree.
		for row in &storage_rows {
			let by_index = storage_row_table_object_id_at(&decoded, row.record_row_index).unwrap();
			assert_eq!(by_index, Some(descriptor.table_object_id));
		}
		// `__row_table_object_id__` must NOT survive into the
		// user-facing decoded batch.
		let user = strip_storage_synthetic_columns(&decoded).unwrap();
		assert!(user.column_by_name("__row_table_object_id__").is_none());
		assert_eq!(user.schema().as_ref(), batch.schema().as_ref());
	}
}

#[test]
fn append_only_matcher_isolates_distinct_descriptors_with_same_user_schema() {
	// ADR 045 §5 / Phase 8: two AppendOnly tables in the same
	// tenant with the same user Arrow schema MUST be isolated
	// from each other on the read side. Pack a row for table A
	// and verify that its stored `__row_table_object_id__` does
	// NOT match a same-shape descriptor for table B.
	let descriptor_a = {
		use arrow::datatypes::TimeUnit;
		let schema = Schema::new(vec![
			Field::new(
				"event_time",
				DataType::Timestamp(TimeUnit::Millisecond, None),
				false,
			),
			Field::new("payload", DataType::LargeBinary, true),
		]);
		TableSchemaDescriptorV1::new_with_layout(
			TableObjectId::new([0x45; 16]),
			1,
			"analytics",
			"events_a",
			&schema,
			RowIdentityPolicy::AppendOnly,
			LocalityPolicyV1::default_table_spread(),
			RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::default()),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.unwrap()
	};
	let descriptor_b = {
		use arrow::datatypes::TimeUnit;
		let schema = Schema::new(vec![
			Field::new(
				"event_time",
				DataType::Timestamp(TimeUnit::Millisecond, None),
				false,
			),
			Field::new("payload", DataType::LargeBinary, true),
		]);
		TableSchemaDescriptorV1::new_with_layout(
			TableObjectId::new([0xBB; 16]),
			1,
			"analytics",
			"events_b",
			&schema,
			RowIdentityPolicy::AppendOnly,
			LocalityPolicyV1::default_table_spread(),
			RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::default()),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.unwrap()
	};
	assert_ne!(descriptor_a.table_object_id, descriptor_b.table_object_id);

	let batch = append_only_batch(vec![Some(b"event")], vec![10_000]);
	let mut meta_a = meta(&descriptor_a, batch.num_rows());
	meta_a.table_object_id = descriptor_a.table_object_id;
	let writer = WriterNodeId::new([0xab; 16]);
	let commit_hlc = HybridTimestamp {
		physical_ms: 1_700_000_000_000,
		logical: 1,
		node_id: 1,
	};
	let packed = pack_record_batch_by_bucket(&meta_a, &descriptor_a, &batch, writer, commit_hlc)
		.expect("pack a");

	let decoded = decode_batch(&packed[0].frame_bytes).unwrap();
	let stored =
		storage_row_table_object_id_at(&decoded, wendelon::schema::RecordRowIndex::new(0)).unwrap();
	assert_eq!(stored, Some(descriptor_a.table_object_id));
	assert_ne!(
		stored,
		Some(descriptor_b.table_object_id),
		"row stamped for table A must not match table B's table_object_id"
	);
}
