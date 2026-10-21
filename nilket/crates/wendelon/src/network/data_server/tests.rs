use super::*;
// ADR 077: the encode quanta live in their own submodule; the fixtures that pin their
// bounds reach them directly rather than widening the root's imports for test-only use.
use super::query_encode::{
	ENCODE_QUANTUM_BYTES, ENCODE_QUANTUM_ROWS, bounded_blocking_encode, encode_blocking_slot_count,
	encode_quantum_len,
};
use crate::crypto::CryptoManager;
use crate::integration::compute::{
	COMPUTE_STORAGE_CONTRACT_VERSION, SchemaGeneration, SchemaIntentId, SchemaIntentOrigin,
	SchemaMutationKind, SchemaObjectKind, SchemaObjectRef, SchemaUpdateEnvelope,
	encode_schema_update_envelope_v1,
};
use crate::schema::pbc::{
	OP_AND, OP_EQ, OP_GTE, OP_OR, OP_PROJECT, OP_PUSH_COL, OP_PUSH_INT, OP_PUSH_STR,
};
use crate::schema::{
	AGG_FORMAT_VERSION, AggregateBudgets, AggregateExpression, AggregateFunctionTag,
	AggregateRequest, AggregateRequestId, AggregateResponse, AggregateStatus, ByteCount,
	ClusteringColumnSpec, ColumnLocalityDescriptor, DurationMillis, IndexArrowTypeTag,
	IndexKeyEncoding, IndexObjectId, LocalityPolicy, LocalityRange, LocationCount,
	MultiRowLayoutDescriptor, PolicyGeneration, PrimaryKeyDescriptor, ProjectionFunction,
	RecordLayoutPolicy, RetentionWindowMillis, RowCount, RowIdentityPolicy, STORAGE_ROW_HLC_COLUMN,
	STORAGE_ROW_KEY_ID_COLUMN, STORAGE_ROW_ROUTING_LOCALITY_COLUMN, SchemaEvolutionPolicy,
	SecondaryIndexDescriptor, SecondaryIndexKeyDescriptor, SecondaryIndexLifecycleComplete,
	SlabRecordHeader, TableObjectId, TableRetentionPolicy, TableSchemaCatalog,
	TableSpreadLocalityDescriptor, column_projection_prefix_ranges,
	encode_secondary_index_descriptor, encode_secondary_index_lifecycle_complete,
	encode_table_schema_descriptor_v1, encode_table_write_row_count, table_row_identity,
	table_spread_prefix_ranges, write_payload_schema,
};
use crate::storage::core::StorageEngine;
use crate::storage::io_uring::spawn_io_thread;
use crate::storage::l2::{
	InjectedMockObjectStoreFault, MockObjectStore, MockObjectStoreFaultAction, MockObjectStoreOp,
	ObjectStore,
};
use crate::storage::manager::{SlabManagerConfig, TieredSlabPointer};
use crate::storage::{IndexQuery, SecondaryIndexCoverageKey, SecondaryIndexIdentity};
use arrow::array::{Int64Array, LargeStringArray};
use arrow::datatypes::{DataType, Field};
use std::io::{Read, Seek, SeekFrom};
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::Event;
use tracing::field::{Field as TracingField, Visit};
use tracing::span::{Attributes, Id};
use tracing_subscriber::layer::Context;
use tracing_subscriber::prelude::*;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::{Layer, Registry};

fn n(id: u32) -> NodeId {
	NodeId { id, _reserved: 0 }
}

struct FixedClock {
	now_ms: u64,
}

impl PhysicalClock for FixedClock {
	fn now_ms(&self) -> u64 {
		self.now_ms
	}
}

struct AdjustableClock {
	now_ms: AtomicU64,
}

impl AdjustableClock {
	fn new(now_ms: u64) -> Self {
		Self {
			now_ms: AtomicU64::new(now_ms),
		}
	}

	fn set(&self, now_ms: u64) {
		self.now_ms.store(now_ms, Ordering::SeqCst);
	}
}

impl PhysicalClock for AdjustableClock {
	fn now_ms(&self) -> u64 {
		self.now_ms.load(Ordering::SeqCst)
	}
}

#[tokio::test(start_paused = true)]
async fn lease_refresh_schedule_seeds_once_then_waits_for_the_derived_deadline() {
	let (_epoch_tx, mut epoch_rx) = watch::channel(Arc::new(MembershipEpoch::empty()));
	let refreshes = Arc::new(AtomicU64::new(0));
	let observed = refreshes.clone();
	let task = tokio::spawn(async move {
		run_lease_refresh_schedule(&mut epoch_rx, Duration::from_secs(10), || {
			let observed = observed.clone();
			async move {
				observed.fetch_add(1, Ordering::SeqCst);
			}
		})
		.await;
	});
	tokio::task::yield_now().await;
	assert_eq!(refreshes.load(Ordering::SeqCst), 1);
	tokio::time::advance(Duration::from_secs(9)).await;
	tokio::task::yield_now().await;
	assert_eq!(refreshes.load(Ordering::SeqCst), 1);
	tokio::time::advance(Duration::from_secs(1)).await;
	tokio::task::yield_now().await;
	assert_eq!(refreshes.load(Ordering::SeqCst), 2);
	task.abort();
}

#[tokio::test(start_paused = true)]
async fn lease_refresh_schedule_epoch_change_supersedes_the_old_timer_deadline() {
	let (epoch_tx, mut epoch_rx) = watch::channel(Arc::new(MembershipEpoch::empty()));
	let refreshes = Arc::new(AtomicU64::new(0));
	let observed = refreshes.clone();
	let task = tokio::spawn(async move {
		run_lease_refresh_schedule(&mut epoch_rx, Duration::from_secs(10), || {
			let observed = observed.clone();
			async move {
				observed.fetch_add(1, Ordering::SeqCst);
			}
		})
		.await;
	});
	tokio::task::yield_now().await;
	tokio::time::advance(Duration::from_secs(5)).await;
	let mut next = (*epoch_tx.borrow()).as_ref().clone();
	next.epoch = next.epoch.saturating_add(1);
	epoch_tx.send(Arc::new(next)).unwrap();
	tokio::task::yield_now().await;
	assert_eq!(refreshes.load(Ordering::SeqCst), 2);
	tokio::time::advance(Duration::from_secs(5)).await;
	tokio::task::yield_now().await;
	assert_eq!(refreshes.load(Ordering::SeqCst), 2);
	tokio::time::advance(Duration::from_secs(5)).await;
	tokio::task::yield_now().await;
	assert_eq!(refreshes.load(Ordering::SeqCst), 3);
	task.abort();
}

#[tokio::test(start_paused = true)]
async fn lease_refresh_schedule_delays_missed_ticks_and_stops_when_watch_closes() {
	let (epoch_tx, mut epoch_rx) = watch::channel(Arc::new(MembershipEpoch::empty()));
	let refreshes = Arc::new(AtomicU64::new(0));
	let observed = refreshes.clone();
	let task = tokio::spawn(async move {
		run_lease_refresh_schedule(&mut epoch_rx, Duration::from_secs(10), || {
			let observed = observed.clone();
			async move {
				observed.fetch_add(1, Ordering::SeqCst);
			}
		})
		.await;
	});
	tokio::task::yield_now().await;
	tokio::time::advance(Duration::from_secs(35)).await;
	tokio::task::yield_now().await;
	assert_eq!(refreshes.load(Ordering::SeqCst), 2);
	drop(epoch_tx);
	tokio::task::yield_now().await;
	assert!(task.is_finished());
	assert_eq!(refreshes.load(Ordering::SeqCst), 2);
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct CapturedSpan {
	name: String,
	parent: Option<String>,
	fields: Vec<(String, String)>,
}

#[derive(Clone)]
struct CapturingSpanLayer {
	spans: Arc<StdMutex<Vec<CapturedSpan>>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct CapturedEvent {
	fields: Vec<(String, String)>,
}

#[derive(Clone)]
struct CapturingEventLayer {
	events: Arc<StdMutex<Vec<CapturedEvent>>>,
}

impl<S> Layer<S> for CapturingEventLayer
where
	S: tracing::Subscriber + for<'lookup> LookupSpan<'lookup>,
{
	fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
		let mut visitor = CapturedFieldVisitor::default();
		event.record(&mut visitor);
		self.events
			.lock()
			.expect("captured event mutex")
			.push(CapturedEvent {
				fields: visitor.fields,
			});
	}
}

impl<S> Layer<S> for CapturingSpanLayer
where
	S: tracing::Subscriber + for<'lookup> LookupSpan<'lookup>,
{
	fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
		let _ = id;
		let mut visitor = CapturedFieldVisitor::default();
		attrs.record(&mut visitor);
		let parent = attrs
			.parent()
			.and_then(|parent| ctx.span(parent))
			.map(|span| span.metadata().name().to_string())
			.or_else(|| {
				if attrs.is_contextual() {
					ctx.lookup_current()
						.map(|span| span.metadata().name().to_string())
				} else {
					None
				}
			});
		self.spans
			.lock()
			.expect("captured span mutex")
			.push(CapturedSpan {
				name: attrs.metadata().name().to_string(),
				parent,
				fields: visitor.fields,
			});
	}
}

#[derive(Default)]
struct CapturedFieldVisitor {
	fields: Vec<(String, String)>,
}

impl CapturedFieldVisitor {
	fn push(&mut self, field: &TracingField, value: String) {
		self.fields.push((field.name().to_string(), value));
	}
}

impl Visit for CapturedFieldVisitor {
	fn record_bool(&mut self, field: &TracingField, value: bool) {
		self.push(field, value.to_string());
	}

	fn record_i64(&mut self, field: &TracingField, value: i64) {
		self.push(field, value.to_string());
	}

	fn record_u64(&mut self, field: &TracingField, value: u64) {
		self.push(field, value.to_string());
	}

	fn record_str(&mut self, field: &TracingField, value: &str) {
		self.push(field, value.to_string());
	}

	fn record_debug(&mut self, field: &TracingField, value: &dyn std::fmt::Debug) {
		self.push(field, format!("{value:?}"));
	}
}

fn capture_new_spans(run: impl FnOnce()) -> Vec<CapturedSpan> {
	let spans = Arc::new(StdMutex::new(Vec::new()));
	let subscriber = Registry::default().with(CapturingSpanLayer {
		spans: spans.clone(),
	});
	tracing::subscriber::with_default(subscriber, run);
	Arc::try_unwrap(spans)
		.expect("subscriber dropped")
		.into_inner()
		.expect("captured span mutex")
}

fn captured_span<'a>(spans: &'a [CapturedSpan], name: &str) -> &'a CapturedSpan {
	spans
		.iter()
		.find(|span| span.name == name)
		.unwrap_or_else(|| panic!("missing captured span {name} in {spans:?}"))
}

fn captured_field<'a>(span: &'a CapturedSpan, name: &str) -> Option<&'a str> {
	span.fields
		.iter()
		.find_map(|(field, value)| (field == name).then_some(value.as_str()))
}

fn typed_table_schema() -> Schema {
	Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("name", DataType::LargeUtf8, true),
	])
}

fn assert_user_visible_schema_has_no_storage_columns(batch: &RecordBatch) {
	for field in batch.schema().fields() {
		assert!(
			!matches!(
				field.name().as_str(),
				STORAGE_ROW_HLC_COLUMN
					| STORAGE_ROW_KEY_ID_COLUMN
					| STORAGE_ROW_ROUTING_LOCALITY_COLUMN
			),
			"user-visible batch leaked storage synthetic column {}",
			field.name()
		);
	}
}

fn typed_table_descriptor() -> TableSchemaDescriptor {
	TableSchemaDescriptor::new(
		TableObjectId::new([0x37; 16]),
		3,
		"analytics",
		"events",
		&typed_table_schema(),
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
		LocalityPolicy::default_table_spread(),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.unwrap()
}

fn multirow_typed_table_descriptor() -> TableSchemaDescriptor {
	let mut descriptor = typed_table_descriptor();
	descriptor.locality_policy =
		LocalityPolicy::TableSpreadProjection(TableSpreadLocalityDescriptor {
			spread_factor: 1,
			prefix_bits: 63,
		});
	descriptor.record_layout_policy =
		RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::new(4, 8, 1024 * 1024).unwrap());
	descriptor.record_body_compression_policy =
		RecordBodyCompressionPolicy::default_for_layout(&descriptor.record_layout_policy);
	descriptor.refresh_descriptor_fingerprints().unwrap();
	descriptor.validate().unwrap();
	descriptor
}

fn append_only_table_descriptor() -> TableSchemaDescriptor {
	TableSchemaDescriptor::new_with_layout(
		TableObjectId::new([0x47; 16]),
		3,
		"analytics",
		"append_events",
		&typed_table_schema(),
		RowIdentityPolicy::AppendOnly,
		LocalityPolicy::default_table_spread(),
		RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::new(4, 8, 1024 * 1024).unwrap()),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.unwrap()
}

fn content_hash_table_descriptor() -> TableSchemaDescriptor {
	TableSchemaDescriptor::new_with_layout(
		TableObjectId::new([0x49; 16]),
		3,
		"analytics",
		"content_events",
		&typed_table_schema(),
		RowIdentityPolicy::AppendOnlyContentHash,
		LocalityPolicy::IdentityProjection,
		RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::new(4, 8, 1024 * 1024).unwrap()),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.unwrap()
}

fn identity_typed_table_descriptor(id_byte: u8) -> TableSchemaDescriptor {
	TableSchemaDescriptor::new(
		TableObjectId::new([id_byte; 16]),
		3,
		"analytics",
		"identity_events",
		&typed_table_schema(),
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
		LocalityPolicy::IdentityProjection,
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.unwrap()
}

fn column_projection_table_descriptor() -> TableSchemaDescriptor {
	TableSchemaDescriptor::new(
		TableObjectId::new([0x38; 16]),
		3,
		"analytics",
		"events",
		&typed_table_schema(),
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
		LocalityPolicy::ColumnProjection(ColumnLocalityDescriptor {
			columns: vec![ClusteringColumnSpec {
				name: "id".to_owned(),
				width_bits: 16,
				string_prefix_bytes: None,
			}],
			projection: ProjectionFunction::ZOrder,
			spread_factor: 4,
			prefix_bits: 16,
		}),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.unwrap()
}

fn typed_table_batch(ids: Vec<i64>) -> RecordBatch {
	let names: Vec<&str> = ids
		.iter()
		.enumerate()
		.map(|(idx, _)| match idx {
			0 => "codex",
			1 => "claude",
			_ => "gemini",
		})
		.collect();
	RecordBatch::try_new(
		Arc::new(typed_table_schema()),
		vec![
			Arc::new(Int64Array::from(ids)),
			Arc::new(LargeStringArray::from(names)),
		],
	)
	.unwrap()
}

fn typed_table_batch_with_names(rows: Vec<(i64, &str)>) -> RecordBatch {
	let ids = rows.iter().map(|(id, _)| *id).collect::<Vec<_>>();
	let names = rows.iter().map(|(_, name)| *name).collect::<Vec<_>>();
	RecordBatch::try_new(
		Arc::new(typed_table_schema()),
		vec![
			Arc::new(Int64Array::from(ids)),
			Arc::new(LargeStringArray::from(names)),
		],
	)
	.unwrap()
}

fn same_multirow_locality_ids(descriptor: &TableSchemaDescriptor, count: usize) -> Vec<i64> {
	let mut buckets: HashMap<Locality, Vec<i64>> = HashMap::new();
	for id in 0_i64..10_000 {
		let batch = typed_table_batch(vec![id]);
		let meta = typed_table_meta(descriptor, 1);
		let admission = validate_table_write_request_batch(
			&meta,
			&batch,
			descriptor,
			WriterNodeId::ZERO,
			meta.client_hlc,
		)
		.unwrap();
		let bucket = admission.localities[0];
		let ids = buckets.entry(bucket).or_default();
		ids.push(id);
		if ids.len() == count {
			return ids.clone();
		}
	}
	panic!("failed to find {count} ids with shared test locality");
}

fn typed_table_meta(descriptor: &TableSchemaDescriptor, row_count: usize) -> TableWriteRequestMeta {
	TableWriteRequestMeta {
		parent_write_id: crate::schema::ClientWriteId {
			client_uuid: [0x44; 16],
			sequence_no: 55,
			idempotency_ttl_ms: 30_000,
			_pad: 0,
		},
		client_hlc: hlc(1, 0, 1),
		expected_epoch: 1,
		tenant_id: tenant(),
		table_object_id: descriptor.table_object_id,
		schema_generation: descriptor.schema_generation,
		child_ordinal_base: 1_000,
		proxy_source: n(0),
		row_count: encode_table_write_row_count(row_count).unwrap(),
		proxy_hop: 0,
		durability: DurabilityLevel::QUORUM,
		await_finalization: 0,
		_pad: [0; 3],
		deadline_remaining_millis: 0,
	}
}

fn typed_table_schema_update(descriptor: &TableSchemaDescriptor) -> Vec<u8> {
	let payload = encode_table_schema_descriptor_v1(descriptor).unwrap();
	let envelope = SchemaUpdateEnvelope {
		contract_version: COMPUTE_STORAGE_CONTRACT_VERSION,
		origin: SchemaIntentOrigin::ComputeClient,
		tenant_id: tenant(),
		object: SchemaObjectRef {
			kind: SchemaObjectKind::Table,
			namespace: descriptor.namespace.clone(),
			name: descriptor.table_name.clone(),
		},
		expected_generation: SchemaGeneration::ZERO,
		requested_generation: SchemaGeneration::new(descriptor.schema_generation),
		intent_id: SchemaIntentId::new([0x37; 16]),
		mutation: SchemaMutationKind::Create,
		payload,
	};
	encode_schema_update_envelope_v1(&envelope).unwrap()
}

fn name_secondary_index_descriptor(descriptor: &TableSchemaDescriptor) -> SecondaryIndexDescriptor {
	SecondaryIndexDescriptor {
		index_object_id: IndexObjectId::new([0x52; 16]),
		table_object_id: descriptor.table_object_id,
		namespace: descriptor.namespace.clone(),
		index_name: "events_name_idx".to_owned(),
		table_schema_generation: descriptor.schema_generation,
		logical_schema_fingerprint: descriptor.logical_schema_fingerprint,
		key: SecondaryIndexKeyDescriptor {
			column_name: "name".to_owned(),
			column_ordinal: 1,
			arrow_type_tag: IndexArrowTypeTag::LargeUtf8,
			encoding: IndexKeyEncoding::OrderedScalar,
		},
	}
}

fn secondary_index_schema_update(descriptor: &SecondaryIndexDescriptor) -> Vec<u8> {
	let payload = encode_secondary_index_descriptor(descriptor).unwrap();
	let envelope = SchemaUpdateEnvelope {
		contract_version: COMPUTE_STORAGE_CONTRACT_VERSION,
		origin: SchemaIntentOrigin::ComputeClient,
		tenant_id: tenant(),
		object: SchemaObjectRef {
			kind: SchemaObjectKind::Index,
			namespace: descriptor.namespace.clone(),
			name: descriptor.index_name.clone(),
		},
		expected_generation: SchemaGeneration::ZERO,
		requested_generation: SchemaGeneration::new(1),
		intent_id: SchemaIntentId::new([0x52; 16]),
		mutation: SchemaMutationKind::Create,
		payload,
	};
	encode_schema_update_envelope_v1(&envelope).unwrap()
}

fn secondary_index_backfill_complete_schema_update(
	descriptor: &SecondaryIndexDescriptor,
	replica_id: NodeId,
) -> Vec<u8> {
	let payload = encode_secondary_index_lifecycle_complete(&SecondaryIndexLifecycleComplete {
		index_object_id: descriptor.index_object_id,
		replica_id,
	})
	.unwrap();
	let envelope = SchemaUpdateEnvelope {
		contract_version: COMPUTE_STORAGE_CONTRACT_VERSION,
		origin: SchemaIntentOrigin::WendelonOperator,
		tenant_id: tenant(),
		object: SchemaObjectRef {
			kind: SchemaObjectKind::Index,
			namespace: descriptor.namespace.clone(),
			name: descriptor.index_name.clone(),
		},
		expected_generation: SchemaGeneration::new(1),
		requested_generation: SchemaGeneration::new(2),
		intent_id: SchemaIntentId::new([0x53; 16]),
		mutation: SchemaMutationKind::IndexBackfillComplete,
		payload,
	};
	encode_schema_update_envelope_v1(&envelope).unwrap()
}

fn aggregate_count_request(
	descriptor: &TableSchemaDescriptor,
	request_id: u128,
	as_of_hlc: HybridTimestamp,
) -> AggregateRequest {
	AggregateRequest {
		format_version: AGG_FORMAT_VERSION,
		request_id: AggregateRequestId::new(request_id),
		tenant_id: tenant(),
		table_object_id: descriptor.table_object_id,
		schema_generation: descriptor.schema_generation,
		logical_schema_fingerprint: descriptor.logical_schema_fingerprint,
		consistency: Consistency::SERIALIZABLE,
		min_hlc: hlc(1, 0, 1),
		as_of_hlc,
		locality_ranges: vec![LocalityRange::full()],
		predicate_program: None,
		aggregates: vec![AggregateExpression::count_star()],
		budgets: AggregateBudgets {
			max_locations: LocationCount::new(16),
			max_rows: RowCount::new(16),
			max_wall_time_millis: DurationMillis::new(1_000),
			max_partial_state_bytes: ByteCount::new(4_096),
		},
	}
}

fn table_query_meta_for_test(
	descriptor: &TableSchemaDescriptor,
	as_of_hlc: HybridTimestamp,
	bytecode_len: usize,
) -> TableQueryRequestMeta {
	TableQueryRequestMeta {
		min_hlc: hlc(0, 0, 0),
		expected_epoch: 1,
		tenant_id: tenant(),
		table_object_id: descriptor.table_object_id,
		schema_generation: descriptor.schema_generation,
		physical_descriptor_fingerprint: *descriptor.physical_descriptor_fingerprint.as_bytes(),
		consistency: Consistency::SERIALIZABLE,
		row_identity_projection: RowIdentityProjection::None.as_u8(),
		_pad: [0; 6],
		bytecode_len: bytecode_len as u32,
		runtime_filter_len: 0,
		as_of_hlc,
		row_limit_rows: 0,
		request_id: TableQueryRequestId::from_u128(1),
		deadline_remaining_millis: 0,
		trace_id: [0; 16],
		trace_parent_span_id: [0; 8],
		trace_flags: 0,
		_trace_pad: [0; 7],
	}
}

fn table_query_frame_for_decode_test(
	meta: TableQueryRequestMeta,
	trailing: Option<Vec<u8>>,
) -> HrftFrame {
	let meta_bytes = bytemuck::bytes_of(&meta).to_vec();
	let meta_len = meta_bytes.len() as u32;
	let trailing_len = trailing.as_ref().map(|bytes| bytes.len()).unwrap_or(0) as u32;
	HrftFrame {
		header: FrameHeader {
			magic: HRFT_MAGIC,
			version: 1,
			msg_type: MsgType::TABLE_QUERY_REQ.0,
			flags: 0,
			stream_id: 67,
			meta_len,
			arrow_len: 0,
			trailing_len,
		},
		meta: crate::network::pool::PooledBuf::orphan(meta_bytes),
		arrow_payload: None,
		trailing: trailing.map(crate::network::pool::PooledBuf::orphan),
	}
}

fn runtime_filter_descriptor_for_decode_test(
	descriptor: &TableSchemaDescriptor,
) -> RuntimeFilterDescriptor {
	runtime_filter_descriptor_for_ids(descriptor, &[7])
}

fn runtime_filter_descriptor_for_ids(
	descriptor: &TableSchemaDescriptor,
	ids: &[i64],
) -> RuntimeFilterDescriptor {
	let bit_count = 1024;
	let hash_count = 3;
	let byte_budget = bit_count / 8;
	let mut payload = vec![0; byte_budget as usize];
	let ids = Int64Array::from(ids.to_vec());
	for row in 0..ids.len() {
		let encoded = crate::schema::encode_runtime_filter_key(&ids, row)
			.expect("encode runtime-filter id")
			.expect("non-null id");
		crate::schema::insert_runtime_filter_encoded_key(
			&mut payload,
			bit_count,
			hash_count,
			crate::schema::RUNTIME_FILTER_HASH_SEED,
			&encoded,
		);
	}
	RuntimeFilterDescriptor {
		key_encoding_version: crate::schema::RUNTIME_FILTER_KEY_ENCODING_VERSION,
		key_types: vec![crate::schema::RuntimeFilterKeyType::Int64],
		probe_key_columns: vec![0],
		kind: crate::schema::RuntimeFilterKind::Bloom(crate::schema::RuntimeFilterBloom {
			hash_seed: crate::schema::RUNTIME_FILTER_HASH_SEED,
			bit_count,
			hash_count,
			byte_budget,
			target_fp_rate_bp: 150,
			payload,
		}),
		probe_descriptor_fingerprint: descriptor.physical_descriptor_fingerprint,
	}
}

fn runtime_filter_application_for_ids(
	descriptor: &TableSchemaDescriptor,
	ids: &[i64],
) -> RuntimeFilterApplication {
	RuntimeFilterApplication {
		descriptor: runtime_filter_descriptor_for_ids(descriptor, ids),
	}
}

fn runtime_filter_exact_application_for_ids(
	descriptor: &TableSchemaDescriptor,
	ids: &[i64],
) -> RuntimeFilterApplication {
	let ids = Int64Array::from(ids.to_vec());
	let mut keys = Vec::new();
	for row in 0..ids.len() {
		keys.push(
			crate::schema::encode_runtime_filter_key(&ids, row)
				.expect("encode runtime-filter id")
				.expect("non-null id"),
		);
	}
	RuntimeFilterApplication {
		descriptor: RuntimeFilterDescriptor {
			key_encoding_version: crate::schema::RUNTIME_FILTER_KEY_ENCODING_VERSION,
			key_types: vec![crate::schema::RuntimeFilterKeyType::Int64],
			probe_key_columns: vec![0],
			kind: crate::schema::RuntimeFilterKind::ExactSet(
				crate::schema::runtime_filter_exact_set_from_encoded_keys(keys, 16 * 1024)
					.expect("exact runtime filter"),
			),
			probe_descriptor_fingerprint: descriptor.physical_descriptor_fingerprint,
		},
	}
}

fn projected_typed_batch(rows: Vec<(i64, &str)>) -> ProjectedRecordBatch {
	ProjectedRecordBatch {
		batch: typed_table_batch_with_names(rows),
		ordinal_mapping: crate::storage::columnar::ColumnOrdinalMapping::new(vec![
			Some(0),
			Some(1),
		]),
		storage_mapping: std::collections::BTreeMap::new(),
	}
}

fn pbc_project_name_program() -> pbc::Program {
	let mut bytecode = Vec::new();
	bytecode.push(OP_PROJECT);
	bytecode.extend_from_slice(&1_u16.to_le_bytes());
	bytecode.extend_from_slice(&1_u32.to_le_bytes());
	pbc::Program::compile(&bytecode).expect("compile name projection")
}

fn names_from_batch(batch: &RecordBatch) -> Vec<String> {
	let names = batch
		.column_by_name("name")
		.expect("name column")
		.as_any()
		.downcast_ref::<LargeStringArray>()
		.expect("name column is LargeUtf8");
	(0..names.len())
		.map(|row| names.value(row).to_owned())
		.collect()
}

#[test]
fn table_query_cancel_before_register_is_consumed_once() {
	let registry = Arc::new(ActiveTableQueryRegistry::default());
	let tenant_id = TenantId::new(7);
	let request_id = TableQueryRequestId::from_u128(11);

	assert_eq!(
		registry.cancel(tenant_id, request_id),
		TableQueryCancelStatus::ACCEPTED
	);
	assert!(matches!(
		registry.register(tenant_id, request_id),
		ActiveTableQueryRegistration::CancelledBeforeStart
	));
	let registration = registry.register(tenant_id, request_id);
	let ActiveTableQueryRegistration::Active(guard) = registration else {
		panic!("pending tombstone should be consumed by the first register");
	};
	assert!(!guard.token().is_cancelled());
}

#[test]
fn table_query_cancel_active_signals_then_terminal_after_guard_drop() {
	let registry = Arc::new(ActiveTableQueryRegistry::default());
	let tenant_id = TenantId::new(7);
	let request_id = TableQueryRequestId::from_u128(12);
	let registration = registry.register(tenant_id, request_id);
	let ActiveTableQueryRegistration::Active(guard) = registration else {
		panic!("fresh request should register active");
	};
	let token = guard.token();

	assert_eq!(
		registry.cancel(tenant_id, request_id),
		TableQueryCancelStatus::ACCEPTED
	);
	assert!(token.is_cancelled());
	drop(guard);
	assert_eq!(
		registry.cancel(tenant_id, request_id),
		TableQueryCancelStatus::TERMINAL
	);
}

#[test]
fn table_query_cancel_tombstones_prune_to_bounded_cap() {
	let registry = Arc::new(ActiveTableQueryRegistry::default());
	for i in 1..=(TABLE_QUERY_CANCEL_TOMBSTONE_CAP + 16) {
		let id = TableQueryRequestId::from_u128(i as u128);
		assert_eq!(
			registry.cancel(TenantId::new(7), id),
			TableQueryCancelStatus::ACCEPTED
		);
	}
	let state = registry.state.lock().expect("registry mutex");
	assert!(state.pending.len() <= TABLE_QUERY_CANCEL_TOMBSTONE_CAP);
	assert!(state.terminal.len() <= TABLE_QUERY_CANCEL_TOMBSTONE_CAP);
	assert!(
		state.pending.len().saturating_add(state.terminal.len())
			<= TABLE_QUERY_CANCEL_TOMBSTONE_CAP
	);
}

#[test]
fn table_query_decode_splits_pbc_and_runtime_filter_trailing_bytes() {
	let descriptor = typed_table_descriptor();
	let bytecode = pk_eq_program(7);
	let runtime_filter = runtime_filter_descriptor_for_decode_test(&descriptor);
	let runtime_filter_bytes = postcard::to_allocvec(&runtime_filter).unwrap();
	let mut meta = table_query_meta_for_test(&descriptor, hlc(0, 0, 0), bytecode.len());
	meta.runtime_filter_len = runtime_filter_bytes.len() as u32;
	let mut trailing = bytecode.clone();
	trailing.extend_from_slice(&runtime_filter_bytes);
	let frame = table_query_frame_for_decode_test(meta, Some(trailing));

	let (decoded_meta, decoded_bytecode, decoded_runtime_filter) =
		WendelonDataServer::decode_table_query_request_frame_parts(&frame).unwrap();

	assert_eq!(decoded_meta.bytecode_len, bytecode.len() as u32);
	assert_eq!(
		decoded_meta.runtime_filter_len,
		runtime_filter_bytes.len() as u32
	);
	assert_eq!(decoded_bytecode, bytecode);
	match decoded_runtime_filter {
		DecodedRuntimeFilter::Decoded(decoded) => assert_eq!(decoded, runtime_filter),
		other => panic!("expected decoded runtime filter, got {other:?}"),
	}
}

#[test]
fn table_query_decode_rejects_zero_request_id() {
	let descriptor = typed_table_descriptor();
	let bytecode = pk_eq_program(13);
	let mut meta = table_query_meta_for_test(&descriptor, hlc(0, 0, 0), bytecode.len());
	meta.request_id = TableQueryRequestId::ZERO;
	let frame = table_query_frame_for_decode_test(meta, Some(bytecode));

	let err = WendelonDataServer::decode_table_query_request_frame_parts(&frame)
		.expect_err("zero table-query request id must fail framing");

	assert_eq!(err.kind(), ErrorKind::InvalidData);
	assert!(err.to_string().contains("request_id must be non-zero"));
}

#[tokio::test]
async fn table_query_cancelled_before_planning_returns_query_cancelled() {
	let dir = bootstrap_tempdir("table-query-cancel-pre-plan");
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		73,
		dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();
	let descriptor = typed_table_descriptor();
	let bytecode = pk_eq_program(13);
	let meta = table_query_meta_for_test(&descriptor, hlc(0, 0, 0), bytecode.len());
	let cancellation_token = CancellationToken::new();
	cancellation_token.cancel();

	let plan = node
		.data_server
		.prepare_table_query_execution_plan_with_migration_with_cancellation(
			&meta,
			&bytecode,
			DecodedRuntimeFilter::Absent,
			&cancellation_token,
		)
		.await
		.unwrap();

	assert_eq!(plan.response.status, ReadStatus::QUERY_CANCELLED);
	assert_eq!(plan.response.rows, 0);
	shutdown_lease_node(node).await;
}

#[test]
fn table_query_decode_missing_runtime_filter_trailing_is_hard_error() {
	let descriptor = typed_table_descriptor();
	let mut meta = table_query_meta_for_test(&descriptor, hlc(0, 0, 0), 0);
	meta.runtime_filter_len = 1;
	let frame = table_query_frame_for_decode_test(meta, None);

	let err = WendelonDataServer::decode_table_query_request_frame_parts(&frame)
		.expect_err("missing declared trailing bytes must fail framing");

	assert_eq!(err.kind(), ErrorKind::InvalidData);
	assert!(err.to_string().contains("trailing is absent"));
}

#[test]
fn table_query_decode_short_runtime_filter_trailing_is_hard_error() {
	let descriptor = typed_table_descriptor();
	let bytecode = pk_eq_program(9);
	let mut meta = table_query_meta_for_test(&descriptor, hlc(0, 0, 0), bytecode.len());
	meta.runtime_filter_len = 4;
	let frame = table_query_frame_for_decode_test(meta, Some(bytecode));

	let err = WendelonDataServer::decode_table_query_request_frame_parts(&frame)
		.expect_err("short declared trailing bytes must fail framing");

	assert_eq!(err.kind(), ErrorKind::InvalidData);
	assert!(err.to_string().contains("shorter than declared"));
}

#[test]
fn table_query_decode_invalid_runtime_filter_payload_is_advisory_drop() {
	let descriptor = typed_table_descriptor();
	let bytecode = pk_eq_program(11);
	let invalid_runtime_filter = [0xFF];
	let mut meta = table_query_meta_for_test(&descriptor, hlc(0, 0, 0), bytecode.len());
	meta.runtime_filter_len = invalid_runtime_filter.len() as u32;
	let mut trailing = bytecode.clone();
	trailing.extend_from_slice(&invalid_runtime_filter);
	let frame = table_query_frame_for_decode_test(meta, Some(trailing));

	let (_, decoded_bytecode, decoded_runtime_filter) =
		WendelonDataServer::decode_table_query_request_frame_parts(&frame).unwrap();

	assert_eq!(decoded_bytecode, bytecode);
	match decoded_runtime_filter {
		DecodedRuntimeFilter::DroppedBeforePlanning(RuntimeFilterDropReason::DecodeFailed) => {}
		other => panic!("expected decode_failed advisory drop, got {other:?}"),
	}
}

#[test]
fn table_query_trace_carrier_absent_unsampled_and_sampled_decode() {
	let descriptor = typed_table_descriptor();
	let mut meta = table_query_meta_for_test(&descriptor, hlc(0, 0, 0), 0);
	assert_eq!(
		decode_table_query_trace_carrier(&meta),
		TableQueryTraceCarrier::Absent
	);

	meta.trace_flags = TABLE_QUERY_TRACE_FLAG_PRESENT;
	assert_eq!(
		decode_table_query_trace_carrier(&meta),
		TableQueryTraceCarrier::Unsampled
	);

	let trace_id = [0x11; 16];
	let parent_span_id = [0x22; 8];
	meta.trace_id = trace_id;
	meta.trace_parent_span_id = parent_span_id;
	meta.trace_flags = TABLE_QUERY_TRACE_FLAG_PRESENT | TABLE_QUERY_TRACE_FLAG_SAMPLED;
	assert_eq!(
		decode_table_query_trace_carrier(&meta),
		TableQueryTraceCarrier::Sampled {
			trace_id,
			parent_span_id
		}
	);
}

#[test]
fn table_query_trace_carrier_malformed_fails_open_unsampled() {
	let descriptor = typed_table_descriptor();
	let mut meta = table_query_meta_for_test(&descriptor, hlc(0, 0, 0), 0);
	meta.trace_flags = TABLE_QUERY_TRACE_FLAG_PRESENT | TABLE_QUERY_TRACE_FLAG_SAMPLED;
	meta.trace_parent_span_id = [0x22; 8];
	assert_eq!(
		decode_table_query_trace_carrier(&meta),
		TableQueryTraceCarrier::Unsampled
	);

	meta.trace_id = [0x11; 16];
	meta.trace_parent_span_id = [0; 8];
	assert_eq!(
		decode_table_query_trace_carrier(&meta),
		TableQueryTraceCarrier::Unsampled
	);

	meta.trace_parent_span_id = [0x22; 8];
	meta.trace_flags = TABLE_QUERY_TRACE_FLAG_PRESENT | TABLE_QUERY_TRACE_FLAG_SAMPLED | 0x80;
	assert_eq!(
		decode_table_query_trace_carrier(&meta),
		TableQueryTraceCarrier::Unsampled
	);

	meta.trace_flags = TABLE_QUERY_TRACE_FLAG_SAMPLED;
	assert_eq!(
		decode_table_query_trace_carrier(&meta),
		TableQueryTraceCarrier::Unsampled
	);
}

#[test]
fn table_query_linked_span_id_is_derived_nonzero_per_attempt() {
	let descriptor = typed_table_descriptor();
	let mut meta = table_query_meta_for_test(&descriptor, hlc(0, 0, 0), 0);
	let trace_id = [0x11; 16];
	let parent_span_id = [0x22; 8];
	let first = table_query_linked_span_id(&meta, trace_id, parent_span_id);
	assert!(first.iter().any(|byte| *byte != 0));
	assert_eq!(
		first,
		table_query_linked_span_id(&meta, trace_id, parent_span_id)
	);
	meta.request_id = TableQueryRequestId::from_u128(2);
	assert_ne!(
		first,
		table_query_linked_span_id(&meta, trace_id, parent_span_id)
	);
	assert_ne!(first, parent_span_id);
}

#[test]
fn sampled_trace_carrier_emits_sanitized_linked_span_and_sibling_raw_span() {
	let descriptor = typed_table_descriptor();
	let mut meta = table_query_meta_for_test(&descriptor, hlc(0, 0, 0), 0);
	meta.trace_id = [0x11; 16];
	meta.trace_parent_span_id = [0x22; 8];
	meta.trace_flags = TABLE_QUERY_TRACE_FLAG_PRESENT | TABLE_QUERY_TRACE_FLAG_SAMPLED;
	let carrier = decode_table_query_trace_carrier(&meta);

	let spans = capture_new_spans(|| {
		let linked = linked_table_query_span(&meta, carrier).expect("sampled linked span");
		let raw = local_table_query_span(&meta, 99, carrier);
		drop(raw);
		drop(linked);
	});

	let linked = captured_span(&spans, "wendelon.table.query");
	assert_eq!(linked.parent, None);
	for required in [
		"otel.trace_id",
		"otel.parent_span_id",
		"otel.span_id",
		"otel.sampled",
		"wendelon.table_query.consistency",
		"wendelon.table_query.row_identity_projection",
		"wendelon.table_query.stats_pruning_enabled",
	] {
		assert!(
			captured_field(linked, required).is_some(),
			"missing linked field {required}: {linked:?}"
		);
	}
	assert_eq!(captured_field(linked, "otel.sampled"), Some("true"));
	for forbidden in [
		"tenant_id",
		"table_object_id",
		"request_id",
		"physical_descriptor_fingerprint",
		"query_id",
		"peer",
		"sql",
	] {
		assert_eq!(
			captured_field(linked, forbidden),
			None,
			"linked span leaked {forbidden}: {linked:?}"
		);
	}

	let raw = captured_span(&spans, "data_ring.table_query.request");
	assert_eq!(raw.parent, None);
	assert!(captured_field(raw, "tenant_id").is_some());
	assert!(captured_field(raw, "request_id").is_some());
	assert!(captured_field(raw, "table_object_id").is_some());
	assert_eq!(captured_field(raw, "otel.sampled"), None);
}

#[test]
fn unsampled_trace_carrier_suppresses_linked_and_raw_root_export() {
	let descriptor = typed_table_descriptor();
	let mut meta = table_query_meta_for_test(&descriptor, hlc(0, 0, 0), 0);
	meta.trace_flags = TABLE_QUERY_TRACE_FLAG_PRESENT;
	let carrier = decode_table_query_trace_carrier(&meta);
	assert!(linked_table_query_span(&meta, carrier).is_none());

	let spans = capture_new_spans(|| {
		let raw = local_table_query_span(&meta, 100, carrier);
		drop(raw);
	});

	assert!(
		spans.iter().all(|span| span.name != "wendelon.table.query"),
		"unsampled carrier must not create linked span: {spans:?}"
	);
	let raw = captured_span(&spans, "data_ring.table_query.request");
	assert_eq!(raw.parent, None);
	assert_eq!(captured_field(raw, "otel.sampled"), Some("false"));
}

#[test]
fn secondary_index_projected_runtime_filter_applies_before_pbc_output() {
	let descriptor = typed_table_descriptor();
	let program = pbc_project_name_program();
	let requirement = analyze_pbc_column_requirement(&program, 2).unwrap();
	let projected = projected_typed_batch(vec![
		(1, "filtered-out"),
		(2, "survivor"),
		(3, "also-filtered-out"),
	]);
	let execution = QueryProjectedExecution::Pbc {
		requirement,
		user_column_count: 2,
		runtime_filter: Some(runtime_filter_application_for_ids(&descriptor, &[2])),
	};

	let result = execute_secondary_index_projected_row(projected, &program, &execution)
		.expect("secondary-index projected execution")
		.expect("PBC output batch");

	assert_eq!(result.batch.num_columns(), 1);
	assert!(result.batch.column_by_name("id").is_none());
	assert_eq!(names_from_batch(&result.batch), vec!["survivor"]);
}

#[test]
fn secondary_index_projected_exact_set_runtime_filter_applies_before_pbc_output() {
	let descriptor = typed_table_descriptor();
	let program = pbc_project_name_program();
	let requirement = analyze_pbc_column_requirement(&program, 2).unwrap();
	let projected = projected_typed_batch(vec![
		(1, "filtered-out"),
		(2, "survivor"),
		(3, "also-filtered-out"),
	]);
	let execution = QueryProjectedExecution::Pbc {
		requirement,
		user_column_count: 2,
		runtime_filter: Some(runtime_filter_exact_application_for_ids(&descriptor, &[2])),
	};

	let result = execute_secondary_index_projected_row(projected, &program, &execution)
		.expect("secondary-index projected execution")
		.expect("PBC output batch");

	assert_eq!(result.batch.num_columns(), 1);
	assert!(result.batch.column_by_name("id").is_none());
	assert_eq!(names_from_batch(&result.batch), vec!["survivor"]);
}

#[test]
fn whole_record_runtime_filter_applies_before_pbc_output() {
	let descriptor = typed_table_descriptor();
	let program = pbc_project_name_program();
	let batch = typed_table_batch_with_names(vec![
		(1, "filtered-out"),
		(2, "survivor"),
		(3, "also-filtered-out"),
	]);
	let filtered = apply_runtime_filter_to_batch(
		batch,
		&runtime_filter_application_for_ids(&descriptor, &[2]),
	)
	.expect("whole-record runtime-filter application");
	let output = pbc::execute_program(&program, &filtered).expect("PBC projection");

	assert_eq!(output.num_columns(), 1);
	assert!(output.column_by_name("id").is_none());
	assert_eq!(names_from_batch(&output), vec!["survivor"]);
}

#[test]
fn whole_record_exact_set_runtime_filter_applies_before_pbc_output() {
	let descriptor = typed_table_descriptor();
	let program = pbc_project_name_program();
	let batch = typed_table_batch_with_names(vec![
		(1, "filtered-out"),
		(2, "survivor"),
		(3, "also-filtered-out"),
	]);
	let filtered = apply_runtime_filter_to_batch(
		batch,
		&runtime_filter_exact_application_for_ids(&descriptor, &[2]),
	)
	.expect("whole-record runtime-filter application");
	let output = pbc::execute_program(&program, &filtered).expect("PBC projection");

	assert_eq!(output.num_columns(), 1);
	assert!(output.column_by_name("id").is_none());
	assert_eq!(names_from_batch(&output), vec!["survivor"]);
}

fn pk_eq_program(pk: i64) -> Vec<u8> {
	let mut bytecode = Vec::new();
	bytecode.push(OP_PUSH_COL);
	bytecode.extend_from_slice(&0_u32.to_le_bytes());
	bytecode.push(OP_PUSH_INT);
	bytecode.extend_from_slice(&pk.to_le_bytes());
	bytecode.push(OP_EQ);
	bytecode
}

fn name_eq_program(name: &str) -> Vec<u8> {
	let mut bytecode = Vec::new();
	bytecode.push(OP_PUSH_COL);
	bytecode.extend_from_slice(&1_u32.to_le_bytes());
	bytecode.push(OP_PUSH_STR);
	bytecode.extend_from_slice(&(name.len() as u16).to_le_bytes());
	bytecode.extend_from_slice(name.as_bytes());
	bytecode.push(OP_EQ);
	bytecode
}

fn count_star_partial_value(response: &AggregateResponse) -> u64 {
	u64::from_be_bytes(
		response.partials[0]
			.state
			.as_slice()
			.try_into()
			.expect("count star partial is u64"),
	)
}

async fn aggregate_csb_test_server(
	mgr: Arc<SlabManager>,
	descriptor: &TableSchemaDescriptor,
	node_id: u32,
	temp_tag: &str,
	clock: Arc<dyn PhysicalClock>,
	retention_policy: MvccRetentionPolicy,
) -> (
	crate::test_support::TestDir,
	WendelonNode,
	WendelonDataServer,
) {
	let node_dir = bootstrap_tempdir(temp_tag);
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		node_id,
		node_dir.to_path_buf(),
		format!("loopback://{node_id}"),
	))
	.await
	.unwrap();
	let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
	loop {
		if node.raft.current_leader().await == Some(node.local_node.id) {
			break;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for single-node leader"
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
	node.raft
		.client_write(WendelonRequest::SchemaUpdate(typed_table_schema_update(
			descriptor,
		)))
		.await
		.unwrap();

	let tiers = TransportTierSet::empty();
	let roles = NodeRoleSet::empty().with(NodeRole::Data);
	let mut peers = std::collections::HashMap::new();
	peers.insert(
		node.local_node,
		PeerCapabilities {
			address: format!("loopback://{node_id}"),
			tiers,
			roles,
			capabilities: crate::network::transport::NodeCapabilitySet::empty()
				.with(NodeCapability::PartialAggregatePushdown),
		},
	);
	let epoch = MembershipEpoch { epoch: 1, peers };
	let (_epoch_tx, epoch_rx) = watch::channel(Arc::new(epoch));
	let resolver: Arc<dyn crate::network::transport::CapabilityResolver> = Arc::new(
		crate::network::transport::EpochCapabilityResolver::new(epoch_rx.clone()),
	);
	let factory =
		TransportProviderFactory::new(tiers, std::collections::HashMap::new(), resolver, epoch_rx);
	let server = WendelonDataServer::new(
		mgr,
		node.raft.clone(),
		node.log_store.clone(),
		node.state_machine.clone(),
		node.local_node,
		clock,
		Arc::new(FixedReplicaRouter::new(vec![node.local_node])),
		factory,
		node.pool.clone(),
	)
	.unwrap()
	.with_mvcc_retention_policy(retention_policy);

	(node_dir, node, server)
}

fn fresh_dir(tag: &str) -> crate::test_support::TestDir {
	crate::test_support::TestDir::new(&format!("wendelon-data-server-{tag}"))
}

#[test]
fn pending_finalization_claims_are_exclusive_until_drop() {
	let claims = PendingFinalizationClaims::default();
	let write_id = crate::schema::ClientWriteId {
		client_uuid: [9; 16],
		sequence_no: 42,
		idempotency_ttl_ms: 60_000,
		_pad: 0,
	};
	let finalization = TenantFinalization {
		marker: FinalizationMarker {
			log_index: 0,
			term: 0,
			epoch: 1,
			commit_hlc: HybridTimestamp {
				physical_ms: 123,
				logical: 4,
				node_id: 1,
			},
			payload_hash: [0xAB; 32],
			e_safe_index: 0,
			durability: DurabilityLevel::QUORUM,
			_pad: [0; 7],
		},
		tenant_id: TenantId::ZERO,
		key_identity: Identity::blake3_tenant(TenantId::ZERO, b"claim-key"),
		charged_bytes: 128,
		record_count: 1,
	};
	let id = PendingFinalizationId::for_record(write_id, &finalization);

	let first = claims.try_claim(id).expect("first claim should succeed");
	assert!(
		claims.try_claim(id).is_none(),
		"the same pending finalization must not be processed twice concurrently"
	);
	drop(first);
	assert!(
		claims.try_claim(id).is_some(),
		"dropping the active claim should release the pending finalization"
	);
}

fn hlc(physical_ms: u64, logical: u32, node_id: u32) -> HybridTimestamp {
	HybridTimestamp {
		physical_ms,
		logical,
		node_id,
	}
}

#[test]
fn cdc_retention_reject_code_fires_when_cursor_falls_below_floor() {
	let effective = EffectiveRetention {
		policy: TableRetentionPolicy::FixedWindow(RetentionWindowMillis::try_new(60_000).unwrap()),
		floor: Some(hlc(100, 0, 0)),
	};

	assert_eq!(
		cdc_retention_reject_code_for_cursor(
			CdcCursor::new(hlc(99, 0, 0), Identity::ZERO, RecordRowIndex::ZERO),
			&effective,
		),
		Some(CdcRejectCode::BelowRetentionFloor)
	);
	assert_eq!(
		cdc_retention_reject_code_for_cursor(
			CdcCursor::new(hlc(100, 0, 0), Identity::ZERO, RecordRowIndex::ZERO),
			&effective,
		),
		None
	);
	assert_eq!(
		cdc_retention_reject_code_for_cursor(CdcCursor::MIN, &effective),
		None
	);
}

#[test]
fn cdc_checkpoint_cadence_fires_on_event_count_and_wall_time() {
	let start = Instant::now();
	let mut cadence = CdcCheckpointCadence::new(start);
	cadence.note_events(CDC_CHECKPOINT_EVENT_CADENCE - 1);
	assert!(!cadence.checkpoint_due(start));
	cadence.note_events(1);
	assert!(cadence.checkpoint_due(start));
	cadence.mark_checkpoint(start);
	assert!(!cadence.checkpoint_due(start));
	assert!(cadence.checkpoint_due(start + CDC_CHECKPOINT_WALL_CADENCE));
}

#[test]
fn cdc_finalization_key_ignores_applied_log_coordinates() {
	let pending_marker = FinalizationMarker {
		log_index: 0,
		term: 0,
		epoch: 7,
		commit_hlc: hlc(900, 1, 2),
		payload_hash: [0x5A; 32],
		e_safe_index: 0,
		durability: DurabilityLevel::QUORUM,
		_pad: [0; 7],
	};
	let applied_marker = FinalizationMarker {
		log_index: 42,
		term: 11,
		e_safe_index: 42,
		..pending_marker
	};
	assert_eq!(
		CdcFinalizationKey::from_marker(pending_marker),
		CdcFinalizationKey::from_marker(applied_marker)
	);

	let mut different_payload = applied_marker;
	different_payload.payload_hash = [0xA5; 32];
	assert_ne!(
		CdcFinalizationKey::from_marker(applied_marker),
		CdcFinalizationKey::from_marker(different_payload)
	);
}

#[test]
fn cdc_audit_helpers_pin_lifecycle_reason_and_cursor_shape() {
	assert_eq!(
		cdc_reject_audit_reason(CdcRejectCode::BelowRetentionFloor),
		"reject_below_retention_floor"
	);
	assert_eq!(cdc_bound_audit_str(CdcSubscribeBound::Live), "live");

	let cursor = CdcCursor::new(hlc(123, 4, 5), Identity::ZERO, RecordRowIndex::new(7));
	let value = cdc_cursor_audit_json(cursor);
	assert_eq!(value["row_hlc"]["physical_ms"], 123);
	assert_eq!(value["row_hlc"]["logical"], 4);
	assert_eq!(value["row_hlc"]["node_id"], 5);
	assert_eq!(value["record_row_index"], 7);
	assert_eq!(
		value["row_key_id"],
		"0000000000000000000000000000000000000000000000000000000000000000"
	);
}

#[test]
fn cdc_source_localities_include_cold_exact_ranges_without_rings() {
	let ranges = [
		QueryScanRange {
			start: Locality::new(100),
			end: Locality::new(100),
		},
		QueryScanRange {
			start: Locality::new(200),
			end: Locality::new(202),
		},
	];

	assert_eq!(
		cdc_source_localities_from_ranges_and_rings(&ranges, [Locality::new(101)]),
		vec![
			Locality::new(100),
			Locality::new(200),
			Locality::new(201),
			Locality::new(202),
		]
	);
}

#[test]
fn cdc_source_localities_keep_wide_ranges_bounded_and_include_live_rings() {
	let ranges = [QueryScanRange {
		start: Locality::new(10),
		end: Locality::new(10 + CDC_SOURCE_LOCALITY_ENUMERATION_BUDGET + 10),
	}];

	assert_eq!(
		cdc_source_localities_from_ranges_and_rings(
			&ranges,
			[Locality::new(11), Locality::new(999_999)],
		),
		vec![
			Locality::new(10),
			Locality::new(11),
			Locality::new(10 + CDC_SOURCE_LOCALITY_ENUMERATION_BUDGET + 10),
		]
	);
}

#[test]
fn cdc_heartbeat_primary_ranges_use_bounded_locality_set() {
	assert_eq!(
		cdc_heartbeat_localities_for_primary_ranges([
			(100, 102),
			(1_000, 1_000 + CDC_SOURCE_LOCALITY_ENUMERATION_BUDGET + 10),
		]),
		vec![
			Locality::new(100),
			Locality::new(101),
			Locality::new(102),
			Locality::new(1_000),
			Locality::new(1_000 + CDC_SOURCE_LOCALITY_ENUMERATION_BUDGET + 10),
		]
	);
}

#[test]
fn cdc_replicated_here_rejects_only_when_bounded_ranges_exclude_local_node() {
	let local = n(7);
	let remote = n(8);
	let ranges = [QueryScanRange {
		start: Locality::new(20),
		end: Locality::new(22),
	}];

	assert!(!cdc_ranges_maybe_replicated_by_local_node(
		&ranges,
		local,
		|_| vec![remote]
	));
	assert!(cdc_ranges_maybe_replicated_by_local_node(
		&ranges,
		local,
		|locality| if locality == Locality::new(21) {
			vec![remote, local]
		} else {
			vec![remote]
		}
	));
}

#[test]
fn cdc_replicated_here_accepts_wide_ranges_conservatively() {
	let local = n(7);
	let remote = n(8);
	let ranges = [QueryScanRange {
		start: Locality::new(20),
		end: Locality::new(20 + CDC_SOURCE_LOCALITY_ENUMERATION_BUDGET + 1),
	}];

	assert!(cdc_ranges_maybe_replicated_by_local_node(
		&ranges,
		local,
		|_| vec![remote]
	));
}

#[tokio::test]
async fn cdc_live_recovery_upper_is_table_scoped_and_safe_gated() {
	let dir = bootstrap_tempdir("cdc-live-recovery-upper");
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		46,
		dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();
	let locality = Locality::new(10);
	let descriptor = typed_table_descriptor();
	let target_table = descriptor.table_object_id;
	let other_table = TableObjectId::new([0x99; 16]);
	let ranges = [QueryScanRange {
		start: locality,
		end: locality,
	}];
	let ring = Arc::new(CdcLiveTailRing::new(locality, 1));
	node.data_server
		.cdc_live_tail_rings
		.insert(locality, ring.clone());

	ring.push(CdcLiveEntry::new(
		CdcCursor::new(
			hlc(80, 0, 46),
			Identity::new([0x80; 32]),
			RecordRowIndex::ZERO,
		),
		tenant(),
		other_table,
		descriptor.logical_schema_fingerprint,
		Arc::new(typed_table_batch(vec![80])),
	))
	.await;
	assert_eq!(
		node.data_server
			.cdc_live_recovery_upper_after(
				tenant(),
				target_table,
				&ranges,
				CdcCursor::MIN,
				hlc(100, 0, 46),
			)
			.await,
		None,
		"other-table ring overflow must not force target-table recovery"
	);

	let target_cursor = CdcCursor::new(
		hlc(90, 0, 46),
		Identity::new([0x90; 32]),
		RecordRowIndex::ZERO,
	);
	ring.push(CdcLiveEntry::new(
		target_cursor,
		tenant(),
		target_table,
		descriptor.logical_schema_fingerprint,
		Arc::new(typed_table_batch(vec![90])),
	))
	.await;
	assert_eq!(
		node.data_server
			.cdc_live_recovery_upper_after(
				tenant(),
				target_table,
				&ranges,
				CdcCursor::ceiling_for_hlc(hlc(80, 0, 46)),
				hlc(50, 0, 46),
			)
			.await,
		None,
		"recovery must not emit beyond the safe-emission watermark"
	);
	assert_eq!(
		node.data_server
			.cdc_live_recovery_upper_after(
				tenant(),
				target_table,
				&ranges,
				CdcCursor::ceiling_for_hlc(hlc(80, 0, 46)),
				hlc(100, 0, 46),
			)
			.await,
		Some(target_cursor)
	);

	shutdown_lease_node(node).await;
}

fn kv_batch(key: &[u8], value: &[u8]) -> RecordBatch {
	let schema = Arc::new(write_payload_schema());
	let mut keys = LargeBinaryBuilder::new();
	keys.append_value(key);
	let mut values = LargeBinaryBuilder::new();
	values.append_value(value);
	RecordBatch::try_new(
		schema,
		vec![Arc::new(keys.finish()), Arc::new(values.finish())],
	)
	.unwrap()
}

fn wide_kv_batch(row_count: usize) -> RecordBatch {
	let schema = Arc::new(write_payload_schema());
	let mut keys = LargeBinaryBuilder::new();
	let mut values = LargeBinaryBuilder::new();
	for row in 0..row_count {
		keys.append_value(format!("k{row}").as_bytes());
		values.append_value(format!("v{row}").as_bytes());
	}
	RecordBatch::try_new(
		schema,
		vec![Arc::new(keys.finish()), Arc::new(values.finish())],
	)
	.unwrap()
}

fn id(locality: u64) -> Identity {
	let mut bytes = [0u8; 32];
	bytes[..8].copy_from_slice(&locality.to_le_bytes());
	Identity::new(bytes)
}

fn tenant() -> TenantId {
	TenantId::ZERO
}

fn test_crypto() -> Arc<CryptoManager> {
	let crypto = Arc::new(CryptoManager::new_dev());
	crypto.install_local_only_for_tests(tenant()).unwrap();
	crypto
}

struct OrphanWitnessFixture {
	leader_node: WendelonNode,
	follower_node: WendelonNode,
	leader: Arc<WendelonDataServer>,
	follower: Arc<WendelonDataServer>,
	epoch_tx: watch::Sender<Arc<MembershipEpoch>>,
	_dirs: Vec<crate::test_support::TestDir>,
}

async fn build_orphan_witness_fixture(tag: &str) -> OrphanWitnessFixture {
	let leader_node_dir = bootstrap_tempdir(&format!("{tag}-leader"));
	let leader_node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		26,
		leader_node_dir.to_path_buf(),
		"loopback://26".into(),
	))
	.await
	.unwrap();
	let follower_node_dir = bootstrap_tempdir(&format!("{tag}-follower"));
	let follower_node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		27,
		follower_node_dir.to_path_buf(),
		"loopback://27".into(),
	))
	.await
	.unwrap();

	let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
	loop {
		if leader_node.raft.current_leader().await == Some(leader_node.local_node.id)
			&& follower_node.raft.current_leader().await == Some(follower_node.local_node.id)
		{
			break;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for single-node leaders"
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}

	let pool = leader_node.pool.clone();
	let handlers = LoopbackHandlers::new(pool.clone());
	let loopback_provider: Arc<dyn TransportProvider> = Arc::new(LoopbackProvider {
		handlers: handlers.clone(),
		tier: TransportTier::QuicIoUring,
	});
	let tiers = TransportTierSet::empty().with(TransportTier::QuicIoUring);
	let roles = NodeRoleSet::empty().with(NodeRole::Data);
	let mut peers = StdHashMap::new();
	peers.insert(
		leader_node.local_node,
		PeerCapabilities {
			address: "loopback://26".into(),
			tiers,
			roles,
			capabilities: crate::network::transport::NodeCapabilitySet::empty(),
		},
	);
	peers.insert(
		follower_node.local_node,
		PeerCapabilities {
			address: "loopback://27".into(),
			tiers,
			roles,
			capabilities: crate::network::transport::NodeCapabilitySet::empty(),
		},
	);
	let epoch = MembershipEpoch { epoch: 2, peers };
	let (epoch_tx, epoch_rx) = watch::channel(Arc::new(epoch));
	let resolver: Arc<dyn crate::network::transport::CapabilityResolver> = Arc::new(
		crate::network::transport::EpochCapabilityResolver::new(epoch_rx.clone()),
	);
	let build_factory = || {
		let mut providers: StdHashMap<TransportTier, Arc<dyn TransportProvider>> =
			StdHashMap::new();
		providers.insert(TransportTier::QuicIoUring, loopback_provider.clone());
		TransportProviderFactory::new(tiers, providers, resolver.clone(), epoch_rx.clone())
	};
	let router: Arc<dyn ReplicaRouter> = Arc::new(FixedReplicaRouter::new(vec![
		leader_node.local_node,
		follower_node.local_node,
	]));

	let leader = Arc::new(
		WendelonDataServer::new(
			leader_node.slab.clone(),
			leader_node.raft.clone(),
			leader_node.log_store.clone(),
			leader_node.state_machine.clone(),
			leader_node.local_node,
			leader_node.clock.clone(),
			router.clone(),
			build_factory(),
			pool.clone(),
		)
		.unwrap(),
	);
	let follower = Arc::new(
		WendelonDataServer::new(
			follower_node.slab.clone(),
			follower_node.raft.clone(),
			follower_node.log_store.clone(),
			follower_node.state_machine.clone(),
			follower_node.local_node,
			follower_node.clock.clone(),
			router,
			build_factory(),
			follower_node.pool.clone(),
		)
		.unwrap(),
	);

	let follower_data_ring: Arc<dyn DataRingServer> = follower.clone();
	let follower_service: Arc<dyn HrftService> = Arc::new(
		MsgRouter::builder()
			.with_data_ring(follower_data_ring)
			.build(),
	);
	handlers.register(follower_node.local_node, follower_service);

	OrphanWitnessFixture {
		leader_node,
		follower_node,
		leader,
		follower,
		epoch_tx,
		_dirs: vec![leader_node_dir, follower_node_dir],
	}
}

async fn make_manager() -> (crate::test_support::TestDir, Arc<SlabManager>) {
	let scratch = fresh_dir("query");
	let dir = scratch.to_path_buf();
	let io = spawn_io_thread(64).unwrap();
	(
		scratch,
		Arc::new(
			SlabManager::open(SlabManagerConfig {
				dir,
				max_slab_bytes: 1 << 30,
				io,
				l2: None,
				rehydration: crate::storage::manager::RehydrationPolicy::default(),
				l2_column_fetch: Default::default(),
				row_group_writer: Default::default(),
				l2_row_group_fetch: Default::default(),
				encoding_writer: Default::default(),
				l2_chunk_layout: Default::default(),
				l2_encoding_fetch: Default::default(),
				encoding_compute: Default::default(),
				group_commit: crate::storage::GroupCommitConfig::default(),
				crypto: test_crypto(),
			})
			.await
			.unwrap(),
		),
	)
}

async fn make_manager_with_l2(
	l2: Arc<dyn ObjectStore>,
) -> (crate::test_support::TestDir, Arc<SlabManager>) {
	let scratch = fresh_dir("query-l2");
	let dir = scratch.to_path_buf();
	let io = spawn_io_thread(64).unwrap();
	let mut rehydration = crate::storage::manager::RehydrationPolicy::default();
	rehydration.promote_after_l2_reads = 0;
	rehydration.tail_read_window_bytes = 256 + 16;
	(
		scratch,
		Arc::new(
			SlabManager::open(SlabManagerConfig {
				dir,
				max_slab_bytes: 1 << 30,
				io,
				l2: Some(l2),
				rehydration,
				l2_column_fetch: Default::default(),
				row_group_writer: Default::default(),
				l2_row_group_fetch: Default::default(),
				encoding_writer: Default::default(),
				l2_chunk_layout: Default::default(),
				l2_encoding_fetch: Default::default(),
				encoding_compute: Default::default(),
				group_commit: crate::storage::GroupCommitConfig::default(),
				crypto: test_crypto(),
			})
			.await
			.unwrap(),
		),
	)
}

async fn make_manager_with_l2_projected_fetch(
	l2: Arc<dyn ObjectStore>,
) -> (crate::test_support::TestDir, Arc<SlabManager>) {
	let scratch = fresh_dir("query-l2-projected");
	let dir = scratch.to_path_buf();
	let io = spawn_io_thread(64).unwrap();
	let mut rehydration = crate::storage::manager::RehydrationPolicy::default();
	rehydration.promote_after_l2_reads = 0;
	rehydration.tail_read_window_bytes = 256 + 16;
	(
		scratch,
		Arc::new(
			SlabManager::open(SlabManagerConfig {
				dir,
				max_slab_bytes: 1 << 30,
				io,
				l2: Some(l2),
				rehydration,
				l2_column_fetch: crate::storage::columnar::L2ColumnFetchConfig {
					enabled: true,
					min_record_bytes: crate::schema::RecordBodyByteLen::try_new(1).unwrap(),
					..Default::default()
				},
				row_group_writer: Default::default(),
				l2_row_group_fetch: Default::default(),
				encoding_writer: Default::default(),
				l2_chunk_layout: Default::default(),
				l2_encoding_fetch: Default::default(),
				encoding_compute: Default::default(),
				group_commit: crate::storage::GroupCommitConfig::default(),
				crypto: test_crypto(),
			})
			.await
			.unwrap(),
		),
	)
}

async fn make_manager_with_l2_encoded_compute(
	l2: Arc<dyn ObjectStore>,
) -> (crate::test_support::TestDir, Arc<SlabManager>) {
	let scratch = fresh_dir("query-l2-encoded-compute");
	let dir = scratch.to_path_buf();
	let io = spawn_io_thread(64).unwrap();
	let mut rehydration = crate::storage::manager::RehydrationPolicy::default();
	rehydration.promote_after_l2_reads = 0;
	rehydration.tail_read_window_bytes = 256 + 16;
	(
		scratch,
		Arc::new(
			SlabManager::open(SlabManagerConfig {
				dir,
				max_slab_bytes: 1 << 30,
				io,
				l2: Some(l2),
				rehydration,
				l2_column_fetch: crate::storage::columnar::L2ColumnFetchConfig {
					enabled: true,
					min_record_bytes: crate::schema::RecordBodyByteLen::try_new(1).unwrap(),
					..Default::default()
				},
				row_group_writer:
					crate::storage::row_group::RowGroupWriterConfig::point_read_with_overrides(
						crate::schema::RowGroupRowCount::new(4),
						crate::schema::RowGroupRowCount::new(8),
						RowCount::new(1),
						crate::schema::RecordBodyByteLen::new(1),
					)
					.with_enabled(true),
				l2_row_group_fetch: crate::storage::row_group::L2RowGroupFetchConfig {
					enabled: true,
					max_selected_row_group_fraction_micros:
						crate::storage::row_group::L2_ROW_GROUP_FETCH_FRACTION_DENOMINATOR - 1,
					..Default::default()
				},
				encoding_writer: EncodingWriterConfig {
					enabled: true,
					target_simd_page_values: crate::schema::SimdPageValueCount::new(4),
					min_buffer_bytes_for_encoding: crate::schema::RecordBodyByteLen::new(1),
					..Default::default()
				},
				l2_chunk_layout: Default::default(),
				l2_encoding_fetch: crate::storage::encoding::L2EncodingFetchConfig {
					enabled: true,
					..Default::default()
				},
				encoding_compute: crate::storage::encoding::EncodingComputeConfig {
					enabled: true,
					..Default::default()
				},
				group_commit: crate::storage::GroupCommitConfig::default(),
				crypto: test_crypto(),
			})
			.await
			.unwrap(),
		),
	)
}

async fn append_typed_table_rows(
	mgr: &SlabManager,
	descriptor: &TableSchemaDescriptor,
	ids: Vec<i64>,
) -> SlabId {
	let batch = typed_table_batch(ids);
	let mut write_meta: TableWriteRequestMeta = bytemuck::Zeroable::zeroed();
	write_meta.tenant_id = tenant();
	write_meta.table_object_id = descriptor.table_object_id;
	write_meta.schema_generation = descriptor.schema_generation;
	write_meta.row_count = encode_table_write_row_count(batch.num_rows()).unwrap();
	let admission = validate_table_write_request_batch(
		&write_meta,
		&batch,
		&descriptor,
		WriterNodeId::ZERO,
		write_meta.client_hlc,
	)
	.expect("typed rows should validate");
	for row in 0..batch.num_rows() {
		let row_batch = batch.slice(row, 1);
		let frame = encode_batch(&row_batch).unwrap();
		mgr.append_durable_with_locality(
			tenant(),
			admission.key_identities[row],
			admission.localities[row],
			hlc(10 + row as u64, 0, 1),
			&frame,
		)
		.await
		.unwrap();
	}
	mgr.force_seal_active_for_test().await
}

async fn append_append_only_rows_for_query(
	mgr: &SlabManager,
	descriptor: &TableSchemaDescriptor,
	ids: Vec<i64>,
	row_hlc: HybridTimestamp,
) -> Vec<Identity> {
	let batch = typed_table_batch(ids);
	let mut write_meta = typed_table_meta(descriptor, batch.num_rows());
	write_meta.client_hlc = row_hlc;
	let admission = validate_table_write_request_batch(
		&write_meta,
		&batch,
		descriptor,
		WriterNodeId::ZERO,
		row_hlc,
	)
	.expect("append-only rows should validate");
	let row_key_ids = admission.key_identities.clone();
	let bifurcated =
		bifurcate_table_write(&write_meta, &batch, descriptor, WriterNodeId::ZERO, row_hlc)
			.expect("append-only rows should bifurcate");
	append_bifurcated_table_children_durable(
		mgr,
		write_meta.tenant_id,
		row_hlc,
		&bifurcated.children,
		&[],
	)
	.await
	.unwrap();
	row_key_ids
}

async fn append_append_only_tombstones_for_query(
	mgr: &SlabManager,
	descriptor: &TableSchemaDescriptor,
	row_key_ids: &[Identity],
	delete_hlc: HybridTimestamp,
) {
	let layout = match descriptor.record_layout_policy {
		RecordLayoutPolicy::MultiRow(layout) => layout,
		RecordLayoutPolicy::SingleRow => {
			panic!("append-only tombstones require multi-row layout")
		}
	};
	let rows = row_key_ids
		.iter()
		.enumerate()
		.map(|(source_row_index, row_key_id)| MultiRowSourceRow {
			source_row_index,
			key_identity: *row_key_id,
			routing_locality: mgr
				.routing_locality_for_existing_key(tenant(), *row_key_id)
				.expect("append-only test row must have cached routing locality"),
			row_hlc: delete_hlc,
			is_tombstone: true,
			partition: None,
			table_object_id: Some(descriptor.table_object_id),
		})
		.collect::<Vec<_>>();
	let packed = pack_tombstone_multirow_source_rows_with_policy(
		layout,
		&rows,
		descriptor.record_body_compression_policy,
	)
	.expect("row tombstones should pack");
	for record in packed {
		let decoded = decode_batch(&record.encoded.frame).unwrap();
		let entries = storage_index_entries_from_batch(&decoded).unwrap();
		mgr.append_durable_multirow_with_locality(tenant(), &record.encoded.frame, &entries)
			.await
			.unwrap();
	}
}

async fn tier_active_slab_with_stats_for_query(
	mgr: &SlabManager,
	l2: Arc<MockObjectStore>,
	descriptor: &TableSchemaDescriptor,
	object_name: &str,
) -> SlabId {
	let slab_id = mgr.force_seal_active_for_test().await;
	let raw = mgr.read_sealed_raw(slab_id).await.unwrap();
	let mut catalog = TableSchemaCatalog::default();
	catalog.upsert(tenant(), descriptor.clone()).unwrap();
	let encrypted = mgr
		.encrypt_tiered_object_blob_with_table_stats(slab_id, &raw, &catalog)
		.unwrap();
	let locator = encrypted.stats_locator.expect("stats locator");
	let l2_uri = l2.put(object_name, &encrypted.bytes).await.unwrap();
	mgr.apply_tier_slab(TieredSlabPointer {
		slab_id,
		l2_uri,
		payload_hash: payload_digest(&encrypted.bytes),
		is_encrypted: true,
		tenant_id: Some(encrypted.tenant_id),
		tenant_kek_id: Some(encrypted.tenant_kek_id),
		stats_format_version: Some(locator.stats_format_version),
		stats_block_len: Some(locator.stats_block_len),
		stats_block_hash128: Some(locator.stats_block_hash128),
		tombstone_metadata: TombstoneMetadata::None,
		supersession_metadata: SupersessionMetadata::None,
	})
	.await
	.unwrap();
	slab_id
}

fn sorted_i64_column_values(batches: &[RecordBatch], column: &str) -> Vec<i64> {
	let mut values = Vec::new();
	for batch in batches {
		let ids = batch
			.column_by_name(column)
			.unwrap()
			.as_any()
			.downcast_ref::<Int64Array>()
			.unwrap();
		values.extend(ids.values().iter().copied());
	}
	values.sort_unstable();
	values
}

async fn tier_typed_table_batch_with_stats(
	l2: Arc<MockObjectStore>,
	ids: Vec<i64>,
	object_name: &str,
) -> (
	crate::test_support::TestDir,
	Arc<SlabManager>,
	TableSchemaDescriptor,
) {
	let (mgr_dir, mgr) = make_manager_with_l2(l2.clone()).await;
	let descriptor = typed_table_descriptor();
	let slab_id = append_typed_table_rows(&mgr, &descriptor, ids).await;

	let raw = mgr.read_sealed_raw(slab_id).await.unwrap();
	let mut catalog = TableSchemaCatalog::default();
	catalog.upsert(tenant(), descriptor.clone()).unwrap();
	let encrypted = mgr
		.encrypt_tiered_object_blob_with_table_stats(slab_id, &raw, &catalog)
		.unwrap();
	let locator = encrypted.stats_locator.expect("stats locator");
	let l2_uri = l2.put(object_name, &encrypted.bytes).await.unwrap();
	mgr.apply_tier_slab(TieredSlabPointer {
		slab_id,
		l2_uri,
		payload_hash: payload_digest(&encrypted.bytes),
		is_encrypted: true,
		tenant_id: Some(encrypted.tenant_id),
		tenant_kek_id: Some(encrypted.tenant_kek_id),
		stats_format_version: Some(locator.stats_format_version),
		stats_block_len: Some(locator.stats_block_len),
		stats_block_hash128: Some(locator.stats_block_hash128),
		tombstone_metadata: TombstoneMetadata::None,
		supersession_metadata: SupersessionMetadata::None,
	})
	.await
	.unwrap();

	(mgr_dir, mgr, descriptor)
}

async fn tier_identity_typed_table_batch_with_stats(
	l2: Arc<MockObjectStore>,
	ids: Vec<i64>,
	object_name: &str,
) -> (
	crate::test_support::TestDir,
	Arc<SlabManager>,
	TableSchemaDescriptor,
) {
	let (mgr_dir, mgr) = make_manager_with_l2(l2.clone()).await;
	let descriptor = identity_typed_table_descriptor(0x41);
	let slab_id = append_typed_table_rows(&mgr, &descriptor, ids).await;

	let raw = mgr.read_sealed_raw(slab_id).await.unwrap();
	let mut catalog = TableSchemaCatalog::default();
	catalog.upsert(tenant(), descriptor.clone()).unwrap();
	let encrypted = mgr
		.encrypt_tiered_object_blob_with_table_stats(slab_id, &raw, &catalog)
		.unwrap();
	let locator = encrypted.stats_locator.expect("stats locator");
	let l2_uri = l2.put(object_name, &encrypted.bytes).await.unwrap();
	mgr.apply_tier_slab(TieredSlabPointer {
		slab_id,
		l2_uri,
		payload_hash: payload_digest(&encrypted.bytes),
		is_encrypted: true,
		tenant_id: Some(encrypted.tenant_id),
		tenant_kek_id: Some(encrypted.tenant_kek_id),
		stats_format_version: Some(locator.stats_format_version),
		stats_block_len: Some(locator.stats_block_len),
		stats_block_hash128: Some(locator.stats_block_hash128),
		tombstone_metadata: TombstoneMetadata::None,
		supersession_metadata: SupersessionMetadata::None,
	})
	.await
	.unwrap();

	(mgr_dir, mgr, descriptor)
}

async fn tier_typed_table_batch_without_stats(
	l2: Arc<MockObjectStore>,
	ids: Vec<i64>,
	object_name: &str,
) -> (
	crate::test_support::TestDir,
	Arc<SlabManager>,
	TableSchemaDescriptor,
) {
	let (mgr_dir, mgr) = make_manager_with_l2(l2.clone()).await;
	let descriptor = typed_table_descriptor();
	let slab_id = append_typed_table_rows(&mgr, &descriptor, ids).await;

	let raw = mgr.read_sealed_raw(slab_id).await.unwrap();
	let encrypted = mgr.encrypt_tiered_object_blob(slab_id, &raw).unwrap();
	assert!(encrypted.stats_locator.is_none());
	let l2_uri = l2.put(object_name, &encrypted.bytes).await.unwrap();
	mgr.apply_tier_slab(TieredSlabPointer {
		slab_id,
		l2_uri,
		payload_hash: payload_digest(&encrypted.bytes),
		is_encrypted: true,
		tenant_id: Some(encrypted.tenant_id),
		tenant_kek_id: Some(encrypted.tenant_kek_id),
		stats_format_version: None,
		stats_block_len: None,
		stats_block_hash128: None,
		tombstone_metadata: TombstoneMetadata::None,
		supersession_metadata: SupersessionMetadata::None,
	})
	.await
	.unwrap();

	(mgr_dir, mgr, descriptor)
}

async fn seed_record(
	mgr: &SlabManager,
	locality: u64,
	hlc: HybridTimestamp,
	key: &[u8],
	value: &[u8],
) {
	let batch = kv_batch(key, value);
	let bytes = encode_batch(&batch).unwrap();
	mgr.append(tenant(), id(locality), hlc, &bytes)
		.await
		.unwrap();
}

fn base_meta() -> QueryRequestMeta {
	QueryRequestMeta {
		min_hlc: hlc(0, 0, 0),
		expected_epoch: 0,
		tenant_id: tenant(),
		consistency: Consistency::SERIALIZABLE,
		_pad: [0; 7],
		locality_range_start: 0,
		locality_range_end: u64::MAX,
		bytecode_len: 0,
		_pad2: [0; 4],
		as_of_hlc: hlc(0, 0, 0),
	}
}

#[test]
fn static_router_wraps_around_ring() {
	let router = StaticReplicaRouter::new(vec![n(1), n(2), n(3), n(4)], 3);
	let replicas = router.replicas_for_locality(Locality::new(3));
	assert_eq!(replicas.len(), 3);
	assert_eq!(replicas[0], n(4));
	assert_eq!(replicas[1], n(1));
	assert_eq!(replicas[2], n(2));
}

#[test]
fn static_router_dedups_and_sorts() {
	let router = StaticReplicaRouter::new(vec![n(3), n(1), n(3), n(2)], 5);
	let replicas = router.replicas_for_locality(Locality::ZERO);
	assert_eq!(replicas.len(), 3);
	assert_eq!(replicas[0], n(1));
	assert_eq!(replicas[1], n(2));
	assert_eq!(replicas[2], n(3));
}

#[test]
fn static_router_rf_one_is_single_replica() {
	let router = StaticReplicaRouter::new(vec![n(1), n(2), n(3)], 1);
	assert_eq!(router.replicas_for_locality(Locality::ZERO).len(), 1);
}

#[test]
fn static_router_empty_node_list_returns_empty() {
	let router = StaticReplicaRouter::new(vec![], 3);
	assert!(router.replicas_for_locality(Locality::ZERO).is_empty());
}

#[tokio::test]
async fn scan_collects_entries_in_inclusive_range() {
	let (_mgr_dir, mgr) = make_manager().await;
	seed_record(&mgr, 10, hlc(1, 0, 1), b"k10", b"v10").await;
	seed_record(&mgr, 50, hlc(2, 0, 1), b"k50", b"v50").await;
	seed_record(&mgr, 99, hlc(3, 0, 1), b"k99", b"v99").await;

	let hits = mgr.scan(tenant(), 10, 50);
	let keys: Vec<u64> = hits.iter().map(|(k, _)| *k).collect();
	assert_eq!(keys, vec![10, 50]);

	let full = mgr.scan(tenant(), 0, u64::MAX);
	assert_eq!(full.len(), 3);

	let miss = mgr.scan(tenant(), 60, 90);
	assert!(miss.is_empty());

	// Inverted range → empty, never panics.
	let inverted = mgr.scan(tenant(), 100, 5);
	assert!(inverted.is_empty());
}

#[tokio::test]
async fn query_passthrough_returns_all_rows_in_range() {
	let (_mgr_dir, mgr) = make_manager().await;
	seed_record(&mgr, 1, hlc(1, 0, 1), b"a", b"v1").await;
	seed_record(&mgr, 2, hlc(2, 0, 1), b"b", b"v2").await;
	seed_record(&mgr, 3, hlc(3, 0, 1), b"c", b"v3").await;

	let sm = WendelonStateMachine::new();
	let (resp, arrow) = execute_query_plan(&mgr, &sm, n(1), &base_meta(), &[])
		.await
		.unwrap();
	assert_eq!(resp.status, ReadStatus::OK);
	assert_eq!(resp.rows, 3);
	assert_eq!(resp.value_hlc, hlc(3, 0, 1));
	let bytes = arrow.expect("OK must carry arrow bytes");
	let batch = decode_batch(&bytes).unwrap();
	assert_eq!(batch.num_rows(), 3);
}

#[tokio::test]
async fn query_range_filters_out_of_window_rows() {
	let (_mgr_dir, mgr) = make_manager().await;
	seed_record(&mgr, 5, hlc(1, 0, 1), b"a", b"va").await;
	seed_record(&mgr, 42, hlc(2, 0, 1), b"b", b"vb").await;
	seed_record(&mgr, 100, hlc(3, 0, 1), b"c", b"vc").await;

	let sm = WendelonStateMachine::new();
	let mut meta = base_meta();
	meta.locality_range_start = 10;
	meta.locality_range_end = 50;
	let (resp, _) = execute_query_plan(&mgr, &sm, n(1), &meta, &[])
		.await
		.unwrap();
	assert_eq!(resp.status, ReadStatus::OK);
	assert_eq!(resp.rows, 1);
}

#[tokio::test]
async fn query_empty_scan_returns_key_not_found() {
	let (_mgr_dir, mgr) = make_manager().await;
	let sm = WendelonStateMachine::new();
	let (resp, arrow) = execute_query_plan(&mgr, &sm, n(1), &base_meta(), &[])
		.await
		.unwrap();
	assert_eq!(resp.status, ReadStatus::KEY_NOT_FOUND);
	assert_eq!(resp.rows, 0);
	assert_eq!(resp.arrow_len, 0);
	assert!(arrow.is_none());
}

#[tokio::test]
async fn query_as_of_older_than_retention_window_returns_retention_expired() {
	let (_mgr_dir, mgr) = make_manager().await;
	seed_record(&mgr, 1, hlc(90, 0, 1), b"a", b"va").await;

	let sm = WendelonStateMachine::new();
	let mut meta = base_meta();
	meta.as_of_hlc = hlc(50, 0, 1);
	let (resp, arrow) = execute_query_plan_with_budget_and_retention_floor(
		&mgr,
		&sm,
		n(1),
		&meta,
		&[],
		QueryExecutionBudget::default(),
		Some(hlc(80, 0, 1)),
	)
	.await
	.unwrap();
	assert_eq!(resp.status, ReadStatus::RETENTION_EXPIRED);
	assert_eq!(resp.rows, 0);
	assert!(arrow.is_none());
}

#[tokio::test]
async fn latest_query_filters_locations_below_logical_retention_floor() {
	let (_mgr_dir, mgr) = make_manager().await;
	seed_record(&mgr, 1, hlc(70, 0, 1), b"old", b"old-value").await;
	seed_record(&mgr, 2, hlc(90, 0, 1), b"live", b"live-value").await;

	let program = pbc::Program::compile(&[]).unwrap();
	let plan = plan_query_execution_for_ranges_with_program(
		&mgr,
		n(1),
		&base_meta(),
		&program,
		&[QueryScanRange {
			start: Locality::ZERO,
			end: Locality::MAX,
		}],
		QueryExecutionBudget::default(),
		None,
		None,
		QueryProjectedExecution::Disabled,
		RowIdentityProjection::None,
		Some(hlc(80, 0, 1)),
	)
	.await
	.unwrap();

	assert_eq!(plan.response.status, ReadStatus::OK);
	assert_eq!(plan.response.rows, 1);
}

#[tokio::test]
async fn table_query_row_limit_bounds_successful_output() {
	let (_mgr_dir, mgr) = make_manager().await;
	seed_record(&mgr, 1, hlc(70, 0, 1), b"a", b"a-value").await;
	seed_record(&mgr, 2, hlc(80, 0, 1), b"b", b"b-value").await;
	seed_record(&mgr, 3, hlc(90, 0, 1), b"c", b"c-value").await;

	let program = pbc::Program::compile(&[]).unwrap();
	let plan = plan_query_execution_for_ranges_with_program(
		&mgr,
		n(1),
		&base_meta(),
		&program,
		&[QueryScanRange {
			start: Locality::ZERO,
			end: Locality::MAX,
		}],
		QueryExecutionBudget::default(),
		Some(2),
		None,
		QueryProjectedExecution::Disabled,
		RowIdentityProjection::None,
		None,
	)
	.await
	.unwrap();

	assert_eq!(plan.response.status, ReadStatus::OK);
	assert_eq!(plan.response.rows, 2);
	assert_eq!(
		plan.batches
			.iter()
			.map(RecordBatch::num_rows)
			.sum::<usize>(),
		2
	);
}

#[tokio::test]
async fn table_query_resp_streams_with_terminal_metadata() {
	let (_mgr_dir, mgr) = make_manager().await;
	seed_record(&mgr, 1, hlc(70, 0, 1), b"a", b"a-value").await;
	let typed_bytes = encode_batch(&typed_table_batch(vec![20])).unwrap();
	mgr.append(tenant(), id(2), hlc(80, 0, 1), &typed_bytes)
		.await
		.unwrap();

	let program = pbc::Program::compile(&[]).unwrap();
	let meta = base_meta();
	let (mut client_side, server_side) = tokio::io::duplex(64 * 1024);
	let mut server_stream = DuplexTransportStream { inner: server_side };
	let provisional = query_ok_provisional_meta(&meta, n(1));
	let mut writer = TableQueryResponseStreamWriter::new(&mut server_stream, 118, provisional);

	let plan = plan_query_execution_for_ranges_with_program_maybe_cancelled(
		&mgr,
		n(1),
		&meta,
		&program,
		&[QueryScanRange {
			start: Locality::ZERO,
			end: Locality::MAX,
		}],
		QueryExecutionBudget::default(),
		None,
		None,
		QueryProjectedExecution::Disabled,
		RowIdentityProjection::None,
		None,
		None,
		Some(&mut writer),
	)
	.await
	.unwrap();
	assert_eq!(plan.response.status, ReadStatus::OK);
	assert_eq!(plan.response.rows, 2);
	assert!(
		plan.batches.is_empty(),
		"streaming path must not collect emitted batches"
	);
	assert_eq!(writer.batch_count(), 2);
	writer.emit_terminal(plan.response).await.unwrap();
	drop(writer);
	server_stream.finish().await.unwrap();
	let pool = crate::network::pool::BufPool::new(8, 64 * 1024);

	let first = read_frame(&mut client_side, &pool).await.unwrap();
	assert_eq!(first.header.msg_type, MsgType::TABLE_QUERY_RESP.0);
	assert_eq!(first.header.flags & FRAME_FLAG_STREAM_END, 0);
	assert!(first.arrow_payload.is_some());
	let first_meta: QueryResponseMeta =
		*bytemuck::from_bytes(&first.meta[..std::mem::size_of::<QueryResponseMeta>()]);
	assert_eq!(first_meta.status, ReadStatus::OK);
	assert_eq!(first_meta.rows, 0, "first-frame rows are provisional");

	let continuation = read_frame(&mut client_side, &pool).await.unwrap();
	assert_eq!(continuation.header.msg_type, MsgType::TABLE_QUERY_RESP.0);
	assert_eq!(continuation.header.flags & FRAME_FLAG_STREAM_END, 0);
	assert!(continuation.meta.is_empty());
	assert!(continuation.arrow_payload.is_some());

	let terminal = read_frame(&mut client_side, &pool).await.unwrap();
	assert_ne!(terminal.header.flags & FRAME_FLAG_STREAM_END, 0);
	assert!(terminal.arrow_payload.is_none());
	let terminal_meta: QueryResponseMeta =
		*bytemuck::from_bytes(&terminal.meta[..std::mem::size_of::<QueryResponseMeta>()]);
	assert_eq!(terminal_meta.status, ReadStatus::OK);
	assert_eq!(terminal_meta.rows, 2);
	assert_eq!(terminal_meta.value_hlc, hlc(80, 0, 1));
}

#[tokio::test]
async fn empty_successful_table_query_can_emit_terminal_metadata_only() {
	let meta = base_meta();
	let (mut client_side, server_side) = tokio::io::duplex(4096);
	let mut server_stream = DuplexTransportStream { inner: server_side };
	let provisional = query_ok_provisional_meta(&meta, n(1));
	let mut writer = TableQueryResponseStreamWriter::new(&mut server_stream, 118, provisional);

	writer
		.emit_terminal(query_ok_provisional_meta(&meta, n(1)))
		.await
		.unwrap();
	drop(writer);
	server_stream.finish().await.unwrap();
	let pool = crate::network::pool::BufPool::new(8, 4096);

	let terminal = read_frame(&mut client_side, &pool).await.unwrap();
	assert_eq!(terminal.header.msg_type, MsgType::TABLE_QUERY_RESP.0);
	assert_ne!(terminal.header.flags & FRAME_FLAG_STREAM_END, 0);
	assert!(terminal.arrow_payload.is_none());
	let meta: QueryResponseMeta =
		*bytemuck::from_bytes(&terminal.meta[..std::mem::size_of::<QueryResponseMeta>()]);
	assert_eq!(meta.status, ReadStatus::OK);
	assert_eq!(meta.rows, 0);
}

/// Test transport that cancels a held token after a configured
/// number of complete HRFT frames. The triggering frame write itself still
/// succeeds — only the producer's *between-location* cancel check
/// observes the flip — so downstream cancellation fires
/// deterministically after N batches are on the wire.
struct CancelAfterFrameTransport {
	inner: DuplexStream,
	token: CancellationToken,
	cancel_after_frames: usize,
	frames_seen: usize,
	header_buf: Vec<u8>,
	payload_remaining: Option<usize>,
}

impl CancelAfterFrameTransport {
	fn new(inner: DuplexStream, token: CancellationToken, cancel_after_frames: usize) -> Self {
		Self {
			inner,
			token,
			cancel_after_frames,
			frames_seen: 0,
			header_buf: Vec::with_capacity(std::mem::size_of::<FrameHeader>()),
			payload_remaining: None,
		}
	}

	fn observe_written_bytes(&mut self, mut bytes: &[u8]) {
		while !bytes.is_empty() {
			if let Some(remaining) = self.payload_remaining.as_mut() {
				let consumed = (*remaining).min(bytes.len());
				*remaining -= consumed;
				bytes = &bytes[consumed..];
				if *remaining == 0 {
					self.payload_remaining = None;
					self.note_frame_complete();
				}
				continue;
			}

			let header_size = std::mem::size_of::<FrameHeader>();
			let needed = header_size - self.header_buf.len();
			let consumed = needed.min(bytes.len());
			self.header_buf.extend_from_slice(&bytes[..consumed]);
			bytes = &bytes[consumed..];
			if self.header_buf.len() == header_size {
				let header: FrameHeader = *bytemuck::from_bytes(&self.header_buf);
				self.header_buf.clear();
				let payload_len = header.meta_len as usize
					+ header.arrow_len as usize
					+ header.trailing_len as usize;
				if payload_len == 0 {
					self.note_frame_complete();
				} else {
					self.payload_remaining = Some(payload_len);
				}
			}
		}
	}

	fn note_frame_complete(&mut self) {
		self.frames_seen += 1;
		if self.frames_seen == self.cancel_after_frames {
			self.token.cancel();
		}
	}
}

impl AsyncRead for CancelAfterFrameTransport {
	fn poll_read(
		mut self: Pin<&mut Self>,
		cx: &mut TaskContext<'_>,
		buf: &mut ReadBuf<'_>,
	) -> Poll<std::io::Result<()>> {
		Pin::new(&mut self.inner).poll_read(cx, buf)
	}
}

impl AsyncWrite for CancelAfterFrameTransport {
	fn poll_write(
		mut self: Pin<&mut Self>,
		cx: &mut TaskContext<'_>,
		buf: &[u8],
	) -> Poll<std::io::Result<usize>> {
		match Pin::new(&mut self.inner).poll_write(cx, buf) {
			Poll::Ready(Ok(written)) => {
				self.observe_written_bytes(&buf[..written]);
				Poll::Ready(Ok(written))
			}
			other => other,
		}
	}
	fn poll_flush(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<std::io::Result<()>> {
		Pin::new(&mut self.inner).poll_flush(cx)
	}
	fn poll_shutdown(
		mut self: Pin<&mut Self>,
		cx: &mut TaskContext<'_>,
	) -> Poll<std::io::Result<()>> {
		Pin::new(&mut self.inner).poll_shutdown(cx)
	}
}

#[async_trait]
impl TransportStream for CancelAfterFrameTransport {
	fn stream_id(&self) -> u64 {
		0
	}
	async fn finish(&mut self) -> std::io::Result<()> {
		use tokio::io::AsyncWriteExt;
		self.inner.shutdown().await
	}
}

/// Build the projected (row-group / L2) fixture for the cost-bound
/// tests: six rows tiered to a mock object store, a keep-everything
/// PBC predicate, and the projected requirement. On this path the
/// producer reads and emits one frame *per location inside the scan
/// loop*, so a cancellation that arrives after the first frame halts
/// the scan before the remaining row groups are read — the Toron
/// production path, unlike the whole-record rebatch path which
/// materializes every location before emitting.
/// ADR 0088: a consumer that drops mid-stream now bounds producer work by the
/// response quantum rather than by one row, so a test of that bound needs a
/// source larger than one quantum. Six rows still serve every other caller.
async fn projected_cost_bound_fixture_with_rows(
	tier_name: &str,
	rows: usize,
) -> (
	crate::test_support::TestDir,
	Arc<SlabManager>,
	TableSchemaDescriptor,
	pbc::Program,
	usize,
) {
	let l2 = Arc::new(MockObjectStore::default());
	let (mgr_dir, mgr) = make_manager_with_l2_projected_fetch(l2.clone()).await;
	let descriptor = append_only_table_descriptor();
	append_append_only_rows_for_query(
		&mgr,
		&descriptor,
		(1..=rows as i64).collect(),
		hlc(10, 0, 1),
	)
	.await;
	let _ = tier_active_slab_with_stats_for_query(&mgr, l2, &descriptor, tier_name).await;

	// `col0 >= 0` keeps every row, so a full scan reads and emits
	// every location.
	let mut predicate = Vec::new();
	predicate.push(OP_PUSH_COL);
	predicate.extend_from_slice(&0_u32.to_le_bytes());
	predicate.push(OP_PUSH_INT);
	predicate.extend_from_slice(&0_i64.to_le_bytes());
	predicate.push(OP_GTE);
	let program = pbc::Program::compile(&predicate).unwrap();
	let user_column_count = descriptor.arrow_schema().unwrap().fields().len();
	(mgr_dir, mgr, descriptor, program, user_column_count)
}

async fn descriptor_backed_whole_record_cost_bound_fixture() -> (
	crate::test_support::TestDir,
	Arc<SlabManager>,
	TableSchemaDescriptor,
	pbc::Program,
	QueryRequestMeta,
	[QueryScanRange; 1],
) {
	descriptor_backed_whole_record_cost_bound_fixture_with_rows(6).await
}

/// ADR 0088: see `projected_cost_bound_fixture_with_rows` — a drop-bounding
/// control needs more rows than one response batch holds.
async fn descriptor_backed_whole_record_cost_bound_fixture_with_rows(
	rows: usize,
) -> (
	crate::test_support::TestDir,
	Arc<SlabManager>,
	TableSchemaDescriptor,
	pbc::Program,
	QueryRequestMeta,
	[QueryScanRange; 1],
) {
	let (mgr_dir, mgr) = make_manager().await;
	let descriptor = append_only_table_descriptor();
	append_append_only_rows_for_query(
		&mgr,
		&descriptor,
		(1..=rows as i64).collect(),
		hlc(10, 0, 1),
	)
	.await;
	let program = pbc::Program::compile(&[]).unwrap();
	let mut meta = base_meta();
	meta.as_of_hlc = hlc(99, 0, 1);
	let ranges = [QueryScanRange {
		start: Locality::ZERO,
		end: Locality::MAX,
	}];
	(mgr_dir, mgr, descriptor, program, meta, ranges)
}

#[tokio::test]
async fn cancelled_consumer_stops_before_all_row_groups_scanned() {
	// ADR 118 headline cost bound: a consumer that cancels mid-stream
	// makes Wendelon stop scanning before it reaches the remaining
	// row groups.
	let (_mgr_dir, mgr, descriptor, program, user_column_count) =
		projected_cost_bound_fixture_with_rows("adr118-cost-bound.wnd", 5_000).await;
	let mut meta = base_meta();
	meta.as_of_hlc = hlc(99, 0, 1);
	let ranges = [QueryScanRange {
		start: Locality::ZERO,
		end: Locality::MAX,
	}];

	// Control: an uncancelled projected scan emits one frame per location.
	let (_full_client, full_server) = tokio::io::duplex(256 * 1024);
	let mut full_stream = DuplexTransportStream { inner: full_server };
	let mut full_writer = TableQueryResponseStreamWriter::new(
		&mut full_stream,
		118,
		query_ok_provisional_meta(&meta, n(1)),
	);
	let full = plan_query_execution_for_ranges_with_program_maybe_cancelled(
		&mgr,
		n(1),
		&meta,
		&program,
		&ranges,
		QueryExecutionBudget::default(),
		None,
		Some(&descriptor),
		QueryProjectedExecution::Pbc {
			requirement: analyze_pbc_column_requirement(&program, user_column_count).unwrap(),
			user_column_count,
			runtime_filter: None,
		},
		RowIdentityProjection::None,
		None,
		None,
		Some(&mut full_writer),
	)
	.await
	.unwrap();
	assert_eq!(full.response.status, ReadStatus::OK);
	// ADR 0088: measured as producer work, not frames — and over a source
	// larger than one response quantum, since below that the first frame only
	// arrives after the scan has already finished.
	assert!(
		full.clocks.progress_marks > u64::from(TABLE_QUERY_RESPONSE_TARGET_ROWS as u32),
		"projected scan emits one frame per location (got {})",
		full_writer.batch_count()
	);

	// Cancelled mid-stream: cancellation fires as the first frame is
	// written, so the producer halts at the next location's cancel
	// check — before reading the remaining row groups.
	let token = CancellationToken::new();
	let (_cancel_client, cancel_server) = tokio::io::duplex(256 * 1024);
	let mut cancel_stream = CancelAfterFrameTransport::new(cancel_server, token.clone(), 1);
	let mut cancel_writer = TableQueryResponseStreamWriter::new(
		&mut cancel_stream,
		118,
		query_ok_provisional_meta(&meta, n(1)),
	);
	let cancelled = plan_query_execution_for_ranges_with_program_maybe_cancelled(
		&mgr,
		n(1),
		&meta,
		&program,
		&ranges,
		QueryExecutionBudget::default(),
		None,
		Some(&descriptor),
		QueryProjectedExecution::Pbc {
			requirement: analyze_pbc_column_requirement(&program, user_column_count).unwrap(),
			user_column_count,
			runtime_filter: None,
		},
		RowIdentityProjection::None,
		None,
		Some(&token),
		Some(&mut cancel_writer),
	)
	.await
	.unwrap();
	assert_eq!(cancelled.response.status, ReadStatus::QUERY_CANCELLED);
	assert_eq!(
		cancel_writer.batch_count(),
		1,
		"one response batch is emitted before the cancel check halts the scan"
	);
	assert!(
		cancelled.clocks.progress_marks < full.clocks.progress_marks,
		"cancellation bounds the scan strictly below a full materialization: {} vs {}",
		cancelled.clocks.progress_marks,
		full.clocks.progress_marks
	);
}

#[tokio::test]
async fn whole_record_cancelled_consumer_stops_before_all_locations_scanned() {
	let (_mgr_dir, mgr, descriptor, program, meta, ranges) =
		descriptor_backed_whole_record_cost_bound_fixture_with_rows(5_000).await;

	let (_full_client, full_server) = tokio::io::duplex(256 * 1024);
	let mut full_stream = DuplexTransportStream { inner: full_server };
	let mut full_writer = TableQueryResponseStreamWriter::new(
		&mut full_stream,
		118,
		query_ok_provisional_meta(&meta, n(1)),
	);
	let full = plan_query_execution_for_ranges_with_program_maybe_cancelled(
		&mgr,
		n(1),
		&meta,
		&program,
		&ranges,
		QueryExecutionBudget::default(),
		None,
		Some(&descriptor),
		QueryProjectedExecution::Disabled,
		RowIdentityProjection::None,
		None,
		None,
		Some(&mut full_writer),
	)
	.await
	.unwrap();
	assert_eq!(full.response.status, ReadStatus::OK);
	// ADR 0088: producer work, not frames — over a source larger than one
	// response quantum, since below that the first frame arrives only after
	// the scan is already complete.
	assert!(
		full.clocks.progress_marks > u64::from(TABLE_QUERY_RESPONSE_TARGET_ROWS as u32),
		"whole-record scan should emit multiple per-location batches"
	);

	let token = CancellationToken::new();
	let (_cancel_client, cancel_server) = tokio::io::duplex(256 * 1024);
	let mut cancel_stream = CancelAfterFrameTransport::new(cancel_server, token.clone(), 1);
	let mut cancel_writer = TableQueryResponseStreamWriter::new(
		&mut cancel_stream,
		118,
		query_ok_provisional_meta(&meta, n(1)),
	);
	let cancelled = plan_query_execution_for_ranges_with_program_maybe_cancelled(
		&mgr,
		n(1),
		&meta,
		&program,
		&ranges,
		QueryExecutionBudget::default(),
		None,
		Some(&descriptor),
		QueryProjectedExecution::Disabled,
		RowIdentityProjection::None,
		None,
		Some(&token),
		Some(&mut cancel_writer),
	)
	.await
	.unwrap();

	assert_eq!(cancelled.response.status, ReadStatus::QUERY_CANCELLED);
	assert_eq!(cancel_writer.batch_count(), 1);
	assert!(cancelled.clocks.progress_marks < full.clocks.progress_marks);
}

#[tokio::test]
async fn collect_select_star_limit_3_stops_whole_record_scan_before_full_scan() {
	let (_mgr_dir, mgr, descriptor, program, meta, ranges) =
		descriptor_backed_whole_record_cost_bound_fixture().await;
	let full = plan_query_execution_for_ranges_with_program(
		&mgr,
		n(1),
		&meta,
		&program,
		&ranges,
		QueryExecutionBudget::default(),
		None,
		Some(&descriptor),
		QueryProjectedExecution::Disabled,
		RowIdentityProjection::None,
		None,
	)
	.await
	.unwrap();
	assert_eq!(full.response.status, ReadStatus::OK);
	// ADR 0088: batch count is no longer a proxy for locations visited — rows
	// coalesce into bounded response batches. The guarantee under test is
	// bounded producer work, so it is measured directly.
	assert!(
		full.clocks.progress_marks > 3,
		"control scan must examine more source locations than LIMIT 3: {}",
		full.clocks.progress_marks
	);

	let limited = plan_query_execution_for_ranges_with_program(
		&mgr,
		n(1),
		&meta,
		&program,
		&ranges,
		QueryExecutionBudget::default(),
		Some(3),
		Some(&descriptor),
		QueryProjectedExecution::Disabled,
		RowIdentityProjection::None,
		None,
	)
	.await
	.unwrap();
	assert_eq!(limited.response.status, ReadStatus::OK);
	assert_eq!(limited.response.rows, 3);
	assert_eq!(
		limited.batches.len(),
		1,
		"three rows under the response target arrive as one tail batch"
	);
	assert!(
		limited.clocks.progress_marks < full.clocks.progress_marks,
		"LIMIT 3 must stop examining locations before the full scan completes: {} vs {}",
		limited.clocks.progress_marks,
		full.clocks.progress_marks
	);
	assert_eq!(
		limited
			.batches
			.iter()
			.map(RecordBatch::num_rows)
			.sum::<usize>(),
		3
	);
}

#[tokio::test]
async fn stream_select_star_consume_one_batch_then_drop_stops_whole_record_scan() {
	let (_mgr_dir, mgr, descriptor, program, meta, ranges) =
		descriptor_backed_whole_record_cost_bound_fixture_with_rows(5_000).await;

	let (_full_client, full_server) = tokio::io::duplex(256 * 1024);
	let mut full_stream = DuplexTransportStream { inner: full_server };
	let mut full_writer = TableQueryResponseStreamWriter::new(
		&mut full_stream,
		120,
		query_ok_provisional_meta(&meta, n(1)),
	);
	let full = plan_query_execution_for_ranges_with_program_maybe_cancelled(
		&mgr,
		n(1),
		&meta,
		&program,
		&ranges,
		QueryExecutionBudget::default(),
		None,
		Some(&descriptor),
		QueryProjectedExecution::Disabled,
		RowIdentityProjection::None,
		None,
		None,
		Some(&mut full_writer),
	)
	.await
	.unwrap();
	assert_eq!(full.response.status, ReadStatus::OK);
	// ADR 0088: producer work, not frames — over a source larger than one
	// response quantum, since below that the first frame arrives only after
	// the scan is already complete.
	assert!(
		full.clocks.progress_marks > u64::from(TABLE_QUERY_RESPONSE_TARGET_ROWS as u32),
		"control stream must emit more locations than the consuming cursor"
	);

	let token = CancellationToken::new();
	let (_drop_client, dropped_server) = tokio::io::duplex(256 * 1024);
	let mut dropped_stream = CancelAfterFrameTransport::new(dropped_server, token.clone(), 1);
	let mut dropped_writer = TableQueryResponseStreamWriter::new(
		&mut dropped_stream,
		120,
		query_ok_provisional_meta(&meta, n(1)),
	);
	let bounded = plan_query_execution_for_ranges_with_program_maybe_cancelled(
		&mgr,
		n(1),
		&meta,
		&program,
		&ranges,
		QueryExecutionBudget::default(),
		None,
		Some(&descriptor),
		QueryProjectedExecution::Disabled,
		RowIdentityProjection::None,
		None,
		Some(&token),
		Some(&mut dropped_writer),
	)
	.await
	.unwrap();
	assert_eq!(bounded.response.status, ReadStatus::QUERY_CANCELLED);
	assert!(
		bounded.clocks.progress_marks < full.clocks.progress_marks,
		"dropping after three streamed rows bounds the whole-record scan"
	);
}

#[tokio::test]
async fn whole_record_emitted_batch_matches_descriptor_schema() {
	let (_mgr_dir, mgr, descriptor, program, meta, ranges) =
		descriptor_backed_whole_record_cost_bound_fixture().await;
	let plan = plan_query_execution_for_ranges_with_program(
		&mgr,
		n(1),
		&meta,
		&program,
		&ranges,
		QueryExecutionBudget::default(),
		Some(3),
		Some(&descriptor),
		QueryProjectedExecution::Disabled,
		RowIdentityProjection::None,
		None,
	)
	.await
	.unwrap();
	let schema = descriptor.arrow_schema().unwrap();
	assert!(!plan.batches.is_empty());
	for batch in &plan.batches {
		assert_eq!(batch.schema().as_ref(), &schema);
	}
}

#[tokio::test]
async fn whole_record_descriptorless_path_remains_behavior_compatible() {
	let (_mgr_dir, mgr) = make_manager().await;
	seed_record(&mgr, 1, hlc(70, 0, 1), b"a", b"a-value").await;
	seed_record(&mgr, 2, hlc(80, 0, 1), b"b", b"b-value").await;
	seed_record(&mgr, 3, hlc(90, 0, 1), b"c", b"c-value").await;
	let program = pbc::Program::compile(&[]).unwrap();
	let plan = plan_query_execution_for_ranges_with_program(
		&mgr,
		n(1),
		&base_meta(),
		&program,
		&[QueryScanRange {
			start: Locality::ZERO,
			end: Locality::MAX,
		}],
		QueryExecutionBudget::default(),
		None,
		None,
		QueryProjectedExecution::Disabled,
		RowIdentityProjection::None,
		None,
	)
	.await
	.unwrap();

	assert_eq!(plan.response.status, ReadStatus::OK);
	assert_eq!(plan.response.rows, 3);
	assert_eq!(
		plan.batches.len(),
		1,
		"descriptorless whole-record queries retain legacy schema-grouped output"
	);
	assert_eq!(plan.batches[0].num_rows(), 3);
}

#[tokio::test]
async fn bare_limit_three_bounds_remote_scan_without_row_limit_pushdown() {
	// Row_limit pushdown DISABLED (row_limit = None). A bare `.take(n)`
	// that does not push its bound down still bounds remote work: the
	// executor drops/cancels the scan once it has enough rows and
	// Wendelon stops before reading the remaining row groups. The
	// bound comes from the drop, not from pushdown.
	let (_mgr_dir, mgr, descriptor, program, user_column_count) =
		projected_cost_bound_fixture_with_rows("adr118-bare-limit.wnd", 5_000).await;
	let mut meta = base_meta();
	meta.as_of_hlc = hlc(99, 0, 1);
	let ranges = [QueryScanRange {
		start: Locality::ZERO,
		end: Locality::MAX,
	}];

	// Without pushdown AND without an early drop, every location is scanned.
	let (_full_client, full_server) = tokio::io::duplex(256 * 1024);
	let mut full_stream = DuplexTransportStream { inner: full_server };
	let mut full_writer = TableQueryResponseStreamWriter::new(
		&mut full_stream,
		118,
		query_ok_provisional_meta(&meta, n(1)),
	);
	let full = plan_query_execution_for_ranges_with_program_maybe_cancelled(
		&mgr,
		n(1),
		&meta,
		&program,
		&ranges,
		QueryExecutionBudget::default(),
		None, // row_limit pushdown disabled
		Some(&descriptor),
		QueryProjectedExecution::Pbc {
			requirement: analyze_pbc_column_requirement(&program, user_column_count).unwrap(),
			user_column_count,
			runtime_filter: None,
		},
		RowIdentityProjection::None,
		None,
		None,
		Some(&mut full_writer),
	)
	.await
	.unwrap();
	assert_eq!(full.response.status, ReadStatus::OK);
	// ADR 0088: frames no longer track locations, so producer work is measured
	// directly. The source deliberately exceeds one response quantum — below
	// that, the first frame only arrives once the scan is already complete and
	// a consumer drop cannot bound anything.
	assert!(
		full.clocks.progress_marks > u64::from(TABLE_QUERY_RESPONSE_TARGET_ROWS as u32),
		"the control must examine more locations than one response batch holds (got {})",
		full.clocks.progress_marks
	);

	// The bounded consumer drops after the first batch → cancellation
	// fires → remote scan is bounded even though row_limit is None.
	let token = CancellationToken::new();
	let (_drop_client, dropped_server) = tokio::io::duplex(256 * 1024);
	let mut dropped_stream = CancelAfterFrameTransport::new(dropped_server, token.clone(), 1);
	let mut dropped_writer = TableQueryResponseStreamWriter::new(
		&mut dropped_stream,
		118,
		query_ok_provisional_meta(&meta, n(1)),
	);
	let bounded = plan_query_execution_for_ranges_with_program_maybe_cancelled(
		&mgr,
		n(1),
		&meta,
		&program,
		&ranges,
		QueryExecutionBudget::default(),
		None, // still no row_limit pushdown
		Some(&descriptor),
		QueryProjectedExecution::Pbc {
			requirement: analyze_pbc_column_requirement(&program, user_column_count).unwrap(),
			user_column_count,
			runtime_filter: None,
		},
		RowIdentityProjection::None,
		None,
		Some(&token),
		Some(&mut dropped_writer),
	)
	.await
	.unwrap();
	assert_eq!(bounded.response.status, ReadStatus::QUERY_CANCELLED);
	assert!(
		bounded.clocks.progress_marks < full.clocks.progress_marks,
		"consumer drop bounds remote scan below full materialization without row_limit \
		 pushdown: {} vs {}",
		bounded.clocks.progress_marks,
		full.clocks.progress_marks
	);
}

#[tokio::test]
async fn terminal_metadata_totals_match_legacy_materialized_response() {
	// The streaming producer's authoritative terminal totals
	// (rows / value_hlc) must equal the legacy materialized
	// single-response totals for the same fixture.
	let (_mgr_dir, mgr) = make_manager().await;
	seed_record(&mgr, 1, hlc(70, 0, 1), b"a", b"a-value").await;
	seed_record(&mgr, 2, hlc(95, 0, 1), b"b", b"b-value").await;
	seed_record(&mgr, 3, hlc(88, 0, 1), b"c", b"c-value").await;
	let program = pbc::Program::compile(&[]).unwrap();
	let meta = base_meta();
	let ranges = [QueryScanRange {
		start: Locality::ZERO,
		end: Locality::MAX,
	}];

	// Legacy materialized response: batches are collected, not streamed.
	let legacy = plan_query_execution_for_ranges_with_program(
		&mgr,
		n(1),
		&meta,
		&program,
		&ranges,
		QueryExecutionBudget::default(),
		None,
		None,
		QueryProjectedExecution::Disabled,
		RowIdentityProjection::None,
		None,
	)
	.await
	.unwrap();

	// Streaming producer with terminal metadata.
	let (_client, server) = tokio::io::duplex(256 * 1024);
	let mut stream = DuplexTransportStream { inner: server };
	let mut writer = TableQueryResponseStreamWriter::new(
		&mut stream,
		118,
		query_ok_provisional_meta(&meta, n(1)),
	);
	let streamed = plan_query_execution_for_ranges_with_program_maybe_cancelled(
		&mgr,
		n(1),
		&meta,
		&program,
		&ranges,
		QueryExecutionBudget::default(),
		None,
		None,
		QueryProjectedExecution::Disabled,
		RowIdentityProjection::None,
		None,
		None,
		Some(&mut writer),
	)
	.await
	.unwrap();

	assert_eq!(streamed.response.status, ReadStatus::OK);
	assert_eq!(
		streamed.response.rows, legacy.response.rows,
		"streamed terminal rows equal legacy materialized rows"
	);
	assert_eq!(
		streamed.response.value_hlc, legacy.response.value_hlc,
		"streamed terminal value_hlc equals legacy materialized value_hlc"
	);
	assert_eq!(
		writer.batch_count(),
		legacy.batches.len(),
		"streamed frame count equals legacy batch count"
	);
	assert!(
		streamed.batches.is_empty(),
		"streaming path must not also collect batches"
	);
}

#[tokio::test]
async fn linearizable_gate_fires_when_watermark_behind() {
	let (_mgr_dir, mgr) = make_manager().await;
	seed_record(&mgr, 1, hlc(1, 0, 1), b"k", b"v").await;
	let sm = WendelonStateMachine::new();
	// Watermark remains at default (0,0,0); client demands (5,0,0).
	let mut meta = base_meta();
	meta.consistency = Consistency::LINEARIZABLE;
	meta.min_hlc = hlc(5, 0, 0);
	let (resp, arrow) = execute_query_plan(&mgr, &sm, n(1), &meta, &[])
		.await
		.unwrap();
	assert_eq!(resp.status, ReadStatus::E_SAFE_BEHIND);
	assert_eq!(resp.leader_hint, n(1));
	assert!(arrow.is_none());
}

#[tokio::test]
async fn linearizable_gate_passes_once_watermark_catches_up() {
	let (_mgr_dir, mgr) = make_manager().await;
	seed_record(&mgr, 1, hlc(1, 0, 1), b"k", b"v").await;
	let sm = WendelonStateMachine::new();
	sm.seed_e_safe_for_test(0, hlc(10, 0, 1)).await;
	let mut meta = base_meta();
	meta.consistency = Consistency::LINEARIZABLE;
	meta.min_hlc = hlc(5, 0, 1);
	let (resp, _) = execute_query_plan(&mgr, &sm, n(1), &meta, &[])
		.await
		.unwrap();
	assert_eq!(resp.status, ReadStatus::OK);
	assert_eq!(resp.rows, 1);
}

#[tokio::test]
async fn epoch_stale_returns_redirect() {
	let (_mgr_dir, mgr) = make_manager().await;
	seed_record(&mgr, 1, hlc(1, 0, 1), b"k", b"v").await;
	let sm = WendelonStateMachine::new();
	// The SM has advanced to epoch 7; client still thinks we
	// are on epoch 3 → EPOCH_STALE, not a silent scan.
	sm.seed_e_safe_for_test(7, hlc(1, 0, 1)).await;
	let mut meta = base_meta();
	meta.expected_epoch = 3;
	let (resp, _) = execute_query_plan(&mgr, &sm, n(1), &meta, &[])
		.await
		.unwrap();
	assert_eq!(resp.status, ReadStatus::EPOCH_STALE);
	assert_eq!(resp.leader_hint, n(1));
}

#[tokio::test]
async fn pbc_filter_drops_rows_below_threshold() {
	// Stored records carry an Int64 `age` column so PBC can
	// actually compare against a literal — the write-path
	// schema is LargeBinary and PBC does not support bytes,
	// so tests that exercise filter opcodes use their own
	// schema via direct slab.append.
	use arrow::array::Int64Array;
	use arrow::datatypes::{DataType, Field, Schema};
	let schema = Arc::new(Schema::new(vec![Field::new("age", DataType::Int64, false)]));
	let (_mgr_dir, mgr) = make_manager().await;
	for (locality, age) in [(1u64, 17i64), (2, 22), (3, 40)] {
		let ages = Int64Array::from(vec![age]);
		let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(ages)]).unwrap();
		let bytes = encode_batch(&batch).unwrap();
		mgr.append(tenant(), id(locality), hlc(age as u64, 0, 1), &bytes)
			.await
			.unwrap();
	}

	// age >= 18 AND age >= 18 (duplicated to exercise OP_AND)
	let mut bytecode = Vec::new();
	bytecode.push(OP_PUSH_COL);
	bytecode.extend_from_slice(&0u32.to_le_bytes());
	bytecode.push(OP_PUSH_INT);
	bytecode.extend_from_slice(&18i64.to_le_bytes());
	bytecode.push(OP_GTE);
	bytecode.push(OP_PUSH_COL);
	bytecode.extend_from_slice(&0u32.to_le_bytes());
	bytecode.push(OP_PUSH_INT);
	bytecode.extend_from_slice(&18i64.to_le_bytes());
	bytecode.push(OP_GTE);
	bytecode.push(OP_AND);

	let sm = WendelonStateMachine::new();
	let mut meta = base_meta();
	meta.bytecode_len = bytecode.len() as u32;
	let (resp, _) = execute_query_plan(&mgr, &sm, n(1), &meta, &bytecode)
		.await
		.unwrap();
	assert_eq!(resp.status, ReadStatus::OK);
	// Rows 2 (age 22) and 3 (age 40) survive; row 1 (age 17) drops.
	assert_eq!(resp.rows, 2);
}

#[tokio::test]
async fn pbc_projection_narrows_schema() {
	let (_mgr_dir, mgr) = make_manager().await;
	seed_record(&mgr, 1, hlc(1, 0, 1), b"a", b"va").await;
	seed_record(&mgr, 2, hlc(2, 0, 1), b"b", b"vb").await;
	// PROJECT column 0 (the `key` column) only — PBC compiles
	// a terminal PROJECT with no preceding filter, so every row
	// is kept and only the `key` column survives.
	let mut bytecode = Vec::new();
	bytecode.push(OP_PROJECT);
	bytecode.extend_from_slice(&1u16.to_le_bytes());
	bytecode.extend_from_slice(&0u32.to_le_bytes());

	let sm = WendelonStateMachine::new();
	let mut meta = base_meta();
	meta.bytecode_len = bytecode.len() as u32;
	let (resp, arrow) = execute_query_plan(&mgr, &sm, n(1), &meta, &bytecode)
		.await
		.unwrap();
	assert_eq!(resp.status, ReadStatus::OK);
	assert_eq!(resp.rows, 2);
	let batch = decode_batch(&arrow.unwrap()).unwrap();
	assert_eq!(batch.num_columns(), 1);
	assert_eq!(batch.schema().field(0).name(), "key");
}

#[tokio::test]
async fn storage_metadata_scalar_pbc_opcodes_return_invalid_program() {
	let (_mgr_dir, mgr) = make_manager().await;
	seed_record(&mgr, 1, hlc(1, 0, 1), b"a", b"va").await;

	let sm = WendelonStateMachine::new();
	let mut meta = base_meta();
	meta.bytecode_len = 1;
	let (resp, arrow) = execute_query_plan(&mgr, &sm, n(1), &meta, &[0x40])
		.await
		.unwrap();
	assert_eq!(resp.status, ReadStatus::QUERY_INVALID_PROGRAM);
	assert_eq!(resp.rows, 0);
	assert!(arrow.is_none());
}

#[tokio::test]
async fn invalid_pbc_returns_structured_query_status() {
	let (_mgr_dir, mgr) = make_manager().await;
	seed_record(&mgr, 1, hlc(1, 0, 1), b"a", b"va").await;

	let sm = WendelonStateMachine::new();
	let mut meta = base_meta();
	meta.bytecode_len = 1;
	let (resp, arrow) = execute_query_plan(&mgr, &sm, n(1), &meta, &[0x7F])
		.await
		.unwrap();
	assert_eq!(resp.status, ReadStatus::QUERY_INVALID_PROGRAM);
	assert_eq!(resp.rows, 0);
	assert_eq!(resp.arrow_len, 0);
	assert!(arrow.is_none());
}

#[tokio::test]
async fn query_budget_exceeded_returns_structured_status() {
	let (_mgr_dir, mgr) = make_manager().await;
	seed_record(&mgr, 1, hlc(1, 0, 1), b"a", b"va").await;
	seed_record(&mgr, 2, hlc(2, 0, 1), b"b", b"vb").await;

	let sm = WendelonStateMachine::new();
	let (resp, arrow) = execute_query_plan_with_budget_and_retention_floor(
		&mgr,
		&sm,
		n(1),
		&base_meta(),
		&[],
		QueryExecutionBudget {
			max_locations: 1,
			max_rows: 10,
			max_wall_time: Duration::from_secs(1),
			stats_pruning_enabled: true,
			caller_totals: true,
			caller_deadline: None,
		},
		None,
	)
	.await
	.unwrap();
	assert_eq!(resp.status, ReadStatus::QUERY_BUDGET_EXCEEDED);
	assert_eq!(resp.rows, 0);
	assert_eq!(resp.arrow_len, 0);
	assert!(arrow.is_none());
}

#[tokio::test]
async fn query_budget_counts_scanned_locations_even_when_filter_rejects_rows() {
	use arrow::array::Int64Array;
	use arrow::datatypes::{DataType, Field, Schema};

	let (_mgr_dir, mgr) = make_manager().await;
	let schema = Arc::new(Schema::new(vec![Field::new("age", DataType::Int64, false)]));
	for (locality, age) in [(1_u64, 17_i64), (2, 22)] {
		let ages = Int64Array::from(vec![age]);
		let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(ages)]).unwrap();
		let bytes = encode_batch(&batch).unwrap();
		mgr.append(tenant(), id(locality), hlc(age as u64, 0, 1), &bytes)
			.await
			.unwrap();
	}

	let mut bytecode = Vec::new();
	bytecode.push(OP_PUSH_COL);
	bytecode.extend_from_slice(&0_u32.to_le_bytes());
	bytecode.push(OP_PUSH_INT);
	bytecode.extend_from_slice(&99_i64.to_le_bytes());
	bytecode.push(OP_GTE);

	let sm = WendelonStateMachine::new();
	let mut meta = base_meta();
	meta.bytecode_len = bytecode.len() as u32;
	let (resp, arrow) = execute_query_plan_with_budget_and_retention_floor(
		&mgr,
		&sm,
		n(1),
		&meta,
		&bytecode,
		QueryExecutionBudget {
			max_locations: 1,
			max_rows: 10,
			max_wall_time: Duration::from_secs(1),
			stats_pruning_enabled: true,
			caller_totals: true,
			caller_deadline: None,
		},
		None,
	)
	.await
	.unwrap();
	assert_eq!(resp.status, ReadStatus::QUERY_BUDGET_EXCEEDED);
	assert_eq!(resp.rows, 0);
	assert!(arrow.is_none());
}

#[tokio::test]
async fn query_budget_exceeded_when_single_location_batch_exceeds_row_cap() {
	let (_mgr_dir, mgr) = make_manager().await;
	let bytes = encode_batch(&wide_kv_batch(11)).unwrap();
	mgr.append(tenant(), id(1), hlc(1, 0, 1), &bytes)
		.await
		.unwrap();

	let sm = WendelonStateMachine::new();
	let (resp, arrow) = execute_query_plan_with_budget_and_retention_floor(
		&mgr,
		&sm,
		n(1),
		&base_meta(),
		&[],
		QueryExecutionBudget {
			max_locations: 10,
			max_rows: 10,
			max_wall_time: Duration::from_secs(1),
			stats_pruning_enabled: true,
			caller_totals: true,
			caller_deadline: None,
		},
		None,
	)
	.await
	.unwrap();
	assert_eq!(resp.status, ReadStatus::QUERY_BUDGET_EXCEEDED);
	assert_eq!(resp.rows, 0);
	assert_eq!(resp.arrow_len, 0);
	assert!(arrow.is_none());
}

/// ADR 541: the two expiry causes are distinct at the encode preflight, and a
/// progressing encode never dies by lifetime — each cause gets its own fixture and
/// its own status; no shared assertion.
#[tokio::test]
async fn stream_preflight_encode_expires_by_cause_never_by_lifetime() {
	// Caller deadline in the past: absolute, non-resetting, distinct status.
	let mut expired_deadline = QueryClocks::new(
		Duration::from_secs(3600),
		Some(Instant::now() - Duration::from_millis(1)),
	);
	let encoded =
		encode_query_stream_batches_with_deadline(vec![single_kv_batch()], &mut expired_deadline)
			.await
			.unwrap();
	assert_eq!(encoded, Err(ReadStatus::QUERY_DEADLINE_EXCEEDED));

	// No progress for the stall bound: sliding, distinct status.
	let mut stalled = QueryClocks::new(Duration::from_millis(0), None);
	tokio::time::sleep(Duration::from_millis(2)).await;
	let encoded = encode_query_stream_batches_with_deadline(vec![single_kv_batch()], &mut stalled)
		.await
		.unwrap();
	assert_eq!(encoded, Err(ReadStatus::QUERY_STALLED));

	// A progressing encode with a generous stall bound and no caller deadline
	// completes regardless of how many batches it carries: no lifetime exists.
	let mut progressing = QueryClocks::new(Duration::from_secs(3600), None);
	let batches = vec![single_kv_batch(); 64];
	let encoded = encode_query_stream_batches_with_deadline(batches, &mut progressing)
		.await
		.unwrap();
	assert_eq!(encoded.map(|arrows| arrows.len()), Ok(64));
}

// ========================================================
//  LEASE GATE TESTS (plan 018)
//
//  These tests drive the Data Ring lease gate end-to-end by
//  standing up a single-node cluster via
//  `WendelonNode::bootstrap_single_node`. The gate logic is
//  exercised against the real `WendelonDataServer` from that
//  bootstrap (the node pubs it on the struct).
// ========================================================

use crate::node::{NodeBootstrapConfig, WendelonNode};
use crate::schema::{
	ClientWriteId, Consistency as SchemaConsistency, DurabilityLevel, SlabId, WriteRequestMeta,
};

fn bootstrap_tempdir(tag: &str) -> crate::test_support::TestDir {
	crate::test_support::TestDir::new(&format!("wendelon-lease-{tag}"))
}

#[test]
fn lease_scratch_guard_removes_only_its_owned_directory() {
	let other = bootstrap_tempdir("ownership-other");
	std::fs::write(other.join("slab"), b"keep").unwrap();
	let owned_path;
	{
		let owned = bootstrap_tempdir("ownership-owned");
		owned_path = owned.to_path_buf();
		std::fs::write(owned.join("slab"), b"remove").unwrap();
		assert!(owned_path.exists());
		assert_eq!(std::fs::read(other.join("slab")).unwrap(), b"keep");
	}
	assert!(
		!owned_path.exists(),
		"dropping the guard removes non-empty scratch"
	);
	assert_eq!(std::fs::read(other.join("slab")).unwrap(), b"keep");
}

/// Fixtures that play the secondary-index lifecycle driver's part themselves
/// (declaration cut, backfill, completion) stop the node's own driver first. It
/// ticks every second; a test that outlives a tick under load would otherwise
/// have the driver cut and backfill the slabs its assertions are about.
async fn stop_secondary_index_lifecycle(node: &WendelonNode) {
	node.handles.secondary_index_lifecycle.abort();
	let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
	while !node.handles.secondary_index_lifecycle.is_finished() {
		assert!(
			tokio::time::Instant::now() < deadline,
			"the secondary-index lifecycle driver did not stop"
		);
		tokio::time::sleep(Duration::from_millis(1)).await;
	}
}

/// A hang detector, not a latency assertion. The node bounds quiescence and the
/// storage close at `storage_shutdown_timeout` each (30 s in the dev config); the
/// storage close fsyncs every owned redb store, and on a host saturated with
/// flushes it was measured at up to 6.7 s, past 10 s in bursts. The outer bound
/// therefore sits above the node's own bounds and only catches a real hang.
const LEASE_FIXTURE_SHUTDOWN_BOUND: Duration = Duration::from_secs(120);

async fn shutdown_lease_node(node: WendelonNode) {
	node.handles.abort();
	tokio::time::timeout(LEASE_FIXTURE_SHUTDOWN_BOUND, async {
		node.raft.shutdown().await.unwrap();
		node.shutdown().await.unwrap();
	})
	.await
	.expect("lease fixture node shutdown timed out");
}

fn simple_write_meta(epoch: u64) -> WriteRequestMeta {
	WriteRequestMeta {
		write_id: ClientWriteId {
			client_uuid: [7u8; 16],
			sequence_no: 1,
			idempotency_ttl_ms: 60_000,
			_pad: 0,
		},
		client_hlc: hlc(10, 0, 0),
		expected_epoch: epoch,
		tenant_id: tenant(),
		proxy_hop: 0,
		durability: DurabilityLevel::QUORUM,
		await_finalization: 0,
		_pad1: [0; 5],
		proxy_source: NodeId {
			id: 0,
			_reserved: 0,
		},
		key_identity: Identity::blake3_tenant(tenant(), b"k"),
		key_len: 1,
		value_len: 1,
	}
}

fn single_kv_batch() -> RecordBatch {
	let schema = Arc::new(write_payload_schema());
	let mut keys = LargeBinaryBuilder::new();
	keys.append_value(b"k");
	let mut values = LargeBinaryBuilder::new();
	values.append_value(b"v");
	RecordBatch::try_new(
		schema,
		vec![Arc::new(keys.finish()), Arc::new(values.finish())],
	)
	.unwrap()
}

fn simple_read_meta(epoch: u64) -> ReadRequestMeta {
	ReadRequestMeta {
		min_hlc: hlc(0, 0, 0),
		expected_epoch: epoch,
		tenant_id: tenant(),
		consistency: SchemaConsistency::LINEARIZABLE,
		_pad: [0; 3],
		key_identity: Identity::blake3_tenant(tenant(), b"k"),
		key_len: 1,
		_pad2: [0; 8],
		as_of_hlc: hlc(0, 0, 0),
	}
}

fn single_key_batch() -> RecordBatch {
	let schema = Arc::new(crate::schema::read_key_schema());
	let mut keys = LargeBinaryBuilder::new();
	keys.append_value(b"k");
	RecordBatch::try_new(schema, vec![Arc::new(keys.finish())]).unwrap()
}

fn actor(role: ActorRole, tenant_id: Option<&str>) -> ActorIdentity {
	ActorIdentity::new(
		role,
		None,
		tenant_id.map(str::to_string),
		vec!["cluster".to_string()],
		vec![1, 2, 3, 4],
	)
	.unwrap()
}

async fn wait_for_audit_log(path: &std::path::Path, needles: &[&str]) -> String {
	let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
	loop {
		let audit = std::fs::read_to_string(path).unwrap_or_default();
		if needles.iter().all(|needle| audit.contains(needle)) {
			return audit;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for audit needles {:?}; current log: {}",
			needles,
			audit
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
}

#[tokio::test]
async fn write_rejected_when_lease_gate_on_and_cache_empty() {
	let dir = bootstrap_tempdir("empty-cache");
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		11,
		dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();
	// Stop background renewal before forcing the cache empty; under
	// parallel test runners the lease manager can otherwise repopulate
	// the cache between clear() and the write under test.
	node.handles.lease_manager.abort();
	tokio::task::yield_now().await;
	node.data_server.lease_cache().write().unwrap().clear();

	let mut meta = simple_write_meta(1);
	meta.client_hlc = hlc(u64::MAX / 4, 0, node.local_node.id);
	let (resp, _) = node
		.data_server
		.write(meta, single_kv_batch())
		.await
		.unwrap();
	assert_eq!(resp.status, WriteStatus::LEASE_EXPIRED);
	assert_eq!(resp.leader_hint, node.local_node);
	shutdown_lease_node(node).await;
}

#[tokio::test]
async fn linearizable_read_rejected_when_lease_missing() {
	let dir = bootstrap_tempdir("lin-read");
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		12,
		dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();
	node.data_server.lease_cache().write().unwrap().clear();

	let (resp, _) = node
		.data_server
		.read(simple_read_meta(1), single_key_batch())
		.await
		.unwrap();
	assert_eq!(resp.status, ReadStatus::LEASE_EXPIRED);
	shutdown_lease_node(node).await;
}

#[tokio::test]
async fn refresh_leases_once_clears_stale_epoch_entries() {
	let dir = bootstrap_tempdir("stale-epoch");
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		13,
		dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();
	// Seed a lease from a prior epoch — the cache must drop it
	// on the next refresh tick because the current ring epoch
	// is 1, not 99.
	node.data_server
		.seed_lease_for_test(crate::schema::LeaseDescriptor {
			epoch: 99,
			holder: node.local_node,
			range_start: 0,
			range_end: u64::MAX,
			lease_start: hlc(0, 0, node.local_node.id),
			lease_expiry: hlc(u64::MAX / 2, 0, node.local_node.id),
			min_commit_index: 0,
			issuer: node.local_node,
			signature: [0; 64],
		});
	// Give Raft a moment to elect and ring updater to seed.
	let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
	loop {
		if node.raft.current_leader().await == Some(node.local_node.id) {
			break;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for leader"
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
	let granted = node
		.data_server
		.refresh_leases_once(&node.ring, &*node.clock, 60_000)
		.await;
	// Exactly one primary range on a single-node cluster, so one grant.
	assert!(granted >= 1);
	let cache = node.data_server.lease_cache();
	let entries = cache.read().unwrap().clone();
	assert!(
		entries.iter().all(|l| l.epoch == 1),
		"stale-epoch leases must be evicted; cache={entries:?}"
	);
	assert!(entries.iter().any(|l| l.holder.id == node.local_node.id));
	shutdown_lease_node(node).await;
}

#[tokio::test]
async fn write_accepted_once_lease_manager_has_granted() {
	let dir = bootstrap_tempdir("accept");
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		14,
		dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();
	// Wait for Raft leadership and drive one explicit refresh
	// so the cache is guaranteed to be warm before the write
	// arrives — the spawned manager is on a 1s tick so tests
	// that race against it would be flaky.
	let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
	loop {
		if node.raft.current_leader().await == Some(node.local_node.id) {
			break;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for leader"
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
	node.data_server
		.refresh_leases_once(&node.ring, &*node.clock, 60_000)
		.await;

	let mut meta = simple_write_meta(1);
	// Push client_hlc into the granted window (the manager used
	// `clock.now_ms()` for lease_start; any value at-or-after 0
	// and before expiry is fine).
	meta.client_hlc = hlc(1, 0, 0);
	let (resp, _) = node
		.data_server
		.write(meta, single_kv_batch())
		.await
		.unwrap();
	assert_eq!(resp.status, WriteStatus::OK, "got resp={resp:?}");
	shutdown_lease_node(node).await;
}

#[tokio::test]
async fn finalization_marker_preserves_requested_durability_class() {
	let dir = bootstrap_tempdir("durability-marker");
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		15,
		dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();
	let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
	loop {
		if node.raft.current_leader().await == Some(node.local_node.id) {
			break;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for leader"
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
	node.data_server
		.refresh_leases_once(&node.ring, &*node.clock, 60_000)
		.await;

	let mut meta = simple_write_meta(1);
	meta.client_hlc = hlc(1, 0, 0);
	meta.await_finalization = 1;
	meta.durability = DurabilityLevel::LEADER;

	let (resp, trailing) = node
		.data_server
		.write(meta, single_kv_batch())
		.await
		.unwrap();
	assert_eq!(resp.status, WriteStatus::OK, "got resp={resp:?}");
	assert_eq!(
		trailing
			.expect("await_finalization should return a marker")
			.durability,
		DurabilityLevel::LEADER
	);
	shutdown_lease_node(node).await;
}

#[test]
fn bifurcation_rejects_tampered_key_identity() {
	let mut meta = simple_write_meta(1);
	meta.key_identity = Identity::blake3_tenant(tenant(), b"wrong-key");
	let err = bifurcate_client_write(&meta, &single_kv_batch()).unwrap_err();
	assert!(err.to_string().contains("key_identity mismatch"));
}

#[test]
fn table_bifurcation_splits_rows_into_independent_arrow_frames() {
	let descriptor = typed_table_descriptor();
	let batch = typed_table_batch(vec![7, 8]);
	let meta = typed_table_meta(&descriptor, batch.num_rows());
	let bifurcated = bifurcate_table_write(
		&meta,
		&batch,
		&descriptor,
		WriterNodeId::ZERO,
		meta.client_hlc,
	)
	.unwrap();

	assert_eq!(bifurcated.children.len(), 2);
	assert_eq!(
		bifurcated.charged_bytes,
		bifurcated
			.children
			.iter()
			.map(|child| child.charged_bytes)
			.sum::<u64>()
	);
	assert_ne!(
		bifurcated.children[0].key_identity,
		bifurcated.children[1].key_identity
	);
	assert_eq!(bifurcated.children[0].child_ordinal, 1_000);
	assert_eq!(bifurcated.children[1].child_ordinal, 1_001);
	let ranges = table_spread_prefix_ranges(
		meta.tenant_id,
		meta.table_object_id,
		match descriptor.locality_policy {
			LocalityPolicy::TableSpreadProjection(spread) => spread,
			_ => unreachable!(),
		},
	)
	.unwrap();

	for (idx, child) in bifurcated.children.iter().enumerate() {
		assert!(
			ranges
				.iter()
				.any(|(start, end)| start.get() <= child.locality.get()
					&& child.locality.get() <= end.get())
		);
		assert_eq!(
			child.payload_hash,
			payload_digest(child.frame_bytes.as_slice())
		);
		let row = decode_batch(child.frame_bytes.as_slice()).unwrap();
		assert_eq!(row.num_rows(), 1);
		assert_eq!(row.schema().as_ref(), &typed_table_schema());
		let ids = row
			.column_by_name("id")
			.unwrap()
			.as_any()
			.downcast_ref::<Int64Array>()
			.unwrap();
		assert_eq!(ids.value(0), if idx == 0 { 7 } else { 8 });
	}
}

#[test]
fn parent_table_batch_is_one_commit_unit_across_locality_groups() {
	let descriptor = typed_table_descriptor();
	let batch = typed_table_batch(vec![7, 8]);
	let meta = typed_table_meta(&descriptor, batch.num_rows());
	let bifurcated = bifurcate_table_write(
		&meta,
		&batch,
		&descriptor,
		WriterNodeId::ZERO,
		meta.client_hlc,
	)
	.unwrap();
	assert_eq!(bifurcated.children.len(), 2);
	assert_eq!(bifurcated.record_count, 2);

	let first = table_replica_meta(
		&meta,
		&bifurcated,
		&bifurcated.children[..1],
		meta.client_hlc,
	);
	let second = table_replica_meta(
		&meta,
		&bifurcated,
		&bifurcated.children[1..],
		meta.client_hlc,
	);
	assert_eq!(first.parent_write_id, meta.parent_write_id);
	assert_eq!(second.parent_write_id, meta.parent_write_id);
	assert_eq!(first.payload_hash, bifurcated.payload_hash);
	assert_eq!(second.payload_hash, bifurcated.payload_hash);
	assert_eq!(first.record_count, 2);
	assert_eq!(second.record_count, 2);
	assert_eq!(first.group_record_count, 1);
	assert_eq!(second.group_record_count, 1);
}

#[test]
fn table_bifurcation_packs_multirow_records_with_storage_metadata() {
	let descriptor = multirow_typed_table_descriptor();
	let source_ids = same_multirow_locality_ids(&descriptor, 3);
	let batch = typed_table_batch(source_ids.clone());
	let meta = typed_table_meta(&descriptor, batch.num_rows());
	let bifurcated = bifurcate_table_write(
		&meta,
		&batch,
		&descriptor,
		WriterNodeId::ZERO,
		meta.client_hlc,
	)
	.unwrap();

	assert_eq!(bifurcated.children.len(), 1);
	assert_eq!(bifurcated.record_count, 1);
	let storage = decode_batch(bifurcated.children[0].frame_bytes.as_slice()).unwrap();
	assert_eq!(storage.num_rows(), 3);
	assert!(has_storage_synthetic_columns(&storage));
	let materialized = materialize_record_location_batch(&storage, RecordRowIndex::new(1)).unwrap();
	assert_eq!(materialized.schema().as_ref(), &typed_table_schema());
	assert_user_visible_schema_has_no_storage_columns(&materialized);
	let ids = materialized
		.column_by_name("id")
		.unwrap()
		.as_any()
		.downcast_ref::<Int64Array>()
		.unwrap();
	let storage_ids = storage
		.column_by_name("id")
		.unwrap()
		.as_any()
		.downcast_ref::<Int64Array>()
		.unwrap();
	assert_eq!(ids.value(0), storage_ids.value(1));
	assert!(source_ids.contains(&ids.value(0)));
}

#[test]
fn table_bifurcation_charges_encoded_bytes_for_compressed_quota_axis() {
	let lz4_descriptor = multirow_typed_table_descriptor();
	let mut uncompressed_descriptor = lz4_descriptor.clone();
	uncompressed_descriptor.record_body_compression_policy =
		RecordBodyCompressionPolicy::Uncompressed;
	uncompressed_descriptor
		.refresh_descriptor_fingerprints()
		.unwrap();
	uncompressed_descriptor.validate().unwrap();

	let source_ids = same_multirow_locality_ids(&lz4_descriptor, 32);
	let repeated_name = "same-value".repeat(128);
	let names = (0..source_ids.len())
		.map(|_| repeated_name.as_str())
		.collect::<Vec<_>>();
	let batch = RecordBatch::try_new(
		Arc::new(typed_table_schema()),
		vec![
			Arc::new(Int64Array::from(source_ids)) as Arc<dyn Array>,
			Arc::new(LargeStringArray::from(names)) as Arc<dyn Array>,
		],
	)
	.unwrap();

	let uncompressed_meta = typed_table_meta(&uncompressed_descriptor, batch.num_rows());
	let lz4_meta = typed_table_meta(&lz4_descriptor, batch.num_rows());
	let uncompressed = bifurcate_table_write(
		&uncompressed_meta,
		&batch,
		&uncompressed_descriptor,
		WriterNodeId::ZERO,
		uncompressed_meta.client_hlc,
	)
	.unwrap();
	let lz4 = bifurcate_table_write(
		&lz4_meta,
		&batch,
		&lz4_descriptor,
		WriterNodeId::ZERO,
		lz4_meta.client_hlc,
	)
	.unwrap();

	assert_eq!(
		uncompressed.charged_bytes,
		uncompressed
			.children
			.iter()
			.map(|child| child.frame_bytes.len() as u64)
			.sum::<u64>()
	);
	assert_eq!(
		lz4.charged_bytes,
		lz4.children
			.iter()
			.map(|child| child.frame_bytes.len() as u64)
			.sum::<u64>()
	);
	assert!(
		lz4.charged_bytes < uncompressed.charged_bytes,
		"quota accounting should use encoded bytes: lz4={} uncompressed={}",
		lz4.charged_bytes,
		uncompressed.charged_bytes
	);
	for child in &lz4.children {
		assert_eq!(child.charged_bytes, child.frame_bytes.len() as u64);
		assert_eq!(
			crate::storage::ipc::record_batch_compression_policy_from_ipc(&child.frame_bytes)
				.unwrap(),
			RecordBodyCompressionPolicy::Lz4Frame
		);
	}
}

#[test]
fn table_query_scan_ranges_accepts_column_projection_prefixes() {
	let tenant_id = tenant();
	let projection = ColumnLocalityDescriptor {
		columns: vec![ClusteringColumnSpec {
			name: "id".to_owned(),
			width_bits: 16,
			string_prefix_bytes: None,
		}],
		projection: ProjectionFunction::ZOrder,
		spread_factor: 4,
		prefix_bits: 12,
	};
	let descriptor = TableSchemaDescriptor::new(
		TableObjectId::new([0x38; 16]),
		3,
		"analytics",
		"events",
		&typed_table_schema(),
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
		LocalityPolicy::ColumnProjection(projection.clone()),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.unwrap();
	let ranges = table_query_scan_ranges(tenant_id, &descriptor, None).unwrap();
	let expected =
		column_projection_prefix_ranges(tenant_id, descriptor.table_object_id, &projection)
			.unwrap();

	assert_eq!(ranges.len(), expected.len());
	for (range, (start, end)) in ranges.iter().zip(expected) {
		assert_eq!(range.start, start);
		assert_eq!(range.end, end);
	}
}

#[test]
fn table_query_scan_ranges_prune_column_projection_from_pbc() {
	let tenant_id = tenant();
	let projection = ColumnLocalityDescriptor {
		columns: vec![ClusteringColumnSpec {
			name: "id".to_owned(),
			width_bits: 16,
			string_prefix_bytes: None,
		}],
		projection: ProjectionFunction::ZOrder,
		spread_factor: 4,
		prefix_bits: 12,
	};
	let descriptor = TableSchemaDescriptor::new(
		TableObjectId::new([0x39; 16]),
		3,
		"analytics",
		"events",
		&typed_table_schema(),
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
		LocalityPolicy::ColumnProjection(projection.clone()),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.unwrap();
	let mut bytecode = vec![OP_PUSH_COL];
	bytecode.extend_from_slice(&0_u32.to_le_bytes());
	bytecode.push(OP_PUSH_INT);
	bytecode.extend_from_slice(&7_i64.to_le_bytes());
	bytecode.push(OP_GTE);
	let program = pbc::Program::compile(&bytecode).unwrap();
	let ranges = table_query_scan_ranges(tenant_id, &descriptor, Some(&program)).unwrap();
	let full = column_projection_prefix_ranges(tenant_id, descriptor.table_object_id, &projection)
		.unwrap();

	assert_eq!(ranges.len(), full.len());
	assert!(ranges.iter().zip(full.iter()).all(|(range, (start, end))| {
		start.get() <= range.start.get()
			&& range.end.get() <= end.get()
			&& (range.start != *start || range.end != *end)
	}));
}

#[tokio::test]
async fn table_query_skips_tiered_slab_when_column_stats_cannot_match() {
	let l2 = Arc::new(MockObjectStore::default());
	let (_mgr_dir, mgr) = make_manager_with_l2(l2.clone()).await;
	let descriptor = typed_table_descriptor();
	let batch = typed_table_batch(vec![1, 2]);
	let mut write_meta: TableWriteRequestMeta = bytemuck::Zeroable::zeroed();
	write_meta.tenant_id = tenant();
	write_meta.table_object_id = descriptor.table_object_id;
	write_meta.schema_generation = descriptor.schema_generation;
	write_meta.row_count = encode_table_write_row_count(batch.num_rows()).unwrap();
	let admission = validate_table_write_request_batch(
		&write_meta,
		&batch,
		&descriptor,
		WriterNodeId::ZERO,
		write_meta.client_hlc,
	)
	.expect("typed rows should validate");
	for row in 0..batch.num_rows() {
		let row_batch = batch.slice(row, 1);
		let frame = encode_batch(&row_batch).unwrap();
		mgr.append_durable_with_locality(
			tenant(),
			admission.key_identities[row],
			admission.localities[row],
			hlc(10 + row as u64, 0, 1),
			&frame,
		)
		.await
		.unwrap();
	}

	let slab_id = mgr.force_seal_active_for_test().await;
	let raw = mgr.read_sealed_raw(slab_id).await.unwrap();
	let mut catalog = TableSchemaCatalog::default();
	catalog.upsert(tenant(), descriptor.clone()).unwrap();
	let encrypted = mgr
		.encrypt_tiered_object_blob_with_table_stats(slab_id, &raw, &catalog)
		.unwrap();
	let locator = encrypted.stats_locator.expect("stats locator");
	let l2_uri = l2
		.put(&format!("stats-prune-{slab_id}.wnd"), &encrypted.bytes)
		.await
		.unwrap();
	l2.inject(InjectedMockObjectStoreFault::on(
		MockObjectStoreOp::GetBytes,
		MockObjectStoreFaultAction::Error {
			kind: ErrorKind::Other,
			message: "stats pruning must not fetch the full L2 object".to_owned(),
		},
	));
	l2.inject(
		InjectedMockObjectStoreFault::on(
			MockObjectStoreOp::GetRange,
			MockObjectStoreFaultAction::Error {
				kind: ErrorKind::Other,
				message: "pruned query must not range-read row ciphertext".to_owned(),
			},
		)
		.after_matches(3),
	);
	mgr.apply_tier_slab(TieredSlabPointer {
		slab_id,
		l2_uri,
		payload_hash: payload_digest(&encrypted.bytes),
		is_encrypted: true,
		tenant_id: Some(encrypted.tenant_id),
		tenant_kek_id: Some(encrypted.tenant_kek_id),
		stats_format_version: Some(locator.stats_format_version),
		stats_block_len: Some(locator.stats_block_len),
		stats_block_hash128: Some(locator.stats_block_hash128),
		tombstone_metadata: TombstoneMetadata::None,
		supersession_metadata: SupersessionMetadata::None,
	})
	.await
	.unwrap();

	let mut bytecode = Vec::new();
	bytecode.push(OP_PUSH_COL);
	bytecode.extend_from_slice(&0_u32.to_le_bytes());
	bytecode.push(OP_PUSH_INT);
	bytecode.extend_from_slice(&100_i64.to_le_bytes());
	bytecode.push(OP_GTE);
	let program = pbc::Program::compile(&bytecode).unwrap();
	let meta = base_meta();
	let ranges = [QueryScanRange {
		start: Locality::ZERO,
		end: Locality::MAX,
	}];
	let pruned_before = metrics().pbc_slab_stats_pruned_total.get();
	let consulted_before = metrics().pbc_slab_stats_consulted_total.get();

	let plan = plan_query_execution_for_ranges_with_program(
		&mgr,
		n(1),
		&meta,
		&program,
		&ranges,
		QueryExecutionBudget::default(),
		None,
		Some(&descriptor),
		QueryProjectedExecution::Disabled,
		RowIdentityProjection::None,
		None,
	)
	.await
	.unwrap();

	assert_eq!(plan.response.status, ReadStatus::KEY_NOT_FOUND);
	assert_eq!(
		plan.stats_explain,
		QueryStatsExplainCounters {
			candidate_slabs_before_stats: 1,
			slabs_pruned_by_stats: 1,
			slabs_scanned_stats_missing: 0,
			slabs_scanned_stats_unsupported: 0,
			slabs_scanned_predicate_unrepresentable: 0,
			stats_cache_hits: 1,
			stats_cache_misses: 1,
			stats_decrypt_failed_total: 0,
		}
	);
	assert!(
		metrics().pbc_slab_stats_consulted_total.get() > consulted_before,
		"tiered stats should be consulted before L2 row reads"
	);
	assert!(
		metrics().pbc_slab_stats_pruned_total.get() > pruned_before,
		"the slab-level id min/max should rule out id >= 100"
	);

	l2.inject(InjectedMockObjectStoreFault::on(
		MockObjectStoreOp::Head,
		MockObjectStoreFaultAction::Error {
			kind: ErrorKind::Other,
			message: "cached stats must not HEAD L2".to_owned(),
		},
	));
	l2.inject(InjectedMockObjectStoreFault::on(
		MockObjectStoreOp::GetRange,
		MockObjectStoreFaultAction::Error {
			kind: ErrorKind::Other,
			message: "cached stats must not range-read L2".to_owned(),
		},
	));
	let cache_hits_before = metrics().pbc_slab_stats_cache_hits_total.get();

	let second = plan_query_execution_for_ranges_with_program(
		&mgr,
		n(1),
		&meta,
		&program,
		&ranges,
		QueryExecutionBudget::default(),
		None,
		Some(&descriptor),
		QueryProjectedExecution::Disabled,
		RowIdentityProjection::None,
		None,
	)
	.await
	.unwrap();

	assert_eq!(second.response.status, ReadStatus::KEY_NOT_FOUND);
	assert!(
		metrics().pbc_slab_stats_cache_hits_total.get() > cache_hits_before,
		"second query should reuse the parsed stats block cached by the slab manager"
	);
}

#[tokio::test]
async fn query_request_flag_disables_table_stats_pruning() {
	let (_mgr_dir, mgr) = make_manager().await;
	let descriptor = typed_table_descriptor();
	let batch = typed_table_batch(vec![1, 2]);
	let mut write_meta: TableWriteRequestMeta = bytemuck::Zeroable::zeroed();
	write_meta.tenant_id = tenant();
	write_meta.table_object_id = descriptor.table_object_id;
	write_meta.schema_generation = descriptor.schema_generation;
	write_meta.row_count = encode_table_write_row_count(batch.num_rows()).unwrap();
	let admission = validate_table_write_request_batch(
		&write_meta,
		&batch,
		&descriptor,
		WriterNodeId::ZERO,
		write_meta.client_hlc,
	)
	.expect("typed rows should validate");
	for row in 0..batch.num_rows() {
		let row_batch = batch.slice(row, 1);
		let frame = encode_batch(&row_batch).unwrap();
		mgr.append_durable_with_locality(
			tenant(),
			admission.key_identities[row],
			admission.localities[row],
			hlc(10 + row as u64, 0, 1),
			&frame,
		)
		.await
		.unwrap();
	}

	let program = pbc::Program::compile(&[]).unwrap();
	let mut meta = base_meta();
	meta._pad[0] |= crate::schema::QUERY_REQUEST_FLAG_DISABLE_STATS_PRUNING;
	let plan = plan_query_execution_for_ranges_with_program(
		&mgr,
		n(1),
		&meta,
		&program,
		&[QueryScanRange {
			start: Locality::ZERO,
			end: Locality::MAX,
		}],
		QueryExecutionBudget::default(),
		None,
		Some(&descriptor),
		QueryProjectedExecution::Disabled,
		RowIdentityProjection::None,
		None,
	)
	.await
	.unwrap();

	assert_eq!(plan.response.status, ReadStatus::OK);
	assert_eq!(plan.response.rows, 2);
	assert_eq!(plan.stats_explain, QueryStatsExplainCounters::default());
}

#[tokio::test]
async fn table_stats_admit_slab_then_pbc_filters_rows() {
	let l2 = Arc::new(MockObjectStore::default());
	let (_mgr_dir, mgr) = make_manager_with_l2_projected_fetch(l2.clone()).await;
	let descriptor = typed_table_descriptor();
	let slab_id = append_typed_table_rows(&mgr, &descriptor, vec![1, 2]).await;
	let raw = mgr.read_sealed_raw(slab_id).await.unwrap();
	let mut catalog = TableSchemaCatalog::default();
	catalog.upsert(tenant(), descriptor.clone()).unwrap();
	let encrypted = mgr
		.encrypt_tiered_object_blob_with_table_stats(slab_id, &raw, &catalog)
		.unwrap();
	let locator = encrypted.stats_locator.expect("stats locator");
	let l2_uri = l2
		.put("stats-admit-pbc.wnd", &encrypted.bytes)
		.await
		.unwrap();
	mgr.apply_tier_slab(TieredSlabPointer {
		slab_id,
		l2_uri,
		payload_hash: payload_digest(&encrypted.bytes),
		is_encrypted: true,
		tenant_id: Some(encrypted.tenant_id),
		tenant_kek_id: Some(encrypted.tenant_kek_id),
		stats_format_version: Some(locator.stats_format_version),
		stats_block_len: Some(locator.stats_block_len),
		stats_block_hash128: Some(locator.stats_block_hash128),
		tombstone_metadata: TombstoneMetadata::None,
		supersession_metadata: SupersessionMetadata::None,
	})
	.await
	.unwrap();

	let mut bytecode = Vec::new();
	bytecode.push(OP_PUSH_COL);
	bytecode.extend_from_slice(&0_u32.to_le_bytes());
	bytecode.push(OP_PUSH_INT);
	bytecode.extend_from_slice(&2_i64.to_le_bytes());
	bytecode.push(OP_GTE);
	let program = pbc::Program::compile(&bytecode).unwrap();
	let user_column_count = descriptor.arrow_schema().unwrap().fields().len();
	let requirement = analyze_pbc_column_requirement(&program, user_column_count).unwrap();
	let plan = plan_query_execution_for_ranges_with_program(
		&mgr,
		n(1),
		&base_meta(),
		&program,
		&[QueryScanRange {
			start: Locality::ZERO,
			end: Locality::MAX,
		}],
		QueryExecutionBudget::default(),
		None,
		Some(&descriptor),
		QueryProjectedExecution::Pbc {
			requirement,
			user_column_count,
			runtime_filter: None,
		},
		RowIdentityProjection::None,
		None,
	)
	.await
	.unwrap();

	assert_eq!(plan.response.status, ReadStatus::OK);
	assert_eq!(plan.response.rows, 1);
	assert_eq!(
		plan.stats_explain,
		QueryStatsExplainCounters {
			candidate_slabs_before_stats: 1,
			slabs_pruned_by_stats: 0,
			slabs_scanned_stats_missing: 0,
			slabs_scanned_stats_unsupported: 0,
			slabs_scanned_predicate_unrepresentable: 0,
			stats_cache_hits: 1,
			stats_cache_misses: 1,
			stats_decrypt_failed_total: 0,
		}
	);
	let ids = plan.batches[0]
		.column_by_name("id")
		.unwrap()
		.as_any()
		.downcast_ref::<Int64Array>()
		.unwrap();
	assert_eq!(ids.values(), &[2]);
}

#[tokio::test]
async fn aggregate_plan_folds_projected_l2_inputs() {
	let l2 = Arc::new(MockObjectStore::default());
	let (_mgr_dir, mgr) = make_manager_with_l2_projected_fetch(l2.clone()).await;
	let descriptor = typed_table_descriptor();
	let slab_id = append_typed_table_rows(&mgr, &descriptor, vec![1, 2]).await;
	let raw = mgr.read_sealed_raw(slab_id).await.unwrap();
	let mut catalog = TableSchemaCatalog::default();
	catalog.upsert(tenant(), descriptor.clone()).unwrap();
	let encrypted = mgr
		.encrypt_tiered_object_blob_with_table_stats(slab_id, &raw, &catalog)
		.unwrap();
	let locator = encrypted.stats_locator.expect("stats locator");
	let l2_uri = l2
		.put("stats-admit-aggregate.wnd", &encrypted.bytes)
		.await
		.unwrap();
	mgr.apply_tier_slab(TieredSlabPointer {
		slab_id,
		l2_uri,
		payload_hash: payload_digest(&encrypted.bytes),
		is_encrypted: true,
		tenant_id: Some(encrypted.tenant_id),
		tenant_kek_id: Some(encrypted.tenant_kek_id),
		stats_format_version: Some(locator.stats_format_version),
		stats_block_len: Some(locator.stats_block_len),
		stats_block_hash128: Some(locator.stats_block_hash128),
		tombstone_metadata: TombstoneMetadata::None,
		supersession_metadata: SupersessionMetadata::None,
	})
	.await
	.unwrap();

	let mut predicate = Vec::new();
	predicate.push(OP_PUSH_COL);
	predicate.extend_from_slice(&0_u32.to_le_bytes());
	predicate.push(OP_PUSH_INT);
	predicate.extend_from_slice(&2_i64.to_le_bytes());
	predicate.push(OP_GTE);
	let request = AggregateRequest {
		format_version: AGG_FORMAT_VERSION,
		request_id: AggregateRequestId::new(144),
		tenant_id: tenant(),
		table_object_id: descriptor.table_object_id,
		schema_generation: descriptor.schema_generation,
		logical_schema_fingerprint: descriptor.logical_schema_fingerprint,
		consistency: Consistency::SERIALIZABLE,
		min_hlc: hlc(0, 0, 0),
		as_of_hlc: hlc(0, 0, 0),
		locality_ranges: vec![LocalityRange::full()],
		predicate_program: Some(predicate.clone()),
		aggregates: vec![
			AggregateExpression::count_star(),
			AggregateExpression {
				tag: AggregateFunctionTag::SIGNED_SUM,
				column_ordinal: 0,
				parameter_block: Vec::new(),
			},
		],
		budgets: AggregateBudgets {
			max_locations: LocationCount::new(16),
			max_rows: RowCount::new(16),
			max_wall_time_millis: DurationMillis::new(1_000),
			max_partial_state_bytes: ByteCount::new(4_096),
		},
	};
	let program = pbc::Program::compile(&predicate).unwrap();
	let prepared = PreparedAggregateExecution {
		range_meta: aggregate_as_range_query(&request, 0),
		program,
		descriptor: descriptor.clone(),
		ranges: vec![QueryScanRange {
			start: Locality::ZERO,
			end: Locality::MAX,
		}],
		retention_floor: None,
	};
	let projected_execution = aggregate_projected_execution(&mgr, &request, &prepared).unwrap();
	let mut folded_request =
		aggregate_fold_request_for_execution(&request, &projected_execution).unwrap();
	folded_request.predicate_program = None;

	let plan = plan_query_execution_for_ranges_with_program(
		&mgr,
		n(1),
		&prepared.range_meta,
		&prepared.program,
		&prepared.ranges,
		QueryExecutionBudget {
			max_locations: 1_000_000,
			max_rows: 1_000_000,
			max_wall_time: Duration::from_secs(600),
			stats_pruning_enabled: true,
			caller_totals: true,
			caller_deadline: None,
		},
		None,
		Some(&descriptor),
		projected_execution,
		RowIdentityProjection::None,
		None,
	)
	.await
	.unwrap();

	assert_eq!(plan.response.status, ReadStatus::OK);
	assert_eq!(plan.response.rows, 1);
	assert_eq!(plan.batches[0].num_columns(), 1);
	let result = aggregate_record_batches(&folded_request, &plan.batches, false).unwrap();
	assert_eq!(result.response.status, AggregateStatus::Success);
	assert_eq!(
		result.response.partials[0].state.as_slice(),
		&1_u64.to_be_bytes()
	);
	let expected_sum = encode_aggregate_state(
		AggregateFunctionTag::SIGNED_SUM,
		&AggregateState::SignedSum {
			sum: 2,
			non_null_count: 1,
			overflow: false,
		},
	)
	.unwrap();
	assert_eq!(result.response.partials[1].state, expected_sum);
}

#[tokio::test]
async fn aggregate_plan_uses_encoded_l2_partials_when_supported() {
	let l2 = Arc::new(MockObjectStore::default());
	let (_mgr_dir, mgr) = make_manager_with_l2_encoded_compute(l2.clone()).await;
	let mut descriptor = typed_table_descriptor();
	descriptor.locality_policy =
		LocalityPolicy::TableSpreadProjection(TableSpreadLocalityDescriptor {
			spread_factor: 1,
			prefix_bits: 63,
		});
	descriptor.record_layout_policy =
		RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::new(256, 256, 1024 * 1024).unwrap());
	descriptor.record_body_compression_policy = RecordBodyCompressionPolicy::Uncompressed;
	descriptor.refresh_descriptor_fingerprints().unwrap();
	descriptor.validate().unwrap();
	let ids = same_multirow_locality_ids(&descriptor, 256);
	let batch = typed_table_batch(ids.clone());
	let write_meta = typed_table_meta(&descriptor, batch.num_rows());
	let row_hlc = hlc(50, 0, 1);
	let admission = validate_table_write_request_batch(
		&write_meta,
		&batch,
		&descriptor,
		WriterNodeId::ZERO,
		row_hlc,
	)
	.unwrap();
	let row_group_config =
		crate::storage::row_group::RowGroupWriterConfig::point_read_with_overrides(
			crate::schema::RowGroupRowCount::new(128),
			crate::schema::RowGroupRowCount::new(128),
			RowCount::new(1),
			crate::schema::RecordBodyByteLen::new(1),
		)
		.with_enabled(true);
	let encoding_config = EncodingWriterConfig {
		enabled: true,
		target_simd_page_values: crate::schema::SimdPageValueCount::new(128),
		min_buffer_bytes_for_encoding: crate::schema::RecordBodyByteLen::new(1),
		..Default::default()
	};
	let bifurcated = bifurcate_table_write_with_admission_and_row_group_and_encoding_config(
		&write_meta,
		&batch,
		&descriptor,
		&admission,
		row_hlc,
		&row_group_config,
		&encoding_config,
	)
	.unwrap();
	assert!(
		bifurcated.children.iter().all(|child| {
			crate::storage::encoding::materialize::ipc_stream_contains_encoding_directory(
				child.frame_bytes.as_slice(),
			)
		}),
		"every fixture child must carry an ADR 057 encoding directory; children={}",
		bifurcated.children.len()
	);
	append_bifurcated_table_children_durable(
		&mgr,
		write_meta.tenant_id,
		row_hlc,
		&bifurcated.children,
		&[],
	)
	.await
	.unwrap();
	let slab_id = mgr.force_seal_active_for_test().await;
	let raw = mgr.read_sealed_raw(slab_id).await.unwrap();
	let encrypted = mgr.encrypt_tiered_object_blob(slab_id, &raw).unwrap();
	let l2_uri = l2
		.put("encoded-aggregate.wnd", &encrypted.bytes)
		.await
		.unwrap();
	mgr.apply_tier_slab(TieredSlabPointer {
		slab_id,
		l2_uri,
		payload_hash: payload_digest(&encrypted.bytes),
		is_encrypted: true,
		tenant_id: Some(encrypted.tenant_id),
		tenant_kek_id: Some(encrypted.tenant_kek_id),
		stats_format_version: encrypted
			.stats_locator
			.map(|locator| locator.stats_format_version),
		stats_block_len: encrypted
			.stats_locator
			.map(|locator| locator.stats_block_len),
		stats_block_hash128: encrypted
			.stats_locator
			.map(|locator| locator.stats_block_hash128),
		tombstone_metadata: TombstoneMetadata::None,
		supersession_metadata: SupersessionMetadata::None,
	})
	.await
	.unwrap();

	let request = AggregateRequest {
		format_version: AGG_FORMAT_VERSION,
		request_id: AggregateRequestId::new(145),
		tenant_id: tenant(),
		table_object_id: descriptor.table_object_id,
		schema_generation: descriptor.schema_generation,
		logical_schema_fingerprint: descriptor.logical_schema_fingerprint,
		consistency: Consistency::SERIALIZABLE,
		min_hlc: hlc(0, 0, 0),
		as_of_hlc: hlc(0, 0, 0),
		locality_ranges: vec![LocalityRange::full()],
		predicate_program: None,
		aggregates: vec![AggregateExpression::count_star()],
		budgets: AggregateBudgets {
			max_locations: LocationCount::new(1_000_000),
			max_rows: RowCount::new(1_000_000),
			max_wall_time_millis: DurationMillis::new(600_000),
			max_partial_state_bytes: ByteCount::new(4_096),
		},
	};
	let program = pbc::Program::compile(&[]).unwrap();
	let prepared = PreparedAggregateExecution {
		range_meta: aggregate_as_range_query(&request, 0),
		program,
		descriptor: descriptor.clone(),
		ranges: vec![QueryScanRange {
			start: Locality::ZERO,
			end: Locality::MAX,
		}],
		retention_floor: None,
	};
	let first_location = mgr.lookup(tenant(), admission.key_identities[0]).unwrap();
	let direct_encoded = mgr
		.read_encoded_aggregate_record(first_location, &descriptor, &request, &prepared.program)
		.await
		.unwrap();
	assert!(
		direct_encoded.is_some(),
		"manager should be able to fold a supported encoded aggregate location"
	);
	let projected_execution = aggregate_projected_execution(&mgr, &request, &prepared).unwrap();
	let plan = plan_query_execution_for_ranges_with_program(
		&mgr,
		n(1),
		&prepared.range_meta,
		&prepared.program,
		&prepared.ranges,
		QueryExecutionBudget {
			max_locations: 1_000_000,
			max_rows: 1_000_000,
			max_wall_time: Duration::from_secs(600),
			stats_pruning_enabled: true,
			caller_totals: true,
			caller_deadline: None,
		},
		None,
		Some(&descriptor),
		projected_execution,
		RowIdentityProjection::None,
		None,
	)
	.await
	.unwrap();

	assert_eq!(plan.response.status, ReadStatus::OK);
	assert_eq!(plan.response.rows, ids.len() as u64);
	assert!(
		plan.batches.is_empty(),
		"supported COCD aggregate should avoid dense aggregate batches"
	);
	assert_eq!(plan.aggregate_responses.len(), ids.len());
	let response = merge_aggregate_responses(&request, &plan.aggregate_responses).unwrap();
	assert_eq!(response.status, AggregateStatus::Success);
	assert_eq!(
		response.partials[0].state.as_slice(),
		&(ids.len() as u64).to_be_bytes()
	);
}

#[tokio::test]
async fn append_only_row_tombstones_are_hidden_from_full_and_projected_queries() {
	let l2 = Arc::new(MockObjectStore::default());
	let (_mgr_dir, mgr) = make_manager_with_l2_projected_fetch(l2.clone()).await;
	let descriptor = append_only_table_descriptor();
	let row_key_ids =
		append_append_only_rows_for_query(&mgr, &descriptor, vec![7, 8, 9], hlc(10, 0, 1)).await;
	let data_slab_id =
		tier_active_slab_with_stats_for_query(&mgr, l2, &descriptor, "append-delete-query.wnd")
			.await;
	append_append_only_tombstones_for_query(&mgr, &descriptor, &[row_key_ids[1]], hlc(20, 0, 1))
		.await;

	let mut before_delete_meta = base_meta();
	before_delete_meta.as_of_hlc = hlc(15, 0, 1);
	let program = pbc::Program::compile(&[]).unwrap();
	let before_delete = plan_query_execution_for_ranges_with_program(
		&mgr,
		n(1),
		&before_delete_meta,
		&program,
		&[QueryScanRange {
			start: Locality::ZERO,
			end: Locality::MAX,
		}],
		QueryExecutionBudget::default(),
		None,
		Some(&descriptor),
		QueryProjectedExecution::Disabled,
		RowIdentityProjection::None,
		None,
	)
	.await
	.unwrap();
	assert_eq!(before_delete.response.status, ReadStatus::OK);
	assert_eq!(
		sorted_i64_column_values(&before_delete.batches, "id"),
		vec![7, 8, 9]
	);

	let mut after_delete_meta = base_meta();
	after_delete_meta.as_of_hlc = hlc(99, 0, 1);
	let full = plan_query_execution_for_ranges_with_program(
		&mgr,
		n(1),
		&after_delete_meta,
		&program,
		&[QueryScanRange {
			start: Locality::ZERO,
			end: Locality::MAX,
		}],
		QueryExecutionBudget::default(),
		None,
		Some(&descriptor),
		QueryProjectedExecution::Disabled,
		RowIdentityProjection::None,
		None,
	)
	.await
	.unwrap();
	assert_eq!(full.response.status, ReadStatus::OK);
	assert_eq!(full.response.rows, 2);
	assert_eq!(sorted_i64_column_values(&full.batches, "id"), vec![7, 9]);

	let pointer = mgr.tier_pointer(data_slab_id).await.unwrap();
	assert_eq!(
		pointer.tombstone_metadata,
		TombstoneMetadata::Earliest(hlc(20, 0, 1))
	);

	let mut predicate = Vec::new();
	predicate.push(OP_PUSH_COL);
	predicate.extend_from_slice(&0_u32.to_le_bytes());
	predicate.push(OP_PUSH_INT);
	predicate.extend_from_slice(&0_i64.to_le_bytes());
	predicate.push(OP_GTE);
	let projected_program = pbc::Program::compile(&predicate).unwrap();
	let user_column_count = descriptor.arrow_schema().unwrap().fields().len();
	let requirement =
		analyze_pbc_column_requirement(&projected_program, user_column_count).unwrap();
	let projected = plan_query_execution_for_ranges_with_program(
		&mgr,
		n(1),
		&after_delete_meta,
		&projected_program,
		&[QueryScanRange {
			start: Locality::ZERO,
			end: Locality::MAX,
		}],
		QueryExecutionBudget::default(),
		None,
		Some(&descriptor),
		QueryProjectedExecution::Pbc {
			requirement,
			user_column_count,
			runtime_filter: None,
		},
		RowIdentityProjection::None,
		None,
	)
	.await
	.unwrap();
	assert_eq!(projected.response.status, ReadStatus::OK);
	assert_eq!(projected.response.rows, 2);
	assert_eq!(
		sorted_i64_column_values(&projected.batches, "id"),
		vec![7, 9]
	);
}

#[tokio::test]
async fn append_only_row_tombstones_are_hidden_from_projected_aggregate_inputs() {
	let l2 = Arc::new(MockObjectStore::default());
	let (_mgr_dir, mgr) = make_manager_with_l2_projected_fetch(l2.clone()).await;
	let descriptor = append_only_table_descriptor();
	let row_key_ids =
		append_append_only_rows_for_query(&mgr, &descriptor, vec![7, 8, 9], hlc(10, 0, 1)).await;
	tier_active_slab_with_stats_for_query(&mgr, l2, &descriptor, "append-delete-aggregate.wnd")
		.await;
	append_append_only_tombstones_for_query(&mgr, &descriptor, &[row_key_ids[1]], hlc(20, 0, 1))
		.await;

	let mut predicate = Vec::new();
	predicate.push(OP_PUSH_COL);
	predicate.extend_from_slice(&0_u32.to_le_bytes());
	predicate.push(OP_PUSH_INT);
	predicate.extend_from_slice(&0_i64.to_le_bytes());
	predicate.push(OP_GTE);
	let request = AggregateRequest {
		format_version: AGG_FORMAT_VERSION,
		request_id: AggregateRequestId::new(147),
		tenant_id: tenant(),
		table_object_id: descriptor.table_object_id,
		schema_generation: descriptor.schema_generation,
		logical_schema_fingerprint: descriptor.logical_schema_fingerprint,
		consistency: Consistency::SERIALIZABLE,
		min_hlc: hlc(0, 0, 0),
		as_of_hlc: hlc(99, 0, 1),
		locality_ranges: vec![LocalityRange::full()],
		predicate_program: Some(predicate.clone()),
		aggregates: vec![
			AggregateExpression::count_star(),
			AggregateExpression {
				tag: AggregateFunctionTag::SIGNED_SUM,
				column_ordinal: 0,
				parameter_block: Vec::new(),
			},
		],
		budgets: AggregateBudgets {
			max_locations: LocationCount::new(16),
			max_rows: RowCount::new(16),
			max_wall_time_millis: DurationMillis::new(1_000),
			max_partial_state_bytes: ByteCount::new(4_096),
		},
	};
	let program = pbc::Program::compile(&predicate).unwrap();
	let prepared = PreparedAggregateExecution {
		range_meta: aggregate_as_range_query(&request, 0),
		program,
		descriptor: descriptor.clone(),
		ranges: vec![QueryScanRange {
			start: Locality::ZERO,
			end: Locality::MAX,
		}],
		retention_floor: None,
	};
	let projected_execution = aggregate_projected_execution(&mgr, &request, &prepared).unwrap();
	assert!(matches!(
		projected_execution,
		QueryProjectedExecution::Aggregate { .. }
	));
	let mut folded_request =
		aggregate_fold_request_for_execution(&request, &projected_execution).unwrap();
	folded_request.predicate_program = None;

	let plan = plan_query_execution_for_ranges_with_program(
		&mgr,
		n(1),
		&prepared.range_meta,
		&prepared.program,
		&prepared.ranges,
		aggregate_query_budget(&request.budgets),
		None,
		Some(&descriptor),
		projected_execution,
		RowIdentityProjection::None,
		None,
	)
	.await
	.unwrap();

	assert_eq!(plan.response.status, ReadStatus::OK);
	assert_eq!(plan.response.rows, 2);
	let result = aggregate_record_batches(&folded_request, &plan.batches, false).unwrap();
	assert_eq!(result.response.status, AggregateStatus::Success);
	assert_eq!(
		result.response.partials[0].state.as_slice(),
		&2_u64.to_be_bytes()
	);
	let expected_sum = encode_aggregate_state(
		AggregateFunctionTag::SIGNED_SUM,
		&AggregateState::SignedSum {
			sum: 16,
			non_null_count: 2,
			overflow: false,
		},
	)
	.unwrap();
	assert_eq!(result.response.partials[1].state, expected_sum);
}

#[tokio::test]
async fn table_stats_absence_falls_through_to_full_scan() {
	let l2 = Arc::new(MockObjectStore::default());
	let (_mgr_dir, mgr, descriptor) =
		tier_typed_table_batch_without_stats(l2, vec![1, 2], "stats-missing-scan.wnd").await;

	let mut bytecode = Vec::new();
	bytecode.push(OP_PUSH_COL);
	bytecode.extend_from_slice(&0_u32.to_le_bytes());
	bytecode.push(OP_PUSH_INT);
	bytecode.extend_from_slice(&2_i64.to_le_bytes());
	bytecode.push(OP_GTE);
	let program = pbc::Program::compile(&bytecode).unwrap();
	let plan = plan_query_execution_for_ranges_with_program(
		&mgr,
		n(1),
		&base_meta(),
		&program,
		&[QueryScanRange {
			start: Locality::ZERO,
			end: Locality::MAX,
		}],
		QueryExecutionBudget::default(),
		None,
		Some(&descriptor),
		QueryProjectedExecution::Disabled,
		RowIdentityProjection::None,
		None,
	)
	.await
	.unwrap();

	assert_eq!(plan.response.status, ReadStatus::OK);
	assert_eq!(plan.response.rows, 1);
	assert_eq!(
		plan.stats_explain,
		QueryStatsExplainCounters {
			candidate_slabs_before_stats: 1,
			slabs_pruned_by_stats: 0,
			slabs_scanned_stats_missing: 1,
			slabs_scanned_stats_unsupported: 0,
			slabs_scanned_predicate_unrepresentable: 0,
			stats_cache_hits: 1,
			stats_cache_misses: 1,
			stats_decrypt_failed_total: 0,
		}
	);
	let ids = plan.batches[0]
		.column_by_name("id")
		.unwrap()
		.as_any()
		.downcast_ref::<Int64Array>()
		.unwrap();
	assert_eq!(ids.values(), &[2]);
}

#[tokio::test]
async fn unsupported_stats_predicate_falls_through_to_full_scan() {
	let l2 = Arc::new(MockObjectStore::default());
	let (_mgr_dir, mgr, descriptor) =
		tier_typed_table_batch_with_stats(l2, vec![1, 2], "stats-or-scan.wnd").await;

	let mut bytecode = Vec::new();
	bytecode.push(OP_PUSH_COL);
	bytecode.extend_from_slice(&0_u32.to_le_bytes());
	bytecode.push(OP_PUSH_INT);
	bytecode.extend_from_slice(&3_i64.to_le_bytes());
	bytecode.push(OP_GTE);
	bytecode.push(OP_PUSH_COL);
	bytecode.extend_from_slice(&0_u32.to_le_bytes());
	bytecode.push(OP_PUSH_INT);
	bytecode.extend_from_slice(&2_i64.to_le_bytes());
	bytecode.push(OP_EQ);
	bytecode.push(OP_OR);
	let program = pbc::Program::compile(&bytecode).unwrap();
	let plan = plan_query_execution_for_ranges_with_program(
		&mgr,
		n(1),
		&base_meta(),
		&program,
		&[QueryScanRange {
			start: Locality::ZERO,
			end: Locality::MAX,
		}],
		QueryExecutionBudget::default(),
		None,
		Some(&descriptor),
		QueryProjectedExecution::Disabled,
		RowIdentityProjection::None,
		None,
	)
	.await
	.unwrap();

	assert_eq!(plan.response.status, ReadStatus::OK);
	assert_eq!(plan.response.rows, 1);
	assert_eq!(
		plan.stats_explain,
		QueryStatsExplainCounters {
			candidate_slabs_before_stats: 1,
			slabs_pruned_by_stats: 0,
			slabs_scanned_stats_missing: 0,
			slabs_scanned_stats_unsupported: 0,
			slabs_scanned_predicate_unrepresentable: 1,
			stats_cache_hits: 1,
			stats_cache_misses: 1,
			stats_decrypt_failed_total: 0,
		}
	);
	let ids = plan.batches[0]
		.column_by_name("id")
		.unwrap()
		.as_any()
		.downcast_ref::<Int64Array>()
		.unwrap();
	assert_eq!(ids.values(), &[2]);
}

#[test]
fn slab_stats_pruning_ignores_mismatched_arrow_type_tag() {
	use crate::storage::stats::{
		ARROW_STATS_TAG_LARGE_UTF8, COLUMN_STAT_HAS_MAX, COLUMN_STAT_HAS_MIN,
		COLUMN_STAT_HAS_NON_NULL_COUNT, COLUMN_STAT_HAS_NULL_COUNT, ColumnStatsBlockEntry,
		ColumnStatsBlockHeader, ParsedColumnStatsBlock, encode_length_prefixed_bytes,
	};

	let descriptor = typed_table_descriptor();
	let mut payload = Vec::new();
	encode_length_prefixed_bytes(&mut payload, &encode_signed_stat_value(1, 8).unwrap()).unwrap();
	encode_length_prefixed_bytes(&mut payload, &encode_signed_stat_value(9, 8).unwrap()).unwrap();
	payload.extend_from_slice(&0_u64.to_be_bytes());
	payload.extend_from_slice(&2_u64.to_be_bytes());
	let stats = ParsedColumnStatsBlock {
		header: ColumnStatsBlockHeader {
			table_object_id: descriptor.table_object_id,
			schema_generation: descriptor.schema_generation,
			logical_schema_fingerprint: descriptor.logical_schema_fingerprint,
			row_count: 2,
			column_block_count: 1,
		},
		columns: vec![ColumnStatsBlockEntry {
			column_ordinal: 0,
			arrow_type_tag: ARROW_STATS_TAG_LARGE_UTF8,
			stat_flags: COLUMN_STAT_HAS_MIN
				| COLUMN_STAT_HAS_MAX
				| COLUMN_STAT_HAS_NULL_COUNT
				| COLUMN_STAT_HAS_NON_NULL_COUNT,
			payload,
		}],
	};
	let mut bytecode = Vec::new();
	bytecode.push(OP_PUSH_COL);
	bytecode.extend_from_slice(&0_u32.to_le_bytes());
	bytecode.push(OP_PUSH_INT);
	bytecode.extend_from_slice(&100_i64.to_le_bytes());
	bytecode.push(OP_GTE);
	let program = pbc::Program::compile(&bytecode).unwrap();

	assert!(
		slab_stats_may_match_query(&descriptor, &program, &stats).unwrap(),
		"mismatched stat type tags must fail closed instead of pruning"
	);
}

#[test]
fn slab_stats_pruning_respects_truncated_string_max_prefix() {
	use crate::storage::stats::{
		ARROW_STATS_TAG_LARGE_UTF8, COLUMN_STAT_HAS_MAX, COLUMN_STAT_HAS_NON_NULL_COUNT,
		COLUMN_STAT_HAS_NULL_COUNT, COLUMN_STAT_MAX_TRUNCATED, ColumnStatsBlockEntry,
		ColumnStatsBlockHeader, ParsedColumnStatsBlock, encode_length_prefixed_bytes,
	};

	let descriptor = typed_table_descriptor();
	let mut payload = Vec::new();
	encode_length_prefixed_bytes(&mut payload, b"app").unwrap();
	payload.extend_from_slice(&0_u64.to_be_bytes());
	payload.extend_from_slice(&3_u64.to_be_bytes());
	let stats = ParsedColumnStatsBlock {
		header: ColumnStatsBlockHeader {
			table_object_id: descriptor.table_object_id,
			schema_generation: descriptor.schema_generation,
			logical_schema_fingerprint: descriptor.logical_schema_fingerprint,
			row_count: 3,
			column_block_count: 1,
		},
		columns: vec![ColumnStatsBlockEntry {
			column_ordinal: 1,
			arrow_type_tag: ARROW_STATS_TAG_LARGE_UTF8,
			stat_flags: COLUMN_STAT_HAS_MAX
				| COLUMN_STAT_MAX_TRUNCATED
				| COLUMN_STAT_HAS_NULL_COUNT
				| COLUMN_STAT_HAS_NON_NULL_COUNT,
			payload,
		}],
	};

	let mut boundary = Vec::new();
	boundary.push(OP_PUSH_COL);
	boundary.extend_from_slice(&1_u32.to_le_bytes());
	boundary.push(OP_PUSH_STR);
	boundary.extend_from_slice(&5_u16.to_le_bytes());
	boundary.extend_from_slice(b"apple");
	boundary.push(OP_GTE);
	let boundary_program = pbc::Program::compile(&boundary).unwrap();
	assert!(
		slab_stats_may_match_query(&descriptor, &boundary_program, &stats).unwrap(),
		"truncated max prefix 'app' must not prune predicate value 'apple'"
	);

	let mut outside = Vec::new();
	outside.push(OP_PUSH_COL);
	outside.extend_from_slice(&1_u32.to_le_bytes());
	outside.push(OP_PUSH_STR);
	outside.extend_from_slice(&3_u16.to_le_bytes());
	outside.extend_from_slice(b"apq");
	outside.push(OP_GTE);
	let outside_program = pbc::Program::compile(&outside).unwrap();
	assert!(
		!slab_stats_may_match_query(&descriptor, &outside_program, &stats).unwrap(),
		"values at or beyond the prefix successor can still prune safely"
	);
}

#[test]
fn table_bifurcation_rejects_duplicate_pk_before_encoding_children() {
	let descriptor = typed_table_descriptor();
	let batch = typed_table_batch(vec![7, 7]);
	let meta = typed_table_meta(&descriptor, batch.num_rows());
	let err = bifurcate_table_write(
		&meta,
		&batch,
		&descriptor,
		WriterNodeId::ZERO,
		meta.client_hlc,
	)
	.unwrap_err();
	assert_eq!(err.code, PbcRejectCode::PBC_REJECT_DUPLICATE_PK_IN_BATCH);
}

#[test]
fn write_expiry_keeps_cause_and_commit_knowledge_as_orthogonal_axes() {
	let descriptor = typed_table_descriptor();
	let meta = typed_table_meta(&descriptor, 1);
	let synthetic = table_write_synthetic_meta(&meta, Identity::ZERO, 0);

	let deadline = WriteClocks::new(
		Duration::from_secs(30),
		Some(tokio::time::Instant::now() - Duration::from_millis(1)),
	)
	.next_expiry()
	.1;
	let before_dispatch = mutation_expiry_response(
		&synthetic,
		deadline,
		MutationCommitKnowledge::KNOWN_NOT_COMMITTED,
	);
	assert_eq!(before_dispatch.status, WriteStatus::WRITE_DEADLINE_EXCEEDED);
	assert_eq!(
		before_dispatch.mutation_stop_reason(),
		MutationStopReason::DEADLINE
	);
	assert_eq!(
		before_dispatch.mutation_commit_knowledge(),
		MutationCommitKnowledge::KNOWN_NOT_COMMITTED
	);

	let stalled = WriteClocks::new(Duration::ZERO, None).next_expiry().1;
	let after_dispatch =
		mutation_expiry_response(&synthetic, stalled, MutationCommitKnowledge::INDETERMINATE);
	assert_eq!(after_dispatch.status, WriteStatus::WRITE_STALLED);
	assert_eq!(
		after_dispatch.mutation_stop_reason(),
		MutationStopReason::NO_PROGRESS
	);
	assert_eq!(
		after_dispatch.mutation_commit_knowledge(),
		MutationCommitKnowledge::INDETERMINATE
	);
}

#[tokio::test]
async fn table_write_single_row_commits_typed_arrow_payload() {
	let dir = bootstrap_tempdir("table-write-single-row");
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		37,
		dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();
	let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
	loop {
		if node.raft.current_leader().await == Some(node.local_node.id) {
			break;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for leader"
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
	node.data_server
		.refresh_leases_once(&node.ring, &*node.clock, 60_000)
		.await;

	let descriptor = typed_table_descriptor();
	node.raft
		.client_write(WendelonRequest::SchemaUpdate(typed_table_schema_update(
			&descriptor,
		)))
		.await
		.unwrap();

	let batch = typed_table_batch(vec![7]);
	let meta = typed_table_meta(&descriptor, batch.num_rows());
	let (resp, trailing) = node.data_server.table_write(meta, batch).await.unwrap();
	assert_eq!(resp.write.status, WriteStatus::OK, "got resp={resp:?}");
	assert_eq!(resp.reject_code, PbcRejectCode::NONE.0);
	assert!(trailing.is_none());

	let key_identity =
		table_row_identity(tenant(), descriptor.table_object_id, &7_i64.to_le_bytes());
	let location = node
		.slab
		.lookup(tenant(), key_identity)
		.expect("table row should be indexed by PK-derived identity");
	let stored = decode_batch(&node.slab.read(location).await.unwrap()).unwrap();
	assert_eq!(stored.schema().as_ref(), &typed_table_schema());
	assert_eq!(stored.num_rows(), 1);
	let ids = stored
		.column_by_name("id")
		.unwrap()
		.as_any()
		.downcast_ref::<Int64Array>()
		.unwrap();
	assert_eq!(ids.value(0), 7);

	shutdown_lease_node(node).await;
}

#[tokio::test]
async fn terminal_table_outcome_resolves_and_replays_after_server_reopen_without_reapply() {
	let dir = bootstrap_tempdir("table-write-durable-outcome-reopen");
	let cfg = NodeBootstrapConfig::dev(38, dir.to_path_buf(), "127.0.0.1:0".into());
	let node = WendelonNode::bootstrap_single_node(cfg.clone())
		.await
		.unwrap();
	let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
	loop {
		if node.raft.current_leader().await == Some(node.local_node.id) {
			break;
		}
		assert!(tokio::time::Instant::now() < deadline, "leader timeout");
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
	node.data_server
		.refresh_leases_once(&node.ring, &*node.clock, 60_000)
		.await;
	let descriptor = typed_table_descriptor();
	node.raft
		.client_write(WendelonRequest::SchemaUpdate(typed_table_schema_update(
			&descriptor,
		)))
		.await
		.unwrap();

	let batch = typed_table_batch(vec![73]);
	let meta = typed_table_meta(&descriptor, batch.num_rows());
	let (first, _) = node
		.data_server
		.table_write(meta, batch.clone())
		.await
		.unwrap();
	assert_eq!(first.write.status, WriteStatus::OK);
	let durable_bytes = node.slab.local_l1_bytes().await;

	shutdown_lease_node(node).await;
	tokio::time::sleep(Duration::from_millis(25)).await;
	let reopened = WendelonNode::bootstrap_single_node(cfg).await.unwrap();
	let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
	loop {
		if reopened.raft.current_leader().await == Some(reopened.local_node.id) {
			break;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"reopened leader timeout"
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
	reopened
		.data_server
		.refresh_leases_once(&reopened.ring, &*reopened.clock, 60_000)
		.await;
	let resolved = reopened
		.data_server
		.resolve_mutation_outcome(MutationOutcomeRequestMeta {
			operation_id: meta.parent_write_id,
			tenant_id: meta.tenant_id,
		})
		.await
		.unwrap();
	assert_eq!(resolved.resolution, MutationResolutionStatus::PRESENT);
	assert_eq!(
		resolved.write.mutation_commit_knowledge(),
		MutationCommitKnowledge::COMMITTED
	);

	let (duplicate, _) = reopened.data_server.table_write(meta, batch).await.unwrap();
	assert_eq!(duplicate.write.status, WriteStatus::DUPLICATE);
	assert_eq!(
		reopened.slab.local_l1_bytes().await,
		durable_bytes,
		"restart replay must resolve the retained parent operation, never append again"
	);
	shutdown_lease_node(reopened).await;
}

#[tokio::test]
async fn post_dispatch_expiry_resolves_and_replays_as_one_indeterminate_operation_after_restart() {
	let dir = bootstrap_tempdir("table-write-indeterminate-outcome-reopen");
	let cfg = NodeBootstrapConfig::dev(39, dir.to_path_buf(), "127.0.0.1:0".into());
	let node = WendelonNode::bootstrap_single_node(cfg.clone())
		.await
		.unwrap();
	let leader_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
	loop {
		if node.raft.current_leader().await == Some(node.local_node.id) {
			break;
		}
		assert!(
			tokio::time::Instant::now() < leader_deadline,
			"leader timeout"
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
	node.data_server
		.refresh_leases_once(&node.ring, &*node.clock, 60_000)
		.await;
	let descriptor = typed_table_descriptor();
	node.raft
		.client_write(WendelonRequest::SchemaUpdate(typed_table_schema_update(
			&descriptor,
		)))
		.await
		.unwrap();

	let batch = typed_table_batch(vec![79]);
	let mut meta = typed_table_meta(&descriptor, batch.num_rows());
	// The deadline must expire after dispatch. A short deadline raced against a
	// fixed append delay lets a loaded host spend it before dispatch, which is
	// the KNOWN_NOT_COMMITTED case, not this one. Instead the local append is
	// held at its gate once dispatched, the caller's deadline expires while it
	// is held, and only then is the append released.
	meta.deadline_remaining_millis = 20_000;
	let bytes_before = node.slab.local_l1_bytes().await;
	let entered = Arc::new(Notify::new());
	let release = Arc::new(Notify::new());
	*node.data_server.local_observation_gate.lock().unwrap() =
		Some((entered.clone(), release.clone()));
	let server = node.data_server.clone();
	let write = tokio::spawn({
		let batch = batch.clone();
		async move { server.table_write(meta, batch).await }
	});
	tokio::time::timeout(Duration::from_secs(60), entered.notified())
		.await
		.expect("the write never dispatched its local append");
	let (expired, _) = write.await.unwrap().unwrap();
	release.notify_one();
	assert_eq!(expired.write.status, WriteStatus::WRITE_DEADLINE_EXCEEDED);
	assert_eq!(
		expired.write.mutation_stop_reason(),
		MutationStopReason::DEADLINE
	);
	assert_eq!(
		expired.write.mutation_commit_knowledge(),
		MutationCommitKnowledge::INDETERMINATE
	);
	// `spawn` is deliberately not cancelled with the caller. Wait for the abandoned
	// append to finish while its node-wide permit remains charged.
	let append_deadline = tokio::time::Instant::now() + Duration::from_secs(60);
	while node.slab.local_l1_bytes().await <= bytes_before {
		assert!(
			tokio::time::Instant::now() < append_deadline,
			"the post-dispatch append must have run"
		);
		tokio::time::sleep(Duration::from_millis(10)).await;
	}
	let durable_bytes = node.slab.local_l1_bytes().await;

	shutdown_lease_node(node).await;
	tokio::time::sleep(Duration::from_millis(25)).await;
	let reopened = WendelonNode::bootstrap_single_node(cfg).await.unwrap();
	let leader_deadline = tokio::time::Instant::now() + Duration::from_secs(3);
	loop {
		if reopened.raft.current_leader().await == Some(reopened.local_node.id) {
			break;
		}
		assert!(
			tokio::time::Instant::now() < leader_deadline,
			"reopened leader timeout"
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
	reopened
		.data_server
		.refresh_leases_once(&reopened.ring, &*reopened.clock, 60_000)
		.await;
	let resolved = reopened
		.data_server
		.resolve_mutation_outcome(MutationOutcomeRequestMeta {
			operation_id: meta.parent_write_id,
			tenant_id: meta.tenant_id,
		})
		.await
		.unwrap();
	assert_eq!(resolved.resolution, MutationResolutionStatus::PRESENT);
	assert_eq!(resolved.write.status, WriteStatus::WRITE_DEADLINE_EXCEEDED);
	assert_eq!(
		resolved.write.mutation_commit_knowledge(),
		MutationCommitKnowledge::INDETERMINATE
	);

	let (replay, _) = reopened.data_server.table_write(meta, batch).await.unwrap();
	assert_eq!(replay.write.status, WriteStatus::WRITE_DEADLINE_EXCEEDED);
	assert_eq!(
		reopened.slab.local_l1_bytes().await,
		durable_bytes,
		"replay must resolve the retained operation, never append a second copy"
	);
	shutdown_lease_node(reopened).await;
}

#[tokio::test]
async fn final_attempt_exhaustion_is_a_durable_two_axis_outcome() {
	let dir = bootstrap_tempdir("table-write-attempt-exhaustion-reopen");
	let cfg = NodeBootstrapConfig::dev(40, dir.to_path_buf(), "127.0.0.1:0".into());
	let node = WendelonNode::bootstrap_single_node(cfg.clone())
		.await
		.unwrap();
	let before_bytes = node.slab.local_l1_bytes().await;
	let descriptor = typed_table_descriptor();
	let mut meta = typed_table_meta(&descriptor, 1);
	meta.set_attempts_remaining(1);
	let retryable = node.data_server.table_write_error_response(
		&meta,
		WriteStatus::BACKPRESSURED,
		PbcRejectCode::NONE,
	);
	let stopped = node
		.data_server
		.finalize_table_write_attempt(meta, retryable)
		.await
		.unwrap();
	assert_eq!(stopped.write.status, WriteStatus::WRITE_ATTEMPTS_EXHAUSTED);
	assert_eq!(
		stopped.write.mutation_stop_reason(),
		MutationStopReason::ATTEMPT_EXHAUSTION
	);
	assert_eq!(
		stopped.write.mutation_commit_knowledge(),
		MutationCommitKnowledge::KNOWN_NOT_COMMITTED
	);

	assert_eq!(node.slab.local_l1_bytes().await, before_bytes);
	println!(
		"NO_EFFECT_PIN scope=synthetic_backpressure_finalizer write_status={} stop_reason={} commit_knowledge={} data_bytes_before={} data_bytes_after={}",
		stopped.write.status.0,
		stopped.write.mutation_stop_reason().0,
		stopped.write.mutation_commit_knowledge().0,
		before_bytes,
		node.slab.local_l1_bytes().await
	);
	shutdown_lease_node(node).await;
	let reopened = WendelonNode::bootstrap_single_node(cfg).await.unwrap();
	let resolved = reopened
		.data_server
		.resolve_mutation_outcome(MutationOutcomeRequestMeta {
			operation_id: meta.parent_write_id,
			tenant_id: meta.tenant_id,
		})
		.await
		.unwrap();
	assert_eq!(resolved.resolution, MutationResolutionStatus::PRESENT);
	assert_eq!(resolved.write.status, WriteStatus::WRITE_ATTEMPTS_EXHAUSTED);
	assert_eq!(
		resolved.write.mutation_stop_reason(),
		MutationStopReason::ATTEMPT_EXHAUSTION
	);
	assert_eq!(
		resolved.write.mutation_commit_knowledge(),
		MutationCommitKnowledge::KNOWN_NOT_COMMITTED
	);
	println!(
		"NO_EFFECT_REOPEN_PIN resolution=Present write_status={} stop_reason={} commit_knowledge={} data_bytes={}",
		resolved.write.status.0,
		resolved.write.mutation_stop_reason().0,
		resolved.write.mutation_commit_knowledge().0,
		reopened.slab.local_l1_bytes().await
	);
	shutdown_lease_node(reopened).await;
}

#[tokio::test]
async fn adr0539_two_ordinary_sampler_sized_ingest_cycles_remain_readable() {
	// SMOKE CHECK ONLY — not evidence for ADR 0539. This fixture is green on the
	// wedging base (verified d5e5087d + test in a worktree): point lookups through the
	// runtime key index never touch the secondary-index coverage the live wedge broke.
	// Promoting it back to evidence requires a node-level typed table-query executor in
	// this harness (only frame-decode helpers exist), reading through the scan path
	// after each cycle. The discriminating control for the defect is
	// `storage::manager::tests::prune_compaction_must_not_orphan_secondary_index_sources`,
	// which was authored red against production calls.

	const CYCLE_ROWS: i64 = 2_300;
	const INSERT_ROWS: i64 = 50;

	let dir = bootstrap_tempdir("adr0539-two-cycle-ingest");
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		539,
		dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();
	let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
	loop {
		if node.raft.current_leader().await == Some(node.local_node.id) {
			break;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for leader"
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
	node.data_server
		.refresh_leases_once(&node.ring, &*node.clock, 60_000)
		.await;

	let descriptor = typed_table_descriptor();
	node.raft
		.client_write(WendelonRequest::SchemaUpdate(typed_table_schema_update(
			&descriptor,
		)))
		.await
		.unwrap();

	for cycle in 0..2_i64 {
		for offset in (0..CYCLE_ROWS).step_by(INSERT_ROWS as usize) {
			let ids = (offset..offset + INSERT_ROWS)
				.map(|id| cycle * CYCLE_ROWS + id)
				.collect::<Vec<_>>();
			let batch = typed_table_batch(ids);
			let mut meta = typed_table_meta(&descriptor, batch.num_rows());
			meta.parent_write_id.sequence_no = 539_000 + (cycle as u64 * 10_000) + offset as u64;
			let (response, trailing) = node.data_server.table_write(meta, batch).await.unwrap();
			assert_eq!(
				response.write.status,
				WriteStatus::OK,
				"cycle={cycle}, offset={offset}"
			);
			assert!(trailing.is_none());
		}

		for id in [cycle * CYCLE_ROWS, (cycle + 1) * CYCLE_ROWS - 1] {
			let key_identity =
				table_row_identity(tenant(), descriptor.table_object_id, &id.to_le_bytes());
			let location = node
				.slab
				.lookup(tenant(), key_identity)
				.expect("acknowledged row must remain indexed");
			let stored = decode_batch(&node.slab.read(location).await.unwrap()).unwrap();
			assert!(stored.num_rows() > 0);
		}
		let _slab_inventory = (
			node.slab.active_slab_id().await,
			node.slab.sealed_count().await,
		);
	}

	shutdown_lease_node(node).await;
}

#[tokio::test]
async fn content_hash_table_write_absorbs_visible_duplicate_content() {
	let dir = bootstrap_tempdir("content-hash-table-write-duplicate");
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		49,
		dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();
	let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
	loop {
		if node.raft.current_leader().await == Some(node.local_node.id) {
			break;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for leader"
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
	node.data_server
		.refresh_leases_once(&node.ring, &*node.clock, 60_000)
		.await;

	let descriptor = content_hash_table_descriptor();
	node.raft
		.client_write(WendelonRequest::SchemaUpdate(typed_table_schema_update(
			&descriptor,
		)))
		.await
		.unwrap();

	let batch = typed_table_batch(vec![7]);
	let mut first_meta = typed_table_meta(&descriptor, batch.num_rows());
	first_meta.parent_write_id.sequence_no = 490;
	let admission = validate_table_write_request_batch(
		&first_meta,
		&batch,
		&descriptor,
		WriterNodeId::ZERO,
		first_meta.client_hlc,
	)
	.unwrap();
	let key_identity = admission.key_identities[0];
	assert_eq!(admission.localities[0], Locality::from(key_identity));

	let (first_resp, first_trailing) = node
		.data_server
		.table_write(first_meta, batch.clone())
		.await
		.unwrap();
	assert_eq!(
		first_resp.write.status,
		WriteStatus::OK,
		"got resp={first_resp:?}"
	);
	assert_eq!(first_resp.reject_code, PbcRejectCode::NONE.0);
	assert!(first_trailing.is_none());
	let first_location = node
		.slab
		.lookup(tenant(), key_identity)
		.expect("content-hash row should be indexed by content-derived identity");
	assert!(!first_location.is_tombstone);

	let mut duplicate_meta = typed_table_meta(&descriptor, batch.num_rows());
	duplicate_meta.parent_write_id.sequence_no = 491;
	let (duplicate_resp, duplicate_trailing) = node
		.data_server
		.table_write(duplicate_meta, batch)
		.await
		.unwrap();
	assert_eq!(
		duplicate_resp.write.status,
		WriteStatus::OK,
		"visible duplicate content should be absorbed as idempotent success"
	);
	assert_eq!(duplicate_resp.reject_code, PbcRejectCode::NONE.0);
	assert_eq!(duplicate_resp.write.log_index, 0);
	assert!(duplicate_trailing.is_none());
	assert_eq!(
		node.slab.lookup(tenant(), key_identity),
		Some(first_location),
		"absorbed duplicate must not replace the visible row location"
	);

	shutdown_lease_node(node).await;
}

#[tokio::test]
async fn content_hash_table_write_filters_duplicate_rows_within_batch() {
	let dir = bootstrap_tempdir("content-hash-table-write-in-batch-duplicate");
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		50,
		dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();
	let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
	loop {
		if node.raft.current_leader().await == Some(node.local_node.id) {
			break;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for leader"
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
	node.data_server
		.refresh_leases_once(&node.ring, &*node.clock, 60_000)
		.await;

	let descriptor = content_hash_table_descriptor();
	node.raft
		.client_write(WendelonRequest::SchemaUpdate(typed_table_schema_update(
			&descriptor,
		)))
		.await
		.unwrap();

	let batch = typed_table_batch_with_names(vec![(7, "codex"), (7, "codex")]);
	let mut meta = typed_table_meta(&descriptor, batch.num_rows());
	meta.parent_write_id.sequence_no = 492;
	let admission = validate_table_write_request_batch(
		&meta,
		&batch,
		&descriptor,
		WriterNodeId::ZERO,
		meta.client_hlc,
	)
	.unwrap();
	assert_eq!(admission.key_identities[0], admission.key_identities[1]);
	let key_identity = admission.key_identities[0];

	let (resp, trailing) = node.data_server.table_write(meta, batch).await.unwrap();
	assert_eq!(resp.write.status, WriteStatus::OK, "got resp={resp:?}");
	assert_eq!(resp.reject_code, PbcRejectCode::NONE.0);
	assert!(trailing.is_none());

	let location = node
		.slab
		.lookup(tenant(), key_identity)
		.expect("content-hash row should be indexed after filtering in-batch duplicate");
	assert!(!location.is_tombstone);
	let stored = decode_batch(&node.slab.read(location).await.unwrap()).unwrap();
	assert_eq!(
		stored.num_rows(),
		1,
		"only one physical row should be packed for duplicate content within the same batch"
	);

	shutdown_lease_node(node).await;
}

#[tokio::test]
async fn content_hash_table_reingest_after_logical_retention_floor_stores_new_row() {
	let dir = bootstrap_tempdir("content-hash-logical-retention-reingest");
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		51,
		dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();
	let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
	loop {
		if node.raft.current_leader().await == Some(node.local_node.id) {
			break;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for leader"
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}

	let descriptor = content_hash_table_descriptor();
	node.raft
		.client_write(WendelonRequest::SchemaUpdate(typed_table_schema_update(
			&descriptor,
		)))
		.await
		.unwrap();
	node.raft
		.client_write(WendelonRequest::SetTableRetentionPolicy(
			crate::consensus::SetTableRetentionPolicy {
				tenant_id: tenant(),
				table_object_id: descriptor.table_object_id,
				expected_policy_generation: PolicyGeneration::ZERO,
				new_policy: TableRetentionPolicy::FixedWindow(
					RetentionWindowMillis::try_new(60_000).unwrap(),
				),
				updated_at_hlc: hlc(1, 0, 51),
			},
		))
		.await
		.unwrap();

	let clock = Arc::new(AdjustableClock::new(1_000));
	let tiers = TransportTierSet::empty();
	let roles = NodeRoleSet::empty().with(NodeRole::Data);
	let mut peers = std::collections::HashMap::new();
	peers.insert(
		node.local_node,
		PeerCapabilities {
			address: "loopback://51".into(),
			tiers,
			roles,
			capabilities: crate::network::transport::NodeCapabilitySet::empty(),
		},
	);
	let epoch = MembershipEpoch { epoch: 1, peers };
	let (_epoch_tx, epoch_rx) = watch::channel(Arc::new(epoch));
	let resolver: Arc<dyn crate::network::transport::CapabilityResolver> = Arc::new(
		crate::network::transport::EpochCapabilityResolver::new(epoch_rx.clone()),
	);
	let factory =
		TransportProviderFactory::new(tiers, std::collections::HashMap::new(), resolver, epoch_rx);
	let server = WendelonDataServer::new(
		node.slab.clone(),
		node.raft.clone(),
		node.log_store.clone(),
		node.state_machine.clone(),
		node.local_node,
		clock.clone(),
		Arc::new(FixedReplicaRouter::new(vec![node.local_node])),
		factory,
		node.pool.clone(),
	)
	.unwrap();

	let batch = typed_table_batch_with_names(vec![(7, "codex")]);
	let mut first_meta = typed_table_meta(&descriptor, batch.num_rows());
	first_meta.parent_write_id.sequence_no = 493;
	let admission = validate_table_write_request_batch(
		&first_meta,
		&batch,
		&descriptor,
		WriterNodeId::ZERO,
		first_meta.client_hlc,
	)
	.unwrap();
	let key_identity = admission.key_identities[0];

	let (first_resp, _) = server.table_write(first_meta, batch.clone()).await.unwrap();
	assert_eq!(first_resp.write.status, WriteStatus::OK);
	assert_ne!(
		first_resp.write.log_index, 0,
		"initial content-hash write must store a physical row"
	);
	let first_location = server
		.slab
		.lookup(tenant(), key_identity)
		.expect("first content-hash row should be indexed");
	assert!(!first_location.is_tombstone);

	clock.set(121_000);
	let state = server.state_machine.state_snapshot().await;
	let effective_retention =
		server.table_effective_retention_from_state(&state, tenant(), descriptor.table_object_id);
	assert!(
		!row_visible_for_admission(
			&descriptor,
			tenant(),
			key_identity,
			first_location.hlc,
			&effective_retention,
			&server.slab,
		),
		"old row must be logically hidden before compaction removes the index entry"
	);
	assert_eq!(
		server.slab.lookup(tenant(), key_identity),
		Some(first_location),
		"regression setup requires the expired row to remain physically indexed"
	);

	let mut reingest_meta = typed_table_meta(&descriptor, batch.num_rows());
	reingest_meta.parent_write_id.sequence_no = 494;
	let (reingest_resp, _) = server
		.table_write(reingest_meta, batch.clone())
		.await
		.unwrap();
	assert_eq!(reingest_resp.write.status, WriteStatus::OK);
	assert_ne!(
		reingest_resp.write.log_index, 0,
		"logically expired content must take the store path, not absorb"
	);
	let reingest_location = server
		.slab
		.lookup(tenant(), key_identity)
		.expect("reingested content-hash row should be indexed");
	assert_ne!(
		reingest_location, first_location,
		"reingest after logical retention must write a fresh physical row"
	);
	assert!(!reingest_location.is_tombstone);
	assert!(reingest_location.hlc > first_location.hlc);

	let mut duplicate_meta = typed_table_meta(&descriptor, batch.num_rows());
	duplicate_meta.parent_write_id.sequence_no = 495;
	let (duplicate_resp, _) = server.table_write(duplicate_meta, batch).await.unwrap();
	assert_eq!(duplicate_resp.write.status, WriteStatus::OK);
	assert_eq!(
		duplicate_resp.write.log_index, 0,
		"freshly visible reingested content should absorb subsequent duplicates"
	);
	assert_eq!(
		server.slab.lookup(tenant(), key_identity),
		Some(reingest_location)
	);

	shutdown_lease_node(node).await;
}

#[tokio::test]
async fn column_projection_write_preserves_slab_record_header_layout() {
	let dir = bootstrap_tempdir("column-projection-slab-header");
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		38,
		dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();
	let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
	loop {
		if node.raft.current_leader().await == Some(node.local_node.id) {
			break;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for leader"
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
	node.data_server
		.refresh_leases_once(&node.ring, &*node.clock, 60_000)
		.await;

	let descriptor = column_projection_table_descriptor();
	node.raft
		.client_write(WendelonRequest::SchemaUpdate(typed_table_schema_update(
			&descriptor,
		)))
		.await
		.unwrap();

	let batch = typed_table_batch(vec![7]);
	let meta = typed_table_meta(&descriptor, batch.num_rows());
	let (resp, trailing) = node.data_server.table_write(meta, batch).await.unwrap();
	assert_eq!(resp.write.status, WriteStatus::OK, "got resp={resp:?}");
	assert_eq!(resp.reject_code, PbcRejectCode::NONE.0);
	assert!(trailing.is_none());

	let key_identity =
		table_row_identity(tenant(), descriptor.table_object_id, &7_i64.to_le_bytes());
	let location = node
		.slab
		.lookup(tenant(), key_identity)
		.expect("table row should be indexed by PK-derived identity");
	let slab_path = dir.join(format!("slab_{:016x}.wnd", location.slab_id.get()));
	let mut file = std::fs::File::open(slab_path).unwrap();
	file.seek(SeekFrom::Start(location.offset)).unwrap();
	let mut header_bytes = vec![0_u8; std::mem::size_of::<SlabRecordHeader>()];
	file.read_exact(&mut header_bytes).unwrap();
	let header = bytemuck::pod_read_unaligned::<SlabRecordHeader>(&header_bytes);

	assert_eq!(std::mem::size_of::<SlabRecordHeader>(), 72);
	// ADR 075: an ordinary table-row write stays class STANDARD (the old
	// `_pad_align` zero-fill slot), i.e. byte-identical to pre-075.
	assert_eq!(header.record_class, crate::schema::record_class::STANDARD);
	assert_eq!(header.tenant_id, tenant());
	assert_eq!(header.key_identity, key_identity);

	shutdown_lease_node(node).await;
}

#[tokio::test]
async fn table_write_rejects_stale_schema_generation_distinct_from_unknown() {
	let dir = bootstrap_tempdir("table-write-stale-schema");
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		40,
		dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();
	let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
	loop {
		if node.raft.current_leader().await == Some(node.local_node.id) {
			break;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for leader"
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
	node.data_server
		.refresh_leases_once(&node.ring, &*node.clock, 60_000)
		.await;

	let descriptor = typed_table_descriptor();
	node.raft
		.client_write(WendelonRequest::SchemaUpdate(typed_table_schema_update(
			&descriptor,
		)))
		.await
		.unwrap();

	for (sequence_no, schema_generation) in [
		(58_u64, descriptor.schema_generation - 1),
		(59_u64, descriptor.schema_generation + 1),
	] {
		let batch = typed_table_batch(vec![7]);
		let mut meta = typed_table_meta(&descriptor, batch.num_rows());
		meta.parent_write_id.sequence_no = sequence_no;
		meta.schema_generation = schema_generation;
		let (resp, trailing) = node.data_server.table_write(meta, batch).await.unwrap();
		assert_eq!(resp.write.status, WriteStatus::EPOCH_STALE);
		assert_eq!(resp.reject_code, PbcRejectCode::PBC_REJECT_SCHEMA_STALE.0);
		assert!(trailing.is_none());
	}

	shutdown_lease_node(node).await;
}

#[tokio::test]
async fn pk_table_delete_supersedes_single_row_and_allows_resurrection() {
	let dir = bootstrap_tempdir("pk-delete-single-row-supersession");
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		52,
		dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();
	let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
	loop {
		if node.raft.current_leader().await == Some(node.local_node.id) {
			break;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for leader"
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
	node.data_server
		.refresh_leases_once(&node.ring, &*node.clock, 60_000)
		.await;

	let descriptor = typed_table_descriptor();
	node.raft
		.client_write(WendelonRequest::SchemaUpdate(typed_table_schema_update(
			&descriptor,
		)))
		.await
		.unwrap();

	let first_batch = typed_table_batch_with_names(vec![(7, "before-delete")]);
	let mut first_meta = typed_table_meta(&descriptor, first_batch.num_rows());
	first_meta.parent_write_id.sequence_no = 500;
	let (first_resp, _) = node
		.data_server
		.table_write(first_meta, first_batch.clone())
		.await
		.unwrap();
	assert_eq!(first_resp.write.status, WriteStatus::OK);
	let first_admission = validate_table_write_request_batch(
		&first_meta,
		&first_batch,
		&descriptor,
		WriterNodeId::from_node_id(node.local_node),
		first_resp.write.commit_hlc,
	)
	.unwrap();
	let target = first_admission.key_identities[0];
	let missing = Identity::new([0x52; 32]);
	let first_location = node.slab.lookup(tenant(), target).unwrap();
	assert!(!first_location.is_tombstone);

	let mut delete_meta: TableDeleteRowsRequestMeta = bytemuck::Zeroable::zeroed();
	delete_meta.parent_write_id = crate::schema::ClientWriteId {
		client_uuid: [0x52; 16],
		sequence_no: 1,
		idempotency_ttl_ms: 30_000,
		_pad: 0,
	};
	delete_meta.client_hlc = hlc(20, 0, 52);
	delete_meta.expected_epoch = 1;
	delete_meta.tenant_id = tenant();
	delete_meta.table_object_id = descriptor.table_object_id;
	delete_meta.schema_generation = descriptor.schema_generation;
	delete_meta.row_count = 2;
	delete_meta.durability = DurabilityLevel::QUORUM;
	let (delete_resp, trailing) = node
		.data_server
		.table_delete(delete_meta, vec![target, missing])
		.await
		.unwrap();
	assert_eq!(delete_resp.write.status, WriteStatus::OK);
	assert_eq!(delete_resp.reject_code, PbcRejectCode::NONE.0);
	assert_eq!(delete_resp.deleted_count, 1);
	assert_eq!(delete_resp.already_deleted_count, 0);
	assert_eq!(delete_resp.not_found_count, 1);
	assert!(trailing.is_none());

	let tombstone_location = node.slab.lookup(tenant(), target).unwrap();
	assert!(tombstone_location.is_tombstone);
	assert_eq!(tombstone_location.hlc, delete_resp.write.commit_hlc);
	let tombstone_frame = node.slab.read(tombstone_location).await.unwrap();
	let tombstone_batch = decode_batch(&tombstone_frame).unwrap();
	assert!(
		crate::storage::multirow::storage_row_is_tombstone_at(
			&tombstone_batch,
			tombstone_location.record_row_index,
		)
		.unwrap()
	);
	assert_eq!(
		storage_row_table_object_id_at(&tombstone_batch, tombstone_location.record_row_index)
			.unwrap(),
		Some(descriptor.table_object_id)
	);
	assert_eq!(
		materialize_record_location_batch(&tombstone_batch, tombstone_location.record_row_index)
			.unwrap()
			.num_rows(),
		0
	);
	assert!(
		!node
			.slab
			.lookup_as_of(tenant(), target, first_resp.write.commit_hlc)
			.unwrap()
			.is_tombstone
	);

	let mut already_meta = delete_meta;
	already_meta.parent_write_id.sequence_no = 2;
	already_meta.row_count = 1;
	let (already_resp, _) = node
		.data_server
		.table_delete(already_meta, vec![target])
		.await
		.unwrap();
	assert_eq!(already_resp.deleted_count, 0);
	assert_eq!(already_resp.already_deleted_count, 1);
	assert_eq!(already_resp.not_found_count, 0);

	let resurrection_batch = typed_table_batch_with_names(vec![(7, "after-delete")]);
	let mut resurrection_meta = typed_table_meta(&descriptor, resurrection_batch.num_rows());
	resurrection_meta.parent_write_id.sequence_no = 501;
	let (resurrection_resp, _) = node
		.data_server
		.table_write(resurrection_meta, resurrection_batch)
		.await
		.unwrap();
	assert_eq!(resurrection_resp.write.status, WriteStatus::OK);
	let latest_location = node.slab.lookup(tenant(), target).unwrap();
	assert!(!latest_location.is_tombstone);
	assert!(latest_location.hlc > tombstone_location.hlc);
	assert!(
		node.slab
			.slab_has_supersession_at(tombstone_location.slab_id, latest_location.hlc),
		"resurrection must mark the tombstone slab as superseded for CSB safety"
	);
	let latest_frame = node.slab.read(latest_location).await.unwrap();
	let latest_batch = decode_batch(&latest_frame).unwrap();
	let materialized =
		materialize_record_location_batch(&latest_batch, latest_location.record_row_index).unwrap();
	let names = materialized
		.column_by_name("name")
		.unwrap()
		.as_any()
		.downcast_ref::<LargeStringArray>()
		.unwrap();
	assert_eq!(names.value(0), "after-delete");

	shutdown_lease_node(node).await;
}

#[tokio::test]
async fn pk_delete_read_surfaces_track_tombstone_and_resurrection() {
	let dir = bootstrap_tempdir("pk-delete-read-surfaces");
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		53,
		dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();
	let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
	loop {
		if node.raft.current_leader().await == Some(node.local_node.id) {
			break;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for leader"
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
	node.data_server
		.refresh_leases_once(&node.ring, &*node.clock, 60_000)
		.await;

	let descriptor = typed_table_descriptor();
	node.raft
		.client_write(WendelonRequest::SchemaUpdate(typed_table_schema_update(
			&descriptor,
		)))
		.await
		.unwrap();

	let initial = typed_table_batch_with_names(vec![(1, "keep"), (2, "delete-me")]);
	let mut initial_meta = typed_table_meta(&descriptor, initial.num_rows());
	initial_meta.parent_write_id.sequence_no = 530;
	let (initial_resp, _) = node
		.data_server
		.table_write(initial_meta, initial.clone())
		.await
		.unwrap();
	assert_eq!(initial_resp.write.status, WriteStatus::OK);
	let admission = validate_table_write_request_batch(
		&initial_meta,
		&initial,
		&descriptor,
		WriterNodeId::from_node_id(node.local_node),
		initial_resp.write.commit_hlc,
	)
	.unwrap();
	let keep_key = admission.key_identities[0];
	let deleted_key = admission.key_identities[1];

	let mut delete_meta: TableDeleteRowsRequestMeta = bytemuck::Zeroable::zeroed();
	delete_meta.parent_write_id = crate::schema::ClientWriteId {
		client_uuid: [0x53; 16],
		sequence_no: 1,
		idempotency_ttl_ms: 30_000,
		_pad: 0,
	};
	delete_meta.client_hlc = hlc(20, 0, 53);
	delete_meta.expected_epoch = 1;
	delete_meta.tenant_id = tenant();
	delete_meta.table_object_id = descriptor.table_object_id;
	delete_meta.schema_generation = descriptor.schema_generation;
	delete_meta.row_count = 1;
	delete_meta.durability = DurabilityLevel::QUORUM;
	let (delete_resp, _) = node
		.data_server
		.table_delete(delete_meta, vec![deleted_key])
		.await
		.unwrap();
	assert_eq!(delete_resp.write.status, WriteStatus::OK);
	assert_eq!(delete_resp.deleted_count, 1);

	let pre_delete = node
		.slab
		.lookup_as_of(tenant(), deleted_key, initial_resp.write.commit_hlc)
		.expect("deleted key should have a data version before delete");
	assert!(!pre_delete.is_tombstone);
	let at_delete = node
		.slab
		.lookup_as_of(tenant(), deleted_key, delete_resp.write.commit_hlc)
		.expect("deleted key should have a tombstone at delete HLC");
	assert!(at_delete.is_tombstone);
	let scan_at_delete =
		node.slab
			.scan_as_of_with_identity(tenant(), 0, u64::MAX, delete_resp.write.commit_hlc);
	assert!(scan_at_delete.iter().any(|(key, _)| *key == keep_key));
	assert!(!scan_at_delete.iter().any(|(key, _)| *key == deleted_key));
	let versions = node.slab.scan_all_versions(0, u64::MAX, hlc(0, 0, 0));
	assert!(
		versions
			.iter()
			.any(|(_, key, _, location)| *key == deleted_key && !location.is_tombstone)
	);
	assert!(
		versions
			.iter()
			.any(|(_, key, _, location)| *key == deleted_key && location.is_tombstone)
	);

	let pk_program = pk_eq_program(2);
	let deleted_query_meta =
		table_query_meta_for_test(&descriptor, delete_resp.write.commit_hlc, pk_program.len());
	let deleted_query = node
		.data_server
		.prepare_table_query_execution_plan_with_migration(
			&deleted_query_meta,
			&pk_program,
			DecodedRuntimeFilter::Absent,
		)
		.await
		.unwrap();
	assert_eq!(deleted_query.response.status, ReadStatus::KEY_NOT_FOUND);
	assert_eq!(deleted_query.response.rows, 0);
	let delete_count = node
		.data_server
		.execute_aggregate_local(&aggregate_count_request(
			&descriptor,
			530,
			delete_resp.write.commit_hlc,
		))
		.await
		.unwrap();
	assert_eq!(delete_count.status, AggregateStatus::Success);
	assert_eq!(count_star_partial_value(&delete_count), 1);

	let resurrection = typed_table_batch_with_names(vec![(2, "back")]);
	let mut resurrection_meta = typed_table_meta(&descriptor, resurrection.num_rows());
	resurrection_meta.parent_write_id.sequence_no = 531;
	let (resurrection_resp, _) = node
		.data_server
		.table_write(resurrection_meta, resurrection)
		.await
		.unwrap();
	assert_eq!(resurrection_resp.write.status, WriteStatus::OK);
	let latest = node
		.slab
		.lookup_as_of(tenant(), deleted_key, resurrection_resp.write.commit_hlc)
		.expect("resurrected key should have a data version");
	assert!(!latest.is_tombstone);

	let latest_query_meta = table_query_meta_for_test(
		&descriptor,
		resurrection_resp.write.commit_hlc,
		pk_program.len(),
	);
	let latest_query = node
		.data_server
		.prepare_table_query_execution_plan_with_migration(
			&latest_query_meta,
			&pk_program,
			DecodedRuntimeFilter::Absent,
		)
		.await
		.unwrap();
	assert_eq!(latest_query.response.status, ReadStatus::OK);
	assert_eq!(latest_query.response.rows, 1);
	let materialized = concat_batches(&Arc::new(typed_table_schema()), &latest_query.batches)
		.expect("query batches concatenate");
	let names = materialized
		.column_by_name("name")
		.unwrap()
		.as_any()
		.downcast_ref::<LargeStringArray>()
		.unwrap();
	assert_eq!(names.value(0), "back");
	let resurrected_count = node
		.data_server
		.execute_aggregate_local(&aggregate_count_request(
			&descriptor,
			531,
			resurrection_resp.write.commit_hlc,
		))
		.await
		.unwrap();
	assert_eq!(resurrected_count.status, AggregateStatus::Success);
	assert_eq!(count_star_partial_value(&resurrected_count), 2);

	shutdown_lease_node(node).await;
}

#[tokio::test]
async fn table_query_plan_carries_ready_secondary_index_access_plan() {
	let dir = bootstrap_tempdir("table-query-secondary-index-plan");
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		54,
		dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();
	stop_secondary_index_lifecycle(&node).await;
	let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
	loop {
		if node.raft.current_leader().await == Some(node.local_node.id) {
			break;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for leader"
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
	node.data_server
		.refresh_leases_once(&node.ring, &*node.clock, 60_000)
		.await;

	let descriptor = typed_table_descriptor();
	node.raft
		.client_write(WendelonRequest::SchemaUpdate(typed_table_schema_update(
			&descriptor,
		)))
		.await
		.unwrap();
	let index = name_secondary_index_descriptor(&descriptor);
	node.raft
		.client_write(WendelonRequest::SchemaUpdate(
			secondary_index_schema_update(&index),
		))
		.await
		.unwrap();

	let program = name_eq_program("target");
	let query_meta = table_query_meta_for_test(&descriptor, hlc(0, 0, 0), program.len());
	let building_plan = node
		.data_server
		.prepare_table_query_execution_plan_with_migration(
			&query_meta,
			&program,
			DecodedRuntimeFilter::Absent,
		)
		.await
		.unwrap();
	assert_eq!(building_plan.response.status, ReadStatus::KEY_NOT_FOUND);
	assert!(building_plan.secondary_index_access_plan.is_none());

	node.raft
		.client_write(WendelonRequest::SchemaUpdate(
			secondary_index_backfill_complete_schema_update(&index, node.local_node),
		))
		.await
		.unwrap();
	let ready_plan = node
		.data_server
		.prepare_table_query_execution_plan_with_migration(
			&query_meta,
			&program,
			DecodedRuntimeFilter::Absent,
		)
		.await
		.unwrap();
	assert_eq!(ready_plan.response.status, ReadStatus::KEY_NOT_FOUND);
	let access = ready_plan
		.secondary_index_access_plan
		.expect("Ready index should be selected for the name predicate");
	assert_eq!(access.descriptor, index);
	assert_eq!(
		access.query,
		IndexQuery::PointEqual {
			encoded_key: b"target".to_vec(),
		}
	);

	shutdown_lease_node(node).await;
}

#[tokio::test]
async fn table_write_populates_building_secondary_index_active_buffer() {
	let dir = bootstrap_tempdir("table-write-secondary-index-active-buffer");
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		56,
		dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();
	stop_secondary_index_lifecycle(&node).await;
	let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
	loop {
		if node.raft.current_leader().await == Some(node.local_node.id) {
			break;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for leader"
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
	node.data_server
		.refresh_leases_once(&node.ring, &*node.clock, 60_000)
		.await;

	let descriptor = typed_table_descriptor();
	node.raft
		.client_write(WendelonRequest::SchemaUpdate(typed_table_schema_update(
			&descriptor,
		)))
		.await
		.unwrap();
	let index = name_secondary_index_descriptor(&descriptor);
	node.raft
		.client_write(WendelonRequest::SchemaUpdate(
			secondary_index_schema_update(&index),
		))
		.await
		.unwrap();

	let active_slab_id = node.slab.active_slab_id().await;
	let batch = typed_table_batch_with_names(vec![(1, "target")]);
	let mut write_meta = typed_table_meta(&descriptor, batch.num_rows());
	write_meta.parent_write_id.sequence_no = 560;
	let (write_resp, _) = node
		.data_server
		.table_write(write_meta, batch)
		.await
		.unwrap();
	assert_eq!(write_resp.write.status, WriteStatus::OK);

	let coverage_key =
		SecondaryIndexCoverageKey::from(SecondaryIndexIdentity::from_descriptor(tenant(), &index));
	let coverage = node
		.slab
		.secondary_index_coverage_snapshot(coverage_key)
		.expect("secondary index coverage should exist after indexed write");
	let active = coverage
		.active_buffer
		.expect("Building index should receive active-buffer entries");
	assert_eq!(active.source_slab_id, active_slab_id);
	assert_eq!(active.entry_count, 1);
	assert!(coverage.uncovered_slabs.is_empty());

	let info = node
		.slab
		.seal_secondary_index_active_buffer(tenant(), &index)
		.await
		.unwrap()
		.expect("active buffer should seal into a sidecar");
	assert_eq!(info.entry_count, 1);
	let segment = node
		.slab
		.load_secondary_index_segment(&info)
		.await
		.unwrap()
		.unwrap();
	assert_eq!(segment.entries[0].encoded_key, b"target".to_vec());

	shutdown_lease_node(node).await;
}

#[tokio::test]
async fn table_write_seals_secondary_index_active_buffer_after_slab_rotation() {
	let dir = bootstrap_tempdir("table-write-secondary-index-active-buffer-seal");
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		57,
		dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();
	stop_secondary_index_lifecycle(&node).await;
	let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
	loop {
		if node.raft.current_leader().await == Some(node.local_node.id) {
			break;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for leader"
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
	node.data_server
		.refresh_leases_once(&node.ring, &*node.clock, 60_000)
		.await;

	let descriptor = typed_table_descriptor();
	node.raft
		.client_write(WendelonRequest::SchemaUpdate(typed_table_schema_update(
			&descriptor,
		)))
		.await
		.unwrap();
	let index = name_secondary_index_descriptor(&descriptor);
	node.raft
		.client_write(WendelonRequest::SchemaUpdate(
			secondary_index_schema_update(&index),
		))
		.await
		.unwrap();

	let first_slab_id = node.slab.active_slab_id().await;
	let first_batch = typed_table_batch_with_names(vec![(1, "first")]);
	let mut first_meta = typed_table_meta(&descriptor, first_batch.num_rows());
	first_meta.parent_write_id.sequence_no = 570;
	let (first_resp, _) = node
		.data_server
		.table_write(first_meta, first_batch)
		.await
		.unwrap();
	assert_eq!(first_resp.write.status, WriteStatus::OK);
	assert_eq!(node.slab.force_seal_active_for_test().await, first_slab_id);
	let second_slab_id = node.slab.active_slab_id().await;

	let second_batch = typed_table_batch_with_names(vec![(2, "second")]);
	let mut second_meta = typed_table_meta(&descriptor, second_batch.num_rows());
	second_meta.parent_write_id.sequence_no = 571;
	let (second_resp, _) = node
		.data_server
		.table_write(second_meta, second_batch)
		.await
		.unwrap();
	assert_eq!(second_resp.write.status, WriteStatus::OK);

	let coverage_key =
		SecondaryIndexCoverageKey::from(SecondaryIndexIdentity::from_descriptor(tenant(), &index));
	let coverage = node
		.slab
		.secondary_index_coverage_snapshot(coverage_key)
		.expect("secondary index coverage should exist after indexed writes");
	assert_eq!(coverage.covered_slabs, vec![first_slab_id]);
	let active = coverage
		.active_buffer
		.expect("new active slab should receive its own active buffer");
	assert_eq!(active.source_slab_id, second_slab_id);
	assert_eq!(active.entry_count, 1);
	assert!(coverage.uncovered_slabs.is_empty());

	let segments = node.slab.list_secondary_index_segments(
		tenant(),
		index.table_object_id,
		index.index_object_id,
		index.logical_schema_fingerprint,
	);
	assert_eq!(segments.len(), 1);
	assert_eq!(segments[0].source_slab_id, first_slab_id);
	let segment = node
		.slab
		.load_secondary_index_segment(&segments[0])
		.await
		.unwrap()
		.unwrap();
	assert_eq!(segment.entries.len(), 1);
	assert_eq!(segment.entries[0].encoded_key, b"first".to_vec());

	shutdown_lease_node(node).await;
}

#[tokio::test]
async fn table_query_secondary_index_execution_returns_sidecar_rows() {
	let dir = bootstrap_tempdir("table-query-secondary-index-execution");
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		55,
		dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();
	stop_secondary_index_lifecycle(&node).await;
	let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
	loop {
		if node.raft.current_leader().await == Some(node.local_node.id) {
			break;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for leader"
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
	node.data_server
		.refresh_leases_once(&node.ring, &*node.clock, 60_000)
		.await;

	let descriptor = typed_table_descriptor();
	node.raft
		.client_write(WendelonRequest::SchemaUpdate(typed_table_schema_update(
			&descriptor,
		)))
		.await
		.unwrap();

	let batch = typed_table_batch_with_names(vec![(1, "target"), (2, "other")]);
	let mut write_meta = typed_table_meta(&descriptor, batch.num_rows());
	write_meta.parent_write_id.sequence_no = 550;
	let (write_resp, _) = node
		.data_server
		.table_write(write_meta, batch)
		.await
		.unwrap();
	assert_eq!(write_resp.write.status, WriteStatus::OK);
	let sealed_slab_id = node.slab.force_seal_active_for_test().await;

	let index = name_secondary_index_descriptor(&descriptor);
	node.raft
		.client_write(WendelonRequest::SchemaUpdate(
			secondary_index_schema_update(&index),
		))
		.await
		.unwrap();
	let backfill = node
		.slab
		.run_secondary_index_backfill_once(tenant(), &descriptor, &index, 8, 4096)
		.await
		.unwrap();
	assert_eq!(backfill.completed.len(), 1);
	assert_eq!(
		backfill.completed[0].candidate.source_slab_id(),
		sealed_slab_id
	);
	assert!(backfill.historical_complete);
	node.raft
		.client_write(WendelonRequest::SchemaUpdate(
			secondary_index_backfill_complete_schema_update(&index, node.local_node),
		))
		.await
		.unwrap();

	let program = name_eq_program("target");
	let query_meta =
		table_query_meta_for_test(&descriptor, write_resp.write.commit_hlc, program.len());
	let plan = node
		.data_server
		.prepare_table_query_execution_plan_with_migration(
			&query_meta,
			&program,
			DecodedRuntimeFilter::Absent,
		)
		.await
		.unwrap();
	assert_eq!(plan.response.status, ReadStatus::OK);
	assert_eq!(plan.response.rows, 1);
	assert!(plan.secondary_index_access_plan.is_some());
	let materialized = concat_batches(&Arc::new(typed_table_schema()), &plan.batches)
		.expect("query batches concatenate");
	let ids = materialized
		.column_by_name("id")
		.unwrap()
		.as_any()
		.downcast_ref::<Int64Array>()
		.unwrap();
	let names = materialized
		.column_by_name("name")
		.unwrap()
		.as_any()
		.downcast_ref::<LargeStringArray>()
		.unwrap();
	assert_eq!(ids.value(0), 1);
	assert_eq!(names.value(0), "target");

	shutdown_lease_node(node).await;
}

#[tokio::test]
async fn table_delete_emits_tombstone_and_replays_counts() {
	let dir = bootstrap_tempdir("table-delete-row-tombstone");
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		48,
		dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();
	let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
	loop {
		if node.raft.current_leader().await == Some(node.local_node.id) {
			break;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for leader"
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
	node.data_server
		.refresh_leases_once(&node.ring, &*node.clock, 60_000)
		.await;

	let descriptor = append_only_table_descriptor();
	node.raft
		.client_write(WendelonRequest::SchemaUpdate(typed_table_schema_update(
			&descriptor,
		)))
		.await
		.unwrap();

	let batch = typed_table_batch(vec![7, 8, 9]);
	let mut write_meta = typed_table_meta(&descriptor, batch.num_rows());
	write_meta.parent_write_id.sequence_no = 470;
	let (write_resp, _) = node
		.data_server
		.table_write(write_meta, batch.clone())
		.await
		.unwrap();
	assert_eq!(write_resp.write.status, WriteStatus::OK);
	assert_eq!(write_resp.reject_code, PbcRejectCode::NONE.0);
	let admission = validate_table_write_request_batch(
		&write_meta,
		&batch,
		&descriptor,
		WriterNodeId::from_node_id(node.local_node),
		write_resp.write.commit_hlc,
	)
	.unwrap();
	let target = admission.key_identities[1];
	let missing = Identity::new([0xEE; 32]);
	let original_location = node.slab.lookup(tenant(), target).unwrap();
	assert!(!original_location.is_tombstone);

	let mut delete_meta: TableDeleteRowsRequestMeta = bytemuck::Zeroable::zeroed();
	delete_meta.parent_write_id = crate::schema::ClientWriteId {
		client_uuid: [0x47; 16],
		sequence_no: 1,
		idempotency_ttl_ms: 30_000,
		_pad: 0,
	};
	delete_meta.client_hlc = hlc(10, 0, 48);
	delete_meta.expected_epoch = 1;
	delete_meta.tenant_id = tenant();
	delete_meta.table_object_id = descriptor.table_object_id;
	delete_meta.schema_generation = descriptor.schema_generation;
	delete_meta.row_count = 3;
	delete_meta.durability = DurabilityLevel::QUORUM;
	let row_key_ids = vec![target, target, missing];
	let (delete_resp, trailing) = node
		.data_server
		.table_delete(delete_meta, row_key_ids.clone())
		.await
		.unwrap();
	assert_eq!(delete_resp.write.status, WriteStatus::OK);
	assert_eq!(delete_resp.reject_code, PbcRejectCode::NONE.0);
	assert_eq!(delete_resp.deleted_count, 1);
	assert_eq!(delete_resp.already_deleted_count, 0);
	assert_eq!(delete_resp.not_found_count, 1);
	assert!(trailing.is_none());

	let pre_delete = node
		.slab
		.lookup_as_of(tenant(), target, write_resp.write.commit_hlc)
		.unwrap();
	assert!(!pre_delete.is_tombstone);
	let tombstone_location = node.slab.lookup(tenant(), target).unwrap();
	assert!(tombstone_location.is_tombstone);
	assert_eq!(tombstone_location.hlc, delete_resp.write.commit_hlc);
	let tombstone_frame = node.slab.read(tombstone_location).await.unwrap();
	let tombstone_batch = decode_batch(&tombstone_frame).unwrap();
	assert!(
		crate::storage::multirow::storage_row_is_tombstone_at(
			&tombstone_batch,
			tombstone_location.record_row_index,
		)
		.unwrap()
	);
	assert_eq!(
		storage_row_table_object_id_at(&tombstone_batch, tombstone_location.record_row_index)
			.unwrap(),
		Some(descriptor.table_object_id)
	);
	let materialized =
		materialize_record_location_batch(&tombstone_batch, tombstone_location.record_row_index)
			.unwrap();
	assert_eq!(materialized.num_rows(), 0);

	let (retry_resp, _) = node
		.data_server
		.table_delete(delete_meta, row_key_ids)
		.await
		.unwrap();
	assert_eq!(retry_resp.deleted_count, 1);
	assert_eq!(retry_resp.not_found_count, 1);

	let mut second_delete = delete_meta;
	second_delete.parent_write_id.sequence_no = 2;
	second_delete.row_count = 1;
	let (already_resp, _) = node
		.data_server
		.table_delete(second_delete, vec![target])
		.await
		.unwrap();
	assert_eq!(already_resp.deleted_count, 0);
	assert_eq!(already_resp.already_deleted_count, 1);
	assert_eq!(already_resp.not_found_count, 0);

	shutdown_lease_node(node).await;
}

#[tokio::test]
async fn table_delete_fails_closed_when_target_locality_is_missing() {
	let dir = bootstrap_tempdir("table-delete-missing-locality");
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		49,
		dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();
	let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
	loop {
		if node.raft.current_leader().await == Some(node.local_node.id) {
			break;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for leader"
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
	node.data_server
		.refresh_leases_once(&node.ring, &*node.clock, 60_000)
		.await;

	let descriptor = append_only_table_descriptor();
	node.raft
		.client_write(WendelonRequest::SchemaUpdate(typed_table_schema_update(
			&descriptor,
		)))
		.await
		.unwrap();

	let batch = typed_table_batch(vec![7]);
	let mut write_meta = typed_table_meta(&descriptor, batch.num_rows());
	write_meta.parent_write_id.sequence_no = 471;
	let (write_resp, _) = node
		.data_server
		.table_write(write_meta, batch.clone())
		.await
		.unwrap();
	assert_eq!(write_resp.write.status, WriteStatus::OK);
	let admission = validate_table_write_request_batch(
		&write_meta,
		&batch,
		&descriptor,
		WriterNodeId::from_node_id(node.local_node),
		write_resp.write.commit_hlc,
	)
	.unwrap();
	let target = admission.key_identities[0];
	let original_location = node.slab.lookup(tenant(), target).unwrap();
	assert!(!original_location.is_tombstone);
	assert!(
		node.slab
			.routing_locality_for_existing_key(tenant(), target)
			.is_some()
	);

	node.slab.forget_routing_locality_for_test(tenant(), target);
	assert!(
		node.slab
			.routing_locality_for_existing_key(tenant(), target)
			.is_none()
	);

	let mut delete_meta: TableDeleteRowsRequestMeta = bytemuck::Zeroable::zeroed();
	delete_meta.parent_write_id = crate::schema::ClientWriteId {
		client_uuid: [0x49; 16],
		sequence_no: 1,
		idempotency_ttl_ms: 30_000,
		_pad: 0,
	};
	delete_meta.client_hlc = hlc(10, 0, 49);
	delete_meta.expected_epoch = 1;
	delete_meta.tenant_id = tenant();
	delete_meta.table_object_id = descriptor.table_object_id;
	delete_meta.schema_generation = descriptor.schema_generation;
	delete_meta.row_count = 1;
	delete_meta.durability = DurabilityLevel::QUORUM;

	let (delete_resp, trailing) = node
		.data_server
		.table_delete(delete_meta, vec![target])
		.await
		.unwrap();
	assert_eq!(delete_resp.write.status, WriteStatus::INTERNAL_ERROR);
	assert_eq!(
		delete_resp.reject_code,
		PbcRejectCode::PBC_REJECT_LOCALITY_UNSUPPORTED.0
	);
	assert_eq!(delete_resp.deleted_count, 0);
	assert_eq!(delete_resp.already_deleted_count, 0);
	assert_eq!(delete_resp.not_found_count, 0);
	assert!(trailing.is_none());

	let latest = node.slab.lookup(tenant(), target).unwrap();
	assert!(!latest.is_tombstone);
	assert_eq!(latest.hlc, original_location.hlc);

	shutdown_lease_node(node).await;
}

#[tokio::test]
async fn table_write_multi_row_commits_one_parent_finalization() {
	let dir = bootstrap_tempdir("table-write-multi-row");
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		38,
		dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();
	let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
	loop {
		if node.raft.current_leader().await == Some(node.local_node.id) {
			break;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for leader"
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
	node.data_server
		.refresh_leases_once(&node.ring, &*node.clock, 60_000)
		.await;

	let descriptor = typed_table_descriptor();
	node.raft
		.client_write(WendelonRequest::SchemaUpdate(typed_table_schema_update(
			&descriptor,
		)))
		.await
		.unwrap();

	let batch = typed_table_batch(vec![7, 8]);
	let mut meta = typed_table_meta(&descriptor, batch.num_rows());
	meta.parent_write_id.sequence_no = 56;
	meta.await_finalization = 1;
	let expected_bifurcated = bifurcate_table_write(
		&meta,
		&batch,
		&descriptor,
		WriterNodeId::ZERO,
		meta.client_hlc,
	)
	.unwrap();
	let (resp, trailing) = node
		.data_server
		.table_write(meta, batch.clone())
		.await
		.unwrap();
	assert_eq!(resp.write.status, WriteStatus::OK, "got resp={resp:?}");
	assert_eq!(resp.reject_code, PbcRejectCode::NONE.0);
	assert_eq!(resp.write.has_finalization, 1);
	assert!(trailing.is_some());

	let expected_finalization = TenantFinalization {
		marker: FinalizationMarker {
			log_index: 0,
			term: 0,
			epoch: meta.expected_epoch,
			commit_hlc: resp.write.commit_hlc,
			payload_hash: expected_bifurcated.payload_hash,
			e_safe_index: 0,
			durability: meta.durability,
			_pad: [0; 7],
		},
		tenant_id: meta.tenant_id,
		key_identity: expected_bifurcated.parent_key_identity,
		charged_bytes: expected_bifurcated.charged_bytes,
		record_count: 2,
	};
	assert!(
		node.log_store
			.find_applied_finalization(&expected_finalization, resp.write.log_index)
			.await
			.unwrap()
			.is_some(),
		"expected one aggregate finalization for the parent table write"
	);

	for id_value in [7_i64, 8_i64] {
		let key_identity = table_row_identity(
			tenant(),
			descriptor.table_object_id,
			&id_value.to_le_bytes(),
		);
		let location = node
			.slab
			.lookup(tenant(), key_identity)
			.expect("table row should be indexed by PK-derived identity");
		let stored = decode_batch(&node.slab.read(location).await.unwrap()).unwrap();
		assert_eq!(stored.schema().as_ref(), &typed_table_schema());
		assert_eq!(stored.num_rows(), 1);
		let ids = stored
			.column_by_name("id")
			.unwrap()
			.as_any()
			.downcast_ref::<Int64Array>()
			.unwrap();
		assert_eq!(ids.value(0), id_value);
	}

	shutdown_lease_node(node).await;
}

#[tokio::test]
async fn table_write_cdc_releases_when_pending_intent_persistence_fails_but_finalize_commits() {
	let dir = bootstrap_tempdir("table-cdc-pending-upsert-fail-finalize-ok");
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		43,
		dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();
	let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
	loop {
		if node.raft.current_leader().await == Some(node.local_node.id) {
			break;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for leader"
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
	node.data_server
		.refresh_leases_once(&node.ring, &*node.clock, 60_000)
		.await;

	let descriptor = typed_table_descriptor();
	node.raft
		.client_write(WendelonRequest::SchemaUpdate(typed_table_schema_update(
			&descriptor,
		)))
		.await
		.unwrap();
	node.data_server
		.pending_finalizations
		.fail_next_upsert_for_test("cdc-table-upsert-fail-finalize-ok");

	let batch = typed_table_batch(vec![700]);
	let mut meta = typed_table_meta(&descriptor, batch.num_rows());
	meta.parent_write_id.sequence_no = 700;
	meta.await_finalization = 1;

	let (resp, trailing) = node.data_server.table_write(meta, batch).await.unwrap();
	assert_eq!(resp.write.status, WriteStatus::OK, "got resp={resp:?}");
	assert!(trailing.is_some());
	assert_eq!(
		node.data_server.degraded_reason().as_deref(),
		Some(LOCAL_PERSISTENCE_DISTRUST_REASON)
	);

	let source_ranges =
		table_query_scan_ranges(meta.tenant_id, &descriptor, None).expect("table ranges");
	let entries = node
		.data_server
		.cdc_collect_ring_entries_after(
			meta.tenant_id,
			meta.table_object_id,
			&source_ranges,
			CdcCursor::MIN,
		)
		.await;
	assert_eq!(entries.len(), 1);
	assert_eq!(entries[0].cursor.row_hlc, resp.write.commit_hlc);
	assert!(entries[0].is_finalized());
	assert!(
		node.data_server.cdc_pending_live_entries.is_empty(),
		"immediate finalization success must release the pending CDC staging record"
	);

	shutdown_lease_node(node).await;
}

#[tokio::test]
async fn table_write_cdc_evicts_when_pending_intent_persistence_and_finalize_fail() {
	let dir = bootstrap_tempdir("table-cdc-pending-upsert-finalize-fail");
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		44,
		dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();
	let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
	loop {
		if node.raft.current_leader().await == Some(node.local_node.id) {
			break;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for leader"
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
	node.data_server
		.refresh_leases_once(&node.ring, &*node.clock, 60_000)
		.await;

	let descriptor = typed_table_descriptor();
	node.raft
		.client_write(WendelonRequest::SchemaUpdate(typed_table_schema_update(
			&descriptor,
		)))
		.await
		.unwrap();
	node.data_server
		.pending_finalizations
		.fail_next_upsert_for_test("cdc-table-upsert-and-finalize-fail");
	node.raft.shutdown().await.unwrap();

	let batch = typed_table_batch(vec![701]);
	let mut meta = typed_table_meta(&descriptor, batch.num_rows());
	meta.parent_write_id.sequence_no = 701;
	meta.await_finalization = 1;

	let (resp, trailing) = node.data_server.table_write(meta, batch).await.unwrap();
	assert_eq!(
		resp.write.status,
		WriteStatus::INTERNAL_ERROR,
		"got resp={resp:?}"
	);
	assert!(trailing.is_none());
	assert_eq!(
		node.data_server.degraded_reason().as_deref(),
		Some(LOCAL_PERSISTENCE_DISTRUST_REASON)
	);

	let source_ranges =
		table_query_scan_ranges(meta.tenant_id, &descriptor, None).expect("table ranges");
	let entries = node
		.data_server
		.cdc_collect_ring_entries_after(
			meta.tenant_id,
			meta.table_object_id,
			&source_ranges,
			CdcCursor::MIN,
		)
		.await;
	assert!(
		entries.is_empty(),
		"terminal immediate-finalization failure must evict staged CDC rows"
	);
	assert!(node.data_server.cdc_pending_live_entries.is_empty());

	shutdown_lease_node(node).await;
}

#[tokio::test]
async fn table_replica_cdc_releases_when_parent_finalization_marker_applies() {
	let dir = bootstrap_tempdir("table-replica-cdc-marker-release");
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		45,
		dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();

	let descriptor = typed_table_descriptor();
	let batch = typed_table_batch(vec![800]);
	let mut meta = typed_table_meta(&descriptor, batch.num_rows());
	meta.parent_write_id.sequence_no = 800;
	let commit_hlc = hlc(800, 0, node.local_node.id);
	let bifurcated =
		bifurcate_table_write(&meta, &batch, &descriptor, WriterNodeId::ZERO, commit_hlc).unwrap();
	let replica_meta = table_replica_meta(&meta, &bifurcated, &bifurcated.children, commit_hlc);
	let replica_children = bifurcated
		.children
		.iter()
		.map(|child| TableReplicaChildPayload {
			child_ordinal: child.child_ordinal,
			key_identity: child.key_identity,
			locality: child.locality,
			record_row_index: child.record_row_index,
			frame_bytes: child.frame_bytes.clone(),
			payload_hash: child.payload_hash,
			charged_bytes: child.charged_bytes,
		})
		.collect::<Vec<_>>();
	let staged = node
		.data_server
		.stage_cdc_table_replica_children(
			meta.tenant_id,
			meta.table_object_id,
			&descriptor,
			commit_hlc,
			&replica_children,
			false,
		)
		.await
		.unwrap();
	assert_eq!(staged.len(), 1);
	node.data_server.cdc_stage_pending_finalization_marker(
		CdcFinalizationKey::from_table_replica_meta(&replica_meta),
		staged,
	);

	let source_ranges =
		table_query_scan_ranges(meta.tenant_id, &descriptor, None).expect("table ranges");
	assert!(
		node.data_server
			.cdc_collect_ring_entries_after(
				meta.tenant_id,
				meta.table_object_id,
				&source_ranges,
				CdcCursor::MIN,
			)
			.await
			.is_empty(),
		"replica CDC row must not be visible before the parent finalization applies"
	);

	let applied_marker = FinalizationMarker {
		log_index: 88,
		term: 2,
		epoch: meta.expected_epoch,
		commit_hlc,
		payload_hash: bifurcated.payload_hash,
		e_safe_index: 88,
		durability: meta.durability,
		_pad: [0; 7],
	};
	assert_eq!(
		node.data_server
			.cdc_release_pending_finalization_marker(applied_marker)
			.await,
		1
	);
	assert!(node.data_server.cdc_marker_pending_live_entries.is_empty());
	assert_eq!(
		node.data_server
			.cdc_locality_frontiers
			.min_for_localities([bifurcated.children[0].locality]),
		Some(commit_hlc)
	);

	let entries = node
		.data_server
		.cdc_collect_ring_entries_after(
			meta.tenant_id,
			meta.table_object_id,
			&source_ranges,
			CdcCursor::MIN,
		)
		.await;
	assert_eq!(entries.len(), 1);
	assert_eq!(entries[0].cursor.row_hlc, commit_hlc);
	assert!(entries[0].is_finalized());

	shutdown_lease_node(node).await;
}

#[tokio::test]
async fn adr0094_p2_provisional_rows_are_unreachable_by_direct_object_query() {
	let dir = bootstrap_tempdir("adr0094-provisional-direct-query");
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		94,
		dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();
	let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
	loop {
		if node.raft.current_leader().await == Some(node.local_node.id) {
			break;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for leader"
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
	node.data_server
		.refresh_leases_once(&node.ring, &*node.clock, 60_000)
		.await;

	let descriptor = typed_table_descriptor();
	let batch = typed_table_batch(vec![94]);
	let mut write_meta = typed_table_meta(&descriptor, batch.num_rows());
	write_meta.parent_write_id.sequence_no = 94;
	let mut operation_bytes = [0x94; 16];
	operation_bytes[..5].fill(0);
	operation_bytes[5] = 1;
	operation_bytes[6] = 0x70;
	operation_bytes[8] = 0x80;
	let provision = node
		.raft
		.client_write(WendelonRequest::CtasSaga(
			crate::schema::CtasSagaMutation::Provision {
				tenant_id: tenant(),
				operation_id: crate::schema::CtasOperationId::new(operation_bytes),
				intent_fingerprint: crate::schema::CtasIntentFingerprint::new([0x94; 32]),
				descriptor: descriptor.clone(),
				expected_binding: crate::schema::CtasExpectedBinding::Absent,
				prior_descriptor: None,
				row_write_id: write_meta.parent_write_id,
				owner_epoch: 1,
				observed_now_ms: 1,
				owner_lease_expires_at_ms: 60_000,
				certificate: Some(vec![0x94]),
			},
		))
		.await
		.expect("provision CTAS object");
	assert_eq!(provision.data, WendelonResponse::Success);
	node.data_server
		.refresh_leases_once(&node.ring, &*node.clock, 60_000)
		.await;
	let (write_response, _) = node
		.data_server
		.table_write(write_meta, batch)
		.await
		.expect("write rows through the saga-owned provisional descriptor");
	assert_eq!(write_response.write.status, WriteStatus::OK);
	let mut undeclared_child = typed_table_meta(&descriptor, 1);
	undeclared_child.parent_write_id = write_response.write.echoed_write_id;
	undeclared_child.parent_write_id.sequence_no += 1;
	let (undeclared_response, _) = node
		.data_server
		.table_write(undeclared_child, typed_table_batch(vec![95]))
		.await
		.expect("an undeclared child receives a typed refusal");
	assert_eq!(
		undeclared_response.reject_code,
		PbcRejectCode::PBC_REJECT_SCHEMA_UNKNOWN.0,
		"a pre-declaration saga authorizes only its historical row_write_id"
	);

	let state = node.state_machine.state_snapshot().await;
	assert!(
		state
			.table_schemas
			.latest_by_name(tenant(), "analytics", "events")
			.is_none(),
		"the provisional name is not public"
	);
	drop(state);
	let query = table_query_frame_for_decode_test(
		table_query_meta_for_test(&descriptor, write_response.write.commit_hlc, 0),
		None,
	);
	let response = DataRingServer::handle_data_frame(
		node.data_server.as_ref(),
		RequestContext {
			remote: Some(node.local_node),
			actor: actor(ActorRole::Operator, None),
		},
		query,
	)
	.await
	.unwrap()
	.unwrap();
	let response_meta: QueryResponseMeta =
		*bytemuck::from_bytes(&response.meta[..std::mem::size_of::<QueryResponseMeta>()]);
	assert_eq!(response_meta.status, ReadStatus::QUERY_INVALID_PROGRAM);
	assert!(response.arrow_payload.is_none());

	shutdown_lease_node(node).await;
}

#[tokio::test]
async fn adr0094_declared_child_writes_share_the_provisional_descriptor() {
	let dir = bootstrap_tempdir("adr0094-provisional-write-chain");
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		95,
		dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();
	let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
	loop {
		if node.raft.current_leader().await == Some(node.local_node.id) {
			break;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for leader"
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
	node.data_server
		.refresh_leases_once(&node.ring, &*node.clock, 60_000)
		.await;

	let descriptor = typed_table_descriptor();
	let mut first_meta = typed_table_meta(&descriptor, 1);
	first_meta.parent_write_id.sequence_no = 950;
	let operation_id = crate::schema::CtasOperationId::new([
		0, 0, 0, 0, 0, 1, 0x70, 0x95, 0x80, 0x95, 0x95, 0x95, 0x95, 0x95, 0x95, 0x95,
	]);
	let provision = node
		.raft
		.client_write(WendelonRequest::CtasSaga(
			crate::schema::CtasSagaMutation::Provision {
				tenant_id: tenant(),
				operation_id,
				intent_fingerprint: crate::schema::CtasIntentFingerprint::new([0x95; 32]),
				descriptor: descriptor.clone(),
				expected_binding: crate::schema::CtasExpectedBinding::Absent,
				prior_descriptor: None,
				row_write_id: first_meta.parent_write_id,
				owner_epoch: 1,
				observed_now_ms: 1,
				owner_lease_expires_at_ms: 60_000,
				certificate: Some(vec![0x95]),
			},
		))
		.await
		.expect("provision CTAS object");
	assert_eq!(provision.data, WendelonResponse::Success);
	let declaration = node
		.raft
		.client_write(WendelonRequest::CtasSaga(
			crate::schema::CtasSagaMutation::DeclareChain {
				tenant_id: tenant(),
				operation_id,
				owner_epoch: 1,
				row_count: 2,
				chunk_count: 2,
			},
		))
		.await
		.expect("declare two-child CTAS chain");
	assert_eq!(declaration.data, WendelonResponse::Success);
	node.data_server
		.refresh_leases_once(&node.ring, &*node.clock, 60_000)
		.await;

	let (first, _) = node
		.data_server
		.table_write(first_meta, typed_table_batch(vec![95]))
		.await
		.expect("write first declared child");
	assert_eq!(first.write.status, WriteStatus::OK);
	assert_eq!(first.reject_code, PbcRejectCode::NONE.0);

	let mut second_meta = typed_table_meta(&descriptor, 1);
	second_meta.parent_write_id.sequence_no = 951;
	second_meta.parent_write_id.idempotency_ttl_ms = 1_234;
	let (second, _) = node
		.data_server
		.table_write(second_meta, typed_table_batch(vec![96]))
		.await
		.expect("write second declared child");
	assert_eq!(second.write.status, WriteStatus::OK);
	assert_eq!(
		second.reject_code,
		PbcRejectCode::NONE.0,
		"the second child must resolve the saga's provisional descriptor"
	);
	let mut outside_meta = typed_table_meta(&descriptor, 1);
	outside_meta.parent_write_id.sequence_no = 952;
	let (outside, _) = node
		.data_server
		.table_write(outside_meta, typed_table_batch(vec![97]))
		.await
		.expect("an out-of-range write receives a typed refusal");
	assert_eq!(
		outside.reject_code,
		PbcRejectCode::PBC_REJECT_SCHEMA_UNKNOWN.0,
		"the declared half-open child range must not authorize its end"
	);

	shutdown_lease_node(node).await;
}

#[tokio::test(flavor = "current_thread")]
async fn adr0095_rf1_sweeper_reclaims_attests_and_settles_the_exact_debt() {
	let node_dir = bootstrap_tempdir("adr0095-rf1-settlement");
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		195,
		node_dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();
	let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
	loop {
		if node.raft.current_leader().await == Some(node.local_node.id) {
			break;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"leader election timed out"
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
	// Keep this control deterministic: the production task uses this same method,
	// but the test drives exactly one pass at a time.
	node.handles.abort();
	let descriptor = multirow_typed_table_descriptor();
	let installed = node
		.raft
		.client_write(WendelonRequest::SchemaUpdate(typed_table_schema_update(
			&descriptor,
		)))
		.await
		.unwrap();
	assert_eq!(installed.data, WendelonResponse::Success);
	let epoch = crate::consensus::EpochConfig {
		epoch: 1,
		peers: std::collections::BTreeMap::from([(
			node.local_node.id,
			crate::consensus::EpochPeer {
				node_id: node.local_node,
				address: "127.0.0.1:0".to_string(),
				tier_bits: 0,
				role_bits: 1,
				capability_bits: 0,
			},
		)]),
	};
	let epoch_write = node
		.raft
		.client_write(WendelonRequest::AdvanceEpoch(
			crate::consensus::AdvanceEpochRecord {
				from: crate::consensus::EpochConfig {
					epoch: 0,
					peers: std::collections::BTreeMap::new(),
				},
				to: epoch,
			},
		))
		.await
		.unwrap();
	assert_eq!(epoch_write.data, WendelonResponse::Success);
	let mut keys = append_append_only_rows_for_query(
		&node.slab,
		&descriptor,
		vec![1, 2, 3, 4],
		hlc(30, 0, 195),
	)
	.await;
	let operation_id = crate::integration::compute::DropOperationId {
		tenant_id: tenant(),
		table_object_id: descriptor.table_object_id,
		schema_generation: SchemaGeneration::new(descriptor.schema_generation),
	};
	let prepared = node
		.raft
		.client_write(WendelonRequest::TableDropPrepare(
			crate::consensus::TableDropPrepareRequest::current(
				operation_id,
				SchemaGeneration::new(descriptor.schema_generation),
			),
		))
		.await
		.unwrap();
	assert_eq!(prepared.data, WendelonResponse::Success);
	assert!(
		node.data_server
			.reconcile_drop_reclaim_once()
			.await
			.unwrap(),
		"the sweeper rolls a durable prepared fence forward before reclaim"
	);
	let debt = node
		.state_machine
		.state_snapshot()
		.await
		.table_drop_reclaim_debts
		.get(tenant(), descriptor.table_object_id)
		.cloned()
		.expect("logical finalize durably records reclaim debt");
	assert_eq!(
		debt.participants.iter().collect::<Vec<_>>(),
		vec![node.local_node]
	);
	const WITHHELD_COMPLETION_CYCLES: usize = 4;
	for cycle in 0..WITHHELD_COMPLETION_CYCLES {
		let stale_coordinator = node
			.data_server
			.run_drop_reclaim_slice(DropReclaimSliceRequest {
				operation_id,
				finalize_log_index: debt.finalize_log_index,
				ownership_epoch: debt.ownership_epoch,
				examined_budget: 1,
				coordinator: n(9_999),
			})
			.await
			.unwrap_err();
		assert_eq!(
			stale_coordinator.kind(),
			std::io::ErrorKind::PermissionDenied
		);
		assert!(
			stale_coordinator
				.to_string()
				.contains("current Raft leader")
		);
		assert!(
			node.state_machine
				.state_snapshot()
				.await
				.table_drop_reclaim_debts
				.get(tenant(), descriptor.table_object_id)
				.is_some(),
			"withheld completion cycle {cycle} must not settle the debt"
		);
	}
	let stale_epoch = node
		.data_server
		.run_drop_reclaim_slice(DropReclaimSliceRequest {
			operation_id,
			finalize_log_index: debt.finalize_log_index,
			ownership_epoch: debt.ownership_epoch + 1,
			examined_budget: 1,
			coordinator: node.local_node,
		})
		.await
		.unwrap_err();
	assert_eq!(stale_epoch.kind(), std::io::ErrorKind::PermissionDenied);
	assert!(stale_epoch.to_string().contains("ownership epoch is stale"));

	let current_request = DropReclaimSliceRequest {
		operation_id,
		finalize_log_index: debt.finalize_log_index,
		ownership_epoch: debt.ownership_epoch,
		examined_budget: DROP_RECLAIM_MAX_EXAMINED_PER_SLICE,
		coordinator: node.local_node,
	};
	let phase_events = Arc::new(StdMutex::new(Vec::new()));
	let phase_subscriber = Registry::default().with(CapturingEventLayer {
		events: phase_events.clone(),
	});
	let _phase_subscriber_guard = tracing::subscriber::set_default(phase_subscriber);
	assert!(
		!node
			.data_server
			.run_drop_reclaim_slice(current_request)
			.await
			.unwrap()
			.done
	);
	// Model a competing rewrite that held the slab claim first and published a
	// replacement above the L1 cursor's frozen upper bound. This direct storage
	// append deliberately bypasses lifecycle admission: it represents physical
	// maintenance, not an admitted post-tombstone user mutation.
	keys.extend(
		append_append_only_rows_for_query(&node.slab, &descriptor, vec![5], hlc(31, 0, 195)).await,
	);
	let post_cursor_references = node
		.slab
		.table_drop_reference_counts(tenant(), descriptor.table_object_id);
	assert!(
		!post_cursor_references.is_empty(),
		"the race fixture must publish a target-bearing replacement: {post_cursor_references:?}"
	);
	assert!(
		!node
			.data_server
			.run_drop_reclaim_slice(current_request)
			.await
			.unwrap()
			.done
	);
	assert!(
		!node
			.data_server
			.run_drop_reclaim_slice(current_request)
			.await
			.expect("indexed post-cursor references restart bounded L1 work")
			.done,
		"verification must not attest while a replacement slab remains"
	);

	let drain_started = std::time::Instant::now();
	for _ in 0..8 {
		node.data_server
			.reconcile_drop_reclaim_once()
			.await
			.unwrap();
		if node
			.state_machine
			.state_snapshot()
			.await
			.table_drop_reclaim_debts
			.get(tenant(), descriptor.table_object_id)
			.is_none()
		{
			break;
		}
	}
	let drain_elapsed = drain_started.elapsed();
	eprintln!(
		"ADR0095 RF=1 bounded reclaim receipt: rows={} drain_us={}",
		keys.len(),
		drain_elapsed.as_micros()
	);
	let settled = node.state_machine.state_snapshot().await;
	assert!(
		settled
			.table_drop_reclaim_debts
			.get(tenant(), descriptor.table_object_id)
			.is_none(),
		"the exact RF=1 receipt set permits settlement"
	);
	assert!(
		settled
			.table_drop_tombstones
			.get(tenant(), descriptor.table_object_id)
			.is_none()
	);
	assert!(
		settled
			.table_schemas
			.latest_by_id(tenant(), descriptor.table_object_id)
			.is_none(),
		"settlement retires the unbound descriptor"
	);
	drop(settled);
	assert!(
		node.slab
			.table_drop_reference_counts(tenant(), descriptor.table_object_id)
			.is_empty()
	);
	for key in keys {
		assert!(node.slab.lookup(tenant(), key).is_none());
	}
	let phase_events = phase_events.lock().expect("captured phase event mutex");
	let transitions = phase_events
		.iter()
		.filter(|event| {
			event.fields.iter().any(|(name, value)| {
				name == "message" && value.contains("table drop reclaim phase transitioned")
			})
		})
		.collect::<Vec<_>>();
	assert!(
		!transitions.is_empty(),
		"phase transitions must be routine signals"
	);
	for required in [
		"tenant_id",
		"table_object_id",
		"schema_generation",
		"from_phase",
		"to_phase",
		"cursor_after_slab_id",
		"cursor_upper_bound_slab_id",
		"examined_total",
		"ownership_epoch",
	] {
		assert!(
			transitions
				.iter()
				.any(|event| event.fields.iter().any(|(name, _)| name == required)),
			"phase transition lines lack {required}: {transitions:?}"
		);
	}
	drop(transitions);
	drop(phase_events);
	shutdown_lease_node(node).await;
}

#[tokio::test]
async fn table_query_dispatch_materializes_multirow_records_before_pbc() {
	let dir = bootstrap_tempdir("table-query-rebatch");
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		39,
		dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();
	let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
	loop {
		if node.raft.current_leader().await == Some(node.local_node.id) {
			break;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for leader"
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
	node.data_server
		.refresh_leases_once(&node.ring, &*node.clock, 60_000)
		.await;

	let descriptor = multirow_typed_table_descriptor();
	node.raft
		.client_write(WendelonRequest::SchemaUpdate(typed_table_schema_update(
			&descriptor,
		)))
		.await
		.unwrap();

	let source_ids = same_multirow_locality_ids(&descriptor, 2);
	let batch = typed_table_batch(source_ids.clone());
	let mut write_meta = typed_table_meta(&descriptor, batch.num_rows());
	write_meta.parent_write_id.sequence_no = 57;
	let (write_resp, _) = node
		.data_server
		.table_write(write_meta, batch)
		.await
		.unwrap();
	assert_eq!(write_resp.write.status, WriteStatus::OK);
	let stored_key = table_row_identity(
		tenant(),
		descriptor.table_object_id,
		&source_ids[0].to_le_bytes(),
	);
	let stored_location = node
		.slab
		.lookup(tenant(), stored_key)
		.expect("compressed multi-row frame should be indexed");
	let stored_frame = node.slab.read(stored_location).await.unwrap();
	assert_eq!(
		crate::storage::ipc::record_batch_compression_policy_from_ipc(&stored_frame).unwrap(),
		RecordBodyCompressionPolicy::Lz4Frame
	);
	assert!(has_storage_synthetic_columns(
		&decode_batch(&stored_frame).unwrap()
	));

	let mut bytecode = Vec::new();
	bytecode.push(OP_PUSH_COL);
	bytecode.extend_from_slice(&0_u32.to_le_bytes());
	bytecode.push(OP_PUSH_INT);
	bytecode.extend_from_slice(&source_ids[1].to_le_bytes());
	bytecode.push(OP_GTE);
	let query_meta = TableQueryRequestMeta {
		min_hlc: hlc(0, 0, 0),
		expected_epoch: 1,
		tenant_id: tenant(),
		table_object_id: descriptor.table_object_id,
		schema_generation: descriptor.schema_generation,
		physical_descriptor_fingerprint: *descriptor.physical_descriptor_fingerprint.as_bytes(),
		consistency: Consistency::SERIALIZABLE,
		row_identity_projection: RowIdentityProjection::None.as_u8(),
		_pad: [0; 6],
		bytecode_len: bytecode.len() as u32,
		runtime_filter_len: 0,
		as_of_hlc: hlc(0, 0, 0),
		row_limit_rows: 0,
		deadline_remaining_millis: 0,
		request_id: TableQueryRequestId::from_u128(2),
		trace_id: [0; 16],
		trace_parent_span_id: [0; 8],
		trace_flags: 0,
		_trace_pad: [0; 7],
	};
	let mut bad_query_meta = query_meta;
	bad_query_meta.physical_descriptor_fingerprint = [0xFF; 32];
	let mut bad_meta_buf = node.pool.acquire();
	bad_meta_buf
		.as_mut_vec()
		.extend_from_slice(bytemuck::bytes_of(&bad_query_meta));
	let mut bad_trailing = node.pool.acquire();
	bad_trailing.as_mut_vec().extend_from_slice(&bytecode);
	let bad_frame = HrftFrame {
		header: FrameHeader {
			magic: HRFT_MAGIC,
			version: 1,
			msg_type: MsgType::TABLE_QUERY_REQ.0,
			flags: 0,
			stream_id: 38,
			meta_len: bad_meta_buf.len() as u32,
			arrow_len: 0,
			trailing_len: bad_trailing.len() as u32,
		},
		meta: bad_meta_buf,
		arrow_payload: None,
		trailing: Some(bad_trailing),
	};
	let bad_resp = DataRingServer::handle_data_frame(
		node.data_server.as_ref(),
		RequestContext {
			remote: Some(node.local_node),
			actor: actor(ActorRole::Operator, None),
		},
		bad_frame,
	)
	.await
	.unwrap()
	.unwrap();
	let bad_meta: QueryResponseMeta =
		*bytemuck::from_bytes(&bad_resp.meta[..std::mem::size_of::<QueryResponseMeta>()]);
	assert_eq!(bad_meta.status, ReadStatus::TABLE_DESCRIPTOR_EVOLVED);

	let mut meta_buf = node.pool.acquire();
	meta_buf
		.as_mut_vec()
		.extend_from_slice(bytemuck::bytes_of(&query_meta));
	let mut trailing = node.pool.acquire();
	trailing.as_mut_vec().extend_from_slice(&bytecode);
	let frame = HrftFrame {
		header: FrameHeader {
			magic: HRFT_MAGIC,
			version: 1,
			msg_type: MsgType::TABLE_QUERY_REQ.0,
			flags: 0,
			stream_id: 39,
			meta_len: meta_buf.len() as u32,
			arrow_len: 0,
			trailing_len: trailing.len() as u32,
		},
		meta: meta_buf,
		arrow_payload: None,
		trailing: Some(trailing),
	};

	let resp = DataRingServer::handle_data_frame(
		node.data_server.as_ref(),
		RequestContext {
			remote: Some(node.local_node),
			actor: actor(ActorRole::Operator, None),
		},
		frame,
	)
	.await
	.unwrap()
	.unwrap();
	assert_eq!(resp.header.msg_type, MsgType::TABLE_QUERY_RESP.0);
	let meta: QueryResponseMeta =
		*bytemuck::from_bytes(&resp.meta[..std::mem::size_of::<QueryResponseMeta>()]);
	assert_eq!(meta.status, ReadStatus::OK, "got meta={meta:?}");
	assert_eq!(meta.rows, 1);
	let result = decode_batch(resp.arrow_payload.as_ref().expect("query result batch"))
		.expect("query result decodes");
	assert_eq!(result.num_rows(), 1);
	assert_eq!(result.schema().as_ref(), &typed_table_schema());
	assert_user_visible_schema_has_no_storage_columns(&result);
	let ids = result
		.column_by_name("id")
		.unwrap()
		.as_any()
		.downcast_ref::<Int64Array>()
		.unwrap();
	assert_eq!(ids.value(0), source_ids[1]);

	shutdown_lease_node(node).await;
}

#[tokio::test]
async fn aggregate_dispatch_folds_filtered_table_rows() {
	let dir = bootstrap_tempdir("aggregate-dispatch");
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		44,
		dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();
	let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
	loop {
		if node.raft.current_leader().await == Some(node.local_node.id) {
			break;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for leader"
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
	node.data_server
		.refresh_leases_once(&node.ring, &*node.clock, 60_000)
		.await;

	let descriptor = multirow_typed_table_descriptor();
	node.raft
		.client_write(WendelonRequest::SchemaUpdate(typed_table_schema_update(
			&descriptor,
		)))
		.await
		.unwrap();

	let source_ids = same_multirow_locality_ids(&descriptor, 2);
	let batch = typed_table_batch(source_ids.clone());
	let mut write_meta = typed_table_meta(&descriptor, batch.num_rows());
	write_meta.parent_write_id.sequence_no = 58;
	let (write_resp, _) = node
		.data_server
		.table_write(write_meta, batch)
		.await
		.unwrap();
	assert_eq!(write_resp.write.status, WriteStatus::OK);
	let stored_key = table_row_identity(
		tenant(),
		descriptor.table_object_id,
		&source_ids[0].to_le_bytes(),
	);
	let stored_location = node
		.slab
		.lookup(tenant(), stored_key)
		.expect("compressed multi-row frame should be indexed");
	let stored_frame = node.slab.read(stored_location).await.unwrap();
	assert_eq!(
		crate::storage::ipc::record_batch_compression_policy_from_ipc(&stored_frame).unwrap(),
		RecordBodyCompressionPolicy::Lz4Frame
	);
	assert!(has_storage_synthetic_columns(
		&decode_batch(&stored_frame).unwrap()
	));

	let mut predicate = Vec::new();
	predicate.push(OP_PUSH_COL);
	predicate.extend_from_slice(&0_u32.to_le_bytes());
	predicate.push(OP_PUSH_INT);
	predicate.extend_from_slice(&source_ids[1].to_le_bytes());
	predicate.push(OP_GTE);
	let request = AggregateRequest {
		format_version: AGG_FORMAT_VERSION,
		request_id: AggregateRequestId::new(99),
		tenant_id: tenant(),
		table_object_id: descriptor.table_object_id,
		schema_generation: descriptor.schema_generation,
		logical_schema_fingerprint: descriptor.logical_schema_fingerprint,
		consistency: Consistency::SERIALIZABLE,
		min_hlc: hlc(0, 0, 0),
		as_of_hlc: hlc(0, 0, 0),
		locality_ranges: vec![LocalityRange::full()],
		predicate_program: Some(predicate),
		aggregates: vec![
			AggregateExpression::count_star(),
			AggregateExpression {
				tag: AggregateFunctionTag::SIGNED_SUM,
				column_ordinal: 0,
				parameter_block: Vec::new(),
			},
		],
		budgets: AggregateBudgets {
			max_locations: LocationCount::new(16),
			max_rows: RowCount::new(16),
			max_wall_time_millis: DurationMillis::new(1_000),
			max_partial_state_bytes: ByteCount::new(4_096),
		},
	};
	let encoded = wire::encode(&request).unwrap();
	let mut trailing = node.pool.acquire();
	trailing.as_mut_vec().extend_from_slice(&encoded);
	let frame = HrftFrame {
		header: FrameHeader {
			magic: HRFT_MAGIC,
			version: 1,
			msg_type: MsgType::AGG_REQ.0,
			flags: 0,
			stream_id: 41,
			meta_len: 0,
			arrow_len: 0,
			trailing_len: trailing.len() as u32,
		},
		meta: node.pool.acquire(),
		arrow_payload: None,
		trailing: Some(trailing),
	};

	let resp = DataRingServer::handle_data_frame(
		node.data_server.as_ref(),
		RequestContext {
			remote: Some(node.local_node),
			actor: actor(ActorRole::Operator, None),
		},
		frame,
	)
	.await
	.unwrap()
	.unwrap();
	assert_eq!(resp.header.msg_type, MsgType::AGG_RESP.0);
	let response: AggregateResponse =
		wire::decode(resp.trailing.as_ref().expect("aggregate response trailing")).unwrap();
	assert_eq!(response.status, AggregateStatus::Success);
	assert_eq!(response.request_id, request.request_id);
	assert_eq!(response.partials.len(), 2);
	assert_eq!(response.diagnostics.rows_admitted, 1);
	assert_eq!(response.partials[0].state.as_slice(), &1_u64.to_be_bytes());
	let expected_sum = encode_aggregate_state(
		AggregateFunctionTag::SIGNED_SUM,
		&AggregateState::SignedSum {
			sum: i128::from(source_ids[1]),
			non_null_count: 1,
			overflow: false,
		},
	)
	.unwrap();
	assert_eq!(response.partials[1].state, expected_sum);

	shutdown_lease_node(node).await;
}

async fn aggregate_coordinator_fixture(
	node_id: u32,
	peer_capabilities: crate::network::transport::NodeCapabilitySet,
) -> (
	crate::test_support::TestDir,
	WendelonNode,
	Arc<WendelonDataServer>,
	Arc<LoopbackHandlers>,
	NodeId,
	AggregateRequest,
) {
	let node_dir = bootstrap_tempdir(&format!("aggregate-coordinator-{node_id}"));
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		node_id,
		node_dir.to_path_buf(),
		format!("loopback://{node_id}"),
	))
	.await
	.unwrap();
	let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
	loop {
		if node.raft.current_leader().await == Some(node.local_node.id) {
			break;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for single-node leader"
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
	node.data_server
		.refresh_leases_once(&node.ring, &*node.clock, 60_000)
		.await;
	let descriptor = TableSchemaDescriptor::new(
		TableObjectId::new([0x40; 16]),
		3,
		"analytics",
		"full_scan",
		&typed_table_schema(),
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
		LocalityPolicy::IdentityProjection,
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.unwrap();
	node.raft
		.client_write(WendelonRequest::SchemaUpdate(typed_table_schema_update(
			&descriptor,
		)))
		.await
		.unwrap();

	let pool = node.pool.clone();
	let handlers = LoopbackHandlers::new(pool.clone());
	let loopback_provider: Arc<dyn TransportProvider> = Arc::new(LoopbackProvider {
		handlers: handlers.clone(),
		tier: TransportTier::QuicIoUring,
	});
	let tiers = TransportTierSet::empty().with(TransportTier::QuicIoUring);
	let roles = NodeRoleSet::empty().with(NodeRole::Data);
	let peer = n(node_id + 100);
	let mut peers = std::collections::HashMap::new();
	peers.insert(
		node.local_node,
		PeerCapabilities {
			address: format!("loopback://{node_id}"),
			tiers,
			roles,
			capabilities: crate::network::transport::NodeCapabilitySet::empty()
				.with(NodeCapability::PartialAggregatePushdown),
		},
	);
	peers.insert(
		peer,
		PeerCapabilities {
			address: format!("loopback://{}", node_id + 100),
			tiers,
			roles,
			capabilities: peer_capabilities,
		},
	);
	let epoch = MembershipEpoch { epoch: 1, peers };
	let (_epoch_tx, epoch_rx) = watch::channel(Arc::new(epoch));
	let resolver: Arc<dyn crate::network::transport::CapabilityResolver> = Arc::new(
		crate::network::transport::EpochCapabilityResolver::new(epoch_rx.clone()),
	);
	let mut providers = std::collections::HashMap::new();
	providers.insert(TransportTier::QuicIoUring, loopback_provider);
	let factory = TransportProviderFactory::new(tiers, providers, resolver, epoch_rx);
	let router: Arc<dyn ReplicaRouter> = Arc::new(FixedReplicaRouter::new(vec![node.local_node]));
	let coordinator = Arc::new(
		WendelonDataServer::new(
			node.slab.clone(),
			node.raft.clone(),
			node.log_store.clone(),
			node.state_machine.clone(),
			node.local_node,
			node.clock.clone(),
			router,
			factory,
			pool,
		)
		.unwrap(),
	);
	let request = AggregateRequest {
		format_version: AGG_FORMAT_VERSION,
		request_id: AggregateRequestId::new(u128::from(node_id)),
		tenant_id: tenant(),
		table_object_id: descriptor.table_object_id,
		schema_generation: descriptor.schema_generation,
		logical_schema_fingerprint: descriptor.logical_schema_fingerprint,
		consistency: Consistency::SERIALIZABLE,
		min_hlc: hlc(0, 0, 0),
		as_of_hlc: hlc(0, 0, 0),
		locality_ranges: vec![LocalityRange::full()],
		predicate_program: None,
		aggregates: vec![AggregateExpression::count_star()],
		budgets: AggregateBudgets {
			max_locations: LocationCount::new(1_024),
			max_rows: RowCount::new(1_024),
			max_wall_time_millis: DurationMillis::new(1_000),
			max_partial_state_bytes: ByteCount::new(4_096),
		},
	};
	(node_dir, node, coordinator, handlers, peer, request)
}

fn aggregate_request_frame(pool: &Arc<BufPool>, request: &AggregateRequest) -> HrftFrame {
	let encoded = wire::encode(request).unwrap();
	let mut trailing = pool.acquire();
	trailing.as_mut_vec().extend_from_slice(&encoded);
	HrftFrame {
		header: FrameHeader {
			magic: HRFT_MAGIC,
			version: 1,
			msg_type: MsgType::AGG_REQ.0,
			flags: 0,
			stream_id: 45,
			meta_len: 0,
			arrow_len: 0,
			trailing_len: trailing.len() as u32,
		},
		meta: pool.acquire(),
		arrow_payload: None,
		trailing: Some(trailing),
	}
}

async fn dispatch_aggregate_request(
	server: &Arc<WendelonDataServer>,
	request: &AggregateRequest,
) -> AggregateResponse {
	let frame = aggregate_request_frame(&server.pool, request);
	let resp = DataRingServer::handle_data_frame(
		server.as_ref(),
		RequestContext {
			remote: Some(server.local_node),
			actor: actor(ActorRole::Operator, None),
		},
		frame,
	)
	.await
	.unwrap()
	.unwrap();
	assert_eq!(resp.header.msg_type, MsgType::AGG_RESP.0);
	wire::decode(resp.trailing.as_ref().expect("aggregate response trailing")).unwrap()
}

struct StaticAggregateService {
	pool: Arc<BufPool>,
	count: u64,
	mismatched_crossbind: bool,
}

#[async_trait::async_trait]
impl crate::network::server::HrftService for StaticAggregateService {
	async fn handle_frame(
		&self,
		_context: RequestContext,
		frame: HrftFrame,
	) -> std::io::Result<Option<HrftFrame>> {
		assert_eq!(frame.header.msg_type, MsgType::AGG_REQ.0);
		let request: AggregateRequest =
			wire::decode(frame.trailing.as_ref().expect("AGG_REQ trailing")).unwrap();
		let state = self.count.to_be_bytes().to_vec();
		let partial = crate::schema::AggregatePartial::new(
			AggregateExpressionIndex::new(0),
			AggregateFunctionTag::COUNT_STAR,
			state,
		)
		.unwrap();
		let response = AggregateResponse {
			format_version: request.format_version,
			request_id: if self.mismatched_crossbind {
				AggregateRequestId::new(request.request_id.get().saturating_add(1))
			} else {
				request.request_id
			},
			status: AggregateStatus::Success,
			reject_code: None,
			table_object_id: request.table_object_id,
			schema_generation: request.schema_generation,
			logical_schema_fingerprint: request.logical_schema_fingerprint,
			aggregate_expression_digest: request.expression_digest(),
			as_of_hlc: request.as_of_hlc,
			max_hlc: request.as_of_hlc,
			partials: vec![partial],
			diagnostics: AggregateDiagnostics {
				rows_scanned: self.count,
				rows_admitted: self.count,
				..AggregateDiagnostics::default()
			},
		};
		let trailing = wire::encode_into(&self.pool, &response)?;
		Ok(Some(HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: MsgType::AGG_RESP.0,
				flags: FRAME_FLAG_STREAM_END,
				stream_id: frame.header.stream_id,
				meta_len: 0,
				arrow_len: 0,
				trailing_len: trailing.len() as u32,
			},
			meta: self.pool.acquire(),
			arrow_payload: None,
			trailing: Some(trailing),
		}))
	}
}

#[tokio::test]
async fn aggregate_coordinator_rejects_peer_without_partial_capability() {
	let (_node_dir, node, coordinator, _handlers, _peer, request) =
		aggregate_coordinator_fixture(45, crate::network::transport::NodeCapabilitySet::empty())
			.await;

	let response = dispatch_aggregate_request(&coordinator, &request).await;
	assert_eq!(response.status, AggregateStatus::Rejected);
	assert_eq!(
		response.reject_code,
		Some(AggRejectCode::CapabilityUnavailable)
	);

	shutdown_lease_node(node).await;
}

#[tokio::test]
async fn aggregate_coordinator_rejects_remote_failure() {
	let (_node_dir, node, coordinator, _handlers, _peer, request) = aggregate_coordinator_fixture(
		46,
		crate::network::transport::NodeCapabilitySet::empty()
			.with(NodeCapability::PartialAggregatePushdown),
	)
	.await;

	let response = dispatch_aggregate_request(&coordinator, &request).await;
	assert_eq!(response.status, AggregateStatus::Rejected);
	assert_eq!(response.reject_code, Some(AggRejectCode::DeadlineExceeded));

	shutdown_lease_node(node).await;
}

#[tokio::test]
async fn aggregate_coordinator_merges_peer_partial_response() {
	let (_node_dir, node, coordinator, handlers, peer, request) = aggregate_coordinator_fixture(
		47,
		crate::network::transport::NodeCapabilitySet::empty()
			.with(NodeCapability::PartialAggregatePushdown),
	)
	.await;
	handlers.register(
		peer,
		Arc::new(StaticAggregateService {
			pool: coordinator.pool.clone(),
			count: 2,
			mismatched_crossbind: false,
		}),
	);

	let response = dispatch_aggregate_request(&coordinator, &request).await;
	assert_eq!(response.status, AggregateStatus::Success);
	assert_eq!(response.partials.len(), 1);
	assert_eq!(response.partials[0].state.as_slice(), &2_u64.to_be_bytes());
	assert_eq!(response.diagnostics.rows_admitted, 2);

	shutdown_lease_node(node).await;
}

#[tokio::test]
async fn aggregate_coordinator_rejects_peer_crossbind_mismatch() {
	let (_node_dir, node, coordinator, handlers, peer, request) = aggregate_coordinator_fixture(
		48,
		crate::network::transport::NodeCapabilitySet::empty()
			.with(NodeCapability::PartialAggregatePushdown),
	)
	.await;
	handlers.register(
		peer,
		Arc::new(StaticAggregateService {
			pool: coordinator.pool.clone(),
			count: 2,
			mismatched_crossbind: true,
		}),
	);

	let response = dispatch_aggregate_request(&coordinator, &request).await;
	assert_eq!(response.status, AggregateStatus::Rejected);
	assert_eq!(
		response.reject_code,
		Some(AggRejectCode::PartialCrossbindFailed)
	);

	shutdown_lease_node(node).await;
}

#[test]
fn aggregate_reject_mapping_preserves_retention_expired() {
	assert_eq!(
		aggregate_reject_from_read_status(ReadStatus::RETENTION_EXPIRED),
		AggRejectCode::RetentionExpired
	);
	assert_eq!(
		aggregate_reject_to_read_status(AggRejectCode::RetentionExpired),
		ReadStatus::RETENTION_EXPIRED
	);
}

#[tokio::test]
async fn aggregate_retention_floor_reject_uses_retention_expired_code() {
	let (_mgr_dir, mgr) = make_manager().await;
	let descriptor = append_only_table_descriptor();
	let node_dir = bootstrap_tempdir("aggregate-retention-expired-node");
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		52,
		node_dir.to_path_buf(),
		"loopback://52".into(),
	))
	.await
	.unwrap();
	let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
	loop {
		if node.raft.current_leader().await == Some(node.local_node.id) {
			break;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for single-node leader"
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
	node.raft
		.client_write(WendelonRequest::SchemaUpdate(typed_table_schema_update(
			&descriptor,
		)))
		.await
		.unwrap();
	node.raft
		.client_write(WendelonRequest::SetTableRetentionPolicy(
			crate::consensus::SetTableRetentionPolicy {
				tenant_id: tenant(),
				table_object_id: descriptor.table_object_id,
				expected_policy_generation: PolicyGeneration::ZERO,
				new_policy: TableRetentionPolicy::FixedWindow(
					RetentionWindowMillis::try_new(60_000).unwrap(),
				),
				updated_at_hlc: hlc(1, 0, 1),
			},
		))
		.await
		.unwrap();

	let tiers = TransportTierSet::empty();
	let roles = NodeRoleSet::empty().with(NodeRole::Data);
	let mut peers = std::collections::HashMap::new();
	peers.insert(
		node.local_node,
		PeerCapabilities {
			address: "loopback://52".into(),
			tiers,
			roles,
			capabilities: crate::network::transport::NodeCapabilitySet::empty()
				.with(NodeCapability::PartialAggregatePushdown),
		},
	);
	let epoch = MembershipEpoch { epoch: 1, peers };
	let (_epoch_tx, epoch_rx) = watch::channel(Arc::new(epoch));
	let resolver: Arc<dyn crate::network::transport::CapabilityResolver> = Arc::new(
		crate::network::transport::EpochCapabilityResolver::new(epoch_rx.clone()),
	);
	let factory =
		TransportProviderFactory::new(tiers, std::collections::HashMap::new(), resolver, epoch_rx);
	let server = WendelonDataServer::new(
		mgr,
		node.raft.clone(),
		node.log_store.clone(),
		node.state_machine.clone(),
		node.local_node,
		node.clock.clone(),
		Arc::new(FixedReplicaRouter::new(vec![node.local_node])),
		factory,
		node.pool.clone(),
	)
	.unwrap();
	let request = AggregateRequest {
		format_version: AGG_FORMAT_VERSION,
		request_id: AggregateRequestId::new(493),
		tenant_id: tenant(),
		table_object_id: descriptor.table_object_id,
		schema_generation: descriptor.schema_generation,
		logical_schema_fingerprint: descriptor.logical_schema_fingerprint,
		consistency: Consistency::SERIALIZABLE,
		min_hlc: hlc(1, 0, 1),
		as_of_hlc: hlc(1, 0, 1),
		locality_ranges: vec![LocalityRange::full()],
		predicate_program: None,
		aggregates: vec![AggregateExpression::count_star()],
		budgets: AggregateBudgets {
			max_locations: LocationCount::new(16),
			max_rows: RowCount::new(16),
			max_wall_time_millis: DurationMillis::new(1_000),
			max_partial_state_bytes: ByteCount::new(4_096),
		},
	};
	let floor_rejects_before = metrics()
		.storage_table_retention_read_floor_rejects_total
		.get();

	let response = server.execute_aggregate_local(&request).await.unwrap();

	assert_eq!(response.status, AggregateStatus::Rejected);
	assert_eq!(response.reject_code, Some(AggRejectCode::RetentionExpired));
	let query_meta = aggregate_response_as_query_meta(
		&request,
		&response,
		node.local_node,
		node.state_machine
			.current_admission_snapshot()
			.current_epoch,
	);
	assert_eq!(query_meta.status, ReadStatus::RETENTION_EXPIRED);
	assert!(
		metrics()
			.storage_table_retention_read_floor_rejects_total
			.get() > floor_rejects_before,
		"aggregate retention admission should increment the read-floor reject metric"
	);

	shutdown_lease_node(node).await;
}

#[test]
fn aggregate_csb_response_marks_truncated_min_partial() {
	use crate::schema::aggregate::AGG_MIN_MAX_PREFIX_BYTES;
	use crate::storage::stats::{
		ARROW_STATS_TAG_LARGE_UTF8, COLUMN_STAT_HAS_MIN, COLUMN_STAT_HAS_NON_NULL_COUNT,
		COLUMN_STAT_HAS_NULL_COUNT, COLUMN_STAT_MIN_TRUNCATED, ColumnStatsBlockEntry,
		ColumnStatsBlockHeader, ParsedColumnStatsBlock, encode_length_prefixed_bytes,
	};

	let descriptor = typed_table_descriptor();
	let prefix = vec![b'a'; AGG_MIN_MAX_PREFIX_BYTES];
	let mut payload = Vec::new();
	encode_length_prefixed_bytes(&mut payload, &prefix).unwrap();
	payload.extend_from_slice(&0_u64.to_be_bytes());
	payload.extend_from_slice(&1_u64.to_be_bytes());
	let stats = ParsedColumnStatsBlock {
		header: ColumnStatsBlockHeader {
			table_object_id: descriptor.table_object_id,
			schema_generation: descriptor.schema_generation,
			logical_schema_fingerprint: descriptor.logical_schema_fingerprint,
			row_count: 1,
			column_block_count: 1,
		},
		columns: vec![ColumnStatsBlockEntry {
			column_ordinal: 1,
			arrow_type_tag: ARROW_STATS_TAG_LARGE_UTF8,
			stat_flags: COLUMN_STAT_HAS_MIN
				| COLUMN_STAT_MIN_TRUNCATED
				| COLUMN_STAT_HAS_NULL_COUNT
				| COLUMN_STAT_HAS_NON_NULL_COUNT,
			payload,
		}],
	};
	let request = AggregateRequest {
		format_version: AGG_FORMAT_VERSION,
		request_id: AggregateRequestId::new(491),
		tenant_id: tenant(),
		table_object_id: descriptor.table_object_id,
		schema_generation: descriptor.schema_generation,
		logical_schema_fingerprint: descriptor.logical_schema_fingerprint,
		consistency: Consistency::SERIALIZABLE,
		min_hlc: hlc(1, 0, 1),
		as_of_hlc: hlc(99, 0, 1),
		locality_ranges: vec![LocalityRange::full()],
		predicate_program: None,
		aggregates: vec![AggregateExpression {
			tag: AggregateFunctionTag::MIN,
			column_ordinal: 1,
			parameter_block: Vec::new(),
		}],
		budgets: AggregateBudgets {
			max_locations: LocationCount::new(1),
			max_rows: RowCount::new(1),
			max_wall_time_millis: DurationMillis::new(1_000),
			max_partial_state_bytes: ByteCount::new(4_096),
		},
	};

	let response = aggregate_csb_response_for_stats(&request, &descriptor, &stats)
		.unwrap()
		.unwrap();
	assert_eq!(response.status, AggregateStatus::Partial);
	assert_eq!(
		&response.partials[0].state[4..4 + prefix.len()],
		prefix.as_slice()
	);
	assert_eq!(response.partials[0].state.last(), Some(&1));
	assert_eq!(response.diagnostics.slabs_answered_from_csb, 1);
}

#[test]
fn aggregate_csb_logical_slot_cliff_is_observable() {
	use crate::storage::stats::{ColumnStatsBlockHeader, ParsedColumnStatsBlock};

	let mut descriptor = typed_table_descriptor();
	descriptor.table_object_id = TableObjectId::new([0xEC; 16]);
	descriptor.refresh_descriptor_fingerprints().unwrap();
	let request = aggregate_count_request(&descriptor, 492, hlc(99, 0, 1));
	let mismatch_before = metrics().storage_csb_merge_logical_fingerprint_mismatch_total_for_test(
		request.tenant_id,
		request.table_object_id,
	);
	let cliff_before = metrics()
		.storage_csb_pre_adr_051_cliff_total_for_test(request.tenant_id, request.table_object_id);
	let stats = ParsedColumnStatsBlock {
		header: ColumnStatsBlockHeader {
			table_object_id: descriptor.table_object_id,
			schema_generation: descriptor.schema_generation,
			logical_schema_fingerprint: crate::schema::LogicalSchemaFingerprint::from_bytes(
				[0xEE; 32],
			),
			row_count: 2,
			column_block_count: 0,
		},
		columns: Vec::new(),
	};

	let err = aggregate_csb_response_for_stats(&request, &descriptor, &stats).unwrap_err();

	assert_eq!(err, AggRejectCode::PartialCrossbindFailed);
	assert_eq!(
		metrics().storage_csb_merge_logical_fingerprint_mismatch_total_for_test(
			request.tenant_id,
			request.table_object_id,
		),
		mismatch_before + 1
	);
	assert_eq!(
		metrics().storage_csb_pre_adr_051_cliff_total_for_test(
			request.tenant_id,
			request.table_object_id,
		),
		cliff_before + 1
	);
}

#[tokio::test]
async fn aggregate_csb_direct_answer_handles_count_min_and_max() {
	let l2 = Arc::new(MockObjectStore::default());
	let (_mgr_dir, mgr, descriptor) =
		tier_identity_typed_table_batch_with_stats(l2, vec![1, 2], "agg-csb.wnd").await;
	let node_dir = bootstrap_tempdir("aggregate-csb-node");
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		49,
		node_dir.to_path_buf(),
		"loopback://49".into(),
	))
	.await
	.unwrap();
	let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
	loop {
		if node.raft.current_leader().await == Some(node.local_node.id) {
			break;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for single-node leader"
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
	node.raft
		.client_write(WendelonRequest::SchemaUpdate(typed_table_schema_update(
			&descriptor,
		)))
		.await
		.unwrap();

	let tiers = TransportTierSet::empty();
	let roles = NodeRoleSet::empty().with(NodeRole::Data);
	let mut peers = std::collections::HashMap::new();
	peers.insert(
		node.local_node,
		PeerCapabilities {
			address: "loopback://49".into(),
			tiers,
			roles,
			capabilities: crate::network::transport::NodeCapabilitySet::empty()
				.with(NodeCapability::PartialAggregatePushdown),
		},
	);
	let epoch = MembershipEpoch { epoch: 1, peers };
	let (_epoch_tx, epoch_rx) = watch::channel(Arc::new(epoch));
	let resolver: Arc<dyn crate::network::transport::CapabilityResolver> = Arc::new(
		crate::network::transport::EpochCapabilityResolver::new(epoch_rx.clone()),
	);
	let factory =
		TransportProviderFactory::new(tiers, std::collections::HashMap::new(), resolver, epoch_rx);
	let server = WendelonDataServer::new(
		mgr,
		node.raft.clone(),
		node.log_store.clone(),
		node.state_machine.clone(),
		node.local_node,
		node.clock.clone(),
		Arc::new(FixedReplicaRouter::new(vec![node.local_node])),
		factory,
		node.pool.clone(),
	)
	.unwrap();
	let request = AggregateRequest {
		format_version: AGG_FORMAT_VERSION,
		request_id: AggregateRequestId::new(490),
		tenant_id: tenant(),
		table_object_id: descriptor.table_object_id,
		schema_generation: descriptor.schema_generation,
		logical_schema_fingerprint: descriptor.logical_schema_fingerprint,
		consistency: Consistency::SERIALIZABLE,
		min_hlc: hlc(1, 0, 1),
		as_of_hlc: hlc(99, 0, 1),
		locality_ranges: vec![LocalityRange::full()],
		predicate_program: None,
		aggregates: vec![
			AggregateExpression::count_star(),
			AggregateExpression {
				tag: AggregateFunctionTag::COUNT_NON_NULL,
				column_ordinal: 1,
				parameter_block: Vec::new(),
			},
			AggregateExpression {
				tag: AggregateFunctionTag::MIN,
				column_ordinal: 0,
				parameter_block: Vec::new(),
			},
			AggregateExpression {
				tag: AggregateFunctionTag::MAX,
				column_ordinal: 0,
				parameter_block: Vec::new(),
			},
		],
		budgets: AggregateBudgets {
			max_locations: LocationCount::new(16),
			max_rows: RowCount::new(16),
			max_wall_time_millis: DurationMillis::new(1_000),
			max_partial_state_bytes: ByteCount::new(4_096),
		},
	};

	let metric_before = metrics().agg_csb_short_circuit_total.get();
	let response = server.execute_aggregate_local(&request).await.unwrap();
	assert_eq!(response.status, AggregateStatus::Success);
	assert_eq!(response.diagnostics.slabs_answered_from_csb, 1);
	assert_eq!(response.diagnostics.rows_admitted, 2);
	assert!(
		metrics().agg_csb_short_circuit_total.get() > metric_before,
		"CSB direct answer should increment the global counter at least once"
	);
	assert_eq!(response.partials[0].state.as_slice(), &2_u64.to_be_bytes());
	assert_eq!(response.partials[1].state.as_slice(), &2_u64.to_be_bytes());
	let min = encode_signed_stat_value(1, 8).unwrap();
	let max = encode_signed_stat_value(2, 8).unwrap();
	assert_eq!(&response.partials[2].state[4..12], min.as_slice());
	assert_eq!(&response.partials[3].state[4..12], max.as_slice());

	shutdown_lease_node(node).await;
}

#[tokio::test]
async fn aggregate_csb_retention_floor_below_slab_allows_direct_answer() {
	let l2 = Arc::new(MockObjectStore::default());
	let (_mgr_dir, mgr, descriptor) =
		tier_identity_typed_table_batch_with_stats(l2, vec![1, 2], "agg-csb-retention-ok.wnd")
			.await;
	let (_node_dir, node, server) = aggregate_csb_test_server(
		mgr,
		&descriptor,
		53,
		"aggregate-csb-retention-ok-node",
		Arc::new(FixedClock { now_ms: 1_000 }),
		MvccRetentionPolicy::FixedWindow(Duration::from_millis(1_000)),
	)
	.await;
	let request = aggregate_count_request(&descriptor, 494, hlc(99, 0, 1));

	let response = server.execute_aggregate_local(&request).await.unwrap();

	assert_eq!(response.status, AggregateStatus::Success);
	assert_eq!(response.diagnostics.slabs_answered_from_csb, 1);
	assert_eq!(response.diagnostics.rows_admitted, 2);
	assert_eq!(count_star_partial_value(&response), 2);

	shutdown_lease_node(node).await;
}

#[tokio::test]
async fn aggregate_csb_retention_floor_above_slab_returns_zero_contribution() {
	let l2 = Arc::new(MockObjectStore::default());
	let (_mgr_dir, mgr, descriptor) =
		tier_identity_typed_table_batch_with_stats(l2, vec![1, 2], "agg-csb-retention-zero.wnd")
			.await;
	let (_node_dir, node, server) = aggregate_csb_test_server(
		mgr,
		&descriptor,
		54,
		"aggregate-csb-retention-zero-node",
		Arc::new(FixedClock { now_ms: 1_000 }),
		MvccRetentionPolicy::FixedWindow(Duration::from_millis(500)),
	)
	.await;
	let request = aggregate_count_request(&descriptor, 495, hlc(900, 0, 1));

	let response = server.execute_aggregate_local(&request).await.unwrap();

	assert_eq!(response.status, AggregateStatus::Success);
	assert_eq!(response.diagnostics.slabs_answered_from_csb, 0);
	assert_eq!(response.diagnostics.slabs_pruned_by_csb, 1);
	assert_eq!(response.diagnostics.rows_admitted, 0);
	assert_eq!(response.diagnostics.rows_filtered_out, 2);
	assert_eq!(count_star_partial_value(&response), 0);

	shutdown_lease_node(node).await;
}

#[tokio::test]
async fn aggregate_csb_retention_floor_straddling_slab_falls_back_to_row_filter() {
	let l2 = Arc::new(MockObjectStore::default());
	let (_mgr_dir, mgr, descriptor) = tier_identity_typed_table_batch_with_stats(
		l2,
		vec![1, 2],
		"agg-csb-retention-straddle.wnd",
	)
	.await;
	let (_node_dir, node, server) = aggregate_csb_test_server(
		mgr,
		&descriptor,
		55,
		"aggregate-csb-retention-straddle-node",
		Arc::new(FixedClock { now_ms: 1_011 }),
		MvccRetentionPolicy::FixedWindow(Duration::from_millis(1_000)),
	)
	.await;
	let request = aggregate_count_request(&descriptor, 496, hlc(99, 0, 1));
	let prepared = server
		.prepare_aggregate_execution_context_with_migration(&request)
		.await
		.unwrap()
		.unwrap();

	assert!(
		server
			.try_aggregate_csb_direct_answer(&request, &prepared)
			.await
			.unwrap()
			.is_none(),
		"slabs straddling the retention floor must fall back to row filtering"
	);
	let response = server.execute_aggregate_local(&request).await.unwrap();
	assert_eq!(response.status, AggregateStatus::Success);
	assert_eq!(response.diagnostics.slabs_answered_from_csb, 0);
	assert_eq!(response.diagnostics.rows_admitted, 1);
	assert_eq!(count_star_partial_value(&response), 1);

	shutdown_lease_node(node).await;
}

#[tokio::test]
async fn aggregate_csb_direct_answer_falls_through_when_sm_tombstone_affects_slab() {
	let l2 = Arc::new(MockObjectStore::default());
	let (_mgr_dir, mgr, descriptor) =
		tier_identity_typed_table_batch_with_stats(l2, vec![1, 2], "agg-csb-tombstone.wnd").await;
	let node_dir = bootstrap_tempdir("aggregate-csb-tombstone-node");
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		50,
		node_dir.to_path_buf(),
		"loopback://50".into(),
	))
	.await
	.unwrap();
	let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
	loop {
		if node.raft.current_leader().await == Some(node.local_node.id) {
			break;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for single-node leader"
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
	node.raft
		.client_write(WendelonRequest::SchemaUpdate(typed_table_schema_update(
			&descriptor,
		)))
		.await
		.unwrap();
	node.state_machine
		.seed_migrated_range_tombstone_for_test(crate::consensus::MigratedRangeTombstone {
			migration_id: 77,
			range_start: 0,
			range_end: u64::MAX,
			to_epoch: 2,
			completed_by: n(50),
			completed_at_hlc: hlc(50, 0, 1),
			migration_hlc_floor: hlc(0, 0, 0),
		})
		.await;

	let tiers = TransportTierSet::empty();
	let roles = NodeRoleSet::empty().with(NodeRole::Data);
	let mut peers = std::collections::HashMap::new();
	peers.insert(
		node.local_node,
		PeerCapabilities {
			address: "loopback://50".into(),
			tiers,
			roles,
			capabilities: crate::network::transport::NodeCapabilitySet::empty()
				.with(NodeCapability::PartialAggregatePushdown),
		},
	);
	let epoch = MembershipEpoch { epoch: 1, peers };
	let (_epoch_tx, epoch_rx) = watch::channel(Arc::new(epoch));
	let resolver: Arc<dyn crate::network::transport::CapabilityResolver> = Arc::new(
		crate::network::transport::EpochCapabilityResolver::new(epoch_rx.clone()),
	);
	let factory =
		TransportProviderFactory::new(tiers, std::collections::HashMap::new(), resolver, epoch_rx);
	let server = WendelonDataServer::new(
		mgr,
		node.raft.clone(),
		node.log_store.clone(),
		node.state_machine.clone(),
		node.local_node,
		node.clock.clone(),
		Arc::new(FixedReplicaRouter::new(vec![node.local_node])),
		factory,
		node.pool.clone(),
	)
	.unwrap();
	let request = AggregateRequest {
		format_version: AGG_FORMAT_VERSION,
		request_id: AggregateRequestId::new(491),
		tenant_id: tenant(),
		table_object_id: descriptor.table_object_id,
		schema_generation: descriptor.schema_generation,
		logical_schema_fingerprint: descriptor.logical_schema_fingerprint,
		consistency: Consistency::SERIALIZABLE,
		min_hlc: hlc(1, 0, 1),
		as_of_hlc: hlc(99, 0, 1),
		locality_ranges: vec![LocalityRange::full()],
		predicate_program: None,
		aggregates: vec![AggregateExpression::count_star()],
		budgets: AggregateBudgets {
			max_locations: LocationCount::new(16),
			max_rows: RowCount::new(16),
			max_wall_time_millis: DurationMillis::new(1_000),
			max_partial_state_bytes: ByteCount::new(4_096),
		},
	};

	let prepared = server
		.prepare_aggregate_execution_context_with_migration(&request)
		.await
		.unwrap()
		.unwrap();
	assert!(
		server
			.try_aggregate_csb_direct_answer(&request, &prepared)
			.await
			.unwrap()
			.is_none(),
		"state-machine tombstones must fence CSB direct answer even when the pointer says None"
	);

	shutdown_lease_node(node).await;
}

#[tokio::test]
async fn aggregate_csb_direct_answer_falls_through_when_tiered_slab_has_superseded_row() {
	let l2 = Arc::new(MockObjectStore::default());
	let (_mgr_dir, mgr, descriptor) =
		tier_identity_typed_table_batch_with_stats(l2.clone(), vec![1, 2], "agg-csb-old.wnd").await;

	let replacement = typed_table_batch(vec![1]);
	let mut write_meta: TableWriteRequestMeta = bytemuck::Zeroable::zeroed();
	write_meta.tenant_id = tenant();
	write_meta.table_object_id = descriptor.table_object_id;
	write_meta.schema_generation = descriptor.schema_generation;
	write_meta.row_count = encode_table_write_row_count(replacement.num_rows()).unwrap();
	let admission = validate_table_write_request_batch(
		&write_meta,
		&replacement,
		&descriptor,
		WriterNodeId::ZERO,
		write_meta.client_hlc,
	)
	.unwrap();
	let frame = encode_batch(&replacement).unwrap();
	mgr.append_durable_with_locality(
		tenant(),
		admission.key_identities[0],
		admission.localities[0],
		hlc(50, 0, 1),
		&frame,
	)
	.await
	.unwrap();
	let replacement_slab_id = mgr.force_seal_active_for_test().await;
	let raw = mgr.read_sealed_raw(replacement_slab_id).await.unwrap();
	let mut catalog = TableSchemaCatalog::default();
	catalog.upsert(tenant(), descriptor.clone()).unwrap();
	let encrypted = mgr
		.encrypt_tiered_object_blob_with_table_stats(replacement_slab_id, &raw, &catalog)
		.unwrap();
	let locator = encrypted.stats_locator.expect("stats locator");
	let l2_uri = l2.put("agg-csb-new.wnd", &encrypted.bytes).await.unwrap();
	mgr.apply_tier_slab(TieredSlabPointer {
		slab_id: replacement_slab_id,
		l2_uri,
		payload_hash: payload_digest(&encrypted.bytes),
		is_encrypted: true,
		tenant_id: Some(encrypted.tenant_id),
		tenant_kek_id: Some(encrypted.tenant_kek_id),
		stats_format_version: Some(locator.stats_format_version),
		stats_block_len: Some(locator.stats_block_len),
		stats_block_hash128: Some(locator.stats_block_hash128),
		tombstone_metadata: TombstoneMetadata::None,
		supersession_metadata: SupersessionMetadata::None,
	})
	.await
	.unwrap();

	let node_dir = bootstrap_tempdir("aggregate-csb-supersession-node");
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		51,
		node_dir.to_path_buf(),
		"loopback://51".into(),
	))
	.await
	.unwrap();
	let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
	loop {
		if node.raft.current_leader().await == Some(node.local_node.id) {
			break;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for single-node leader"
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
	node.raft
		.client_write(WendelonRequest::SchemaUpdate(typed_table_schema_update(
			&descriptor,
		)))
		.await
		.unwrap();

	let tiers = TransportTierSet::empty();
	let roles = NodeRoleSet::empty().with(NodeRole::Data);
	let mut peers = std::collections::HashMap::new();
	peers.insert(
		node.local_node,
		PeerCapabilities {
			address: "loopback://51".into(),
			tiers,
			roles,
			capabilities: crate::network::transport::NodeCapabilitySet::empty()
				.with(NodeCapability::PartialAggregatePushdown),
		},
	);
	let epoch = MembershipEpoch { epoch: 1, peers };
	let (_epoch_tx, epoch_rx) = watch::channel(Arc::new(epoch));
	let resolver: Arc<dyn crate::network::transport::CapabilityResolver> = Arc::new(
		crate::network::transport::EpochCapabilityResolver::new(epoch_rx.clone()),
	);
	let factory =
		TransportProviderFactory::new(tiers, std::collections::HashMap::new(), resolver, epoch_rx);
	let server = WendelonDataServer::new(
		mgr,
		node.raft.clone(),
		node.log_store.clone(),
		node.state_machine.clone(),
		node.local_node,
		node.clock.clone(),
		Arc::new(FixedReplicaRouter::new(vec![node.local_node])),
		factory,
		node.pool.clone(),
	)
	.unwrap();
	let request = AggregateRequest {
		format_version: AGG_FORMAT_VERSION,
		request_id: AggregateRequestId::new(492),
		tenant_id: tenant(),
		table_object_id: descriptor.table_object_id,
		schema_generation: descriptor.schema_generation,
		logical_schema_fingerprint: descriptor.logical_schema_fingerprint,
		consistency: Consistency::SERIALIZABLE,
		min_hlc: hlc(1, 0, 1),
		as_of_hlc: hlc(99, 0, 1),
		locality_ranges: vec![LocalityRange::full()],
		predicate_program: None,
		aggregates: vec![AggregateExpression::count_star()],
		budgets: AggregateBudgets {
			max_locations: LocationCount::new(16),
			max_rows: RowCount::new(16),
			max_wall_time_millis: DurationMillis::new(1_000),
			max_partial_state_bytes: ByteCount::new(4_096),
		},
	};

	let prepared = server
		.prepare_aggregate_execution_context_with_migration(&request)
		.await
		.unwrap()
		.unwrap();
	assert!(
		server
			.try_aggregate_csb_direct_answer(&request, &prepared)
			.await
			.unwrap()
			.is_none(),
		"superseded rows in a tiered slab must fence CSB direct answer"
	);

	shutdown_lease_node(node).await;
}

#[tokio::test]
async fn table_write_fans_out_child_groups_to_distinct_replica_sets() {
	let leader_node_dir = bootstrap_tempdir("table-fanout-leader");
	let leader_node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		40,
		leader_node_dir.to_path_buf(),
		"loopback://40".into(),
	))
	.await
	.unwrap();
	let durable_a_node_dir = bootstrap_tempdir("table-fanout-a");
	let durable_a_node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		41,
		durable_a_node_dir.to_path_buf(),
		"loopback://41".into(),
	))
	.await
	.unwrap();
	let durable_b_node_dir = bootstrap_tempdir("table-fanout-b");
	let durable_b_node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		42,
		durable_b_node_dir.to_path_buf(),
		"loopback://42".into(),
	))
	.await
	.unwrap();
	let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
	loop {
		if leader_node.raft.current_leader().await == Some(leader_node.local_node.id)
			&& durable_a_node.raft.current_leader().await == Some(durable_a_node.local_node.id)
			&& durable_b_node.raft.current_leader().await == Some(durable_b_node.local_node.id)
		{
			break;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for single-node leaders"
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}

	let descriptor = typed_table_descriptor();
	leader_node
		.raft
		.client_write(WendelonRequest::SchemaUpdate(typed_table_schema_update(
			&descriptor,
		)))
		.await
		.unwrap();
	let batch = typed_table_batch(vec![7, 8]);
	let mut meta = typed_table_meta(&descriptor, batch.num_rows());
	meta.parent_write_id.sequence_no = 58;
	let expected = bifurcate_table_write(
		&meta,
		&batch,
		&descriptor,
		WriterNodeId::ZERO,
		meta.client_hlc,
	)
	.unwrap();
	assert_eq!(expected.children.len(), 2);

	let pool = leader_node.pool.clone();
	let handlers = LoopbackHandlers::new(pool.clone());
	let loopback_provider: Arc<dyn TransportProvider> = Arc::new(LoopbackProvider {
		handlers: handlers.clone(),
		tier: TransportTier::QuicIoUring,
	});
	let tiers = TransportTierSet::empty().with(TransportTier::QuicIoUring);
	let roles = NodeRoleSet::empty().with(NodeRole::Data);
	let mut peers = StdHashMap::new();
	for (node_id, address) in [
		(leader_node.local_node, "loopback://40"),
		(durable_a_node.local_node, "loopback://41"),
		(durable_b_node.local_node, "loopback://42"),
	] {
		peers.insert(
			node_id,
			PeerCapabilities {
				address: address.into(),
				tiers,
				roles,
				capabilities: crate::network::transport::NodeCapabilitySet::empty(),
			},
		);
	}
	let epoch = MembershipEpoch { epoch: 1, peers };
	let (_epoch_tx, epoch_rx) = watch::channel(Arc::new(epoch));
	let resolver: Arc<dyn crate::network::transport::CapabilityResolver> = Arc::new(
		crate::network::transport::EpochCapabilityResolver::new(epoch_rx.clone()),
	);
	let build_factory = || {
		let mut providers: StdHashMap<TransportTier, Arc<dyn TransportProvider>> =
			StdHashMap::new();
		providers.insert(TransportTier::QuicIoUring, loopback_provider.clone());
		TransportProviderFactory::new(tiers, providers, resolver.clone(), epoch_rx.clone())
	};
	let router: Arc<dyn ReplicaRouter> = Arc::new(
		FixedReplicaRouter::new(vec![leader_node.local_node])
			.with_route(
				expected.children[0].locality,
				vec![leader_node.local_node, durable_a_node.local_node],
			)
			.with_route(
				expected.children[1].locality,
				vec![durable_b_node.local_node],
			),
	);

	let leader = Arc::new(
		WendelonDataServer::new(
			leader_node.slab.clone(),
			leader_node.raft.clone(),
			leader_node.log_store.clone(),
			leader_node.state_machine.clone(),
			leader_node.local_node,
			leader_node.clock.clone(),
			router.clone(),
			build_factory(),
			pool.clone(),
		)
		.unwrap(),
	);
	let durable_a = Arc::new(
		WendelonDataServer::new(
			durable_a_node.slab.clone(),
			durable_a_node.raft.clone(),
			durable_a_node.log_store.clone(),
			durable_a_node.state_machine.clone(),
			durable_a_node.local_node,
			durable_a_node.clock.clone(),
			router.clone(),
			build_factory(),
			durable_a_node.pool.clone(),
		)
		.unwrap(),
	);
	let durable_b = Arc::new(
		WendelonDataServer::new(
			durable_b_node.slab.clone(),
			durable_b_node.raft.clone(),
			durable_b_node.log_store.clone(),
			durable_b_node.state_machine.clone(),
			durable_b_node.local_node,
			durable_b_node.clock.clone(),
			router,
			build_factory(),
			durable_b_node.pool.clone(),
		)
		.unwrap(),
	);
	handlers.register(
		durable_a_node.local_node,
		Arc::new(
			MsgRouter::builder()
				.with_data_ring(durable_a.clone())
				.build(),
		),
	);
	handlers.register(
		durable_b_node.local_node,
		Arc::new(
			MsgRouter::builder()
				.with_data_ring(durable_b.clone())
				.build(),
		),
	);

	let (resp, _) = leader.table_write(meta, batch).await.unwrap();
	assert_eq!(resp.write.status, WriteStatus::OK, "got resp={resp:?}");

	assert!(
		leader_node
			.slab
			.lookup(tenant(), expected.children[0].key_identity)
			.is_some(),
		"leader should store the first child group where it is a replica"
	);
	assert!(
		durable_a_node
			.slab
			.lookup(tenant(), expected.children[0].key_identity)
			.is_some(),
		"durable_a should receive the first child group"
	);
	assert!(
		durable_b_node
			.slab
			.lookup(tenant(), expected.children[1].key_identity)
			.is_some(),
		"durable_b should receive the remote-only child group"
	);
	assert!(
		leader_node
			.slab
			.lookup(tenant(), expected.children[1].key_identity)
			.is_none(),
		"leader must not locally append a child for a replica set it is not in"
	);

	shutdown_lease_node(leader_node).await;
	shutdown_lease_node(durable_a_node).await;
	shutdown_lease_node(durable_b_node).await;
}

#[test]
fn durable_quorum_tracker_requires_local_leg_when_leader_is_replica() {
	let mut tracker = DurableQuorumTracker::new(2, true);
	tracker.note_remote_ack(ReplicaAckLevel::DURABLE);
	tracker.note_remote_ack(ReplicaAckLevel::DURABLE);
	assert_eq!(tracker.durable_ack_count(), 2);
	assert!(tracker.quorum_reached());
	assert!(
		!tracker.is_satisfied(),
		"pure-remote quorum must not satisfy the write when the leader is a replica"
	);

	tracker.note_local_durable();
	assert!(tracker.is_satisfied());
}

#[test]
fn durable_quorum_tracker_allows_remote_quorum_when_local_leg_not_required() {
	let mut tracker = DurableQuorumTracker::new(2, false);
	tracker.note_remote_ack(ReplicaAckLevel::APPEND_ONLY);
	tracker.note_remote_ack(ReplicaAckLevel::DURABLE);
	assert_eq!(tracker.durable_ack_count(), 1);
	assert!(!tracker.is_satisfied());

	tracker.note_remote_ack(ReplicaAckLevel::DURABLE);
	assert_eq!(tracker.durable_ack_count(), 2);
	assert!(tracker.is_satisfied());
}

#[tokio::test]
async fn duplicate_write_replays_finalization_marker_and_emits_one_audit_record() {
	let dir = bootstrap_tempdir("duplicate-finalization");
	let audit_path = dir.join("audit.ndjson");
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		16,
		dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();
	let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
	loop {
		if node.raft.current_leader().await == Some(node.local_node.id) {
			break;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for leader"
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
	node.data_server
		.refresh_leases_once(&node.ring, &*node.clock, 60_000)
		.await;

	let mut meta = simple_write_meta(1);
	meta.client_hlc = hlc(1, 0, 0);
	meta.await_finalization = 1;

	let first = node
		.data_server
		.write(meta, single_kv_batch())
		.await
		.unwrap();
	let duplicate = node
		.data_server
		.write(meta, single_kv_batch())
		.await
		.unwrap();

	assert_eq!(first.0.status, WriteStatus::OK, "got resp={:?}", first.0);
	assert_eq!(
		duplicate.0.status,
		WriteStatus::DUPLICATE,
		"got resp={:?}",
		duplicate.0
	);
	assert_eq!(first.1, duplicate.1);

	let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
	loop {
		let audit = std::fs::read_to_string(&audit_path).unwrap_or_default();
		let finalized_user_write_count = audit
			.lines()
			.filter(|line| {
				line.contains("\"event_type\":\"data.write_finalized\"")
					&& line.contains("\"record_count\":1")
			})
			.count();
		if finalized_user_write_count == 1 {
			break;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for write audit; current log: {}",
			audit
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
	shutdown_lease_node(node).await;
}

#[tokio::test]
async fn duplicate_write_replays_terminal_failure_without_recovery() {
	let dir = bootstrap_tempdir("duplicate-terminal-failure");
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		18,
		dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();
	let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
	loop {
		if node.raft.current_leader().await == Some(node.local_node.id) {
			break;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for leader"
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
	node.data_server
		.refresh_leases_once(&node.ring, &*node.clock, 60_000)
		.await;

	let mut meta = simple_write_meta(1);
	meta.client_hlc = hlc(1, 0, 0);
	meta.await_finalization = 1;
	let terminal = node.data_server.terminal_failure_response_for_test(
		meta.write_id,
		meta.expected_epoch,
		hlc(2, 0, 0),
	);
	node.data_server.idempotency.insert(
		WendelonDataServer::idempotency_key_for(meta.tenant_id, meta.write_id),
		CachedWriteOutcome::new(WriteOutcome::FinalizationTerminalFailure { response: terminal }),
	);

	let (duplicate, trailing) = node
		.data_server
		.write(meta, single_kv_batch())
		.await
		.unwrap();

	assert_eq!(
		duplicate.status,
		WriteStatus::DUPLICATE,
		"got resp={duplicate:?}"
	);
	assert_eq!(duplicate.commit_hlc, terminal.commit_hlc);
	assert_eq!(duplicate.echoed_write_id, terminal.echoed_write_id);
	assert!(trailing.is_none());
	assert_eq!(
		node.data_server.pending_finalization_count_for_test().await,
		0
	);
	shutdown_lease_node(node).await;
}

#[tokio::test]
async fn duplicate_write_stays_retryable_while_finalization_is_pending() {
	let dir = bootstrap_tempdir("duplicate-recoverable-pending");
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		19,
		dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();
	let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
	loop {
		if node.raft.current_leader().await == Some(node.local_node.id) {
			break;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for leader"
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
	node.data_server
		.refresh_leases_once(&node.ring, &*node.clock, 60_000)
		.await;
	node.raft.shutdown().await.unwrap();

	let mut meta = simple_write_meta(1);
	meta.client_hlc = hlc(1, 0, 0);
	meta.await_finalization = 1;

	let (first, trailing) = node
		.data_server
		.write(meta, single_kv_batch())
		.await
		.unwrap();
	assert_eq!(first.status, WriteStatus::FINALIZATION_PENDING);
	assert!(trailing.is_none());

	let (duplicate, trailing) = node
		.data_server
		.write(meta, single_kv_batch())
		.await
		.unwrap();
	assert_eq!(duplicate.status, WriteStatus::FINALIZATION_PENDING);
	assert_eq!(duplicate.commit_hlc, first.commit_hlc);
	assert!(trailing.is_none());
	assert_eq!(
		node.data_server.pending_finalization_count_for_test().await,
		1
	);
	shutdown_lease_node(node).await;
}

#[tokio::test]
async fn duplicate_write_replays_persisted_pending_intent_after_cache_loss() {
	let dir = bootstrap_tempdir("duplicate-persisted-pending");
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		21,
		dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();
	let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
	loop {
		if node.raft.current_leader().await == Some(node.local_node.id) {
			break;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for leader"
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
	node.data_server
		.refresh_leases_once(&node.ring, &*node.clock, 60_000)
		.await;
	node.raft.shutdown().await.unwrap();

	let mut meta = simple_write_meta(1);
	meta.client_hlc = hlc(1, 0, 0);
	meta.await_finalization = 1;

	let before_bytes = node.slab.local_l1_bytes().await;
	let (first, trailing) = node
		.data_server
		.write(meta, single_kv_batch())
		.await
		.unwrap();
	assert_eq!(first.status, WriteStatus::FINALIZATION_PENDING);
	assert!(trailing.is_none());
	let after_first_bytes = node.slab.local_l1_bytes().await;
	assert!(
		after_first_bytes > before_bytes,
		"first write must durably append local bytes before finalization stalls"
	);

	// Intentionally reuse the shut-down raft handle here: the
	// invariant under test is that a cache-loss replay must not
	// re-append the payload bytes, even when reconciliation falls
	// back to the retryable pending status.
	let replay_server = WendelonDataServer::new(
		node.slab.clone(),
		node.raft.clone(),
		node.log_store.clone(),
		node.state_machine.clone(),
		node.local_node,
		node.clock.clone(),
		node.data_server.router.clone(),
		node.data_server.factory.clone(),
		node.pool.clone(),
	)
	.unwrap();

	let (duplicate, trailing) = replay_server.write(meta, single_kv_batch()).await.unwrap();
	assert_eq!(duplicate.status, WriteStatus::FINALIZATION_PENDING);
	assert_eq!(duplicate.commit_hlc, first.commit_hlc);
	assert!(trailing.is_none());
	assert_eq!(
		node.slab.local_l1_bytes().await,
		after_first_bytes,
		"duplicate replay after cache loss must not append the payload again"
	);
	assert_eq!(replay_server.pending_finalization_count_for_test().await, 1);
	shutdown_lease_node(node).await;
}

#[tokio::test]
async fn pending_finalization_replays_after_restart_when_control_branch_failed() {
	let dir = bootstrap_tempdir("pending-finalization-restart");
	let audit_path = dir.join("audit.ndjson");
	let cfg = NodeBootstrapConfig::dev(17, dir.to_path_buf(), "127.0.0.1:0".into());
	let node = WendelonNode::bootstrap_single_node(cfg.clone())
		.await
		.unwrap();
	let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
	loop {
		if node.raft.current_leader().await == Some(node.local_node.id) {
			break;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for leader"
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
	node.data_server
		.refresh_leases_once(&node.ring, &*node.clock, 60_000)
		.await;

	let mut meta = simple_write_meta(1);
	meta.client_hlc = hlc(1, 0, 0);
	meta.await_finalization = 1;

	node.raft.shutdown().await.unwrap();

	let (resp, trailing) = node
		.data_server
		.write(meta, single_kv_batch())
		.await
		.unwrap();
	assert_eq!(
		resp.status,
		WriteStatus::FINALIZATION_PENDING,
		"got resp={resp:?}"
	);
	assert!(trailing.is_none());
	assert_eq!(
		node.data_server.pending_finalization_count_for_test().await,
		1
	);
	assert!(
		node.slab.lookup(tenant(), meta.key_identity).is_some(),
		"data branch must stay durable locally across the control-branch failure"
	);

	shutdown_lease_node(node).await;
	tokio::time::sleep(Duration::from_millis(25)).await;

	let recovered = WendelonNode::bootstrap_single_node(cfg).await.unwrap();
	let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
	loop {
		if recovered.raft.current_leader().await == Some(recovered.local_node.id) {
			break;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for recovered leader"
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
	recovered
		.data_server
		.refresh_leases_once(&recovered.ring, &*recovered.clock, 60_000)
		.await;

	let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
	loop {
		let (read_resp, batch) = recovered
			.data_server
			.read(simple_read_meta(1), single_key_batch())
			.await
			.unwrap();
		if recovered
			.data_server
			.pending_finalization_count_for_test()
			.await == 0
			&& read_resp.status == ReadStatus::OK
			&& batch.is_some()
		{
			break;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for pending finalization replay; read_resp={read_resp:?}"
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}

	let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
	loop {
		let audit = std::fs::read_to_string(&audit_path).unwrap_or_default();
		let finalized_user_write_count = audit
			.lines()
			.filter(|line| {
				line.contains("\"event_type\":\"data.write_finalized\"")
					&& line.contains("\"record_count\":1")
			})
			.count();
		if finalized_user_write_count == 1 {
			break;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for replayed write audit; current log: {}",
			audit
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}

	shutdown_lease_node(recovered).await;
}

#[tokio::test]
async fn leader_recovers_orphan_witnesses_from_peer_before_reconciliation() {
	let fx = build_orphan_witness_fixture("orphan-witness").await;

	let record = PendingFinalizationRecord::new(
		crate::schema::ClientWriteId {
			client_uuid: [0x5A; 16],
			sequence_no: 77,
			idempotency_ttl_ms: 60_000,
			_pad: 0,
		},
		true,
		TenantFinalization {
			marker: FinalizationMarker {
				log_index: 0,
				term: 0,
				epoch: 1,
				commit_hlc: hlc(100, 0, fx.leader_node.local_node.id),
				payload_hash: [0xCD; 32],
				e_safe_index: 0,
				durability: DurabilityLevel::QUORUM,
				_pad: [0; 7],
			},
			tenant_id: tenant(),
			key_identity: id(9001),
			charged_bytes: 512,
			record_count: 1,
		},
	);
	fx.follower
		.pending_finalizations
		.upsert(record.clone())
		.await
		.unwrap();

	let current_term = fx.leader.raft.metrics().borrow().current_term;
	assert!(
		current_term > 0,
		"single-node leader must have entered a positive Raft term"
	);
	*fx.leader
		.orphan_witness_recovery_stamp
		.write()
		.expect("orphan witness recovery stamp poisoned") = Some(OrphanWitnessRecoveryStamp {
		term: current_term - 1,
		epoch: 2,
	});

	assert_eq!(fx.leader.pending_finalization_count_for_test().await, 0);
	assert!(
		fx.leader
			.ensure_orphan_witnesses_recovered_for_leadership()
			.await
			.unwrap()
	);
	assert_eq!(fx.leader.pending_finalization_count_for_test().await, 1);

	let reconciled = fx
		.leader
		.reconcile_pending_finalizations_once()
		.await
		.unwrap();
	assert_eq!(reconciled, 1);
	assert_eq!(fx.leader.pending_finalization_count_for_test().await, 0);

	let applied_upto = fx
		.leader
		.state_machine
		.last_applied_log_index()
		.await
		.expect("finalization must advance the log");
	let marker = fx
		.leader
		.log_store
		.find_applied_finalization(&record.finalization, applied_upto)
		.await
		.unwrap();
	assert!(
		marker.is_some(),
		"leader must finalize the imported orphan witness"
	);

	shutdown_lease_node(fx.leader_node).await;
	shutdown_lease_node(fx.follower_node).await;
}

#[tokio::test]
async fn orphan_witness_recovery_barrier_rejects_without_leadership() {
	let fx = build_orphan_witness_fixture("orphan-witness-no-leader").await;
	fx.leader
		.set_orphan_witness_recovery_leadership_for_test(false);
	let meta = simple_write_meta(1);
	let (response, trailing) = fx.leader.write(meta, single_kv_batch()).await.unwrap();
	assert_eq!(
		response.status,
		WriteStatus::NOT_LEADER,
		"got resp={response:?}"
	);
	assert!(trailing.is_none());
	assert!(
		!fx.leader
			.ensure_orphan_witnesses_recovered_for_leadership()
			.await
			.unwrap()
	);

	shutdown_lease_node(fx.leader_node).await;
	shutdown_lease_node(fx.follower_node).await;
}

#[tokio::test]
async fn fresh_leader_recovers_peer_witness_before_duplicate_write_replays() {
	let fx = build_orphan_witness_fixture("orphan-witness-duplicate").await;
	let mut meta = simple_write_meta(1);
	meta.write_id.sequence_no = 88;
	meta.await_finalization = 1;
	let batch = single_kv_batch();
	let bifurcated = bifurcate_client_write(&meta, &batch).unwrap();
	let record = PendingFinalizationRecord::new(
		meta.write_id,
		true,
		TenantFinalization {
			marker: FinalizationMarker {
				log_index: 0,
				term: 0,
				epoch: meta.expected_epoch,
				commit_hlc: hlc(200, 0, fx.leader_node.local_node.id),
				payload_hash: bifurcated.payload_hash,
				e_safe_index: 0,
				durability: meta.durability,
				_pad: [0; 7],
			},
			tenant_id: meta.tenant_id,
			key_identity: meta.key_identity,
			charged_bytes: bifurcated.charged_bytes,
			record_count: 1,
		},
	);
	fx.follower
		.pending_finalizations
		.upsert(record)
		.await
		.unwrap();

	let before_bytes = fx.leader_node.slab.local_l1_bytes().await;
	let unconfirmed_before = crate::metrics::global()
		.data_ring_orphan_witness_recovery_leadership_unconfirmed_total
		.get();
	let (response, trailing) = fx.leader.write(meta, batch).await.unwrap();
	// Diagnostic only: concurrent tests in this process share the global
	// counter, so the delta is attributable only when nothing else in the
	// binary runs at the same time. No assertion depends on it.
	let unconfirmed = crate::metrics::global()
		.data_ring_orphan_witness_recovery_leadership_unconfirmed_total
		.get() - unconfirmed_before;
	assert_eq!(
		response.status,
		WriteStatus::DUPLICATE,
		"got resp={response:?} leadership_unconfirmed_delta={unconfirmed}"
	);
	assert_eq!(
		fx.leader_node.slab.local_l1_bytes().await,
		before_bytes,
		"fresh-leader recovery must import the peer witness before re-appending payload bytes"
	);
	assert!(trailing.is_some());

	shutdown_lease_node(fx.leader_node).await;
	shutdown_lease_node(fx.follower_node).await;
}

#[tokio::test]
async fn cached_duplicate_replays_without_orphan_witness_sweep() {
	let fx = build_orphan_witness_fixture("orphan-witness-cache-bypass").await;
	let mut meta = simple_write_meta(1);
	meta.await_finalization = 1;
	let batch = single_kv_batch();

	let (first, first_trailing) = fx.leader.write(meta, batch.clone()).await.unwrap();
	assert_eq!(first.status, WriteStatus::OK, "got resp={first:?}");

	let tiers = TransportTierSet::empty().with(TransportTier::QuicIoUring);
	let roles = NodeRoleSet::empty().with(NodeRole::Data);
	let mut peers = fx.epoch_tx.borrow().as_ref().peers.clone();
	peers.insert(
		n(99),
		PeerCapabilities {
			address: "loopback://99".into(),
			tiers,
			roles,
			capabilities: crate::network::transport::NodeCapabilitySet::empty(),
		},
	);
	fx.epoch_tx
		.send(Arc::new(MembershipEpoch { epoch: 3, peers }))
		.unwrap();

	let (duplicate, trailing) = fx.leader.write(meta, batch).await.unwrap();
	assert_eq!(
		duplicate.status,
		WriteStatus::DUPLICATE,
		"cached duplicate replay must not be blocked by an unreachable sweep peer"
	);
	assert_eq!(trailing, first_trailing);

	shutdown_lease_node(fx.leader_node).await;
	shutdown_lease_node(fx.follower_node).await;
}

#[tokio::test]
async fn write_commits_and_fences_when_pending_intent_persistence_fails() {
	let dir = bootstrap_tempdir("pending-intent-upsert-fail");
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		20,
		dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();
	let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
	loop {
		if node.raft.current_leader().await == Some(node.local_node.id) {
			break;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for leader"
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
	node.data_server
		.refresh_leases_once(&node.ring, &*node.clock, 60_000)
		.await;
	node.data_server
		.pending_finalizations
		.fail_next_upsert_for_test("injected-by-b2");

	let mut meta = simple_write_meta(1);
	meta.client_hlc = hlc(1, 0, 0);
	meta.await_finalization = 1;

	let (resp, trailing) = node
		.data_server
		.write(meta, single_kv_batch())
		.await
		.unwrap();

	assert_eq!(resp.status, WriteStatus::OK, "got resp={resp:?}");
	assert!(trailing.is_some());
	assert_eq!(
		node.data_server.pending_finalization_count_for_test().await,
		0
	);
	assert!(
		node.slab.lookup(tenant(), meta.key_identity).is_some(),
		"data branch must remain durable even though finalization intent persistence failed"
	);
	assert_eq!(
		node.data_server.degraded_reason().as_deref(),
		Some(LOCAL_PERSISTENCE_DISTRUST_REASON)
	);

	let (duplicate, trailing) = node
		.data_server
		.write(meta, single_kv_batch())
		.await
		.unwrap();
	assert_eq!(duplicate.status, WriteStatus::DUPLICATE);
	assert!(trailing.is_some());
	shutdown_lease_node(node).await;
}

#[tokio::test]
async fn write_fences_when_pending_intent_persistence_and_immediate_finalization_fail() {
	let dir = bootstrap_tempdir("pending-intent-upsert-and-finalize-fail");
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		25,
		dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();
	let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
	loop {
		if node.raft.current_leader().await == Some(node.local_node.id) {
			break;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for leader"
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
	node.data_server
		.refresh_leases_once(&node.ring, &*node.clock, 60_000)
		.await;
	node.data_server
		.pending_finalizations
		.fail_next_upsert_for_test("injected-by-b2");
	node.raft.shutdown().await.unwrap();

	let mut meta = simple_write_meta(1);
	meta.client_hlc = hlc(1, 0, 0);
	meta.await_finalization = 1;

	let (resp, trailing) = node
		.data_server
		.write(meta, single_kv_batch())
		.await
		.unwrap();
	assert_eq!(
		resp.status,
		WriteStatus::INTERNAL_ERROR,
		"got resp={resp:?}"
	);
	assert!(trailing.is_none());
	assert_eq!(
		node.data_server.degraded_reason().as_deref(),
		Some(LOCAL_PERSISTENCE_DISTRUST_REASON)
	);
	assert_eq!(
		node.data_server.pending_finalization_count_for_test().await,
		0
	);
	assert!(
		node.slab.lookup(tenant(), meta.key_identity).is_some(),
		"data branch must remain durable even though finalization cannot be recovered on this node"
	);

	let (duplicate, trailing) = node
		.data_server
		.write(meta, single_kv_batch())
		.await
		.unwrap();
	assert_eq!(duplicate.status, WriteStatus::INTERNAL_ERROR);
	assert!(trailing.is_none());
	shutdown_lease_node(node).await;
}

#[tokio::test]
async fn local_persistence_failure_fences_future_writes() {
	let dir = bootstrap_tempdir("local-persistence-fence");
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		22,
		dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();
	let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
	loop {
		if node.raft.current_leader().await == Some(node.local_node.id) {
			break;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for leader"
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
	node.data_server
		.refresh_leases_once(&node.ring, &*node.clock, 60_000)
		.await;

	node.data_server
		.fail_next_local_durable_append_for_test(ErrorKind::PermissionDenied, "b6-fence");

	let mut first_meta = simple_write_meta(1);
	first_meta.client_hlc = hlc(1, 0, 0);
	let before_first = node.slab.local_l1_bytes().await;
	let (first, trailing) = node
		.data_server
		.write(first_meta, single_kv_batch())
		.await
		.unwrap();
	assert_eq!(first.status, WriteStatus::INTERNAL_ERROR);
	assert!(trailing.is_none());
	assert_eq!(
		node.data_server.degraded_reason().as_deref(),
		Some(LOCAL_PERSISTENCE_DISTRUST_REASON)
	);
	assert_eq!(
		node.slab.local_l1_bytes().await,
		before_first,
		"failed leader-local durable append must not publish bytes locally"
	);

	let mut second_meta = simple_write_meta(1);
	second_meta.write_id.sequence_no = 2;
	second_meta.client_hlc = hlc(2, 0, 0);
	let before_second = node.slab.local_l1_bytes().await;
	let (second, trailing) = node
		.data_server
		.write(second_meta, single_kv_batch())
		.await
		.unwrap();
	assert_eq!(second.status, WriteStatus::INTERNAL_ERROR);
	assert!(trailing.is_none());
	assert_eq!(
		node.slab.local_l1_bytes().await,
		before_second,
		"fenced node must reject later writes before attempting another local append"
	);
	shutdown_lease_node(node).await;
}

// ========================================================
//  MIGRATION HELPERS (plan 019)
// ========================================================

#[test]
fn pending_owned_entry_finds_matching_range_owned_by_self() {
	use crate::consensus::{MigrationLedgerEntry, MigrationState};
	use std::collections::HashMap;

	let local = n(7);
	let entry = MigrationLedgerEntry {
		migration_id: 1,
		range_start: 100,
		range_end: 200,
		from_epoch: 1,
		to_epoch: 2,
		logical_owner: local,
		upstream_migration_id: None,
		source_replicas: vec![n(1), n(2)],
		migration_hlc_floor: hlc(0, 0, 0),
		state: MigrationState::Pending,
		completed_by: None,
		completed_at_hlc: None,
	};
	let mut migrations = HashMap::new();
	migrations.insert(1, entry);

	// In-range, owned: hit.
	assert!(pending_owned_entry(&migrations, Locality::new(150), local).is_some());
	// In-range, owned by someone else: miss.
	assert!(pending_owned_entry(&migrations, Locality::new(150), n(9)).is_none());
	// Out of range: miss.
	assert!(pending_owned_entry(&migrations, Locality::new(50), local).is_none());
}

#[test]
fn pending_owned_entry_skips_completed_entries() {
	use crate::consensus::{MigrationLedgerEntry, MigrationState};
	use std::collections::HashMap;

	let local = n(7);
	let entry = MigrationLedgerEntry {
		migration_id: 1,
		range_start: 0,
		range_end: 100,
		from_epoch: 1,
		to_epoch: 2,
		logical_owner: local,
		upstream_migration_id: None,
		source_replicas: vec![n(1)],
		migration_hlc_floor: hlc(0, 0, 0),
		state: MigrationState::Completed,
		completed_by: Some(local),
		completed_at_hlc: Some(hlc(10, 0, 0)),
	};
	let mut migrations = HashMap::new();
	migrations.insert(1, entry);

	// Completed entries are not pull candidates — the caller
	// must treat the local miss as a real not-found rather
	// than re-issuing a MIGRATE_REQ against a drained source.
	assert!(pending_owned_entry(&migrations, Locality::new(50), local).is_none());
}

/// Multi-Page Fat Key (Plan 019 adjustment #2).
///
/// A single key carries 100 retained versions. Paging with
/// `max_records = 10` must surface every version exactly once
/// across ten pages: the `(cursor_hash, cursor_hlc)` cursor is
/// what lets the source resume *mid-key* instead of skipping to
/// the next key on page boundaries.
#[tokio::test]
async fn migrate_range_page_streams_every_version_of_fat_key() {
	let (_mgr_dir, mgr) = make_manager().await;
	let key: u64 = 42;
	let floor = hlc(0, 0, 0);
	for i in 0..100u32 {
		let v = hlc(i as u64 + 1, 0, 1);
		seed_record(&mgr, key, v, b"k", format!("v{i}").as_bytes()).await;
	}

	// Emulate the paging driver exactly the way `handle_migrate`
	// does: scan, truncate at `max_records`, emit
	// `(next_cursor_hash, next_cursor_hlc)` as the last row's
	// `(key, hlc)`, loop until `has_more == 0`.
	let mut collected: Vec<HybridTimestamp> = Vec::new();
	let mut cursor_identity = Identity::ZERO;
	let mut cursor_hlc: HybridTimestamp = hlc(0, 0, 0);
	let max_records: usize = 10;
	for _ in 0..15 {
		let locations = mgr.scan_all_versions_from(key, key, floor, cursor_identity, cursor_hlc);
		if locations.is_empty() {
			break;
		}
		let take = locations.len().min(max_records);
		for (_tenant_id, h, _routing_locality, loc) in locations.iter().take(take) {
			assert_eq!(*h, id(key));
			collected.push(loc.hlc);
		}
		let truncated = locations.len() > max_records;
		if !truncated {
			break;
		}
		let (_last_tenant_id, last_h, _last_routing_locality, last_loc) = locations[take - 1];
		cursor_identity = last_h;
		cursor_hlc = last_loc.hlc;
	}

	assert_eq!(collected.len(), 100, "every version must cross the wire");
	// HLC is monotonic ascending across the stream.
	for w in collected.windows(2) {
		assert!(w[0] < w[1], "versions must emerge in HLC-ascending order");
	}
	// No duplicates.
	let mut unique = collected.clone();
	unique.sort();
	unique.dedup();
	assert_eq!(unique.len(), 100, "no version may be emitted twice");
}

/// Quorum Miss Fallback (Plan 019 adjustment #3).
///
/// Exercises the observable inputs to the exhaustive fallback
/// loop: a source that simply missed the original replication
/// returns `KEY_NOT_FOUND` for a POINT_LOOKUP, and a source that
/// holds the key returns a record the destination can ingest
/// via `append_migration`. After walking both the "empty" and
/// the "populated" source in ledger order, the destination's
/// local index carries the key.
#[tokio::test]
async fn migrate_point_read_falls_through_missing_source_to_populated_one() {
	let (_source_empty_dir, source_empty) = make_manager().await;
	let (_source_full_dir, source_full) = make_manager().await;
	let (_destination_dir, destination) = make_manager().await;
	let key: u64 = 7;
	let v = hlc(100, 0, 1);
	// Only `source_full` holds the key.
	seed_record(&source_full, key, v, b"k7", b"v7").await;

	let floor = hlc(0, 0, 0);
	// Mirror the inputs `serve_migrate_ok` sees for POINT_LOOKUP:
	// an empty scan on `source_empty` is what becomes
	// `KEY_NOT_FOUND` on the wire, telling the caller to try the
	// next candidate.
	let miss = source_empty.scan_all_versions(key, key, floor);
	assert!(miss.is_empty(), "empty source must not surface a row");

	// Next candidate in the ledger's `source_replicas` list
	// actually has the record. The destination ingests it via
	// the same `append_migration` path the real loop uses.
	let hits = source_full.scan_all_versions(key, key, floor);
	assert_eq!(hits.len(), 1);
	let (tenant_id, h, routing_locality, loc) = hits[0];
	assert_eq!(tenant_id, tenant());
	let frame = source_full.read(loc).await.unwrap();
	let payload_hash = payload_digest(&frame);
	destination
		.append_migration_with_locality(
			1,
			tenant_id,
			h,
			routing_locality,
			loc.hlc,
			payload_hash,
			&frame,
		)
		.await
		.unwrap();

	let found = destination.lookup_as_of(tenant(), id(key), v);
	assert!(
		found.is_some(),
		"destination must resolve the key after falling through to a populated source"
	);
}

/// Dirty Range Scan (Plan 019 adjustment #1).
///
/// Half of the query window lives locally (already migrated or
/// natively written); the other half lives only on a remote
/// source. The merge path drains the source via
/// `scan_all_versions_from` + `append_migration` and a final
/// `scan_as_of` on the destination then sees the union.
#[tokio::test]
async fn query_merge_over_partially_migrated_range_sees_every_row() {
	let (_destination_dir, destination) = make_manager().await;
	let (_source_dir, source) = make_manager().await;
	let floor = hlc(0, 0, 0);
	let v = hlc(50, 0, 1);

	// Locally-present half: keys 0..50 already on `destination`.
	for k in 0..50u64 {
		seed_record(&destination, k, v, b"k", b"v-local").await;
	}
	// Remote-only half: keys 50..100 live only on the `source`.
	for k in 50..100u64 {
		seed_record(&source, k, v, b"k", b"v-remote").await;
	}

	// Drain the full ledger range from the source into the
	// destination, mimicking `pull_migration_range` without the
	// network. Every record goes through `append_migration`, the
	// idempotent ingest boundary — so a rerun would be a no-op.
	let pulled = source.scan_all_versions_from(0, 99, floor, Identity::ZERO, floor);
	for (tenant_id, h, routing_locality, loc) in pulled {
		let frame = source.read(loc).await.unwrap();
		let payload_hash = payload_digest(&frame);
		destination
			.append_migration_with_locality(
				1,
				tenant_id,
				h,
				routing_locality,
				loc.hlc,
				payload_hash,
				&frame,
			)
			.await
			.unwrap();
	}

	// A window scan on the destination now sees the union.
	let merged = destination.scan_as_of(tenant(), 0, 99, v);
	let mut keys: Vec<u64> = merged.iter().map(|(k, _)| *k).collect();
	keys.sort();
	keys.dedup();
	assert_eq!(
		keys.len(),
		100,
		"merged scan must cover 0..=99 exactly once"
	);
	assert_eq!(*keys.first().unwrap(), 0);
	assert_eq!(*keys.last().unwrap(), 99);
}

/// Regression for High #1: the drain-all semantic.
///
/// Under quorum writes a single source can hold only a partial
/// subset of the committed history. The pull loop must visit
/// every source and union their contributions — not return on
/// the first `has_more == 0`.
///
/// Two sources each hold a disjoint half of the range; the
/// destination must see both halves after iterating through the
/// full candidate list. `append_migration`'s idempotency means
/// a later source that re-emits a key already ingested from an
/// earlier source is a no-op, which keeps the drain-all policy
/// correct rather than double-counting.
#[tokio::test]
async fn drain_all_sources_unions_disjoint_halves() {
	let (_destination_dir, destination) = make_manager().await;
	let (_source_a_dir, source_a) = make_manager().await;
	let (_source_b_dir, source_b) = make_manager().await;
	let floor = hlc(0, 0, 0);
	let v = hlc(10, 0, 1);

	for k in 0..50u64 {
		seed_record(&source_a, k, v, b"k", b"v-a").await;
	}
	for k in 50..100u64 {
		seed_record(&source_b, k, v, b"k", b"v-b").await;
	}

	// Simulate pull_migration_range's drain-all behavior: walk
	// *every* candidate in source_replicas, not just the first.
	for src in [&source_a, &source_b] {
		let rows = src.scan_all_versions_from(0, 99, floor, Identity::ZERO, floor);
		for (tenant_id, h, routing_locality, loc) in rows {
			let frame = src.read(loc).await.unwrap();
			let payload_hash = payload_digest(&frame);
			destination
				.append_migration_with_locality(
					1,
					tenant_id,
					h,
					routing_locality,
					loc.hlc,
					payload_hash,
					&frame,
				)
				.await
				.unwrap();
		}
	}

	let merged = destination.scan_as_of(tenant(), 0, 99, v);
	let mut keys: Vec<u64> = merged.iter().map(|(k, _)| *k).collect();
	keys.sort();
	keys.dedup();
	assert_eq!(keys.len(), 100, "union must cover both halves");
	assert!(keys.contains(&0));
	assert!(keys.contains(&49));
	assert!(keys.contains(&50));
	assert!(keys.contains(&99));
}

/// Regression for High #2: MIGRATION_INCOMPLETE surfaces when the
/// pull fan-out cannot complete. A ledger entry overlapping the
/// query window names a source that cannot be reached; the
/// merge path must fail closed with a visible non-OK status
/// rather than downgrading to a local-only answer.
#[tokio::test]
async fn query_merge_surfaces_migration_incomplete_on_source_failure() {
	use crate::consensus::{MigrationLedgerEntry, MigrationState};

	let dir = bootstrap_tempdir("merge-incomplete");
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		21,
		dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();

	// Seed a ledger entry whose only source is a node id the
	// transport factory does not know about — any MIGRATE_REQ
	// will fail fast and bubble through pull_migration_range.
	let entry = MigrationLedgerEntry {
		migration_id: 99,
		range_start: 0,
		range_end: u64::MAX,
		from_epoch: 0,
		to_epoch: 1,
		logical_owner: node.local_node,
		upstream_migration_id: None,
		source_replicas: vec![NodeId {
			id: 9999,
			_reserved: 0,
		}],
		migration_hlc_floor: hlc(0, 0, 0),
		state: MigrationState::Pending,
		completed_by: None,
		completed_at_hlc: None,
	};
	node.state_machine.seed_migration_for_test(entry).await;

	let (resp, arrow) = node
		.data_server
		.execute_query_with_migration_merge(&base_meta(), &[])
		.await
		.unwrap();

	assert_eq!(
		resp.status,
		ReadStatus::MIGRATION_INCOMPLETE,
		"unreachable source must surface MIGRATION_INCOMPLETE, not a silent OK"
	);
	assert!(arrow.is_none());
	shutdown_lease_node(node).await;
}

/// Regression for Medium #1: after an OK response that ingested
/// records but none satisfies `as_of_hlc`, the fallback must
/// continue to the next source. A later replica under quorum
/// writes can still carry the older retained version the read
/// needs. The directly testable observable is that
/// `lookup_as_of` remains a miss after a newer-only ingest —
/// which is exactly why the loop must not terminate there.
#[tokio::test]
async fn lookup_as_of_miss_after_newer_only_ingest_proves_fallthrough_needed() {
	let (_destination_dir, destination) = make_manager().await;
	let key: u64 = 13;
	let newer = hlc(200, 0, 1);
	let older = hlc(50, 0, 1);

	// First "source" yields only a newer version (> as_of_hlc).
	let frame_newer = {
		let batch = kv_batch(b"k", b"v-new");
		encode_batch(&batch).unwrap()
	};
	let payload_newer = payload_digest(&frame_newer);
	destination
		.append_migration(1, tenant(), id(key), newer, payload_newer, &frame_newer)
		.await
		.unwrap();

	// Client wants a version ≤ older.physical_ms; newer-only
	// ingest cannot satisfy the read.
	assert!(
		destination.lookup_as_of(tenant(), id(key), older).is_none(),
		"newer-only ingest must not be treated as authoritative for older as_of"
	);

	// Second "source" yields the older version; after falling
	// through, the lookup succeeds.
	let frame_older = {
		let batch = kv_batch(b"k", b"v-old");
		encode_batch(&batch).unwrap()
	};
	let payload_older = payload_digest(&frame_older);
	destination
		.append_migration(1, tenant(), id(key), older, payload_older, &frame_older)
		.await
		.unwrap();
	assert!(
		destination.lookup_as_of(tenant(), id(key), older).is_some(),
		"after ingesting from a second source, the as_of read resolves"
	);
}

/// Regression for Round 3 High #1: on a local point-read miss
/// against a pending ledger entry whose every source is
/// non-authoritative (transport error / REDIRECT / STALE_LEDGER
/// / INTERNAL_ERROR), the read path must surface
/// `MIGRATION_INCOMPLETE` rather than collapsing to
/// `KEY_NOT_FOUND`. A false negative here is client-visible
/// ground truth — the client cannot distinguish "key does not
/// exist" from "we could not prove it one way or the other",
/// so an unreachable source must be a retryable status.
#[tokio::test]
async fn point_read_surfaces_migration_incomplete_on_source_failure() {
	use crate::consensus::{MigrationLedgerEntry, MigrationState};

	let dir = bootstrap_tempdir("point-incomplete");
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		23,
		dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();

	// Seed a ledger entry covering the entire hash range whose
	// sole source is an unknown node id — the transport factory
	// cannot dial it, so every candidate produces an
	// inconclusive (non-authoritative) failure.
	let entry = MigrationLedgerEntry {
		migration_id: 101,
		range_start: 0,
		range_end: u64::MAX,
		from_epoch: 0,
		to_epoch: 1,
		logical_owner: node.local_node,
		upstream_migration_id: None,
		source_replicas: vec![NodeId {
			id: 9999,
			_reserved: 0,
		}],
		migration_hlc_floor: hlc(0, 0, 0),
		state: MigrationState::Pending,
		completed_by: None,
		completed_at_hlc: None,
	};
	node.state_machine.seed_migration_for_test(entry).await;

	// SERIALIZABLE bypasses the LINEARIZABLE lease gate so the
	// fallback path actually runs. Local slab is empty → miss →
	// try_migrate_point_read → all sources inconclusive →
	// PointReadOutcome::Incomplete → MIGRATION_INCOMPLETE.
	let mut meta = simple_read_meta(1);
	meta.consistency = SchemaConsistency::SERIALIZABLE;
	let (resp, arrow) = node
		.data_server
		.read(meta, single_key_batch())
		.await
		.unwrap();

	assert_eq!(
		resp.status,
		ReadStatus::MIGRATION_INCOMPLETE,
		"unreachable source must surface MIGRATION_INCOMPLETE, not KEY_NOT_FOUND"
	);
	assert!(arrow.is_none());
	shutdown_lease_node(node).await;
}

/// Regression for Medium #2: a query rejected by the epoch /
/// linearizability gate must not force remote migration I/O.
/// If the gate check fired *after* the pull, a
/// MIGRATION_INCOMPLETE from the unreachable source would mask
/// the real reason (EPOCH_STALE). Ordering the gate first means
/// the expensive path is skipped entirely.
#[tokio::test]
async fn epoch_stale_gate_short_circuits_before_migration_pull() {
	use crate::consensus::{MigrationLedgerEntry, MigrationState};

	let dir = bootstrap_tempdir("gate-first");
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		22,
		dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();

	// Advance current_epoch so a request at epoch 0 is stale.
	node.state_machine
		.seed_e_safe_for_test(5, hlc(1, 0, 1))
		.await;

	// Seed a ledger entry that *would* fail the merge pull if
	// the gate didn't fire first — its only source is an
	// unknown node id.
	let entry = MigrationLedgerEntry {
		migration_id: 77,
		range_start: 0,
		range_end: u64::MAX,
		from_epoch: 4,
		to_epoch: 5,
		logical_owner: node.local_node,
		upstream_migration_id: None,
		source_replicas: vec![NodeId {
			id: 9999,
			_reserved: 0,
		}],
		migration_hlc_floor: hlc(0, 0, 0),
		state: MigrationState::Pending,
		completed_by: None,
		completed_at_hlc: None,
	};
	node.state_machine.seed_migration_for_test(entry).await;

	// Client believes it's still on epoch 0 — stale.
	let mut meta = base_meta();
	meta.expected_epoch = 0;
	let (resp, _) = node
		.data_server
		.execute_query_with_migration_merge(&meta, &[])
		.await
		.unwrap();

	assert_eq!(
		resp.status,
		ReadStatus::EPOCH_STALE,
		"gate must fire before any pull; otherwise an unreachable source would mask this as MIGRATION_INCOMPLETE"
	);
	shutdown_lease_node(node).await;
}

#[tokio::test]
async fn migration_record_roundtrip_preserves_every_field() {
	// Encode → decode must round-trip every column so the
	// receiver's `append_migration` gets the same (key_identity,
	// hlc, payload_hash, frame_bytes) tuple the sender read
	// off disk.
	let rows = vec![
		(
			tenant(),
			id(7),
			Locality::new(70),
			crate::storage::core::RecordLocation {
				slab_id: SlabId::ZERO,
				offset: 0,
				length: 0,
				hlc: hlc(10, 2, 3),
				record_row_index: crate::schema::RecordRowIndex::ZERO,
				is_tombstone: false,
				placement: PlacementSemantics::RawLocality,
				partition: None,
			},
			b"frame-one".to_vec(),
		),
		(
			tenant(),
			id(42),
			Locality::new(420),
			crate::storage::core::RecordLocation {
				slab_id: SlabId::ZERO,
				offset: 0,
				length: 0,
				hlc: hlc(20, 5, 3),
				record_row_index: crate::schema::RecordRowIndex::ZERO,
				is_tombstone: false,
				placement: PlacementSemantics::RawLocality,
				partition: None,
			},
			b"frame-two".to_vec(),
		),
	];
	let bytes = encode_migration_records(&rows).unwrap();
	let policy = crate::storage::ipc::record_batch_compression_policy_from_ipc(&bytes).unwrap();
	assert_eq!(policy, RecordBodyCompressionPolicy::Lz4Frame);
	let decoded = decode_migration_records(&bytes).unwrap();
	assert_eq!(decoded.len(), 2);
	assert_eq!(decoded[0].key_identity, id(7));
	assert_eq!(decoded[0].routing_locality, Locality::new(70));
	assert_eq!(decoded[0].hlc, hlc(10, 2, 3));
	assert_eq!(decoded[0].frame_bytes, b"frame-one");
	assert_eq!(decoded[0].payload_hash, payload_digest(b"frame-one"));
	assert_eq!(decoded[1].key_identity, id(42));
	assert_eq!(decoded[1].routing_locality, Locality::new(420));
	assert_eq!(decoded[1].frame_bytes, b"frame-two");
}

#[test]
fn adr0095_migration_filters_dropped_rows_inside_a_mixed_packed_record() {
	let dropped_object = TableObjectId::new([0x95; 16]);
	let live_object = TableObjectId::new([0x96; 16]);
	let row_hlc = hlc(20, 0, 1);
	let batch = typed_table_batch(vec![1, 2, 3]);
	let source_rows = vec![
		MultiRowSourceRow {
			source_row_index: 0,
			key_identity: id(1),
			routing_locality: Locality::new(10),
			row_hlc,
			is_tombstone: false,
			partition: Some(PartitionId::new(7)),
			table_object_id: Some(live_object),
		},
		MultiRowSourceRow {
			source_row_index: 1,
			key_identity: id(2),
			routing_locality: Locality::new(11),
			row_hlc,
			is_tombstone: false,
			partition: Some(PartitionId::new(7)),
			table_object_id: Some(live_object),
		},
		MultiRowSourceRow {
			source_row_index: 2,
			key_identity: id(3),
			routing_locality: Locality::new(12),
			row_hlc,
			is_tombstone: false,
			partition: None,
			table_object_id: Some(live_object),
		},
	];
	let encoded = crate::storage::multirow::encode_multirow_from_survivors_with_policy(
		&batch,
		&source_rows,
		RecordBodyCompressionPolicy::Uncompressed,
	)
	.unwrap();
	// The current packer enforces one table-object stamp per record. Construct
	// the cross-object packed state directly to pin migration's defensive
	// row-level contract for legacy/recovered bytes that predate that invariant.
	let packed_batch = decode_batch(&encoded.frame).unwrap();
	let table_column_index = packed_batch
		.schema()
		.index_of(crate::schema::STORAGE_ROW_TABLE_OBJECT_ID_COLUMN)
		.unwrap();
	let mut table_ids = FixedSizeBinaryBuilder::with_capacity(3, 16);
	for table_object_id in [dropped_object, live_object, live_object] {
		table_ids.append_value(table_object_id.as_bytes()).unwrap();
	}
	let mut columns = packed_batch.columns().to_vec();
	columns[table_column_index] = Arc::new(table_ids.finish());
	let mixed_batch = RecordBatch::try_new(packed_batch.schema(), columns).unwrap();
	let mixed_frame = encode_batch(&mixed_batch).unwrap();
	let records = source_rows
		.iter()
		.map(|row| MigrationRecord {
			tenant_id: tenant(),
			key_identity: row.key_identity,
			routing_locality: row.routing_locality,
			hlc: row.row_hlc,
			payload_hash: payload_digest(&mixed_frame),
			frame_bytes: mixed_frame.clone(),
		})
		.collect::<Vec<_>>();
	let operation_id = crate::integration::compute::DropOperationId {
		tenant_id: tenant(),
		table_object_id: dropped_object,
		schema_generation: SchemaGeneration::new(1),
	};
	let mut tombstones = crate::schema::TableDropTombstoneCatalog::default();
	let tombstone = crate::schema::TableDropTombstone {
		operation_id,
		fence_log_index: 4,
		finalize_log_index: 5,
	};
	tombstones.validate_insert(tombstone).unwrap();
	tombstones.insert_validated(tombstone);

	let filtered = migration_records_without_logically_dropped_rows(&records, &tombstones).unwrap();
	assert_eq!(
		filtered
			.iter()
			.map(|row| row.key_identity)
			.collect::<Vec<_>>(),
		vec![id(2), id(3)],
		"the dropped row's own migration entry is absent"
	);
	for record in filtered {
		let rows = storage_rows_from_batch(&decode_batch(&record.frame_bytes).unwrap()).unwrap();
		assert_eq!(
			rows.len(),
			2,
			"one physical frame retains both survivor rows"
		);
		assert!(
			rows.iter()
				.all(|row| row.table_object_id != Some(dropped_object)),
			"no dropped-object row survives inside the packed frame"
		);
		assert_eq!(rows[0].partition, Some(PartitionId::new(7)));
		assert_eq!(rows[1].partition, None, "legacy placement is preserved");
		assert_eq!(record.payload_hash, payload_digest(&record.frame_bytes));
	}
}

// ========================================================
//  LOOPBACK HRFT HARNESS (drain-all integration)
//
//  In-process transport that plugs into the real
//  `TransportProviderFactory` so the composition path —
//  `pull_migration_range` → `migrate_request` →
//  `MsgRouter::handle_frame` → `handle_data_frame` →
//  `handle_migrate` → `encode_migration_records` — is
//  exercised without QUIC. `open_bi` returns one end of a
//  `tokio::io::duplex` pipe; a spawned task reads the
//  request frame, optionally intercepts it via the
//  `fail_after_pages` knob, and dispatches the rest to the
//  registered `HrftService`. Per-target counters let a
//  mid-drain failure target source B's *second* page after
//  its first page succeeds (the reviewer's non-blocking
//  refinement).
// ========================================================

use crate::consensus::{MigrationLedgerEntry, MigrationState};
use crate::network::router::MsgRouter;
use crate::network::server::HrftService;
use crate::network::tls::ActorIdentity;
use crate::network::transport::{
	MembershipEpoch, NodeRole, NodeRoleSet, PeerCapabilities, TransportConnection,
	TransportProvider, TransportStream, TransportTier, TransportTierSet,
};
use async_trait::async_trait;
use dashmap::DashMap as LoopbackDashMap;
use std::collections::HashMap as StdHashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicI32, Ordering as LoopbackOrdering};
use std::task::{Context as TaskContext, Poll};
use tokio::io::{AsyncRead, AsyncWrite, DuplexStream, ReadBuf};
use tokio::sync::{Mutex as TokioMutex, watch};

/// Registry of per-target HRFT services + fault knobs. The
/// `LoopbackProvider` and every `LoopbackConnection` share this
/// via `Arc`, so one fault-injection call affects every
/// subsequent dispatch.
struct LoopbackHandlers {
	// Explicit authenticated sender for selected counter fixtures only.
	senders: LoopbackDashMap<u32, NodeId>,
	services: LoopbackDashMap<u32, Arc<dyn HrftService>>,
	/// Per-target remaining successful pages before the next
	/// `MIGRATE_REQ` is intercepted with `INTERNAL_ERROR`.
	/// Missing entry = unlimited. Counter decrements before
	/// dispatch; `<= 0` means "fail this page now".
	fail_after: LoopbackDashMap<u32, AtomicI32>,
	/// Per-target count of `MIGRATE_REQ` frames observed by the
	/// loopback. Incremented before fault-injection consults
	/// `fail_after`, so it counts every page attempt (including
	/// the one that gets intercepted with INTERNAL_ERROR).
	/// Tests use this to prove paging actually round-tripped
	/// rather than collapsing to a single oversized response.
	page_count: LoopbackDashMap<u32, AtomicI32>,
	pool: Arc<BufPool>,
}

impl LoopbackHandlers {
	fn new(pool: Arc<BufPool>) -> Arc<Self> {
		Arc::new(Self {
			services: LoopbackDashMap::new(),
			senders: LoopbackDashMap::new(),
			fail_after: LoopbackDashMap::new(),
			page_count: LoopbackDashMap::new(),
			pool,
		})
	}

	fn register(&self, node: NodeId, svc: Arc<dyn HrftService>) {
		self.services.insert(node.id, svc);
	}

	/// After `successful_pages` OK responses, the next
	/// `MIGRATE_REQ` to `target` is intercepted and answered
	/// with `INTERNAL_ERROR`. Subsequent pages keep failing —
	/// once the counter is saturated negative, every request
	/// stays in the error branch.
	fn fail_source_after_pages(&self, target: NodeId, successful_pages: u32) {
		self.fail_after
			.insert(target.id, AtomicI32::new(successful_pages as i32));
	}

	/// Number of `MIGRATE_REQ` frames the loopback has observed
	/// for `target`. Used by paging tests to prove that the
	/// page-cap override actually forced multiple round-trips
	/// — without this, a regression that ignored the cap and
	/// returned every record in one page would still satisfy
	/// the coverage assertion.
	fn page_count_for(&self, target: NodeId) -> i32 {
		self.page_count
			.get(&target.id)
			.map(|e| e.load(LoopbackOrdering::SeqCst))
			.unwrap_or(0)
	}
}

/// `TransportStream` newtype over `tokio::io::DuplexStream`.
/// `DuplexStream` is `Unpin`, so the delegation is straight
/// `Pin::new(&mut self.inner)`.
struct DuplexTransportStream {
	inner: DuplexStream,
}

impl AsyncRead for DuplexTransportStream {
	fn poll_read(
		mut self: Pin<&mut Self>,
		cx: &mut TaskContext<'_>,
		buf: &mut ReadBuf<'_>,
	) -> Poll<std::io::Result<()>> {
		Pin::new(&mut self.inner).poll_read(cx, buf)
	}
}

impl AsyncWrite for DuplexTransportStream {
	fn poll_write(
		mut self: Pin<&mut Self>,
		cx: &mut TaskContext<'_>,
		buf: &[u8],
	) -> Poll<std::io::Result<usize>> {
		Pin::new(&mut self.inner).poll_write(cx, buf)
	}
	fn poll_flush(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<std::io::Result<()>> {
		Pin::new(&mut self.inner).poll_flush(cx)
	}
	fn poll_shutdown(
		mut self: Pin<&mut Self>,
		cx: &mut TaskContext<'_>,
	) -> Poll<std::io::Result<()>> {
		Pin::new(&mut self.inner).poll_shutdown(cx)
	}
}

#[async_trait]
impl TransportStream for DuplexTransportStream {
	fn stream_id(&self) -> u64 {
		0
	}
	async fn finish(&mut self) -> std::io::Result<()> {
		use tokio::io::AsyncWriteExt;
		self.inner.shutdown().await
	}
}

struct LoopbackConnection {
	remote: NodeId,
	tier: TransportTier,
	handlers: Arc<LoopbackHandlers>,
	actor: ActorIdentity,
}

#[async_trait]
impl TransportConnection for LoopbackConnection {
	async fn open_bi(&self) -> std::io::Result<Box<dyn TransportStream>> {
		let (client_side, server_side) = tokio::io::duplex(64 * 1024);
		let handlers = self.handlers.clone();
		let remote = self.remote;
		tokio::spawn(serve_one_frame(server_side, handlers, remote));
		Ok(Box::new(DuplexTransportStream { inner: client_side }))
	}
	async fn accept_bi(&self) -> std::io::Result<Box<dyn TransportStream>> {
		unreachable!("loopback connections never accept server-initiated streams")
	}
	fn remote_id(&self) -> Option<NodeId> {
		Some(self.remote)
	}
	fn actor_identity(&self) -> &ActorIdentity {
		&self.actor
	}
	fn transport_type(&self) -> TransportTier {
		self.tier
	}
	fn is_closed(&self) -> bool {
		false
	}
}

async fn serve_one_frame(
	mut stream: DuplexStream,
	handlers: Arc<LoopbackHandlers>,
	remote: NodeId,
) {
	use tokio::io::AsyncWriteExt;
	let pool = handlers.pool.clone();
	let frame = match read_frame(&mut stream, &pool).await {
		Ok(f) => f,
		Err(_) => return,
	};

	// Fault injection: on a `MIGRATE_REQ`, consume one page of
	// the per-target allowance. Once exhausted (counter <= 0
	// before this request), craft an INTERNAL_ERROR
	// MIGRATE_RESP instead of dispatching — models a source
	// that fails mid-drain after the first page succeeded.
	if frame.header.msg_type == MsgType::MIGRATE_REQ.0 {
		// Count every page attempt (including the one we are
		// about to fail). Tests assert on this to prove paging
		// round-tripped rather than collapsing to one response.
		handlers
			.page_count
			.entry(remote.id)
			.or_insert_with(|| AtomicI32::new(0))
			.fetch_add(1, LoopbackOrdering::SeqCst);

		if let Some(entry) = handlers.fail_after.get(&remote.id) {
			let prev = entry.fetch_sub(1, LoopbackOrdering::SeqCst);
			if prev <= 0 {
				if let Some(resp) = craft_internal_error_response(&frame, &pool) {
					let _ = write_frame(
						&mut stream,
						&resp.header,
						&resp.meta,
						resp.arrow_payload.as_deref(),
						None,
					)
					.await;
				}
				let _ = stream.shutdown().await;
				return;
			}
		}
	}

	let svc = match handlers.services.get(&remote.id).map(|e| e.clone()) {
		Some(s) => s,
		None => return,
	};
	let sender = handlers
		.senders
		.get(&remote.id)
		.map(|entry| *entry.value())
		.unwrap_or(ZERO_NODE);
	let response = match svc
		.handle_frame(
			RequestContext {
				remote: Some(sender),
				actor: ActorIdentity::data_node(sender),
			},
			frame,
		)
		.await
	{
		Ok(Some(r)) => r,
		_ => return,
	};
	let _ = write_frame(
		&mut stream,
		&response.header,
		&response.meta,
		response.arrow_payload.as_deref(),
		response.trailing.as_deref(),
	)
	.await;
	let _ = stream.shutdown().await;
}

fn craft_internal_error_response(frame: &HrftFrame, pool: &Arc<BufPool>) -> Option<HrftFrame> {
	if frame.meta.len() < std::mem::size_of::<MigrateRequestMeta>() {
		return None;
	}
	let req_meta: MigrateRequestMeta =
		*bytemuck::from_bytes(&frame.meta[..std::mem::size_of::<MigrateRequestMeta>()]);
	let resp_meta = migrate_error_meta(&req_meta, MigrateStatus::INTERNAL_ERROR, ZERO_NODE);
	let mut body = pool.acquire();
	body.as_mut_vec()
		.extend_from_slice(bytemuck::bytes_of(&resp_meta));
	Some(HrftFrame {
		header: FrameHeader {
			magic: HRFT_MAGIC,
			version: 1,
			msg_type: MsgType::MIGRATE_RESP.0,
			flags: 0,
			stream_id: frame.header.stream_id,
			meta_len: body.len() as u32,
			arrow_len: 0,
			trailing_len: 0,
		},
		meta: body,
		arrow_payload: None,
		trailing: None,
	})
}

struct LoopbackProvider {
	handlers: Arc<LoopbackHandlers>,
	tier: TransportTier,
}

#[async_trait]
impl TransportProvider for LoopbackProvider {
	async fn connect(
		&self,
		target: NodeId,
		_address: &str,
	) -> std::io::Result<Arc<dyn TransportConnection>> {
		if !self.handlers.services.contains_key(&target.id) {
			return Err(Error::new(
				ErrorKind::NotFound,
				format!("loopback: no service registered for {:?}", target),
			));
		}
		Ok(Arc::new(LoopbackConnection {
			remote: target,
			tier: self.tier,
			handlers: self.handlers.clone(),
			actor: ActorIdentity::data_node(target),
		}))
	}
}

struct FixedReplicaRouter {
	routes: StdHashMap<Locality, Vec<NodeId>>,
	default: Vec<NodeId>,
}

impl FixedReplicaRouter {
	fn new(default: Vec<NodeId>) -> Self {
		Self {
			routes: StdHashMap::new(),
			default,
		}
	}

	fn with_route(mut self, locality: Locality, replicas: Vec<NodeId>) -> Self {
		self.routes.insert(locality, replicas);
		self
	}
}

impl ReplicaRouter for FixedReplicaRouter {
	fn replicas_for_locality(&self, locality: Locality) -> Vec<NodeId> {
		self.routes
			.get(&locality)
			.cloned()
			.unwrap_or_else(|| self.default.clone())
	}

	fn replication_factor(&self) -> usize {
		self.routes
			.values()
			.map(|replicas| replicas.len())
			.chain(std::iter::once(self.default.len()))
			.max()
			.unwrap_or(1)
	}

	fn replicas_for_placement(&self, key: crate::schema::table::PlacementKey) -> Vec<NodeId> {
		// The fixed test router routes both spaces by their ring value, which
		// keeps single-space fixtures working; the placement-discriminating
		// route tests use StaticReplicaRouter instead.
		match key {
			crate::schema::table::PlacementKey::RawLocality(locality) => {
				self.replicas_for_locality(locality)
			}
			crate::schema::table::PlacementKey::Partition(partition) => {
				self.replicas_for_locality(Locality::new(partition.get()))
			}
		}
	}
}

struct CrashyReplicaState {
	manager: Arc<SlabManager>,
	volatile_cutoffs: StdHashMap<SlabId, u64>,
}

/// Replica harness that appends to a real slab but deliberately
/// omits the durability barrier. Tests can then "crash" it by
/// truncating every recorded append-only tail and reopening the
/// manager from disk, which deterministically models a crash
/// between append and flush.
struct CrashyReplicaService {
	dir: std::path::PathBuf,
	io: crate::storage::io_uring::IoRingHandle,
	max_slab_bytes: u64,
	pool: Arc<BufPool>,
	state: TokioMutex<CrashyReplicaState>,
}

impl CrashyReplicaService {
	async fn open(dir: std::path::PathBuf, pool: Arc<BufPool>) -> Arc<Self> {
		let io = spawn_io_thread(64).unwrap();
		let max_slab_bytes = 1 << 30;
		let manager = Arc::new(
			SlabManager::open(SlabManagerConfig {
				dir: dir.clone(),
				max_slab_bytes,
				io: io.clone(),
				l2: None,
				rehydration: crate::storage::manager::RehydrationPolicy::default(),
				l2_column_fetch: Default::default(),
				row_group_writer: Default::default(),
				l2_row_group_fetch: Default::default(),
				encoding_writer: Default::default(),
				l2_chunk_layout: Default::default(),
				l2_encoding_fetch: Default::default(),
				encoding_compute: Default::default(),
				group_commit: crate::storage::GroupCommitConfig::default(),
				crypto: test_crypto(),
			})
			.await
			.unwrap(),
		);
		Arc::new(Self {
			dir,
			io,
			max_slab_bytes,
			pool,
			state: TokioMutex::new(CrashyReplicaState {
				manager,
				volatile_cutoffs: StdHashMap::new(),
			}),
		})
	}

	async fn manager(&self) -> Arc<SlabManager> {
		self.state.lock().await.manager.clone()
	}

	async fn crash_and_recover(&self) {
		let mut state = self.state.lock().await;
		for (slab_id, cutoff) in state.volatile_cutoffs.drain() {
			let path = self.dir.join(format!("slab_{slab_id:016x}.wnd"));
			if !path.exists() {
				continue;
			}
			let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
			file.set_len(cutoff).unwrap();
		}
		state.manager = Arc::new(
			SlabManager::open(SlabManagerConfig {
				dir: self.dir.clone(),
				max_slab_bytes: self.max_slab_bytes,
				io: self.io.clone(),
				l2: None,
				rehydration: crate::storage::manager::RehydrationPolicy::default(),
				l2_column_fetch: Default::default(),
				row_group_writer: Default::default(),
				l2_row_group_fetch: Default::default(),
				encoding_writer: Default::default(),
				l2_chunk_layout: Default::default(),
				l2_encoding_fetch: Default::default(),
				encoding_compute: Default::default(),
				group_commit: crate::storage::GroupCommitConfig::default(),
				crypto: test_crypto(),
			})
			.await
			.unwrap(),
		);
	}
}

#[async_trait]
impl HrftService for CrashyReplicaService {
	async fn handle_frame(
		&self,
		_context: RequestContext,
		frame: HrftFrame,
	) -> std::io::Result<Option<HrftFrame>> {
		if frame.header.msg_type == MsgType::PENDING_WITNESS_REQ.0 {
			let resp = PendingWitnessPullResponseMeta {
				status: PendingWitnessPullStatus::OK,
				_pad1: [0; 7],
				witness_count: 0,
			};
			let mut body = self.pool.acquire();
			body.as_mut_vec()
				.extend_from_slice(bytemuck::bytes_of(&resp));
			return Ok(Some(HrftFrame {
				header: FrameHeader {
					magic: HRFT_MAGIC,
					version: 1,
					msg_type: MsgType::PENDING_WITNESS_RESP.0,
					flags: 0,
					stream_id: frame.header.stream_id,
					meta_len: body.len() as u32,
					arrow_len: 0,
					trailing_len: 0,
				},
				meta: body,
				arrow_payload: None,
				trailing: None,
			}));
		}
		if frame.header.msg_type != MsgType::REPLICATE_REQ.0 {
			return Err(Error::new(
				ErrorKind::Unsupported,
				format!(
					"crashy replica handles REPLICATE_REQ only, got msg_type=0x{:02x}",
					frame.header.msg_type
				),
			));
		}
		if frame.meta.len() < std::mem::size_of::<ReplicaWriteMeta>() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"REPLICATE_REQ meta too small",
			));
		}
		let meta: ReplicaWriteMeta =
			*bytemuck::from_bytes(&frame.meta[..std::mem::size_of::<ReplicaWriteMeta>()]);
		let arrow = frame.arrow_payload.as_ref().ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				"REPLICATE_REQ missing arrow payload",
			)
		})?;

		let manager = self.manager().await;
		let arrow_bytes: &[u8] = &arrow[..];
		let (status, ack_level, location) = if payload_digest(arrow_bytes) != meta.payload_hash {
			(
				ReplicaWriteStatus::HASH_MISMATCH,
				ReplicaAckLevel::NONE,
				None,
			)
		} else {
			match manager
				.append(
					meta.tenant_id,
					meta.key_identity,
					meta.commit_hlc,
					arrow_bytes,
				)
				.await
			{
				Ok(location) => (
					ReplicaWriteStatus::OK,
					ReplicaAckLevel::APPEND_ONLY,
					Some(location),
				),
				Err(_) => (
					ReplicaWriteStatus::INTERNAL_ERROR,
					ReplicaAckLevel::NONE,
					None,
				),
			}
		};

		if let Some(location) = location {
			let mut state = self.state.lock().await;
			state
				.volatile_cutoffs
				.entry(location.slab_id)
				.and_modify(|cutoff| *cutoff = (*cutoff).min(location.offset))
				.or_insert(location.offset);
		}

		let resp = ReplicaWriteRespMeta {
			status,
			ack_level,
			_pad: [0; 6],
		};
		let mut body = self.pool.acquire();
		body.as_mut_vec()
			.extend_from_slice(bytemuck::bytes_of(&resp));
		Ok(Some(HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: MsgType::REPLICATE_RESP.0,
				flags: 0,
				stream_id: frame.header.stream_id,
				meta_len: body.len() as u32,
				arrow_len: 0,
				trailing_len: 0,
			},
			meta: body,
			arrow_payload: None,
			trailing: None,
		}))
	}
}

struct DurabilityFixture {
	leader: Arc<WendelonDataServer>,
	leader_slab: Arc<SlabManager>,
	durable_slab: Arc<SlabManager>,
	crashy: Arc<CrashyReplicaService>,
	_leader_node: WendelonNode,
	_durable_node: WendelonNode,
	_epoch_tx: watch::Sender<Arc<MembershipEpoch>>,
	_dirs: Vec<crate::test_support::TestDir>,
}

async fn build_durability_fixture(
	durable_key_locality: u64,
	append_only_key_locality: u64,
) -> DurabilityFixture {
	let leader_node_dir = bootstrap_tempdir("durability-leader");
	let leader_node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		41,
		leader_node_dir.to_path_buf(),
		"loopback://41".into(),
	))
	.await
	.unwrap();
	let durable_node_dir = bootstrap_tempdir("durability-durable");
	let durable_node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		42,
		durable_node_dir.to_path_buf(),
		"loopback://42".into(),
	))
	.await
	.unwrap();

	let leader_id = leader_node.local_node;
	let durable_id = durable_node.local_node;
	let crashy_id = n(43);
	let pool = leader_node.pool.clone();
	let handlers = LoopbackHandlers::new(pool.clone());
	let loopback_provider: Arc<dyn TransportProvider> = Arc::new(LoopbackProvider {
		handlers: handlers.clone(),
		tier: TransportTier::QuicIoUring,
	});

	let tiers = TransportTierSet::empty().with(TransportTier::QuicIoUring);
	let roles = NodeRoleSet::empty().with(NodeRole::Data);
	let mut peers = StdHashMap::new();
	peers.insert(
		leader_id,
		PeerCapabilities {
			address: "loopback://41".into(),
			tiers,
			roles,
			capabilities: crate::network::transport::NodeCapabilitySet::empty(),
		},
	);
	peers.insert(
		durable_id,
		PeerCapabilities {
			address: "loopback://42".into(),
			tiers,
			roles,
			capabilities: crate::network::transport::NodeCapabilitySet::empty(),
		},
	);
	peers.insert(
		crashy_id,
		PeerCapabilities {
			address: "loopback://43".into(),
			tiers,
			roles,
			capabilities: crate::network::transport::NodeCapabilitySet::empty(),
		},
	);
	let epoch = MembershipEpoch { epoch: 1, peers };
	let (epoch_tx, epoch_rx) = watch::channel(Arc::new(epoch));
	let resolver: Arc<dyn crate::network::transport::CapabilityResolver> = Arc::new(
		crate::network::transport::EpochCapabilityResolver::new(epoch_rx.clone()),
	);

	let build_factory = || {
		let mut providers: StdHashMap<TransportTier, Arc<dyn TransportProvider>> =
			StdHashMap::new();
		providers.insert(TransportTier::QuicIoUring, loopback_provider.clone());
		TransportProviderFactory::new(tiers, providers, resolver.clone(), epoch_rx.clone())
	};

	let router: Arc<dyn ReplicaRouter> = Arc::new(
		FixedReplicaRouter::new(vec![leader_id, durable_id, crashy_id])
			.with_route(
				Locality::new(durable_key_locality),
				vec![leader_id, durable_id, crashy_id],
			)
			.with_route(
				Locality::new(append_only_key_locality),
				vec![leader_id, crashy_id],
			),
	);

	let leader = Arc::new(
		WendelonDataServer::new(
			leader_node.slab.clone(),
			leader_node.raft.clone(),
			leader_node.log_store.clone(),
			leader_node.state_machine.clone(),
			leader_id,
			leader_node.clock.clone(),
			router.clone(),
			build_factory(),
			pool.clone(),
		)
		.unwrap(),
	);
	let durable_replica = Arc::new(
		WendelonDataServer::new(
			durable_node.slab.clone(),
			durable_node.raft.clone(),
			durable_node.log_store.clone(),
			durable_node.state_machine.clone(),
			durable_id,
			durable_node.clock.clone(),
			router,
			build_factory(),
			durable_node.pool.clone(),
		)
		.unwrap(),
	);
	let durable_data_ring: Arc<dyn DataRingServer> = durable_replica;
	let durable_service: Arc<dyn HrftService> = Arc::new(
		MsgRouter::builder()
			.with_data_ring(durable_data_ring)
			.build(),
	);
	let crashy_dir = fresh_dir("crashy-replica");
	let crashy = CrashyReplicaService::open(crashy_dir.to_path_buf(), pool.clone()).await;
	let crashy_service: Arc<dyn HrftService> = crashy.clone();
	handlers.register(durable_id, durable_service);
	handlers.register(crashy_id, crashy_service);

	DurabilityFixture {
		leader,
		leader_slab: leader_node.slab.clone(),
		durable_slab: durable_node.slab.clone(),
		crashy,
		_leader_node: leader_node,
		_durable_node: durable_node,
		_epoch_tx: epoch_tx,
		_dirs: vec![leader_node_dir, durable_node_dir, crashy_dir],
	}
}

/// Plan 020 crash-model durability boundary.
///
/// One replica intentionally acks `APPEND_ONLY`, then the test
/// crashes it by truncating the unflushed tail and reopening the
/// slab manager from disk. The leader must ignore that ack for
/// quorum durability:
///
/// * a write with two real durable replicas (`leader` +
///   `durable`) returns `OK` and still survives after the crashy
///   replica loses its copy.
/// * a write with only `leader` durable plus the crashy
///   append-only replica does not return `OK`, and after the
///   simulated crash fewer than quorum copies remain.
#[tokio::test]
async fn write_counts_only_durable_quorum_acks_across_replica_crash_boundary() {
	let durable_key = b"durable-quorum";
	let append_only_key = b"append-only";
	let durable_key_locality = key_locality(durable_key);
	let append_only_key_locality = key_locality(append_only_key);
	let durable_key_identity = Identity::blake3_tenant(tenant(), durable_key);
	let append_only_key_identity = Identity::blake3_tenant(tenant(), append_only_key);
	let fx = build_durability_fixture(durable_key_locality, append_only_key_locality).await;

	let durable_value = b"v-durable";
	let mut durable_meta = simple_write_meta(1);
	durable_meta.write_id.sequence_no = 101;
	durable_meta.key_identity = durable_key_identity;
	durable_meta.key_len = durable_key.len() as u32;
	durable_meta.value_len = durable_value.len() as u32;
	let (durable_resp, _) = fx
		.leader
		.write(durable_meta, kv_batch(durable_key, durable_value))
		.await
		.unwrap();
	assert_eq!(durable_resp.status, WriteStatus::OK);

	fx.crashy.crash_and_recover().await;

	let crashy_after_ok = fx.crashy.manager().await;
	let durable_survivors = [
		fx.leader_slab
			.lookup(tenant(), durable_key_identity)
			.is_some(),
		fx.durable_slab
			.lookup(tenant(), durable_key_identity)
			.is_some(),
		crashy_after_ok
			.lookup(tenant(), durable_key_identity)
			.is_some(),
	]
	.into_iter()
	.filter(|present| *present)
	.count();
	assert_eq!(
		durable_survivors, 2,
		"only the two durably-acked replicas should retain the successful write"
	);
	assert!(
		!crashy_after_ok
			.lookup(tenant(), durable_key_identity)
			.is_some(),
		"append-only replica must lose its copy across the simulated crash"
	);

	let append_only_value = b"v-append-only";
	let mut append_only_meta = simple_write_meta(1);
	append_only_meta.write_id.sequence_no = 102;
	append_only_meta.key_identity = append_only_key_identity;
	append_only_meta.key_len = append_only_key.len() as u32;
	append_only_meta.value_len = append_only_value.len() as u32;
	let (append_only_resp, _) = fx
		.leader
		.write(
			append_only_meta,
			kv_batch(append_only_key, append_only_value),
		)
		.await
		.unwrap();
	assert_eq!(
		append_only_resp.status,
		WriteStatus::INTERNAL_ERROR,
		"leader must not treat append-only replica acks as quorum durable"
	);

	fx.crashy.crash_and_recover().await;

	let crashy_after_fail = fx.crashy.manager().await;
	let append_only_survivors = [
		fx.leader_slab
			.lookup(tenant(), append_only_key_identity)
			.is_some(),
		fx.durable_slab
			.lookup(tenant(), append_only_key_identity)
			.is_some(),
		crashy_after_fail
			.lookup(tenant(), append_only_key_identity)
			.is_some(),
	]
	.into_iter()
	.filter(|present| *present)
	.count();
	assert_eq!(
		append_only_survivors, 1,
		"without durable quorum, the write must not survive on a quorum after crash recovery"
	);
	shutdown_lease_node(fx._leader_node).await;
	shutdown_lease_node(fx._durable_node).await;
}

/// Three-node drain-all fixture.
///
/// * `dest` — the destination data server; its `pull_migration_range`
///   is what each test drives.
/// * `source_a_slab`, `source_b_slab` — seed writes directly
///   through these to populate each source with disjoint or
///   overlapping retained versions.
/// * `handlers` — fault-injection handle. Tests call
///   `fail_source_after_pages` to model mid-drain failures.
/// * `source_a_id`, `source_b_id` — NodeId fields for the
///   ledger entry seed.
struct DrainAllFixture {
	dest: Arc<WendelonDataServer>,
	source_a_slab: Arc<SlabManager>,
	source_b_slab: Arc<SlabManager>,
	handlers: Arc<LoopbackHandlers>,
	dest_id: NodeId,
	source_a_id: NodeId,
	source_b_id: NodeId,
	// Kept alive for the duration of the test so the tracked
	// state machines and factory cache don't get dropped.
	_nodes: Vec<WendelonNode>,
	_epoch_tx: watch::Sender<Arc<MembershipEpoch>>,
	_dirs: Vec<crate::test_support::TestDir>,
}

impl DrainAllFixture {
	/// Seed the same ledger entry on every node's state
	/// machine so each source's `handle_migrate` accepts the
	/// `MIGRATE_REQ` and the destination can reference it
	/// from `pull_migration_range`.
	async fn seed_migration(&self, entry: MigrationLedgerEntry) {
		for node in &self._nodes {
			node.state_machine
				.seed_migration_for_test(entry.clone())
				.await;
		}
		// And on the dest's SM — it is shared with the
		// originating node so the `for` above already covered
		// it, but assert anyway.
		let sm = self.dest.state_machine.state_snapshot().await;
		assert!(sm.migrations.contains_key(&entry.migration_id));
	}

	fn ledger_entry(
		&self,
		migration_id: u64,
		range_start: u64,
		range_end: u64,
	) -> MigrationLedgerEntry {
		MigrationLedgerEntry {
			migration_id,
			range_start,
			range_end,
			from_epoch: 0,
			to_epoch: 1,
			logical_owner: self.dest_id,
			upstream_migration_id: None,
			source_replicas: vec![self.source_a_id, self.source_b_id],
			migration_hlc_floor: hlc(0, 0, 0),
			state: MigrationState::Pending,
			completed_by: None,
			completed_at_hlc: None,
		}
	}
}

async fn build_drain_all_fixture(tag: &str) -> DrainAllFixture {
	// 1. Bootstrap three single-node clusters. Each brings its
	//    own slab, raft, state machine, and pool. We reuse
	//    those handles but rebuild the WendelonDataServer on
	//    top of a loopback-backed factory so the three nodes
	//    can talk to each other.
	let dest_node_dir = bootstrap_tempdir(&format!("{tag}-dest"));
	let dest_node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		31,
		dest_node_dir.to_path_buf(),
		"loopback://31".into(),
	))
	.await
	.unwrap();
	let source_a_node_dir = bootstrap_tempdir(&format!("{tag}-a"));
	let source_a_node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		32,
		source_a_node_dir.to_path_buf(),
		"loopback://32".into(),
	))
	.await
	.unwrap();
	let source_b_node_dir = bootstrap_tempdir(&format!("{tag}-b"));
	let source_b_node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		33,
		source_b_node_dir.to_path_buf(),
		"loopback://33".into(),
	))
	.await
	.unwrap();

	let dest_id = dest_node.local_node;
	let source_a_id = source_a_node.local_node;
	let source_b_id = source_b_node.local_node;
	let pool = dest_node.pool.clone();

	// 2. Shared loopback registry. Services are registered
	//    after each fresh DataServer is built below.
	let handlers = LoopbackHandlers::new(pool.clone());
	let loopback_provider: Arc<dyn TransportProvider> = Arc::new(LoopbackProvider {
		handlers: handlers.clone(),
		tier: TransportTier::QuicIoUring,
	});

	// 3. Membership epoch visible to every node's resolver —
	//    lists all three peers advertising QuicIoUring so the
	//    factory's tier negotiation picks the loopback
	//    provider.
	let tiers = TransportTierSet::empty().with(TransportTier::QuicIoUring);
	let roles = NodeRoleSet::empty().with(NodeRole::Data);
	let mut peers = StdHashMap::new();
	peers.insert(
		dest_id,
		PeerCapabilities {
			address: "loopback://31".into(),
			tiers,
			roles,
			capabilities: crate::network::transport::NodeCapabilitySet::empty(),
		},
	);
	peers.insert(
		source_a_id,
		PeerCapabilities {
			address: "loopback://32".into(),
			tiers,
			roles,
			capabilities: crate::network::transport::NodeCapabilitySet::empty(),
		},
	);
	peers.insert(
		source_b_id,
		PeerCapabilities {
			address: "loopback://33".into(),
			tiers,
			roles,
			capabilities: crate::network::transport::NodeCapabilitySet::empty(),
		},
	);
	let epoch = MembershipEpoch { epoch: 1, peers };
	let (epoch_tx, epoch_rx) = watch::channel(Arc::new(epoch));

	// 4. Build a shared factory per node (every node dials
	//    through the same loopback). The factory caches
	//    sessions per target, so all three nodes end up
	//    producing their own per-target connections.
	let resolver: Arc<dyn crate::network::transport::CapabilityResolver> = Arc::new(
		crate::network::transport::EpochCapabilityResolver::new(epoch_rx.clone()),
	);
	let mut providers: StdHashMap<TransportTier, Arc<dyn TransportProvider>> = StdHashMap::new();
	providers.insert(TransportTier::QuicIoUring, loopback_provider);

	// Each node gets its own factory so invalidation and
	// connection caches are independent — mirrors production.
	let build_factory = || {
		let mut ps = StdHashMap::new();
		ps.insert(
			TransportTier::QuicIoUring,
			providers
				.get(&TransportTier::QuicIoUring)
				.expect("provider registered above")
				.clone(),
		);
		TransportProviderFactory::new(tiers, ps, resolver.clone(), epoch_rx.clone())
	};
	let dest_factory = build_factory();
	let source_a_factory = build_factory();
	let source_b_factory = build_factory();

	// 5. Rebuild each DataServer on top of the loopback
	//    factory. We reuse the slab / raft / state machine /
	//    pool from the bootstrapped node, so the state
	//    machine (which carries the migration ledger) is the
	//    one the bootstrap originally registered.
	let router: Arc<dyn ReplicaRouter> = Arc::new(StaticReplicaRouter::new(
		vec![dest_id, source_a_id, source_b_id],
		1,
	));
	let dest_ds = Arc::new(
		WendelonDataServer::new(
			dest_node.slab.clone(),
			dest_node.raft.clone(),
			dest_node.log_store.clone(),
			dest_node.state_machine.clone(),
			dest_id,
			dest_node.clock.clone(),
			router.clone(),
			dest_factory,
			pool.clone(),
		)
		.unwrap(),
	);
	let source_a_ds = Arc::new(
		WendelonDataServer::new(
			source_a_node.slab.clone(),
			source_a_node.raft.clone(),
			source_a_node.log_store.clone(),
			source_a_node.state_machine.clone(),
			source_a_id,
			source_a_node.clock.clone(),
			router.clone(),
			source_a_factory,
			source_a_node.pool.clone(),
		)
		.unwrap(),
	);
	let source_b_ds = Arc::new(
		WendelonDataServer::new(
			source_b_node.slab.clone(),
			source_b_node.raft.clone(),
			source_b_node.log_store.clone(),
			source_b_node.state_machine.clone(),
			source_b_id,
			source_b_node.clock.clone(),
			router,
			source_b_factory,
			source_b_node.pool.clone(),
		)
		.unwrap(),
	);

	// 6. Wrap each DataServer behind MsgRouter so the
	//    harness hits the full dispatch path
	//    (`handle_frame` → `handle_data_frame` →
	//    `handle_migrate`) rather than calling
	//    `handle_migrate` directly. That covers the
	//    `MsgType::MIGRATE_REQ` arm for free.
	let dest_data_ring: Arc<dyn DataRingServer> = dest_ds.clone();
	let source_a_data_ring: Arc<dyn DataRingServer> = source_a_ds.clone();
	let source_b_data_ring: Arc<dyn DataRingServer> = source_b_ds.clone();
	let dest_svc: Arc<dyn HrftService> =
		Arc::new(MsgRouter::builder().with_data_ring(dest_data_ring).build());
	let source_a_svc: Arc<dyn HrftService> = Arc::new(
		MsgRouter::builder()
			.with_data_ring(source_a_data_ring)
			.build(),
	);
	let source_b_svc: Arc<dyn HrftService> = Arc::new(
		MsgRouter::builder()
			.with_data_ring(source_b_data_ring)
			.build(),
	);
	handlers.register(dest_id, dest_svc);
	handlers.register(source_a_id, source_a_svc);
	handlers.register(source_b_id, source_b_svc);

	DrainAllFixture {
		dest: dest_ds,
		source_a_slab: source_a_node.slab.clone(),
		source_b_slab: source_b_node.slab.clone(),
		handlers,
		dest_id,
		source_a_id,
		source_b_id,
		_nodes: vec![dest_node, source_a_node, source_b_node],
		_epoch_tx: epoch_tx,
		_dirs: vec![dest_node_dir, source_a_node_dir, source_b_node_dir],
	}
}

/// Directly append an Arrow-encoded key/value at `hlc` to a
/// source's slab. Bypasses the write path — we only need the
/// bytes on disk so the source's `handle_migrate` can scan
/// them.
async fn seed_source(slab: &SlabManager, locality: u64, hlc_v: HybridTimestamp, value: &[u8]) {
	let batch = kv_batch(&locality.to_le_bytes(), value);
	let bytes = encode_batch(&batch).unwrap();
	slab.append(tenant(), id(locality), hlc_v, &bytes)
		.await
		.unwrap();
}

/// Regression for drain-all union over HRFT.
///
/// source_a holds keys 0..50, source_b holds keys 50..100.
/// After `pull_migration_range`, the destination slab must
/// contain every key in 0..100 exactly once — proving the
/// pull loop walks *every* entry in `source_replicas` and
/// unions their contributions via the real wire format.
#[tokio::test]
async fn pull_migration_range_unions_disjoint_halves_over_hrft() {
	let fx = build_drain_all_fixture("disjoint").await;
	let v = hlc(10, 0, 1);
	for k in 0..50u64 {
		seed_source(&fx.source_a_slab, k, v, b"v-a").await;
	}
	for k in 50..100u64 {
		seed_source(&fx.source_b_slab, k, v, b"v-b").await;
	}

	let entry = fx.ledger_entry(1001, 0, 99);
	fx.seed_migration(entry.clone()).await;

	fx.dest
		.pull_migration_range(&entry, 0, 99)
		.await
		.expect("drain-all must succeed when both sources are reachable");

	let merged = fx.dest.slab.scan_as_of(tenant(), 0, 99, v);
	let mut keys: Vec<u64> = merged.iter().map(|(k, _)| *k).collect();
	keys.sort();
	keys.dedup();
	assert_eq!(keys.len(), 100, "union must cover every key in 0..=99");
	assert_eq!(*keys.first().unwrap(), 0);
	assert_eq!(*keys.last().unwrap(), 99);
	for node in fx._nodes {
		shutdown_lease_node(node).await;
	}
}

/// Regression for idempotent union: overlapping sources
/// re-emit the same `(key_identity, hlc, payload_hash)` tuples;
/// `append_migration` must discard the duplicate after the
/// first source ingested it. Proves the correctness boundary
/// holds when the wire delivers overlapping rows.
#[tokio::test]
async fn pull_migration_range_idempotent_across_overlapping_sources() {
	let fx = build_drain_all_fixture("overlap").await;
	let v = hlc(42, 0, 1);
	for k in 0..75u64 {
		seed_source(&fx.source_a_slab, k, v, b"shared").await;
	}
	for k in 25..100u64 {
		seed_source(&fx.source_b_slab, k, v, b"shared").await;
	}

	let entry = fx.ledger_entry(1002, 0, 99);
	fx.seed_migration(entry.clone()).await;

	fx.dest
		.pull_migration_range(&entry, 0, 99)
		.await
		.expect("drain-all with overlap must succeed");

	let merged = fx.dest.slab.scan_as_of(tenant(), 0, 99, v);
	let mut keys: Vec<u64> = merged.iter().map(|(k, _)| *k).collect();
	keys.sort();
	keys.dedup();
	assert_eq!(
		keys.len(),
		100,
		"overlap union must still resolve to 100 distinct keys, not 75+75"
	);

	// Every key should resolve to exactly one retained version.
	for &k in &keys {
		let versions = fx.dest.slab.scan_all_versions(k, k, hlc(0, 0, 0));
		assert_eq!(
			versions.len(),
			1,
			"key {k}: overlap must not duplicate versions"
		);
	}
	for node in fx._nodes {
		shutdown_lease_node(node).await;
	}
}

/// Regression for `RANGE_PAGE` paging within a single source.
///
/// Shrink the page caps so even a modest record count forces
/// multiple round-trips. The pull must advance the
/// `(cursor_hash, cursor_hlc)` correctly between pages and
/// deliver every record exactly once. The loopback page
/// counter assertion guards against a regression in which the
/// cap override is silently ignored — without it the coverage
/// check passes even if the source returns all 25 rows in one
/// MIGRATE_RESP.
#[tokio::test]
async fn pull_migration_range_pages_through_single_source() {
	let fx = build_drain_all_fixture("paging").await;
	// Tiny cap → 25 keys / 10 per page = 3 round-trips.
	fx.dest.set_page_caps_for_test(10, 64 * 1024);

	let v = hlc(7, 0, 1);
	for k in 0..25u64 {
		seed_source(&fx.source_a_slab, k, v, b"p").await;
	}
	// source_b stays empty — exercises the
	// empty-page/has_more=0 branch in one round-trip.

	let entry = fx.ledger_entry(1003, 0, 24);
	fx.seed_migration(entry.clone()).await;

	fx.dest
		.pull_migration_range(&entry, 0, 24)
		.await
		.expect("paged drain must succeed");

	let merged = fx.dest.slab.scan_as_of(tenant(), 0, 24, v);
	let mut keys: Vec<u64> = merged.iter().map(|(k, _)| *k).collect();
	keys.sort();
	keys.dedup();
	assert_eq!(
		keys.len(),
		25,
		"paged drain must deliver every record across multiple pages"
	);

	// 25 keys at cap 10 ⇒ 3 pages from source_a (10 + 10 + 5).
	// The empty source_b adds exactly 1 page (has_more=0).
	let pages_a = fx.handlers.page_count_for(fx.source_a_id);
	assert!(
		pages_a >= 3,
		"paging cap must force ≥3 MIGRATE_REQ to source_a; observed {pages_a}"
	);
	assert_eq!(
		fx.handlers.page_count_for(fx.source_b_id),
		1,
		"empty source_b should be drained in a single round-trip"
	);
	for node in fx._nodes {
		shutdown_lease_node(node).await;
	}
}

/// Regression for mid-drain failure surfacing through the
/// merge path.
///
/// source_b is configured to fail on its *second* page after
/// page 1 succeeded. The end-to-end query must surface
/// `ReadStatus::MIGRATION_INCOMPLETE` — a distinct branch from
/// the unreachable-source case already covered by
/// `query_merge_surfaces_migration_incomplete_on_source_failure`,
/// because here the source is reachable, accepts page 1, and
/// only fails partway through. Driving
/// `execute_query_with_migration_merge` (rather than calling
/// `pull_migration_range` directly) is the point: it proves
/// the mid-drain `Err` propagates all the way to the
/// client-visible status, not just the internal pull return.
#[tokio::test]
async fn query_merge_surfaces_migration_incomplete_on_partial_source_failure() {
	let fx = build_drain_all_fixture("partial-fail").await;
	// Small cap so source_b produces >1 page.
	fx.dest.set_page_caps_for_test(5, 64 * 1024);

	let v = hlc(11, 0, 1);
	// source_a exhausts in one page (3 keys, cap 5) — its
	// drain completes before source_b is attempted.
	for k in 0..3u64 {
		seed_source(&fx.source_a_slab, k, v, b"a").await;
	}
	// source_b has 12 keys → 3 pages under cap 5. Page 1
	// succeeds, page 2 is intercepted with INTERNAL_ERROR.
	for k in 100..112u64 {
		seed_source(&fx.source_b_slab, k, v, b"b").await;
	}

	let entry = fx.ledger_entry(1004, 0, u64::MAX);
	fx.seed_migration(entry.clone()).await;

	// Allow source_b exactly one successful page, then fail.
	fx.handlers.fail_source_after_pages(fx.source_b_id, 1);

	let (resp, _arrow) = fx
		.dest
		.execute_query_with_migration_merge(&base_meta(), &[])
		.await
		.expect("merge path returns Ok wrapping a failure status");
	assert_eq!(
		resp.status,
		ReadStatus::MIGRATION_INCOMPLETE,
		"mid-drain failure must surface MIGRATION_INCOMPLETE at the query layer"
	);

	// Confirm the fault model held: source_b saw at least 2
	// page attempts (page 1 OK + page 2 intercepted).
	let pages_b = fx.handlers.page_count_for(fx.source_b_id);
	assert!(
		pages_b >= 2,
		"source_b must have been asked for ≥2 pages before failing; observed {pages_b}"
	);
	for node in fx._nodes {
		shutdown_lease_node(node).await;
	}
}

/// Regression for the `source_replicas` self-skip.
///
/// If the destination is listed among the sources, the pull
/// must not dial itself. We prove this by configuring the
/// destination's own entry in the loopback to fail every
/// `MIGRATE_REQ` — if the loop dialed itself the pull would
/// return `Err`, but the self-skip means the failure is
/// never reached and the pull succeeds over source_a alone.
#[tokio::test]
async fn pull_migration_range_skips_local_node_in_source_list() {
	let fx = build_drain_all_fixture("skip-local").await;
	let v = hlc(3, 0, 1);
	for k in 0..5u64 {
		seed_source(&fx.source_a_slab, k, v, b"a").await;
	}

	let mut entry = fx.ledger_entry(1005, 0, 4);
	// Put dest_id at the head of source_replicas to prove the
	// skip logic fires even when we're the first candidate.
	entry.source_replicas = vec![fx.dest_id, fx.source_a_id];
	fx.seed_migration(entry.clone()).await;

	// Arm the self-loopback to fail immediately, so a stray
	// self-dial would surface as a pull Err.
	fx.handlers.fail_source_after_pages(fx.dest_id, 0);

	fx.dest
		.pull_migration_range(&entry, 0, 4)
		.await
		.expect("self must be skipped; pull succeeds via source_a");

	let merged = fx.dest.slab.scan_as_of(tenant(), 0, 4, v);
	let mut keys: Vec<u64> = merged.iter().map(|(k, _)| *k).collect();
	keys.sort();
	assert_eq!(keys, vec![0, 1, 2, 3, 4]);
	for node in fx._nodes {
		shutdown_lease_node(node).await;
	}
}

#[tokio::test]
async fn dispatcher_rejects_cross_tenant_write_before_handler() {
	let dir = bootstrap_tempdir("auth-write");
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		21,
		dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();

	let mut meta = simple_write_meta(1);
	meta.tenant_id = TenantId::new(9);
	let payload = encode_batch(&single_kv_batch()).unwrap();
	let mut meta_buf = node.pool.acquire();
	meta_buf
		.as_mut_vec()
		.extend_from_slice(bytemuck::bytes_of(&meta));
	let mut arrow_buf = node.pool.acquire();
	arrow_buf.as_mut_vec().extend_from_slice(&payload);
	let frame = HrftFrame {
		header: FrameHeader {
			magic: HRFT_MAGIC,
			version: 1,
			msg_type: MsgType::WRITE_REQ.0,
			flags: 0,
			stream_id: 1,
			meta_len: meta_buf.len() as u32,
			arrow_len: arrow_buf.len() as u32,
			trailing_len: 0,
		},
		meta: meta_buf,
		arrow_payload: Some(arrow_buf),
		trailing: None,
	};

	let resp = DataRingServer::handle_data_frame(
		node.data_server.as_ref(),
		RequestContext {
			remote: Some(node.local_node),
			actor: actor(ActorRole::Operator, Some("7")),
		},
		frame,
	)
	.await
	.unwrap()
	.unwrap();
	let meta: WriteResponseMeta =
		*bytemuck::from_bytes(&resp.meta[..std::mem::size_of::<WriteResponseMeta>()]);
	assert_eq!(meta.status, WriteStatus::AUTHORIZATION_FAILED);

	shutdown_lease_node(node).await;
}

// ADR 075 cut-3b: COUNTER_REPLICATE_REQ dispatch appends a tentative AND
// leaves NO pending-finalization witness (the no-witness behavioral guard —
// it must not ride ordinary replication semantics). Restart-skips-orphan is
// proven at the service level (CounterReplicaService tests).
#[tokio::test]
async fn counter_replicate_dispatch_appends_tentative_with_no_witness() {
	let dir = bootstrap_tempdir("counter-replicate");
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		31,
		dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();

	let key_identity = crate::storage::counter::counter_identity(
		&crate::storage::counter::CounterKey(b"evidence".to_vec()),
	);
	let meta = CounterReplicateRequestMeta {
		commit_hlc: HybridTimestamp {
			physical_ms: 1,
			logical: 0,
			node_id: 0,
		},
		op_id: 1,
		value: 5,
		epoch: 0,
		tenant_id: TenantId::new(7),
		key_identity,
		phase: CounterReplicaPhase::TENTATIVE,
		_pad: [0; 7],
	};
	let mut meta_buf = node.pool.acquire();
	meta_buf
		.as_mut_vec()
		.extend_from_slice(bytemuck::bytes_of(&meta));
	let frame = HrftFrame {
		header: FrameHeader {
			magic: HRFT_MAGIC,
			version: 1,
			msg_type: MsgType::COUNTER_REPLICATE_REQ.0,
			flags: 0,
			stream_id: 1,
			meta_len: meta_buf.len() as u32,
			arrow_len: 0,
			trailing_len: 0,
		},
		meta: meta_buf,
		arrow_payload: None,
		trailing: None,
	};
	let resp = DataRingServer::handle_data_frame(
		node.data_server.as_ref(),
		RequestContext {
			remote: Some(node.local_node),
			actor: ActorIdentity::data_node(node.local_node),
		},
		frame,
	)
	.await
	.unwrap()
	.unwrap();
	assert_eq!(resp.header.msg_type, MsgType::COUNTER_REPLICATE_RESP.0);
	let ack: CounterReplicaAck =
		*bytemuck::from_bytes(&resp.meta[..std::mem::size_of::<CounterReplicaAck>()]);
	assert_eq!(ack.status, CounterReplicaStatus::OK);
	assert_eq!(ack.op_id, 1);
	// The no-witness guard: a counter replicate creates NO finalization
	// witness (an ordinary REPLICATE_REQ would).
	assert_eq!(
		node.data_server.pending_finalization_count_for_test().await,
		0,
		"counter replicate must not create a pending-finalization witness"
	);

	shutdown_lease_node(node).await;
}

// ADR 075 cut-3b: COUNTER_ROLLBACK_REQ dispatch acks OK; rollback of an
// unknown op is an idempotent no-op OK (and creates no witness).
#[tokio::test]
async fn counter_rollback_dispatch_acks_and_is_idempotent() {
	let dir = bootstrap_tempdir("counter-rollback");
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		32,
		dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();

	let meta = CounterRollbackRequestMeta {
		key_identity: crate::storage::counter::counter_identity(
			&crate::storage::counter::CounterKey(b"evidence".to_vec()),
		),
		op_id: 99,
		epoch: 0,
		tenant_id: TenantId::new(7),
	};
	let mut meta_buf = node.pool.acquire();
	meta_buf
		.as_mut_vec()
		.extend_from_slice(bytemuck::bytes_of(&meta));
	let frame = HrftFrame {
		header: FrameHeader {
			magic: HRFT_MAGIC,
			version: 1,
			msg_type: MsgType::COUNTER_ROLLBACK_REQ.0,
			flags: 0,
			stream_id: 1,
			meta_len: meta_buf.len() as u32,
			arrow_len: 0,
			trailing_len: 0,
		},
		meta: meta_buf,
		arrow_payload: None,
		trailing: None,
	};
	let resp = DataRingServer::handle_data_frame(
		node.data_server.as_ref(),
		RequestContext {
			remote: Some(node.local_node),
			actor: ActorIdentity::data_node(node.local_node),
		},
		frame,
	)
	.await
	.unwrap()
	.unwrap();
	assert_eq!(resp.header.msg_type, MsgType::COUNTER_ROLLBACK_RESP.0);
	let ack: CounterReplicaAck =
		*bytemuck::from_bytes(&resp.meta[..std::mem::size_of::<CounterReplicaAck>()]);
	assert_eq!(
		ack.status,
		CounterReplicaStatus::OK,
		"rollback of an unknown op is an idempotent OK"
	);
	assert_eq!(
		node.data_server.pending_finalization_count_for_test().await,
		0
	);

	shutdown_lease_node(node).await;
}

/// The UUIDv7 client uuid the counter-add fixtures write under for the node
/// whose data directory is `data_dir`: minted when that node first adds, so a
/// retry (after a restart over the same directory too) is the same write id,
/// and its age is the test's, never the process's (E0, E1).
fn counter_test_client_uuid(data_dir: &std::path::Path) -> [u8; 16] {
	static MINTED: std::sync::OnceLock<
		std::sync::Mutex<std::collections::HashMap<std::path::PathBuf, [u8; 16]>>,
	> = std::sync::OnceLock::new();
	*MINTED
		.get_or_init(Default::default)
		.lock()
		.unwrap()
		.entry(data_dir.to_path_buf())
		.or_insert_with(|| {
			let now_ms = crate::consensus::monitor::SystemClock.now_ms();
			let write_key = crate::storage::counter::test_write_key_at(now_ms, 0);
			write_key[..16].try_into().expect("16-byte client uuid")
		})
}

/// Build + dispatch one `COUNTER_ADD_REQ` (key `b"k"`, `seq`/`delta`) and return
/// the parsed response meta.
async fn dispatch_counter_add(
	node: &WendelonNode,
	seq: u64,
	delta: u64,
	key_identity: Identity,
) -> CounterAddResponseMeta {
	let client_uuid = counter_test_client_uuid(node.data_server.slab.data_dir());
	dispatch_counter_add_as(node, client_uuid, seq, delta, key_identity).await
}

/// [`dispatch_counter_add`] under the caller's `client_uuid`.
async fn dispatch_counter_add_as(
	node: &WendelonNode,
	client_uuid: [u8; 16],
	seq: u64,
	delta: u64,
	key_identity: Identity,
) -> CounterAddResponseMeta {
	let payload = encode_batch(&single_key_batch()).unwrap();
	let meta = CounterAddRequestMeta {
		write_id: ClientWriteId {
			client_uuid,
			sequence_no: seq,
			idempotency_ttl_ms: 60_000,
			_pad: 0,
		},
		client_hlc: hlc(10, 0, 0),
		expected_epoch: 0,
		tenant_id: tenant(),
		proxy_hop: 0,
		durability: DurabilityLevel::QUORUM,
		await_finalization: 0,
		_pad1: [0; 5],
		proxy_source: NodeId {
			id: 0,
			_reserved: 0,
		},
		key_identity,
		key_len: 1,
		_pad2: [0; 4],
		delta,
	};
	let mut meta_buf = node.pool.acquire();
	meta_buf
		.as_mut_vec()
		.extend_from_slice(bytemuck::bytes_of(&meta));
	let mut arrow_buf = node.pool.acquire();
	arrow_buf.as_mut_vec().extend_from_slice(&payload);
	let frame = HrftFrame {
		header: FrameHeader {
			magic: HRFT_MAGIC,
			version: 1,
			msg_type: MsgType::COUNTER_ADD_REQ.0,
			flags: 0,
			stream_id: 1,
			meta_len: meta_buf.len() as u32,
			arrow_len: arrow_buf.len() as u32,
			trailing_len: 0,
		},
		meta: meta_buf,
		arrow_payload: Some(arrow_buf),
		trailing: None,
	};
	let resp = DataRingServer::handle_data_frame(
		node.data_server.as_ref(),
		RequestContext {
			remote: Some(node.local_node),
			actor: actor(ActorRole::Operator, None),
		},
		frame,
	)
	.await
	.unwrap()
	.unwrap();
	assert_eq!(resp.header.msg_type, MsgType::COUNTER_ADD_RESP.0);
	*bytemuck::from_bytes(&resp.meta[..std::mem::size_of::<CounterAddResponseMeta>()])
}

// ADR 075 cut-4: the public COUNTER_ADD_REQ path end-to-end on a single node.
// The dispatch drives the per-tenant CounterLeader over the real local replica;
// a distinct write_id accumulates, and a duplicate write_id replays the original
// previous/current WITHOUT re-applying (D4 — public idempotency on the server).
#[tokio::test]
async fn counter_add_dispatch_applies_accumulates_and_replays_idempotently() {
	let dir = bootstrap_tempdir("counter-add");
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		33,
		dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();
	let key_identity = crate::storage::counter::counter_identity(
		&crate::storage::counter::CounterKey(b"k".to_vec()),
	);

	// First add on an absent counter: 0 -> 5 (D6 bases at 0, creates it).
	let r1 = dispatch_counter_add(&node, 1, 5, key_identity).await;
	assert_eq!(r1.status, WriteStatus::OK);
	assert_eq!((r1.previous_value, r1.current_value), (0, 5));

	// A distinct write_id accumulates on the same key: 5 -> 8.
	let r2 = dispatch_counter_add(&node, 2, 3, key_identity).await;
	assert_eq!(r2.status, WriteStatus::OK);
	assert_eq!((r2.previous_value, r2.current_value), (5, 8));

	// A duplicate of the FIRST write_id replays the original (0 -> 5) as DUPLICATE.
	let dup = dispatch_counter_add(&node, 1, 5, key_identity).await;
	assert_eq!(dup.status, WriteStatus::DUPLICATE);
	assert_eq!((dup.previous_value, dup.current_value), (0, 5));

	// ...and it did NOT re-apply: a fresh add still sees base 8 (not 13).
	let r3 = dispatch_counter_add(&node, 3, 1, key_identity).await;
	assert_eq!(r3.status, WriteStatus::OK);
	assert_eq!(
		(r3.previous_value, r3.current_value),
		(8, 9),
		"the duplicate replayed without re-applying its delta"
	);

	shutdown_lease_node(node).await;
}

// ADR 075 cut-4b / D5: a zero-delta COUNTER_ADD is rejected and mutates nothing
// (the counter stays absent, so a later real add still bases at 0).
#[tokio::test]
async fn counter_add_zero_delta_rejects_without_creating() {
	let dir = bootstrap_tempdir("counter-add-zero");
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		34,
		dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();
	let key_identity = crate::storage::counter::counter_identity(
		&crate::storage::counter::CounterKey(b"k".to_vec()),
	);

	let zd = dispatch_counter_add(&node, 1, 0, key_identity).await;
	assert_eq!(zd.status, WriteStatus::COUNTER_ZERO_DELTA);

	// The counter was never created — a real add still bases at 0.
	let ok = dispatch_counter_add(&node, 2, 7, key_identity).await;
	assert_eq!(ok.status, WriteStatus::OK);
	assert_eq!(
		(ok.previous_value, ok.current_value),
		(0, 7),
		"the zero-delta reject created nothing"
	);

	shutdown_lease_node(node).await;
}

// ADR 075 cut-4b / D5: an overflowing COUNTER_ADD is rejected WITHOUT mutating —
// no wrap, no saturate. Proven by a following non-overflowing add that still sees
// the pre-overflow base (10), not a wrapped value.
#[tokio::test]
async fn counter_add_overflow_rejects_without_mutating() {
	let dir = bootstrap_tempdir("counter-add-overflow");
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		35,
		dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();
	let key_identity = crate::storage::counter::counter_identity(
		&crate::storage::counter::CounterKey(b"k".to_vec()),
	);

	let base = dispatch_counter_add(&node, 1, 10, key_identity).await;
	assert_eq!((base.previous_value, base.current_value), (0, 10));

	// 10 + u64::MAX overflows -> reject, no mutation.
	let ovf = dispatch_counter_add(&node, 2, u64::MAX, key_identity).await;
	assert_eq!(ovf.status, WriteStatus::COUNTER_OVERFLOW);

	// The base is intact at 10 (not wrapped/saturated): 10 + 5 = 15.
	let after = dispatch_counter_add(&node, 3, 5, key_identity).await;
	assert_eq!(after.status, WriteStatus::OK);
	assert_eq!(
		(after.previous_value, after.current_value),
		(10, 15),
		"the overflow reject left the counter unmutated"
	);

	shutdown_lease_node(node).await;
}

/// Build one `COUNTER_ADD_REQ` frame (key `b"k"`) without dispatching it — for
/// the concurrent fixtures that fan many frames out at once. Takes the pool
/// directly so a spawned (`'static`) task can rebuild a frame on retry.
fn counter_add_frame(
	pool: &Arc<BufPool>,
	client_uuid: [u8; 16],
	seq: u64,
	delta: u64,
	key_identity: Identity,
) -> HrftFrame {
	let payload = encode_batch(&single_key_batch()).unwrap();
	let meta = CounterAddRequestMeta {
		write_id: ClientWriteId {
			client_uuid,
			sequence_no: seq,
			idempotency_ttl_ms: 60_000,
			_pad: 0,
		},
		client_hlc: hlc(10, 0, 0),
		expected_epoch: 0,
		tenant_id: tenant(),
		proxy_hop: 0,
		durability: DurabilityLevel::QUORUM,
		await_finalization: 0,
		_pad1: [0; 5],
		proxy_source: NodeId {
			id: 0,
			_reserved: 0,
		},
		key_identity,
		key_len: 1,
		_pad2: [0; 4],
		delta,
	};
	let mut meta_buf = pool.acquire();
	meta_buf
		.as_mut_vec()
		.extend_from_slice(bytemuck::bytes_of(&meta));
	let mut arrow_buf = pool.acquire();
	arrow_buf.as_mut_vec().extend_from_slice(&payload);
	HrftFrame {
		header: FrameHeader {
			magic: HRFT_MAGIC,
			version: 1,
			msg_type: MsgType::COUNTER_ADD_REQ.0,
			flags: 0,
			stream_id: seq as u32,
			meta_len: meta_buf.len() as u32,
			arrow_len: arrow_buf.len() as u32,
			trailing_len: 0,
		},
		meta: meta_buf,
		arrow_payload: Some(arrow_buf),
		trailing: None,
	}
}

/// Dispatch a `COUNTER_ADD` (key `b"k"`, `seq`/`delta`) against a shared server,
/// retrying on transient `BACKPRESSURED` admission (pre-handler, never applied —
/// so a retry with the same idempotent `write_id` lands the add exactly once).
async fn dispatch_counter_add_shared(
	server: Arc<WendelonDataServer>,
	pool: Arc<BufPool>,
	local: NodeId,
	seq: u64,
	delta: u64,
	key_identity: Identity,
) -> CounterAddResponseMeta {
	loop {
		let client_uuid = counter_test_client_uuid(server.slab.data_dir());
		let frame = counter_add_frame(&pool, client_uuid, seq, delta, key_identity);
		let resp = DataRingServer::handle_data_frame(
			server.as_ref(),
			RequestContext {
				remote: Some(local),
				actor: actor(ActorRole::Operator, None),
			},
			frame,
		)
		.await
		.unwrap()
		.unwrap();
		let meta: CounterAddResponseMeta =
			*bytemuck::from_bytes(&resp.meta[..std::mem::size_of::<CounterAddResponseMeta>()]);
		if meta.status == WriteStatus::BACKPRESSURED {
			tokio::time::sleep(Duration::from_millis(1)).await;
			continue;
		}
		break meta;
	}
}

struct CounterReplicationFixture {
	leader_node: WendelonNode,
	follower_node: WendelonNode,
	leader: Arc<WendelonDataServer>,
	follower: Arc<WendelonDataServer>,
	_dirs: Vec<crate::test_support::TestDir>,
}

async fn build_counter_replication_fixture(
	tag: &str,
	key_identity: Identity,
) -> CounterReplicationFixture {
	build_counter_replication_fixture_with_remote_registration(tag, key_identity, true).await
}

async fn build_counter_replication_fixture_with_remote_registration(
	tag: &str,
	key_identity: Identity,
	register_follower: bool,
) -> CounterReplicationFixture {
	let leader_node_dir = bootstrap_tempdir(&format!("{tag}-leader"));
	let leader_node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		43,
		leader_node_dir.to_path_buf(),
		"loopback://43".into(),
	))
	.await
	.unwrap();
	let follower_node_dir = bootstrap_tempdir(&format!("{tag}-follower"));
	let follower_node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		44,
		follower_node_dir.to_path_buf(),
		"loopback://44".into(),
	))
	.await
	.unwrap();
	let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
	loop {
		if leader_node.raft.current_leader().await == Some(leader_node.local_node.id)
			&& follower_node.raft.current_leader().await == Some(follower_node.local_node.id)
		{
			break;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for single-node leaders"
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}

	let pool = leader_node.pool.clone();
	let handlers = LoopbackHandlers::new(pool.clone());
	handlers
		.senders
		.insert(follower_node.local_node.id, leader_node.local_node);
	let loopback_provider: Arc<dyn TransportProvider> = Arc::new(LoopbackProvider {
		handlers: handlers.clone(),
		tier: TransportTier::QuicIoUring,
	});
	let tiers = TransportTierSet::empty().with(TransportTier::QuicIoUring);
	let roles = NodeRoleSet::empty().with(NodeRole::Data);
	let mut peers = StdHashMap::new();
	for (node_id, address) in [
		(leader_node.local_node, "loopback://43"),
		(follower_node.local_node, "loopback://44"),
	] {
		peers.insert(
			node_id,
			PeerCapabilities {
				address: address.into(),
				tiers,
				roles,
				capabilities: crate::network::transport::NodeCapabilitySet::empty(),
			},
		);
	}
	let epoch = MembershipEpoch { epoch: 0, peers };
	let (_epoch_tx, epoch_rx) = watch::channel(Arc::new(epoch));
	let resolver: Arc<dyn crate::network::transport::CapabilityResolver> = Arc::new(
		crate::network::transport::EpochCapabilityResolver::new(epoch_rx.clone()),
	);
	let build_factory = || {
		let mut providers: StdHashMap<TransportTier, Arc<dyn TransportProvider>> =
			StdHashMap::new();
		providers.insert(TransportTier::QuicIoUring, loopback_provider.clone());
		TransportProviderFactory::new(tiers, providers, resolver.clone(), epoch_rx.clone())
	};
	let route = vec![leader_node.local_node, follower_node.local_node];
	let router: Arc<dyn ReplicaRouter> = Arc::new(
		FixedReplicaRouter::new(route.clone()).with_route(Locality::from(key_identity), route),
	);

	let leader = Arc::new(
		WendelonDataServer::new(
			leader_node.slab.clone(),
			leader_node.raft.clone(),
			leader_node.log_store.clone(),
			leader_node.state_machine.clone(),
			leader_node.local_node,
			leader_node.clock.clone(),
			router.clone(),
			build_factory(),
			pool.clone(),
		)
		.unwrap(),
	);
	let follower = Arc::new(
		WendelonDataServer::new(
			follower_node.slab.clone(),
			follower_node.raft.clone(),
			follower_node.log_store.clone(),
			follower_node.state_machine.clone(),
			follower_node.local_node,
			follower_node.clock.clone(),
			router,
			build_factory(),
			follower_node.pool.clone(),
		)
		.unwrap(),
	);
	if register_follower {
		handlers.register(
			follower_node.local_node,
			Arc::new(
				MsgRouter::builder()
					.with_data_ring(follower.clone())
					.build(),
			),
		);
	}

	CounterReplicationFixture {
		leader_node,
		follower_node,
		leader,
		follower,
		_dirs: vec![leader_node_dir, follower_node_dir],
	}
}

// ADR 076: a healthy replicated COUNTER_ADD uses the key's current replica
// set, explicitly treats the local primary as the local arm, and sends the
// remote arm over the counter-specific replicate/commit wire. This is the
// regression guard for retiring the old rf>1 unsupported gate.
#[tokio::test]
async fn counter_add_primary_replicates_to_remote_participant() {
	let key_identity = crate::storage::counter::counter_identity(
		&crate::storage::counter::CounterKey(b"k".to_vec()),
	);
	let fx = build_counter_replication_fixture("counter-replicated-primary", key_identity).await;

	let resp = dispatch_counter_add_shared(
		fx.leader.clone(),
		fx.leader_node.pool.clone(),
		fx.leader_node.local_node,
		1,
		5,
		key_identity,
	)
	.await;
	assert_eq!(resp.status, WriteStatus::OK, "got resp={resp:?}");
	assert_eq!((resp.previous_value, resp.current_value), (0, 5));
	assert_eq!(
		fx.leader
			.counter_replica
			.read_committed(tenant(), key_identity)
			.await
			.unwrap(),
		Some(5),
		"primary local arm committed the add"
	);
	assert_eq!(
		fx.follower
			.counter_replica
			.read_committed(tenant(), key_identity)
			.await
			.unwrap(),
		Some(5),
		"remote participant received the counter commit marker"
	);

	shutdown_lease_node(fx.leader_node).await;
	shutdown_lease_node(fx.follower_node).await;
}

// ADR 076 required fixture: rf=2 must not return primary-only success. If
// the remote participant cannot be reached, the local tentative is rolled
// back and the client observes a closed failure rather than OK.
#[tokio::test]
async fn no_primary_only_success_under_replication() {
	let key_identity = crate::storage::counter::counter_identity(
		&crate::storage::counter::CounterKey(b"k".to_vec()),
	);
	let fx = build_counter_replication_fixture_with_remote_registration(
		"counter-replicated-primary-only",
		key_identity,
		false,
	)
	.await;

	let resp = dispatch_counter_add_shared(
		fx.leader.clone(),
		fx.leader_node.pool.clone(),
		fx.leader_node.local_node,
		1,
		5,
		key_identity,
	)
	.await;
	assert_eq!(
		resp.status,
		WriteStatus::INTERNAL_ERROR,
		"got resp={resp:?}"
	);
	assert_eq!(
		(resp.previous_value, resp.current_value),
		(0, 0),
		"failed replicated counter add must not report an applied outcome"
	);
	assert_eq!(
		fx.leader
			.counter_replica
			.read_committed(tenant(), key_identity)
			.await
			.unwrap(),
		None,
		"primary-only tentative was rolled back instead of becoming committed"
	);
	assert_eq!(
		fx.follower
			.counter_replica
			.read_committed(tenant(), key_identity)
			.await
			.unwrap(),
		None,
		"unreachable remote did not receive a committed counter value"
	);

	shutdown_lease_node(fx.leader_node).await;
	shutdown_lease_node(fx.follower_node).await;
}

// ADR 076 headline: rf=2 over two live data services, with the remote arm
// reached through the counter-specific HRFT loopback transport. Concurrent
// unique write ids sum exactly; a duplicate write id replays the original
// previous/current instead of adding again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replicated_counter_fetch_add_concurrent_adds_sum_exactly() {
	let key_identity = crate::storage::counter::counter_identity(
		&crate::storage::counter::CounterKey(b"k".to_vec()),
	);
	let fx = build_counter_replication_fixture("counter-replicated-concurrent", key_identity).await;

	const N: u64 = 16;
	let mut handles = Vec::new();
	for seq in 1..=N {
		handles.push(tokio::spawn(dispatch_counter_add_shared(
			fx.leader.clone(),
			fx.leader_node.pool.clone(),
			fx.leader_node.local_node,
			seq,
			1,
			key_identity,
		)));
	}
	let mut seq1 = None;
	for (idx, handle) in handles.into_iter().enumerate() {
		let meta = handle.await.unwrap();
		assert_eq!(meta.status, WriteStatus::OK, "got resp={meta:?}");
		if idx == 0 {
			seq1 = Some(meta);
		}
	}

	let duplicate = dispatch_counter_add_shared(
		fx.leader.clone(),
		fx.leader_node.pool.clone(),
		fx.leader_node.local_node,
		1,
		1,
		key_identity,
	)
	.await;
	let seq1 = seq1.expect("seq 1 response captured");
	assert_eq!(duplicate.status, WriteStatus::DUPLICATE);
	assert_eq!(
		(duplicate.previous_value, duplicate.current_value),
		(seq1.previous_value, seq1.current_value),
		"duplicate write id replayed the original rf=2 outcome"
	);

	let final_add = dispatch_counter_add_shared(
		fx.leader.clone(),
		fx.leader_node.pool.clone(),
		fx.leader_node.local_node,
		N + 1,
		1,
		key_identity,
	)
	.await;
	assert_eq!(final_add.status, WriteStatus::OK);
	assert_eq!(
		(final_add.previous_value, final_add.current_value),
		(N, N + 1),
		"concurrent rf=2 adds summed exactly"
	);
	assert_eq!(
		fx.follower
			.counter_replica
			.read_committed(tenant(), key_identity)
			.await
			.unwrap(),
		Some(N + 1),
		"remote replica committed the final counter value"
	);

	shutdown_lease_node(fx.leader_node).await;
	shutdown_lease_node(fx.follower_node).await;
}

// ADR 076: a non-primary server does not mutate, even when it is in the
// replica set. It returns NOT_LEADER with the ring primary as the owner hint.
#[tokio::test]
async fn counter_add_non_primary_returns_owner_hint_without_mutating() {
	let key_identity = crate::storage::counter::counter_identity(
		&crate::storage::counter::CounterKey(b"k".to_vec()),
	);
	let fx = build_counter_replication_fixture("counter-replicated-nonprimary", key_identity).await;

	let resp = dispatch_counter_add_shared(
		fx.follower.clone(),
		fx.follower_node.pool.clone(),
		fx.follower_node.local_node,
		1,
		5,
		key_identity,
	)
	.await;
	assert_eq!(resp.status, WriteStatus::NOT_LEADER, "got resp={resp:?}");
	assert_eq!(resp.leader_hint, fx.leader_node.local_node);
	assert_eq!(
		fx.follower
			.counter_replica
			.read_committed(tenant(), key_identity)
			.await
			.unwrap(),
		None,
		"non-primary counter add returned before any local mutation"
	);

	shutdown_lease_node(fx.leader_node).await;
	shutdown_lease_node(fx.follower_node).await;
}

// ADR 075 HEADLINE fixture (D2): N concurrent unique-write_id `+1` adds to one
// absent counter sum to EXACTLY N. The leader's per-key read-modify-write
// critical section is what makes this hold — a racing read-compute-append
// without it would lose increments and the final `previous` would be < N.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn counter_add_concurrent_unique_writers_sum_exactly() {
	let dir = bootstrap_tempdir("counter-add-concurrent");
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		36,
		dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();
	let key_identity = crate::storage::counter::counter_identity(
		&crate::storage::counter::CounterKey(b"k".to_vec()),
	);

	const N: u64 = 64;
	let mut handles = Vec::new();
	for seq in 1..=N {
		handles.push(tokio::spawn(dispatch_counter_add_shared(
			node.data_server.clone(),
			node.pool.clone(),
			node.local_node,
			seq,
			1,
			key_identity,
		)));
	}
	for handle in handles {
		let status = handle.await.unwrap().status;
		assert!(
			status == WriteStatus::OK || status == WriteStatus::DUPLICATE,
			"each unique-writer add must land (got {status:?})"
		);
	}

	// A final add observes previous == N: every concurrent +1 landed, none lost.
	let final_add = dispatch_counter_add(&node, N + 1, 1, key_identity).await;
	assert_eq!(final_add.status, WriteStatus::OK);
	assert_eq!(
		final_add.previous_value, N,
		"all concurrent unique-writer +1 adds summed exactly (no lost increment)"
	);

	shutdown_lease_node(node).await;
}

// ADR 075 D4 (concurrency): N concurrent adds carrying the SAME canonical
// `ClientWriteId` apply the delta EXACTLY ONCE. The cache check in
// `handle_counter_add` is check-then-act; without the per-write-id in-flight gate
// two concurrent duplicates both miss the cache, mint distinct internal op-ids,
// and double-apply. Exactly one response is OK, the rest replay DUPLICATE, and a
// later fresh add bases at the single applied value (delta), not N*delta.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn counter_add_concurrent_same_write_id_applies_exactly_once() {
	let dir = bootstrap_tempdir("counter-add-dup-concurrent");
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		38,
		dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();
	let key_identity = crate::storage::counter::counter_identity(
		&crate::storage::counter::CounterKey(b"k".to_vec()),
	);

	const N: u64 = 32;
	const DELTA: u64 = 5;
	// Every task shares seq == 1, hence the same canonical write_id.
	let mut handles = Vec::new();
	for _ in 0..N {
		handles.push(tokio::spawn(dispatch_counter_add_shared(
			node.data_server.clone(),
			node.pool.clone(),
			node.local_node,
			1,
			DELTA,
			key_identity,
		)));
	}
	let mut ok_count = 0u64;
	for handle in handles {
		let meta = handle.await.unwrap();
		assert!(
			meta.status == WriteStatus::OK || meta.status == WriteStatus::DUPLICATE,
			"a same-write_id add must apply or replay (got {:?})",
			meta.status
		);
		assert_eq!(
			(meta.previous_value, meta.current_value),
			(0, DELTA),
			"every same-write_id response carries the single applied outcome 0->{DELTA}"
		);
		if meta.status == WriteStatus::OK {
			ok_count += 1;
		}
	}
	assert_eq!(
		ok_count, 1,
		"exactly one of the {N} duplicate adds applied; the rest replayed DUPLICATE"
	);

	// A fresh add bases at DELTA — the delta applied once, not N times.
	let fresh = dispatch_counter_add(&node, 2, 1, key_identity).await;
	assert_eq!(fresh.status, WriteStatus::OK);
	assert_eq!(
		(fresh.previous_value, fresh.current_value),
		(DELTA, DELTA + 1),
		"the duplicate adds applied the delta exactly once"
	);

	shutdown_lease_node(node).await;
}

// ADR 075 D4 (across restart): a duplicate `ClientWriteId` retry that lands
// AFTER a node restart replays `DUPLICATE` with the original outcome instead
// of re-adding against the recovered committed counter. Discriminating: with
// the dedup record held only in a process-lifetime map (no durable applied
// log, no leader recovery in `counter_leader_for`), the retry would re-apply
// (status OK, 5 → 10) — exactly-once would silently become at-least-once on
// every node restart.
#[tokio::test]
async fn counter_add_duplicate_replays_across_server_restart() {
	let dir = bootstrap_tempdir("counter-add-server-restart");
	let cfg = NodeBootstrapConfig::dev(39, dir.to_path_buf(), "127.0.0.1:0".into());
	let key_identity = crate::storage::counter::counter_identity(
		&crate::storage::counter::CounterKey(b"k".to_vec()),
	);
	{
		let node = WendelonNode::bootstrap_single_node(cfg.clone())
			.await
			.unwrap();
		let r1 = dispatch_counter_add(&node, 1, 5, key_identity).await;
		assert_eq!(r1.status, WriteStatus::OK);
		assert_eq!((r1.previous_value, r1.current_value), (0, 5));
		shutdown_lease_node(node).await;
	}
	tokio::time::sleep(Duration::from_millis(25)).await;

	let recovered = WendelonNode::bootstrap_single_node(cfg).await.unwrap();
	// The committed value survived; the duplicate retry replays, not re-adds.
	let dup = dispatch_counter_add(&recovered, 1, 5, key_identity).await;
	assert_eq!(
		dup.status,
		WriteStatus::DUPLICATE,
		"a post-restart duplicate must replay (got {:?})",
		dup.status
	);
	assert_eq!(
		(dup.previous_value, dup.current_value),
		(0, 5),
		"the replay carries the ORIGINAL outcome"
	);
	// A fresh add bases at the recovered 5 — the retry did not re-apply.
	let fresh = dispatch_counter_add(&recovered, 2, 1, key_identity).await;
	assert_eq!(fresh.status, WriteStatus::OK);
	assert_eq!(
		(fresh.previous_value, fresh.current_value),
		(5, 6),
		"exactly-once held across the restart (5, not 10)"
	);

	shutdown_lease_node(recovered).await;
}

// ADR 075 fixture (D2 critical-section proof): two concurrent adders to an absent
// key observe (0->1) and (1->2) in SOME order — never both (0->1). A shared
// `previous` would be the lost-update bug this ADR exists to prevent.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn counter_add_two_racing_adders_do_not_share_previous() {
	let dir = bootstrap_tempdir("counter-add-racing");
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		37,
		dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();
	let key_identity = crate::storage::counter::counter_identity(
		&crate::storage::counter::CounterKey(b"k".to_vec()),
	);

	let mut handles = Vec::new();
	for seq in 1..=2u64 {
		handles.push(tokio::spawn(dispatch_counter_add_shared(
			node.data_server.clone(),
			node.pool.clone(),
			node.local_node,
			seq,
			1,
			key_identity,
		)));
	}
	let mut pairs = Vec::new();
	for handle in handles {
		let meta = handle.await.unwrap();
		pairs.push((meta.previous_value, meta.current_value));
	}
	pairs.sort();
	assert_eq!(
		pairs,
		vec![(0, 1), (1, 2)],
		"racing adders observed DISTINCT previous values (no shared-previous lost update)"
	);

	shutdown_lease_node(node).await;
}

#[tokio::test]
async fn dispatcher_rejects_read_only_observer_data_read() {
	let dir = bootstrap_tempdir("auth-read");
	let audit_path = dir.join("audit.ndjson");
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		22,
		dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();

	let meta = simple_read_meta(1);
	let key = encode_batch(&single_key_batch()).unwrap();
	let mut meta_buf = node.pool.acquire();
	meta_buf
		.as_mut_vec()
		.extend_from_slice(bytemuck::bytes_of(&meta));
	let mut arrow_buf = node.pool.acquire();
	arrow_buf.as_mut_vec().extend_from_slice(&key);
	let frame = HrftFrame {
		header: FrameHeader {
			magic: HRFT_MAGIC,
			version: 1,
			msg_type: MsgType::READ_REQ.0,
			flags: 0,
			stream_id: 1,
			meta_len: meta_buf.len() as u32,
			arrow_len: arrow_buf.len() as u32,
			trailing_len: 0,
		},
		meta: meta_buf,
		arrow_payload: Some(arrow_buf),
		trailing: None,
	};

	let resp = DataRingServer::handle_data_frame(
		node.data_server.as_ref(),
		RequestContext {
			remote: Some(node.local_node),
			actor: actor(ActorRole::ReadOnlyObserver, None),
		},
		frame,
	)
	.await
	.unwrap()
	.unwrap();
	let meta: ReadResponseMeta =
		*bytemuck::from_bytes(&resp.meta[..std::mem::size_of::<ReadResponseMeta>()]);
	assert_eq!(meta.status, ReadStatus::AUTHORIZATION_FAILED);
	let audit = wait_for_audit_log(
		&audit_path,
		&[
			"\"event_type\":\"data.read\"",
			"\"phase\":\"denied\"",
			"\"status\":\"authorization_failed\"",
			"\"actor_role\":\"read_only_observer\"",
		],
	)
	.await;
	assert!(audit.contains("\"type\":\"data_access\""));

	shutdown_lease_node(node).await;
}

#[tokio::test]
async fn dispatcher_emits_write_request_and_completion_audit_events() {
	let dir = bootstrap_tempdir("audit-write-events");
	let audit_path = dir.join("audit.ndjson");
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		31,
		dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();
	let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
	loop {
		if node.raft.current_leader().await == Some(node.local_node.id) {
			break;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for leader"
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
	node.data_server
		.refresh_leases_once(&node.ring, &*node.clock, 60_000)
		.await;

	let meta = simple_write_meta(1);
	let payload = encode_batch(&single_kv_batch()).unwrap();
	let mut meta_buf = node.pool.acquire();
	meta_buf
		.as_mut_vec()
		.extend_from_slice(bytemuck::bytes_of(&meta));
	let mut arrow_buf = node.pool.acquire();
	arrow_buf.as_mut_vec().extend_from_slice(&payload);
	let frame = HrftFrame {
		header: FrameHeader {
			magic: HRFT_MAGIC,
			version: 1,
			msg_type: MsgType::WRITE_REQ.0,
			flags: 0,
			stream_id: 1,
			meta_len: meta_buf.len() as u32,
			arrow_len: arrow_buf.len() as u32,
			trailing_len: 0,
		},
		meta: meta_buf,
		arrow_payload: Some(arrow_buf),
		trailing: None,
	};

	let resp = DataRingServer::handle_data_frame(
		node.data_server.as_ref(),
		RequestContext {
			remote: Some(node.local_node),
			actor: actor(ActorRole::Operator, None),
		},
		frame,
	)
	.await
	.unwrap()
	.unwrap();
	let meta: WriteResponseMeta =
		*bytemuck::from_bytes(&resp.meta[..std::mem::size_of::<WriteResponseMeta>()]);
	assert_eq!(meta.status, WriteStatus::OK);

	let audit = wait_for_audit_log(
		&audit_path,
		&[
			"\"event_type\":\"data.write\"",
			"\"phase\":\"requested\"",
			"\"phase\":\"completed\"",
			"\"status\":\"ok\"",
		],
	)
	.await;
	assert!(audit.contains("\"record_identity\""));

	shutdown_lease_node(node).await;
}

#[tokio::test]
async fn dispatcher_refuses_write_when_data_access_audit_emit_fails() {
	struct RejectingAuditSink;

	impl AuditSink for RejectingAuditSink {
		fn emit(&self, _event: AuditEvent) -> std::io::Result<()> {
			Err(Error::other("simulated data-access audit failure"))
		}
	}

	let dir = bootstrap_tempdir("audit-write-fail-closed");
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		34,
		dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();
	let fail_closed_server = Arc::new(
		WendelonDataServer::new(
			node.slab.clone(),
			node.raft.clone(),
			node.log_store.clone(),
			node.state_machine.clone(),
			node.local_node,
			node.clock.clone(),
			node.data_server.router.clone(),
			node.data_server.factory.clone(),
			node.pool.clone(),
		)
		.unwrap()
		.with_audit_sink(Arc::new(RejectingAuditSink)),
	);

	let meta = simple_write_meta(1);
	let payload = encode_batch(&single_kv_batch()).unwrap();
	let before_bytes = node.slab.local_l1_bytes().await;
	let mut meta_buf = node.pool.acquire();
	meta_buf
		.as_mut_vec()
		.extend_from_slice(bytemuck::bytes_of(&meta));
	let mut arrow_buf = node.pool.acquire();
	arrow_buf.as_mut_vec().extend_from_slice(&payload);
	let frame = HrftFrame {
		header: FrameHeader {
			magic: HRFT_MAGIC,
			version: 1,
			msg_type: MsgType::WRITE_REQ.0,
			flags: 0,
			stream_id: 1,
			meta_len: meta_buf.len() as u32,
			arrow_len: arrow_buf.len() as u32,
			trailing_len: 0,
		},
		meta: meta_buf,
		arrow_payload: Some(arrow_buf),
		trailing: None,
	};

	let err = DataRingServer::handle_data_frame(
		fail_closed_server.as_ref(),
		RequestContext {
			remote: Some(node.local_node),
			actor: actor(ActorRole::Operator, None),
		},
		frame,
	)
	.await
	.unwrap_err();
	assert!(err.to_string().contains("data access audit emit failed"));
	assert_eq!(
		node.slab.local_l1_bytes().await,
		before_bytes,
		"failed local-chain emission must reject the write before any payload bytes append"
	);

	shutdown_lease_node(node).await;
}

#[tokio::test]
async fn row_group_corrupt_directory_error_emits_invariant_audit_event() {
	let dir = bootstrap_tempdir("row-group-invariant-audit");
	let audit_path = dir.join("audit.ndjson");
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		35,
		dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();
	let error = Error::new(
		ErrorKind::InvalidData,
		crate::storage::columnar::ColumnFetchError::RowGroupRejected(
			crate::storage::row_group::RowGroupRejectCode::PageDirectoryParseError,
		),
	);

	node.data_server
		.emit_row_group_invariant_audit_for_error(
			&actor(ActorRole::Operator, None),
			TenantId::new(35),
			&error,
			"corrupt_directory_read",
		)
		.unwrap();

	let audit = wait_for_audit_log(
		&audit_path,
		&[
			"\"event_type\":\"row_group.invariant_violation\"",
			"\"tenant_id\":35",
			"\"context\":\"corrupt_directory_read\"",
			"\"violation_kind\":\"page_directory_parse_error\"",
		],
	)
	.await;
	assert!(audit.contains("\"outcome\":\"denied\""));

	shutdown_lease_node(node).await;
}

#[tokio::test]
async fn dispatcher_emits_completed_read_audit_event() {
	let dir = bootstrap_tempdir("audit-read-events");
	let audit_path = dir.join("audit.ndjson");
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		32,
		dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();
	let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
	loop {
		if node.raft.current_leader().await == Some(node.local_node.id) {
			break;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for leader"
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
	node.data_server
		.refresh_leases_once(&node.ring, &*node.clock, 60_000)
		.await;
	let (_write_resp, _) = node
		.data_server
		.write(simple_write_meta(1), single_kv_batch())
		.await
		.unwrap();

	let meta = simple_read_meta(1);
	let key = encode_batch(&single_key_batch()).unwrap();
	let mut meta_buf = node.pool.acquire();
	meta_buf
		.as_mut_vec()
		.extend_from_slice(bytemuck::bytes_of(&meta));
	let mut arrow_buf = node.pool.acquire();
	arrow_buf.as_mut_vec().extend_from_slice(&key);
	let frame = HrftFrame {
		header: FrameHeader {
			magic: HRFT_MAGIC,
			version: 1,
			msg_type: MsgType::READ_REQ.0,
			flags: 0,
			stream_id: 1,
			meta_len: meta_buf.len() as u32,
			arrow_len: arrow_buf.len() as u32,
			trailing_len: 0,
		},
		meta: meta_buf,
		arrow_payload: Some(arrow_buf),
		trailing: None,
	};

	let resp = DataRingServer::handle_data_frame(
		node.data_server.as_ref(),
		RequestContext {
			remote: Some(node.local_node),
			actor: actor(ActorRole::Operator, None),
		},
		frame,
	)
	.await
	.unwrap()
	.unwrap();
	let meta: ReadResponseMeta =
		*bytemuck::from_bytes(&resp.meta[..std::mem::size_of::<ReadResponseMeta>()]);
	assert_eq!(meta.status, ReadStatus::OK);

	let audit = wait_for_audit_log(
		&audit_path,
		&[
			"\"event_type\":\"data.read\"",
			"\"phase\":\"completed\"",
			"\"status\":\"ok\"",
			"\"result_count\":1",
		],
	)
	.await;
	assert!(audit.contains("\"returned_bytes\":1"));

	shutdown_lease_node(node).await;
}

#[tokio::test]
async fn dispatcher_emits_completed_range_query_audit_event() {
	let dir = bootstrap_tempdir("audit-range-query-events");
	let audit_path = dir.join("audit.ndjson");
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		33,
		dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();
	let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
	loop {
		if node.raft.current_leader().await == Some(node.local_node.id) {
			break;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for leader"
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
	node.data_server
		.refresh_leases_once(&node.ring, &*node.clock, 60_000)
		.await;
	let (_write_resp, _) = node
		.data_server
		.write(simple_write_meta(1), single_kv_batch())
		.await
		.unwrap();

	let mut meta = base_meta();
	meta.expected_epoch = 1;
	let mut meta_buf = node.pool.acquire();
	meta_buf
		.as_mut_vec()
		.extend_from_slice(bytemuck::bytes_of(&meta));
	let frame = HrftFrame {
		header: FrameHeader {
			magic: HRFT_MAGIC,
			version: 1,
			msg_type: MsgType::QUERY_REQ.0,
			flags: 0,
			stream_id: 1,
			meta_len: meta_buf.len() as u32,
			arrow_len: 0,
			trailing_len: 0,
		},
		meta: meta_buf,
		arrow_payload: None,
		trailing: None,
	};

	let resp = DataRingServer::handle_data_frame(
		node.data_server.as_ref(),
		RequestContext {
			remote: Some(node.local_node),
			actor: actor(ActorRole::Operator, None),
		},
		frame,
	)
	.await
	.unwrap()
	.unwrap();
	let meta: QueryResponseMeta =
		*bytemuck::from_bytes(&resp.meta[..std::mem::size_of::<QueryResponseMeta>()]);
	assert_eq!(meta.status, ReadStatus::OK);
	assert_eq!(meta.rows, 1);

	let audit = wait_for_audit_log(
		&audit_path,
		&[
			"\"event_type\":\"data.range_query\"",
			"\"phase\":\"completed\"",
			"\"status\":\"ok\"",
			"\"result_count\":1",
		],
	)
	.await;
	assert!(audit.contains("\"locality_end\":18446744073709551615"));

	shutdown_lease_node(node).await;
}

#[tokio::test]
async fn dispatcher_rejects_tenant_scoped_actor_opening_finalization_stream() {
	use tokio::io::AsyncWriteExt;

	let dir = bootstrap_tempdir("auth-stream-fin");
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		23,
		dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();

	let request = StreamFinalizationRequestMeta {
		start_log_index: 0,
		epoch: 1,
	};
	let first_header = FrameHeader {
		magic: HRFT_MAGIC,
		version: 1,
		msg_type: MsgType::STREAM_FIN_REQ.0,
		flags: 0,
		stream_id: 1,
		meta_len: std::mem::size_of::<StreamFinalizationRequestMeta>() as u32,
		arrow_len: 0,
		trailing_len: 0,
	};
	let (mut client_side, server_side) = tokio::io::duplex(1024);
	client_side
		.write_all(bytemuck::bytes_of(&request))
		.await
		.unwrap();
	client_side.shutdown().await.unwrap();

	let err = DataRingServer::handle_data_stream(
		node.data_server.as_ref(),
		RequestContext {
			remote: Some(node.local_node),
			actor: actor(ActorRole::Operator, Some("7")),
		},
		first_header,
		Box::new(DuplexTransportStream { inner: server_side }),
		node.pool.clone(),
	)
	.await
	.unwrap_err();
	assert_eq!(err.kind(), ErrorKind::PermissionDenied);
	assert!(err.to_string().contains("STREAM_FIN_REQ"));

	shutdown_lease_node(node).await;
}

#[tokio::test]
async fn dispatcher_backpressures_write_when_tenant_limit_is_saturated() {
	let dir = bootstrap_tempdir("bp-write-tenant");
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		24,
		dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();
	node.data_server
		.set_request_backpressure_limits_for_test(1, 1);

	let tenant_actor = actor(ActorRole::Operator, Some("0"));
	let _guard = node
		.data_server
		.try_admit_data_request(&tenant_actor, Some(tenant()), DataOperation::Write)
		.unwrap()
		.unwrap();

	let before = metrics()
		.data_ring_backpressure_rejections_tenant_total
		.get();
	let meta = simple_write_meta(1);
	let payload = encode_batch(&single_kv_batch()).unwrap();
	let mut meta_buf = node.pool.acquire();
	meta_buf
		.as_mut_vec()
		.extend_from_slice(bytemuck::bytes_of(&meta));
	let mut arrow_buf = node.pool.acquire();
	arrow_buf.as_mut_vec().extend_from_slice(&payload);
	let frame = HrftFrame {
		header: FrameHeader {
			magic: HRFT_MAGIC,
			version: 1,
			msg_type: MsgType::WRITE_REQ.0,
			flags: 0,
			stream_id: 1,
			meta_len: meta_buf.len() as u32,
			arrow_len: arrow_buf.len() as u32,
			trailing_len: 0,
		},
		meta: meta_buf,
		arrow_payload: Some(arrow_buf),
		trailing: None,
	};

	let resp = DataRingServer::handle_data_frame(
		node.data_server.as_ref(),
		RequestContext {
			remote: Some(node.local_node),
			actor: tenant_actor,
		},
		frame,
	)
	.await
	.unwrap()
	.unwrap();
	let meta: WriteResponseMeta =
		*bytemuck::from_bytes(&resp.meta[..std::mem::size_of::<WriteResponseMeta>()]);
	assert_eq!(meta.status, WriteStatus::BACKPRESSURED);
	assert_eq!(
		metrics()
			.data_ring_backpressure_rejections_tenant_total
			.get(),
		before + 1
	);

	shutdown_lease_node(node).await;
}

#[tokio::test]
async fn dispatcher_backpressures_cross_tenant_read_when_actor_limit_is_saturated() {
	let dir = bootstrap_tempdir("bp-read-actor");
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		25,
		dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();
	node.data_server
		.set_request_backpressure_limits_for_test(8, 1);

	let operator = actor(ActorRole::Operator, None);
	let _guard = node
		.data_server
		.try_admit_data_request(&operator, Some(tenant()), DataOperation::Read)
		.unwrap()
		.unwrap();

	let before = metrics()
		.data_ring_backpressure_rejections_actor_total
		.get();
	let mut meta = simple_read_meta(1);
	meta.tenant_id = TenantId::new(9);
	meta.key_identity = Identity::blake3_tenant(meta.tenant_id, b"k");
	let key = encode_batch(&single_key_batch()).unwrap();
	let mut meta_buf = node.pool.acquire();
	meta_buf
		.as_mut_vec()
		.extend_from_slice(bytemuck::bytes_of(&meta));
	let mut arrow_buf = node.pool.acquire();
	arrow_buf.as_mut_vec().extend_from_slice(&key);
	let frame = HrftFrame {
		header: FrameHeader {
			magic: HRFT_MAGIC,
			version: 1,
			msg_type: MsgType::READ_REQ.0,
			flags: 0,
			stream_id: 1,
			meta_len: meta_buf.len() as u32,
			arrow_len: arrow_buf.len() as u32,
			trailing_len: 0,
		},
		meta: meta_buf,
		arrow_payload: Some(arrow_buf),
		trailing: None,
	};

	let resp = DataRingServer::handle_data_frame(
		node.data_server.as_ref(),
		RequestContext {
			remote: Some(node.local_node),
			actor: operator,
		},
		frame,
	)
	.await
	.unwrap()
	.unwrap();
	let meta: ReadResponseMeta =
		*bytemuck::from_bytes(&resp.meta[..std::mem::size_of::<ReadResponseMeta>()]);
	assert_eq!(meta.status, ReadStatus::BACKPRESSURED);
	// This process-global counter is shared by concurrently running server tests. The
	// response above proves this request was rejected; concurrent rejections may add
	// further increments before this observation.
	assert!(
		metrics()
			.data_ring_backpressure_rejections_actor_total
			.get() >= before + 1
	);

	shutdown_lease_node(node).await;
}

#[tokio::test]
async fn query_stream_backpressure_response_marks_stream_end() {
	use tokio::io::AsyncWriteExt;

	let dir = bootstrap_tempdir("bp-query-stream-end");
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		26,
		dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();
	node.data_server
		.set_request_backpressure_limits_for_test(1, 1);

	let tenant_actor = actor(ActorRole::Operator, Some("0"));
	let _guard = node
		.data_server
		.try_admit_data_request(&tenant_actor, Some(tenant()), DataOperation::Query)
		.unwrap()
		.unwrap();

	let meta = base_meta();
	let first_header = FrameHeader {
		magic: HRFT_MAGIC,
		version: 1,
		msg_type: MsgType::QUERY_REQ.0,
		flags: 0,
		stream_id: 7,
		meta_len: std::mem::size_of::<QueryRequestMeta>() as u32,
		arrow_len: 0,
		trailing_len: 0,
	};
	let (mut client_side, server_side) = tokio::io::duplex(1024);
	client_side
		.write_all(bytemuck::bytes_of(&meta))
		.await
		.unwrap();
	client_side.shutdown().await.unwrap();

	DataRingServer::handle_data_stream(
		node.data_server.as_ref(),
		RequestContext {
			remote: Some(node.local_node),
			actor: tenant_actor,
		},
		first_header,
		Box::new(DuplexTransportStream { inner: server_side }),
		node.pool.clone(),
	)
	.await
	.unwrap();

	let response = read_frame(&mut client_side, &node.pool).await.unwrap();
	assert_ne!(response.header.flags & FRAME_FLAG_STREAM_END, 0);
	let meta: QueryResponseMeta =
		*bytemuck::from_bytes(&response.meta[..std::mem::size_of::<QueryResponseMeta>()]);
	assert_eq!(meta.status, ReadStatus::BACKPRESSURED);

	shutdown_lease_node(node).await;
}

#[tokio::test]
async fn cached_tenant_policy_recovers_after_cache_lock_poison() {
	let dir = bootstrap_tempdir("tenant-policy-poison");
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		27,
		dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();

	let cache = node.data_server.tenant_policy_cache.clone();
	let _ = std::panic::catch_unwind(AssertUnwindSafe(|| {
		let _guard = cache.write().unwrap();
		panic!("poison tenant policy cache");
	}));

	let _ = node.data_server.cached_tenant_policy(TenantId::ZERO).await;

	shutdown_lease_node(node).await;
}

#[tokio::test]
async fn lease_gate_recovers_after_cache_lock_poison() {
	let dir = bootstrap_tempdir("lease-cache-poison");
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		28,
		dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();

	let lease = node.data_server.make_signed_lease(
		1,
		node.local_node,
		0,
		100,
		hlc(10, 0, node.local_node.id),
		hlc(10_000, 0, node.local_node.id),
	);
	node.data_server.cache_granted_lease(lease);

	let leases = node.data_server.leases.clone();
	let _ = std::panic::catch_unwind(AssertUnwindSafe(|| {
		let _guard = leases.write().unwrap();
		panic!("poison lease cache");
	}));

	assert_eq!(
		node.data_server
			.lease_gate_result(Locality::ZERO, 1, hlc(11, 0, node.local_node.id))
			.await,
		LeaseGateResult::Valid
	);

	shutdown_lease_node(node).await;
}

// ============================================================
//  ADR 045 Phase 8.5 — projected fast path AppendOnly
//  table-isolation verification
// ============================================================

#[test]
fn augment_requirement_includes_table_object_id_for_append_only() {
	use crate::schema::TenantId;
	let descriptor = TableSchemaDescriptor::new_with_layout(
		TableObjectId::new([0x45; 16]),
		1,
		"analytics",
		"events",
		&arrow::datatypes::Schema::new(vec![arrow::datatypes::Field::new(
			"value",
			arrow::datatypes::DataType::Int64,
			false,
		)]),
		RowIdentityPolicy::AppendOnly,
		LocalityPolicy::default_table_spread(),
		RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::default()),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.unwrap();
	let _ = TenantId::new(7);
	let req = ColumnFetchRequirement::default();
	let augmented = augment_requirement_for_descriptor_verification(&req, &descriptor);
	assert!(
		augmented
			.storage_columns
			.contains(crate::storage::columnar::StorageColumn::RowTableObjectId),
		"AppendOnly descriptor-filtered fetch must include `__row_table_object_id__`"
	);
}

#[test]
fn augment_requirement_includes_table_object_id_for_pk_multirow() {
	let descriptor = TableSchemaDescriptor::new_with_layout(
		TableObjectId::new([0x44; 16]),
		1,
		"analytics",
		"events_pk",
		&arrow::datatypes::Schema::new(vec![
			arrow::datatypes::Field::new("id", arrow::datatypes::DataType::LargeBinary, false),
			arrow::datatypes::Field::new("value", arrow::datatypes::DataType::Int64, false),
		]),
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
		LocalityPolicy::default_table_spread(),
		RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::default()),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.unwrap();
	let req = ColumnFetchRequirement::default();
	let augmented = augment_requirement_for_descriptor_verification(&req, &descriptor);
	assert!(
		augmented
			.storage_columns
			.contains(crate::storage::columnar::StorageColumn::RowTableObjectId),
		"multi-row PK descriptor must pull `__row_table_object_id__` into the projected \
			 fetch — Phase 8 stamps it on every multi-row record (PK + AppendOnly)"
	);
}

#[test]
fn augment_requirement_unchanged_for_pk_single_row() {
	let descriptor = TableSchemaDescriptor::new(
		TableObjectId::new([0x46; 16]),
		1,
		"analytics",
		"events_pk_single",
		&arrow::datatypes::Schema::new(vec![
			arrow::datatypes::Field::new("id", arrow::datatypes::DataType::LargeBinary, false),
			arrow::datatypes::Field::new("value", arrow::datatypes::DataType::Int64, false),
		]),
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
		LocalityPolicy::default_table_spread(),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.unwrap();
	let req = ColumnFetchRequirement::default();
	let augmented = augment_requirement_for_descriptor_verification(&req, &descriptor);
	assert!(
		!augmented
			.storage_columns
			.contains(crate::storage::columnar::StorageColumn::RowTableObjectId),
		"single-row PK records carry no synthetic columns; descriptor-filtered fetch \
			 must not pull a column that is not on disk"
	);
}

#[test]
fn projected_match_falls_back_when_table_object_id_column_missing() {
	// AppendOnly descriptor + projected batch without
	// `__row_table_object_id__` (e.g., a sibling PK record
	// under a colliding table-spread prefix). The verifier
	// must return false so the caller falls back to the
	// whole-record path.
	use std::collections::BTreeMap;
	use std::sync::Arc;
	let descriptor = TableSchemaDescriptor::new_with_layout(
		TableObjectId::new([0x45; 16]),
		1,
		"analytics",
		"events",
		&arrow::datatypes::Schema::new(vec![arrow::datatypes::Field::new(
			"value",
			arrow::datatypes::DataType::Int64,
			false,
		)]),
		RowIdentityPolicy::AppendOnly,
		LocalityPolicy::default_table_spread(),
		RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::default()),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.unwrap();
	let batch = arrow::record_batch::RecordBatch::try_new(
		Arc::new(arrow::datatypes::Schema::new(vec![
			arrow::datatypes::Field::new("value", arrow::datatypes::DataType::Int64, false),
		])),
		vec![Arc::new(arrow::array::Int64Array::from(vec![1])) as arrow::array::ArrayRef],
	)
	.unwrap();
	let projected = ProjectedRecordBatch {
		batch,
		ordinal_mapping: crate::storage::columnar::ColumnOrdinalMapping::default(),
		storage_mapping: BTreeMap::new(),
	};
	let result = projected_descriptor_match(&descriptor, &projected).unwrap();
	assert!(
		!result,
		"AppendOnly projected verifier must force fallback when \
			 `__row_table_object_id__` is absent from the projected fetch"
	);
}

#[test]
fn projected_match_rejects_mismatched_table_object_id() {
	use std::collections::BTreeMap;
	use std::sync::Arc;
	let descriptor = TableSchemaDescriptor::new_with_layout(
		TableObjectId::new([0x45; 16]),
		1,
		"analytics",
		"events_a",
		&arrow::datatypes::Schema::new(vec![arrow::datatypes::Field::new(
			"value",
			arrow::datatypes::DataType::Int64,
			false,
		)]),
		RowIdentityPolicy::AppendOnly,
		LocalityPolicy::default_table_spread(),
		RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::default()),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.unwrap();
	// Stored row's table_object_id stamped from a *different*
	// AppendOnly table (same tenant, same Arrow schema,
	// colliding table-spread prefix would put them on the
	// same range).
	let stored_id = [0xBB_u8; 16];
	let mut id_builder = arrow::array::FixedSizeBinaryBuilder::with_capacity(1, 16);
	id_builder.append_value(stored_id).unwrap();
	let id_array = Arc::new(id_builder.finish()) as arrow::array::ArrayRef;
	let value_array = Arc::new(arrow::array::Int64Array::from(vec![1])) as arrow::array::ArrayRef;
	let batch = arrow::record_batch::RecordBatch::try_new(
		Arc::new(arrow::datatypes::Schema::new(vec![
			arrow::datatypes::Field::new("value", arrow::datatypes::DataType::Int64, false),
			arrow::datatypes::Field::new(
				"__row_table_object_id__",
				arrow::datatypes::DataType::FixedSizeBinary(16),
				false,
			),
		])),
		vec![value_array, id_array],
	)
	.unwrap();
	let mut storage_mapping = BTreeMap::new();
	storage_mapping.insert(crate::storage::columnar::StorageColumn::RowTableObjectId, 1);
	let projected = ProjectedRecordBatch {
		batch,
		ordinal_mapping: crate::storage::columnar::ColumnOrdinalMapping::default(),
		storage_mapping,
	};
	let result = projected_descriptor_match(&descriptor, &projected).unwrap();
	assert!(
		!result,
		"AppendOnly projected verifier must reject rows stamped with a \
			 different table_object_id than the candidate descriptor"
	);
}

#[test]
fn projected_match_rejects_pk_multirow_with_mismatched_table_object_id() {
	use std::collections::BTreeMap;
	use std::sync::Arc;
	let schema = arrow::datatypes::Schema::new(vec![
		arrow::datatypes::Field::new("id", arrow::datatypes::DataType::LargeBinary, false),
		arrow::datatypes::Field::new("value", arrow::datatypes::DataType::Int64, false),
	]);
	let descriptor = TableSchemaDescriptor::new_with_layout(
		TableObjectId::new([0xAA; 16]),
		1,
		"analytics",
		"events_pk",
		&schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
		LocalityPolicy::default_table_spread(),
		RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::default()),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.unwrap();
	// Row stamped with a sibling PK descriptor's table_object_id.
	let stored_id = [0xBB_u8; 16];
	let mut id_builder = arrow::array::FixedSizeBinaryBuilder::with_capacity(1, 16);
	id_builder.append_value(stored_id).unwrap();
	let stamp_array = Arc::new(id_builder.finish()) as arrow::array::ArrayRef;
	let id_array =
		Arc::new(arrow::array::LargeBinaryArray::from(vec![&b"k1"[..]])) as arrow::array::ArrayRef;
	let value_array = Arc::new(arrow::array::Int64Array::from(vec![1])) as arrow::array::ArrayRef;
	let batch = arrow::record_batch::RecordBatch::try_new(
		Arc::new(arrow::datatypes::Schema::new(vec![
			arrow::datatypes::Field::new("id", arrow::datatypes::DataType::LargeBinary, false),
			arrow::datatypes::Field::new("value", arrow::datatypes::DataType::Int64, false),
			arrow::datatypes::Field::new(
				"__row_table_object_id__",
				arrow::datatypes::DataType::FixedSizeBinary(16),
				false,
			),
		])),
		vec![id_array, value_array, stamp_array],
	)
	.unwrap();
	let mut storage_mapping = BTreeMap::new();
	storage_mapping.insert(crate::storage::columnar::StorageColumn::RowTableObjectId, 2);
	let projected = ProjectedRecordBatch {
		batch,
		ordinal_mapping: crate::storage::columnar::ColumnOrdinalMapping::default(),
		storage_mapping,
	};
	let result = projected_descriptor_match(&descriptor, &projected).unwrap();
	assert!(
		!result,
		"PK multi-row projected verifier must reject rows stamped with a \
			 different table_object_id (Phase 8 PK extension)"
	);
}

#[test]
fn projected_match_accepts_same_table_object_id() {
	use std::collections::BTreeMap;
	use std::sync::Arc;
	let descriptor = TableSchemaDescriptor::new_with_layout(
		TableObjectId::new([0x45; 16]),
		1,
		"analytics",
		"events_a",
		&arrow::datatypes::Schema::new(vec![arrow::datatypes::Field::new(
			"value",
			arrow::datatypes::DataType::Int64,
			false,
		)]),
		RowIdentityPolicy::AppendOnly,
		LocalityPolicy::default_table_spread(),
		RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::default()),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.unwrap();
	let stored_id = [0x45_u8; 16];
	let mut id_builder = arrow::array::FixedSizeBinaryBuilder::with_capacity(1, 16);
	id_builder.append_value(stored_id).unwrap();
	let id_array = Arc::new(id_builder.finish()) as arrow::array::ArrayRef;
	let value_array = Arc::new(arrow::array::Int64Array::from(vec![1])) as arrow::array::ArrayRef;
	let batch = arrow::record_batch::RecordBatch::try_new(
		Arc::new(arrow::datatypes::Schema::new(vec![
			arrow::datatypes::Field::new("value", arrow::datatypes::DataType::Int64, false),
			arrow::datatypes::Field::new(
				"__row_table_object_id__",
				arrow::datatypes::DataType::FixedSizeBinary(16),
				false,
			),
		])),
		vec![value_array, id_array],
	)
	.unwrap();
	let mut storage_mapping = BTreeMap::new();
	storage_mapping.insert(crate::storage::columnar::StorageColumn::RowTableObjectId, 1);
	let projected = ProjectedRecordBatch {
		batch,
		ordinal_mapping: crate::storage::columnar::ColumnOrdinalMapping::default(),
		storage_mapping,
	};
	let result = projected_descriptor_match(&descriptor, &projected).unwrap();
	assert!(
		result,
		"AppendOnly projected verifier must accept rows stamped with the \
			 same table_object_id as the candidate descriptor"
	);
}

// ============================================================
//  ADR 045 Phase 8 idempotency canonicalization — retry-stable
//  derivations off `ClientWriteId` must hash only canonical
//  bytes (`client_uuid` + `sequence_no`).
// ============================================================

#[test]
fn table_write_parent_identity_ignores_idempotency_ttl_and_pad() {
	let mut base: TableWriteRequestMeta = bytemuck::Zeroable::zeroed();
	base.tenant_id = TenantId::new(11);
	base.table_object_id = TableObjectId::new([0x91; 16]);
	base.schema_generation = 5;
	base.row_count = 3;
	base.parent_write_id = ClientWriteId {
		client_uuid: [0xAA; 16],
		sequence_no: 17,
		idempotency_ttl_ms: 60_000,
		_pad: 0,
	};
	let mut drifted = base;
	drifted.parent_write_id.idempotency_ttl_ms = 12_345;
	drifted.parent_write_id._pad = 0xDEAD_BEEF;
	assert_eq!(
		table_write_parent_identity(&base),
		table_write_parent_identity(&drifted),
		"table_write_parent_identity must depend only on the canonical \
			 idempotency identity (`client_uuid` + `sequence_no`)"
	);
}

/// ADR 541 Acceptance 1 (in-repo form): a full-table scan an order of magnitude past
/// the former 250 ms-lifetime failure point completes under the DEFAULT caps. The
/// old semantics fail this twice over — 25,000 locations breaks the 4,096 total cap,
/// and a lifetime clock never resets — so this fixture is discriminating against both
/// retired behaviors at once.
///
/// Both are asserted without racing a wall clock. The caps are the defaults. The
/// clock's sliding nature is asserted from its progress marks: `progressed()` is the
/// one place that both resets the stall clock and counts a mark, so a mark per
/// examined location is a reset per location. The stall bound itself is lifted:
/// 250 ms of wall time between two locations is what a starved scheduler can lose,
/// and that would measure the host, not the clock.
#[tokio::test]
async fn full_scan_completes_far_beyond_the_former_lifetime_cap() {
	let (_mgr_dir, mgr) = make_manager().await;
	for locality in 0..25_000u64 {
		seed_record(
			&mgr,
			locality,
			hlc(1, 0, 1),
			&locality.to_le_bytes(),
			b"row",
		)
		.await;
	}
	let sm = WendelonStateMachine::new();
	let defaults = QueryExecutionBudget::default();
	assert!(
		25_000 > defaults.max_locations,
		"premise: the scan exceeds the default location cap"
	);
	let plan = plan_query_execution_with_budget_and_retention_floor(
		&mgr,
		&sm,
		n(1),
		&base_meta(),
		&[],
		QueryExecutionBudget {
			max_wall_time: Duration::from_secs(3600),
			..defaults
		},
		None,
	)
	.await
	.unwrap();
	assert_eq!(plan.response.status, ReadStatus::OK);
	assert_eq!(plan.response.rows, 25_000);
	assert!(!plan.batches.is_empty());
	assert!(
		plan.clocks.progress_marks >= 25_000,
		"the stall clock must reset per examined location, got {} marks",
		plan.clocks.progress_marks
	);

	// The full execution path too: concatenation and wire encoding of all
	// 25,000 rows, under the same caps and the same lifted stall bound.
	let (resp, arrow) = execute_query_plan_with_budget_and_retention_floor(
		&mgr,
		&sm,
		n(1),
		&base_meta(),
		&[],
		QueryExecutionBudget {
			max_wall_time: Duration::from_secs(3600),
			..defaults
		},
		None,
	)
	.await
	.unwrap();
	assert_eq!(resp.status, ReadStatus::OK);
	assert_eq!(resp.rows, 25_000);
	assert!(arrow.is_some());
}

/// The server-side stall bound is product policy (ADR 541), and the operator is
/// weighing a change to it: on a starved host 250 ms between two units of
/// progress fails queries that are still progressing. Pinned so the value only
/// changes deliberately, together with this test.
#[test]
fn the_default_query_stall_bound_is_250_ms() {
	assert_eq!(
		QueryExecutionBudget::default().max_wall_time,
		Duration::from_millis(250)
	);
}

/// ADR 541 Acceptance 3: a caller deadline tighter than any server policy is honored
/// with its own status, and a generous caller deadline does not resurrect
/// lifetime-style failures on a progressing scan.
#[tokio::test]
async fn caller_deadline_is_absolute_and_attributable() {
	let (_mgr_dir, mgr) = make_manager().await;
	for locality in 0..64u64 {
		seed_record(
			&mgr,
			locality,
			hlc(1, 0, 1),
			&locality.to_le_bytes(),
			b"row",
		)
		.await;
	}
	let sm = WendelonStateMachine::new();
	let expired = QueryExecutionBudget {
		caller_deadline: Some(Instant::now() - Duration::from_millis(1)),
		..QueryExecutionBudget::default()
	};
	let (resp, arrow) = execute_query_plan_with_budget_and_retention_floor(
		&mgr,
		&sm,
		n(1),
		&base_meta(),
		&[],
		expired,
		None,
	)
	.await
	.unwrap();
	assert_eq!(resp.status, ReadStatus::QUERY_DEADLINE_EXCEEDED);
	assert!(arrow.is_none());

	let generous = QueryExecutionBudget {
		caller_deadline: Some(Instant::now() + Duration::from_secs(3600)),
		..QueryExecutionBudget::default()
	};
	let (resp, _arrow) = execute_query_plan_with_budget_and_retention_floor(
		&mgr,
		&sm,
		n(1),
		&base_meta(),
		&[],
		generous,
		None,
	)
	.await
	.unwrap();
	assert_eq!(resp.status, ReadStatus::OK);
	assert_eq!(resp.rows, 64);
}

/// ADR 541: a genuinely blocked operation is preempted by whichever clock is nearer,
/// and the expiry names that clock. Polling between work units can never see this —
/// a wedged read holds the query open forever, and a slow-but-successful one resets
/// the clock after the fact, hiding the overrun.
#[tokio::test]
async fn blocked_operation_is_preempted_and_attributed() {
	// Wedged forever, no caller deadline: the stall bound fires and names itself.
	let mut stalled = QueryClocks::new(Duration::from_millis(10), None);
	let outcome = stalled.bound(std::future::pending::<()>()).await;
	assert_eq!(outcome, Err(ReadStatus::QUERY_STALLED));

	// Wedged forever with the caller deadline nearer than the stall bound: the
	// deadline fires and names itself.
	let mut deadline = QueryClocks::new(
		Duration::from_secs(3600),
		Some(Instant::now() + Duration::from_millis(10)),
	);
	let outcome = deadline.bound(std::future::pending::<()>()).await;
	assert_eq!(outcome, Err(ReadStatus::QUERY_DEADLINE_EXCEEDED));

	// A completing operation marks progress; nothing expires.
	let mut progressing = QueryClocks::new(Duration::from_secs(3600), None);
	let before = progressing.progress_marks;
	let outcome = progressing.bound(async { 42u32 }).await;
	assert_eq!(outcome, Ok(42));
	assert_eq!(progressing.progress_marks, before + 1);
}

/// ADR 541: progress is a property of examination, not of output. A full scan whose
/// predicate matches nothing examines every location and must mark progress at each,
/// even though it never produces a batch.
#[tokio::test]
async fn no_match_full_scan_progresses_per_examined_location() {
	use arrow::array::Int64Array;
	use arrow::datatypes::{DataType, Field, Schema};
	let schema = Arc::new(Schema::new(vec![Field::new("age", DataType::Int64, false)]));
	let (_mgr_dir, mgr) = make_manager().await;
	for locality in 0..512u64 {
		let ages = Int64Array::from(vec![locality as i64]);
		let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(ages)]).unwrap();
		let bytes = encode_batch(&batch).unwrap();
		mgr.append(tenant(), id(locality), hlc(1 + locality, 0, 1), &bytes)
			.await
			.unwrap();
	}

	// age >= i64::MAX matches nothing anywhere.
	let mut bytecode = Vec::new();
	bytecode.push(OP_PUSH_COL);
	bytecode.extend_from_slice(&0u32.to_le_bytes());
	bytecode.push(OP_PUSH_INT);
	bytecode.extend_from_slice(&i64::MAX.to_le_bytes());
	bytecode.push(OP_GTE);

	let sm = WendelonStateMachine::new();
	let mut meta = base_meta();
	meta.bytecode_len = bytecode.len() as u32;
	let plan = plan_query_execution_with_budget_and_retention_floor(
		&mgr,
		&sm,
		n(1),
		&meta,
		&bytecode,
		// The marks are what is measured. The server's 250 ms stall bound is a
		// wall clock a starved scheduler can trip between two locations, which
		// would end the scan before it examined them all.
		QueryExecutionBudget {
			max_wall_time: Duration::from_secs(3600),
			..QueryExecutionBudget::default()
		},
		None,
	)
	.await
	.unwrap();
	assert_eq!(plan.response.status, ReadStatus::KEY_NOT_FOUND);
	assert!(
		plan.clocks.progress_marks >= 512,
		"a zero-result scan must progress per examined location, got {} marks",
		plan.clocks.progress_marks
	);
}

/// ADR 541: work caps stay real when a caller owns them. The same limits that no
/// longer cap a server-side scan's totality still terminate a caller-budgeted query
/// — the conversion re-scoped the caps, it did not delete them.
#[tokio::test]
async fn work_caps_bind_totals_only_for_caller_budgets() {
	let (_mgr_dir, mgr) = make_manager().await;
	for locality in 0..64u64 {
		seed_record(
			&mgr,
			locality,
			hlc(1, 0, 1),
			&locality.to_le_bytes(),
			b"row",
		)
		.await;
	}
	let sm = WendelonStateMachine::new();

	// Server-side defaults with tiny limits: work-per-yield derivations, not totals.
	let per_yield = QueryExecutionBudget {
		max_locations: 4,
		max_rows: 8,
		caller_totals: false,
		..QueryExecutionBudget::default()
	};
	let (resp, _arrow) = execute_query_plan_with_budget_and_retention_floor(
		&mgr,
		&sm,
		n(1),
		&base_meta(),
		&[],
		per_yield,
		None,
	)
	.await
	.unwrap();
	assert_eq!(resp.status, ReadStatus::OK);
	assert_eq!(resp.rows, 64);

	// The same limits caller-owned: a hard total, with the budget status.
	let caller_totals = QueryExecutionBudget {
		max_locations: 4,
		max_rows: 8,
		caller_totals: true,
		..QueryExecutionBudget::default()
	};
	let (resp, arrow) = execute_query_plan_with_budget_and_retention_floor(
		&mgr,
		&sm,
		n(1),
		&base_meta(),
		&[],
		caller_totals,
		None,
	)
	.await
	.unwrap();
	assert_eq!(resp.status, ReadStatus::QUERY_BUDGET_EXCEEDED);
	assert!(arrow.is_none());
}

/// ADR 541: expiry causes survive the aggregate protocol distinctly, in both
/// directions, without disturbing the neighbors they were once collapsed into.
#[test]
fn aggregate_reject_mapping_keeps_expiry_causes_distinct() {
	assert_eq!(
		aggregate_reject_from_read_status(ReadStatus::QUERY_STALLED),
		AggRejectCode::Stalled
	);
	assert_eq!(
		aggregate_reject_to_read_status(AggRejectCode::Stalled),
		ReadStatus::QUERY_STALLED
	);
	assert_eq!(
		aggregate_reject_from_read_status(ReadStatus::QUERY_DEADLINE_EXCEEDED),
		AggRejectCode::QueryDeadlineExceeded
	);
	assert_eq!(
		aggregate_reject_to_read_status(AggRejectCode::QueryDeadlineExceeded),
		ReadStatus::QUERY_DEADLINE_EXCEEDED
	);
	// The neighbors keep their meanings: budget stays budget, and the e-safe read
	// deadline keeps its historical code.
	assert_eq!(
		aggregate_reject_from_read_status(ReadStatus::QUERY_BUDGET_EXCEEDED),
		AggRejectCode::BudgetExceeded
	);
	assert_eq!(
		aggregate_reject_to_read_status(AggRejectCode::DeadlineExceeded),
		ReadStatus::E_SAFE_BEHIND
	);
}

/// ADR 541: a location ruled out by column stats is still an examined location. A
/// scan whose every slab is stats-pruned produces no output and reads no rows, yet
/// must mark progress for each location it dispositioned.
#[tokio::test]
async fn stats_pruned_full_scan_progresses_per_examined_location() {
	let l2 = Arc::new(MockObjectStore::default());
	let (_mgr_dir, mgr) = make_manager_with_l2(l2.clone()).await;
	let descriptor = typed_table_descriptor();
	let batch = typed_table_batch(vec![1, 2]);
	let mut write_meta: TableWriteRequestMeta = bytemuck::Zeroable::zeroed();
	write_meta.tenant_id = tenant();
	write_meta.table_object_id = descriptor.table_object_id;
	write_meta.schema_generation = descriptor.schema_generation;
	write_meta.row_count = encode_table_write_row_count(batch.num_rows()).unwrap();
	let admission = validate_table_write_request_batch(
		&write_meta,
		&batch,
		&descriptor,
		WriterNodeId::ZERO,
		write_meta.client_hlc,
	)
	.expect("typed rows should validate");
	for row in 0..batch.num_rows() {
		let row_batch = batch.slice(row, 1);
		let frame = encode_batch(&row_batch).unwrap();
		mgr.append_durable_with_locality(
			tenant(),
			admission.key_identities[row],
			admission.localities[row],
			hlc(10 + row as u64, 0, 1),
			&frame,
		)
		.await
		.unwrap();
	}
	let slab_id = mgr.force_seal_active_for_test().await;
	let raw = mgr.read_sealed_raw(slab_id).await.unwrap();
	let mut catalog = TableSchemaCatalog::default();
	catalog.upsert(tenant(), descriptor.clone()).unwrap();
	let encrypted = mgr
		.encrypt_tiered_object_blob_with_table_stats(slab_id, &raw, &catalog)
		.unwrap();
	let locator = encrypted.stats_locator.expect("stats locator");
	let l2_uri = l2
		.put(
			&format!("adr541-stats-prune-{slab_id}.wnd"),
			&encrypted.bytes,
		)
		.await
		.unwrap();
	mgr.apply_tier_slab(TieredSlabPointer {
		slab_id,
		l2_uri,
		payload_hash: payload_digest(&encrypted.bytes),
		is_encrypted: true,
		tenant_id: Some(encrypted.tenant_id),
		tenant_kek_id: Some(encrypted.tenant_kek_id),
		stats_format_version: Some(locator.stats_format_version),
		stats_block_len: Some(locator.stats_block_len),
		stats_block_hash128: Some(locator.stats_block_hash128),
		tombstone_metadata: TombstoneMetadata::None,
		supersession_metadata: SupersessionMetadata::None,
	})
	.await
	.unwrap();

	// id >= 100 is outside the slab's id min/max: every location prunes by stats.
	let mut bytecode = Vec::new();
	bytecode.push(OP_PUSH_COL);
	bytecode.extend_from_slice(&0_u32.to_le_bytes());
	bytecode.push(OP_PUSH_INT);
	bytecode.extend_from_slice(&100_i64.to_le_bytes());
	bytecode.push(OP_GTE);
	let program = pbc::Program::compile(&bytecode).unwrap();
	let meta = base_meta();
	let ranges = [QueryScanRange {
		start: Locality::ZERO,
		end: Locality::MAX,
	}];

	let plan = plan_query_execution_for_ranges_with_program(
		&mgr,
		n(1),
		&meta,
		&program,
		&ranges,
		QueryExecutionBudget::default(),
		None,
		Some(&descriptor),
		QueryProjectedExecution::Disabled,
		RowIdentityProjection::None,
		None,
	)
	.await
	.unwrap();

	assert_eq!(plan.response.status, ReadStatus::KEY_NOT_FOUND);
	assert_eq!(plan.stats_explain.slabs_pruned_by_stats, 1);
	assert!(
		plan.clocks.progress_marks >= 2,
		"stats-pruned locations are examined locations and must mark progress, got {}",
		plan.clocks.progress_marks
	);
}

/// ADR 541: the table-query wire and a caller-transmitted budget carry *different*
/// absent-sentinels, and conflating them inverts the strictest budget into the loosest.
/// On the wire, zero means "no deadline sent" — the client transmits one millisecond for
/// an already-expired deadline so expiry stays attributable server-side. In an aggregate
/// budget, `u64::MAX` means "no cap" and zero is a real budget of no time at all.
#[test]
fn deadline_sentinels_do_not_collapse() {
	// Wire rule: zero is absence, not a zero-length deadline.
	assert_eq!(caller_deadline_from_remaining_millis(0), None);
	// The one-millisecond expired-deadline encoding still yields a real instant.
	assert!(caller_deadline_from_remaining_millis(1).is_some());

	// Budget rule: u64::MAX is absence...
	assert_eq!(caller_deadline_from_budget_millis(u64::MAX), None);
	// ...and zero is an already-expired deadline, NOT the absence of one.
	let expired = caller_deadline_from_budget_millis(0)
		.expect("a zero wall-time budget is a deadline of no time, not an absent deadline");
	assert!(expired <= Instant::now());

	// A *present* deadline stays present at any magnitude. An unrepresentable instant
	// clamps to the furthest the platform can hold; it must never come back as `None`,
	// because that would let an absurd wire value delete the caller's cap entirely —
	// presence degrading into absence is the same permissive inversion as conflating
	// the two sentinels above.
	assert!(
		caller_deadline_from_remaining_millis(u64::MAX).is_some(),
		"an unrepresentable wire deadline must clamp, never degrade to no deadline"
	);
	assert!(
		caller_deadline_from_budget_millis(u64::MAX - 1).is_some(),
		"an unrepresentable caller budget must clamp, never degrade to no deadline"
	);
}

/// ADR 541: a zero aggregate wall-time budget must expire the query attributably rather
/// than removing its deadline. This is the discriminating control for the sentinel
/// collapse — under the wire rule it would read as "no caller deadline" and the caller's
/// strictest possible budget would become an uncapped one.
#[test]
fn zero_aggregate_wall_budget_expires_rather_than_uncaps() {
	let budgets = AggregateBudgets {
		max_wall_time_millis: DurationMillis::new(0),
		..AggregateBudgets::default()
	};

	let budget = aggregate_query_budget(&budgets);

	let deadline = budget
		.caller_deadline
		.expect("a zero wall-time budget must yield a deadline, not remove it");
	let clocks = QueryClocks::from_budget(&budget);
	assert_eq!(
		clocks.expired(),
		Some(ReadStatus::QUERY_DEADLINE_EXCEEDED),
		"an exhausted caller wall budget expires as the deadline cause"
	);
	assert!(deadline <= Instant::now());

	// The unlimited sentinel still means unlimited.
	let unlimited = aggregate_query_budget(&AggregateBudgets::default());
	assert_eq!(unlimited.caller_deadline, None);
}

/// ADR 541: encoding is synchronous CPU work, so the stall bound can only hold if each
/// encode call is *small*. A batch larger than the quantum must be sliced into bounded
/// units rather than handed to the encoder whole — otherwise one oversize batch overruns
/// the bound with nothing able to observe it, which is the hole the review named. This
/// fails if quantum slicing regresses to whole-batch encoding.
#[tokio::test]
async fn oversize_batch_encodes_in_bounded_quanta() {
	use arrow::array::Int64Array;
	use arrow::datatypes::{DataType, Field, Schema};

	let rows = ENCODE_QUANTUM_ROWS * 2 + 7;
	let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
	let values = Int64Array::from((0..rows as i64).collect::<Vec<_>>());
	let batch = RecordBatch::try_new(schema, vec![Arc::new(values)]).unwrap();

	let mut clocks = QueryClocks::new(Duration::from_secs(3600), None);
	let encoded = encode_query_stream_batches_with_deadline(vec![batch], &mut clocks)
		.await
		.unwrap()
		.expect("a progressing encode never expires");

	// ceil(rows / quantum) quanta — one indivisible unit would be a single blob.
	let expected = rows.div_ceil(ENCODE_QUANTUM_ROWS);
	assert_eq!(
		encoded.len(),
		expected,
		"an oversize batch must encode in {expected} bounded quanta, got {}",
		encoded.len()
	);
	assert!(expected > 1, "the fixture must actually exceed one quantum");

	// Every quantum is a unit of progress, so the marks track the units of work.
	assert!(
		clocks.progress_marks >= expected as u64,
		"each encoded quantum marks progress; got {} marks for {expected} quanta",
		clocks.progress_marks
	);
}

/// ADR 541: a batch at or under the quantum must be untouched by slicing — the bound is
/// a ceiling on one unit of work, not a re-chunking of every result.
#[tokio::test]
async fn batch_within_the_quantum_encodes_as_one_blob() {
	let mut clocks = QueryClocks::new(Duration::from_secs(3600), None);
	let encoded = encode_query_stream_batches_with_deadline(vec![single_kv_batch()], &mut clocks)
		.await
		.unwrap()
		.expect("a progressing encode never expires");

	assert_eq!(encoded.len(), 1);
}

/// ADR 541: a row bound alone does not bound encode work. This batch is far below
/// `ENCODE_QUANTUM_ROWS` yet far above the byte bound, so a row-only quantum would hand
/// the whole thing to one synchronous encode — the exact hole the quantum exists to
/// close. Fails if the byte bound stops binding.
#[tokio::test]
async fn byte_heavy_batch_below_the_row_bound_still_splits() {
	use arrow::array::StringArray;
	use arrow::datatypes::{DataType, Field, Schema};

	let rows = 64usize;
	let value = "x".repeat(512 * 1024);
	let schema = Arc::new(Schema::new(vec![Field::new("blob", DataType::Utf8, false)]));
	let array = StringArray::from(vec![value.as_str(); rows]);
	let batch = RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap();

	assert!(
		rows < ENCODE_QUANTUM_ROWS,
		"the fixture must stay under the row bound, else it proves nothing about bytes"
	);
	let quantum = encode_quantum_len(&batch, 0).expect("no single row exceeds the bound");
	assert!(
		quantum < rows,
		"the byte bound must bind first: {quantum} rows/quantum for {rows} rows"
	);

	let mut clocks = QueryClocks::new(Duration::from_secs(3600), None);
	let encoded = encode_query_stream_batches_with_deadline(vec![batch], &mut clocks)
		.await
		.unwrap()
		.expect("a progressing encode never expires");

	assert_eq!(encoded.len(), rows.div_ceil(quantum));
	assert!(
		encoded.len() > 1,
		"a byte-heavy batch must not encode whole"
	);
}

/// ADR 541: a row wider than the byte bound is *refused*, not encoded anyway. There is
/// nothing beneath one row to split, so the bound cannot be met for it — and a bound that
/// is knowingly exceeded is not a bound. The refusal is typed and happens before any
/// encode work is spawned.
#[tokio::test]
async fn a_row_wider_than_the_byte_bound_is_rejected_before_encoding() {
	use arrow::array::StringArray;
	use arrow::datatypes::{DataType, Field, Schema};

	let rows = 3usize;
	let value = "x".repeat(ENCODE_QUANTUM_BYTES + 1024);
	let schema = Arc::new(Schema::new(vec![Field::new("blob", DataType::Utf8, false)]));
	let array = StringArray::from(vec![value.as_str(); rows]);
	let batch = RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap();

	assert_eq!(
		encode_quantum_len(&batch, 0),
		None,
		"a row wider than the bound admits no satisfying quantum"
	);

	let mut clocks = QueryClocks::new(Duration::from_secs(3600), None);
	let outcome = encode_query_stream_batches_with_deadline(vec![batch], &mut clocks)
		.await
		.unwrap();

	assert_eq!(outcome, Err(ReadStatus::QUERY_BUDGET_EXCEEDED));
	assert_eq!(
		clocks.progress_marks, 0,
		"the refusal must precede any encode work"
	);
}

/// ADR 541: skewed row widths are the case an averaged byte estimate gets wrong. With
/// `get_array_memory_size() / rows`, many narrow values beside one wide one average to a
/// small per-row width and the wide row is grouped with its neighbours. Slice-aware
/// measurement must isolate it wherever it sits — first, middle, or last — and must reject
/// it outright once it alone exceeds the bound.
///
/// Sizing matters here: the wide row is chosen so that it fits the bound *alone* but not
/// beside any neighbour. A wide row merely close to the bound with negligible neighbours
/// would legitimately group with them, which would test nothing.
#[tokio::test]
async fn skewed_batch_isolates_or_rejects_the_wide_row() {
	use arrow::array::StringArray;
	use arrow::datatypes::{DataType, Field, Schema};

	const NARROW_BYTES: usize = 1024 * 1024;
	let rows = 32usize;

	fn skewed(rows: usize, wide_at: usize, wide_bytes: usize, narrow_bytes: usize) -> RecordBatch {
		let schema = Arc::new(Schema::new(vec![Field::new("blob", DataType::Utf8, false)]));
		let wide = "x".repeat(wide_bytes);
		let narrow = "n".repeat(narrow_bytes);
		let values: Vec<&str> = (0..rows)
			.map(|index| {
				if index == wide_at {
					wide.as_str()
				} else {
					narrow.as_str()
				}
			})
			.collect();
		let array = StringArray::from(values);
		RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap()
	}

	for wide_at in [0usize, 16, 31] {
		// Fits alone, cannot fit beside a neighbour: must be isolated.
		let isolatable = ENCODE_QUANTUM_BYTES - NARROW_BYTES / 2;
		let batch = skewed(rows, wide_at, isolatable, NARROW_BYTES);
		assert_eq!(
			encode_quantum_len(&batch, wide_at),
			Some(1),
			"the wide row at {wide_at} must be isolated, not grouped with neighbours"
		);
		let mut clocks = QueryClocks::new(Duration::from_secs(3600), None);
		let encoded = encode_query_stream_batches_with_deadline(vec![batch], &mut clocks)
			.await
			.unwrap()
			.expect("an isolatable wide row still encodes");
		assert!(encoded.len() > 1, "isolation implies more than one quantum");

		// Exceeds the bound alone: refused, wherever it sits.
		let batch = skewed(rows, wide_at, ENCODE_QUANTUM_BYTES + 1024, NARROW_BYTES);
		let mut clocks = QueryClocks::new(Duration::from_secs(3600), None);
		let outcome = encode_query_stream_batches_with_deadline(vec![batch], &mut clocks)
			.await
			.unwrap();
		assert_eq!(
			outcome,
			Err(ReadStatus::QUERY_BUDGET_EXCEEDED),
			"a wide row at {wide_at} exceeding the bound must be refused"
		);
	}
}

/// ADR 541: the blocking-worker invariant, proved with an injected slow encoder rather
/// than inferred from the generic clock helper. Holds every slot occupied and then shows
/// that a further attempt (a) expires with a typed cause, (b) never enters blocking work
/// at all, and (c) that permits recover once the held work truly ends. Without the
/// concurrency bound, abandoned encodes would accumulate on the blocking pool — which is
/// precisely what a detached `spawn_blocking` cannot prevent.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn blocking_encode_slots_bound_concurrency_and_keep_expiry_typed() {
	use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

	let limit = encode_blocking_slot_count();
	let entered = Arc::new(AtomicUsize::new(0));
	// Held work self-releases on a deadline rather than rendezvousing with this task.
	// A barrier would park the blocking pool forever the moment an assertion below
	// panicked, turning a failing test into a hanging one — strictly worse than a red.
	let release = Arc::new(AtomicBool::new(false));
	let hold_limit = Duration::from_secs(120);

	let mut holders = Vec::with_capacity(limit);
	for _ in 0..limit {
		let entered = Arc::clone(&entered);
		let release = Arc::clone(&release);
		holders.push(tokio::spawn(async move {
			let mut clocks = QueryClocks::new(Duration::from_secs(3600), None);
			bounded_blocking_encode(&mut clocks, move || {
				entered.fetch_add(1, Ordering::SeqCst);
				let deadline = std::time::Instant::now() + hold_limit;
				while !release.load(Ordering::SeqCst) && std::time::Instant::now() < deadline {
					std::thread::sleep(Duration::from_millis(5));
				}
				Ok(Vec::<u8>::new())
			})
			.await
			.map(|inner| inner.is_ok())
		}));
	}

	// Every slot genuinely occupied before the queued attempt is made — but bounded, and
	// releasing before it fails. The platform may simply be unable to start
	// `available_parallelism()` blocking workers, and an unbounded wait would turn that
	// into a hang rather than a diagnosis.
	let waited_from = std::time::Instant::now();
	while entered.load(Ordering::SeqCst) < limit {
		if waited_from.elapsed() >= Duration::from_secs(60) {
			release.store(true, Ordering::SeqCst);
			panic!(
				"only {} of {limit} blocking workers started within 60s; this platform \
				 cannot supply the slots this fixture assumes",
				entered.load(Ordering::SeqCst)
			);
		}
		tokio::time::sleep(Duration::from_millis(5)).await;
	}

	let queued_ran = Arc::new(AtomicUsize::new(0));
	let mut tight = QueryClocks::new(Duration::from_millis(20), None);
	let outcome = {
		let queued_ran = Arc::clone(&queued_ran);
		bounded_blocking_encode(&mut tight, move || {
			queued_ran.fetch_add(1, Ordering::SeqCst);
			Ok(Vec::<u8>::new())
		})
		.await
	};
	let queued_entered = queued_ran.load(Ordering::SeqCst);
	let peak_entered = entered.load(Ordering::SeqCst);
	let expiry = outcome.err();

	// Release before asserting, so a red is a red and never a hang.
	release.store(true, Ordering::SeqCst);

	assert_eq!(
		expiry,
		Some(ReadStatus::QUERY_STALLED),
		"a queued attempt expires through the clocks with its cause named"
	);
	assert_eq!(
		queued_entered, 0,
		"a queued attempt must never enter blocking work"
	);
	assert_eq!(
		peak_entered, limit,
		"no more than the slot bound may occupy blocking work at once"
	);

	for holder in holders {
		let encoded_ok = holder.await.expect("holder task joins");
		assert_eq!(encoded_ok, Ok(true));
	}

	// Permits recover only when the work truly ended — not when a caller stopped waiting.
	let recovered = Arc::new(AtomicUsize::new(0));
	let mut after = QueryClocks::new(Duration::from_secs(3600), None);
	let outcome = {
		let recovered = Arc::clone(&recovered);
		bounded_blocking_encode(&mut after, move || {
			recovered.fetch_add(1, Ordering::SeqCst);
			Ok(Vec::<u8>::new())
		})
		.await
	};
	assert!(outcome.is_ok(), "a released slot admits new work");
	assert_eq!(recovered.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn adr0088_encoded_frame_cap_is_enforced_on_real_bytes_not_an_estimate() {
	// The row target and the in-memory estimate are both heuristics over
	// uncompressed shape. Only the encoded payload is what the wire carries, so
	// a batch whose rows are individually small but collectively encode past
	// the cap must still be split — an estimate-only bound would pass it whole.
	let schema = Arc::new(Schema::new(vec![Field::new(
		"payload",
		DataType::Utf8,
		false,
	)]));
	// The payload must resist compression: the encoder compresses this path, so
	// repetitive filler would shrink far under the cap and the split would
	// never be exercised. A xorshift stream is close enough to incompressible.
	fn incompressible(seed: u64, len: usize) -> String {
		let mut state = seed | 1;
		let mut out = String::with_capacity(len);
		for _ in 0..len {
			state ^= state << 13;
			state ^= state >> 7;
			state ^= state << 17;
			// Printable ASCII, uniformly spread.
			out.push((b'!' + (state % 90) as u8) as char);
		}
		out
	}
	// 4,000 rows x ~4 KiB. Deliberately under the 4,096-row target, so the row
	// bound cannot be what splits this — only the encoded byte bound can.
	let rows: Vec<String> = (0..4_000)
		.map(|row| incompressible(row as u64 + 1, 4_096))
		.collect();
	let batch = RecordBatch::try_new(
		schema.clone(),
		vec![Arc::new(arrow::array::StringArray::from(rows)) as ArrayRef],
	)
	.unwrap();

	let whole = wire_encode_batch_for_path(&batch, NetworkCompressionPath::QueryResponse).unwrap();
	println!(
		"PROBE encoded_len={} cap={}",
		whole.len(),
		MAX_SECTION_BYTES
	);

	// The premise, checked rather than assumed: this fixture must actually
	// exceed the cap, or the split below proves nothing. And it must stay under
	// the row target, or the row bound could be what split it.
	let whole = wire_encode_batch_for_path(&batch, NetworkCompressionPath::QueryResponse).unwrap();
	assert!(
		whole.len() > MAX_SECTION_BYTES as usize,
		"the fixture must encode past the cap: {} vs {}",
		whole.len(),
		MAX_SECTION_BYTES
	);
	assert!(batch.num_rows() < TABLE_QUERY_RESPONSE_TARGET_ROWS);

	let mut accumulator = TableQueryResponseAccumulator::default();
	let mut batches = Vec::new();
	accumulator
		.push(batch.clone(), None, &mut batches)
		.await
		.unwrap();
	accumulator.flush(None, &mut batches).await.unwrap();

	assert!(
		batches.len() > 1,
		"a payload past the encoded cap must be split, not emitted whole (len={}, rows={:?})",
		batches.len(),
		batches
			.iter()
			.map(RecordBatch::num_rows)
			.collect::<Vec<_>>()
	);
	assert_eq!(
		batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
		batch.num_rows(),
		"splitting preserves every row"
	);
	for emitted in &batches {
		let encoded =
			wire_encode_batch_for_path(emitted, NetworkCompressionPath::QueryResponse).unwrap();
		assert!(
			encoded.len() <= MAX_SECTION_BYTES as usize,
			"every emitted batch must fit the frame cap, got {} bytes",
			encoded.len()
		);
	}

	// Order survives the split: the first emitted row is the first source row.
	let first = batches[0]
		.column(0)
		.as_any()
		.downcast_ref::<arrow::array::StringArray>()
		.unwrap();
	let source = batch
		.column(0)
		.as_any()
		.downcast_ref::<arrow::array::StringArray>()
		.unwrap();
	assert_eq!(first.value(0), source.value(0));
}

#[tokio::test]
async fn adr0088_a_single_row_over_the_frame_cap_is_refused_by_name() {
	// Splitting cannot rescue one row that does not fit. That is the
	// pre-existing oversized-payload condition, and it must stay a typed
	// refusal rather than an emitted frame a receiver will reject.
	let schema = Arc::new(Schema::new(vec![Field::new(
		"payload",
		DataType::Utf8,
		false,
	)]));
	// Incompressible, for the same reason as above.
	let huge: String = {
		let mut state = 0x9E3779B97F4A7C15u64;
		let mut out = String::with_capacity(8 * 1024 * 1024);
		for _ in 0..8 * 1024 * 1024 {
			state ^= state << 13;
			state ^= state >> 7;
			state ^= state << 17;
			out.push((b'!' + (state % 90) as u8) as char);
		}
		out
	};
	let batch = RecordBatch::try_new(
		schema,
		vec![Arc::new(arrow::array::StringArray::from(vec![huge])) as ArrayRef],
	)
	.unwrap();

	let mut accumulator = TableQueryResponseAccumulator::default();
	let mut batches = Vec::new();
	// The estimate can flush inside `push`, so the refusal may surface from
	// either call; what matters is that it surfaces and names the bound.
	let error = match accumulator.push(batch, None, &mut batches).await {
		Err(error) => error,
		Ok(()) => accumulator
			.flush(None, &mut batches)
			.await
			.expect_err("one oversized row cannot be split into a legal frame"),
	};
	assert!(
		error.to_string().contains("frame cap"),
		"the refusal names the bound it hit: {error}"
	);
}

/// ADR 579: a row limit bounds producer work to exactly the limit, over a source
/// larger than one response quantum. The strict-inequality form of this claim —
/// used when the response quantum landed — cannot distinguish stopping at the
/// limit from stopping at the next quantum boundary, which is precisely the gap
/// that let a bounded read cost a full quantum of speculative locations.
#[tokio::test]
async fn adr0579_row_limit_bounds_producer_work_to_exactly_the_limit() {
	const SOURCE_ROWS: usize = 5_000;
	const LIMIT: usize = 2_000;
	assert!(
		SOURCE_ROWS > TABLE_QUERY_RESPONSE_TARGET_ROWS,
		"the source must exceed one response quantum or there is no rounding to observe"
	);
	assert!(
		LIMIT < TABLE_QUERY_RESPONSE_TARGET_ROWS,
		"the limit must fall inside the first quantum or rounding is unobservable"
	);
	let (_mgr_dir, mgr, descriptor, program, meta, ranges) =
		descriptor_backed_whole_record_cost_bound_fixture_with_rows(SOURCE_ROWS).await;

	// Premise: the counter tracks locations one-for-one, so an equality on it below
	// is a statement about producer work rather than about batching.
	let full = plan_query_execution_for_ranges_with_program(
		&mgr,
		n(1),
		&meta,
		&program,
		&ranges,
		QueryExecutionBudget::default(),
		None,
		Some(&descriptor),
		QueryProjectedExecution::Disabled,
		RowIdentityProjection::None,
		None,
	)
	.await
	.unwrap();
	assert_eq!(full.response.status, ReadStatus::OK);
	// ADR 0089 changed the exactness instrument: progress marks are no longer a
	// whole multiple of rows, because rows of one packed record share per-record
	// work. The per-request decode audit IS row-exact producer work now: the
	// append-only fixture packs sequential records, so a scan of N rows decodes
	// exactly the records containing them.
	let full_decodes = full.scan_decode_audit.distinct_records_decoded;
	assert!(
		full_decodes > 0 && full_decodes < SOURCE_ROWS as u64,
		"premise: the source is genuinely packed ({full_decodes} records for {SOURCE_ROWS} rows)"
	);
	assert_eq!(
		SOURCE_ROWS as u64 % full_decodes,
		0,
		"premise: uniform records, so decode counts divide exactly"
	);
	let rows_per_record = SOURCE_ROWS as u64 / full_decodes;

	let limited = plan_query_execution_for_ranges_with_program(
		&mgr,
		n(1),
		&meta,
		&program,
		&ranges,
		QueryExecutionBudget::default(),
		Some(LIMIT),
		Some(&descriptor),
		QueryProjectedExecution::Disabled,
		RowIdentityProjection::None,
		None,
	)
	.await
	.unwrap();
	assert_eq!(limited.response.status, ReadStatus::OK);
	assert_eq!(limited.response.rows, LIMIT as u64);
	assert_eq!(
		limited.scan_decode_audit.distinct_records_decoded,
		(LIMIT as u64).div_ceil(rows_per_record),
		"a bounded read must stop at the limit, not at the next response quantum \
		 (rounding to the quantum would decode {})",
		(TABLE_QUERY_RESPONSE_TARGET_ROWS as u64).div_ceil(rows_per_record)
	);
	assert!(
		limited.clocks.progress_marks < full.clocks.progress_marks,
		"bounded producer work stays under the full scan's"
	);
}

/// ADR 0090 route-selection gate. Route primacy for a point read is judged from
/// the record's own stamped placement BEFORE any slab byte is touched: a
/// partitioned entry travels the real READ_REQ route and is owned by its
/// partition's replica set, while a legacy entry stays on its raw locality.
/// Every installed location dangles (slab 0xDEAD), so any typed response at all
/// proves the route decision happened pre-slab.
pub(super) mod adr0090_route_selection {
	use super::*;
	use crate::schema::PartitionId;
	use crate::schema::table::PlacementSemantics;

	async fn routed_server(
		node: &WendelonNode,
		mgr: Arc<SlabManager>,
		nodes: Vec<NodeId>,
	) -> WendelonDataServer {
		routed_server_with_lease_gate(node, mgr, nodes, true).await
	}

	pub(super) async fn routed_server_with_lease_gate(
		node: &WendelonNode,
		mgr: Arc<SlabManager>,
		nodes: Vec<NodeId>,
		lease_gate: bool,
	) -> WendelonDataServer {
		let tiers = TransportTierSet::empty();
		let roles = NodeRoleSet::empty().with(NodeRole::Data);
		let mut peers = std::collections::HashMap::new();
		peers.insert(
			node.local_node,
			PeerCapabilities {
				address: "loopback://77".to_string(),
				tiers,
				roles,
				capabilities: crate::network::transport::NodeCapabilitySet::empty(),
			},
		);
		let epoch = MembershipEpoch { epoch: 1, peers };
		let (_epoch_tx, epoch_rx) = watch::channel(Arc::new(epoch));
		let resolver: Arc<dyn crate::network::transport::CapabilityResolver> = Arc::new(
			crate::network::transport::EpochCapabilityResolver::new(epoch_rx.clone()),
		);
		let factory = TransportProviderFactory::new(
			tiers,
			std::collections::HashMap::new(),
			resolver,
			epoch_rx,
		);
		WendelonDataServer::new(
			mgr,
			node.raft.clone(),
			node.log_store.clone(),
			node.state_machine.clone(),
			node.local_node,
			node.clock.clone(),
			Arc::new(StaticReplicaRouter::new(nodes, 1)),
			factory,
			node.pool.clone(),
		)
		.unwrap()
		.with_lease_gate(lease_gate)
	}

	async fn dispatch_point_read(server: &WendelonDataServer, node: &WendelonNode) -> ReadStatus {
		let meta = simple_read_meta(1);
		let key = encode_batch(&single_key_batch()).unwrap();
		let mut meta_buf = node.pool.acquire();
		meta_buf
			.as_mut_vec()
			.extend_from_slice(bytemuck::bytes_of(&meta));
		let mut arrow_buf = node.pool.acquire();
		arrow_buf.as_mut_vec().extend_from_slice(&key);
		let frame = HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: MsgType::READ_REQ.0,
				flags: 0,
				stream_id: 1,
				meta_len: meta_buf.len() as u32,
				arrow_len: arrow_buf.len() as u32,
				trailing_len: 0,
			},
			meta: meta_buf,
			arrow_payload: Some(arrow_buf),
			trailing: None,
		};
		let resp = DataRingServer::handle_data_frame(
			server,
			RequestContext {
				remote: Some(node.local_node),
				actor: actor(ActorRole::Operator, None),
			},
			frame,
		)
		.await
		.unwrap()
		.unwrap();
		let meta: ReadResponseMeta =
			*bytemuck::from_bytes(&resp.meta[..std::mem::size_of::<ReadResponseMeta>()]);
		meta.status
	}

	fn dangling(
		hlc_ms: u64,
		placement: PlacementSemantics,
		partition: Option<PartitionId>,
	) -> RecordLocation {
		RecordLocation {
			slab_id: SlabId::new(0xDEAD),
			offset: 0,
			length: 8,
			hlc: hlc(hlc_ms, 0, 1),
			record_row_index: RecordRowIndex::ZERO,
			is_tombstone: false,
			placement,
			partition,
		}
	}

	#[tokio::test]
	async fn adr0090_a_partitioned_entry_routes_by_partition_and_legacy_by_locality() {
		let dir = bootstrap_tempdir("adr0090-route");
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			77,
			dir.to_path_buf(),
			"127.0.0.1:0".into(),
		))
		.await
		.unwrap();
		let (_mgr_dir, mgr) = make_manager().await;
		let local = node.local_node;
		let key_identity = simple_read_meta(1).key_identity;
		let raw = Locality::from(key_identity).get();

		// Static router, two nodes, RF 1: the primary is nodes[value % 2] with
		// nodes sorted by id. Choose the peer id so the key's RAW locality
		// routes to the peer — the local node is NOT primary for the legacy
		// reading of this key. Parities for the partition values follow.
		let (peer, local_parity) = if raw % 2 == 0 {
			(
				NodeId {
					id: local.id.checked_sub(1).expect("dev node id is nonzero"),
					_reserved: 0,
				},
				1u64,
			)
		} else {
			(
				NodeId {
					id: local.id + 1,
					_reserved: 0,
				},
				0u64,
			)
		};
		let server = routed_server(&node, mgr.clone(), vec![local, peer]).await;
		let tenant_id = simple_read_meta(1).tenant_id;

		// 1. Legacy entry: primacy judged on the raw locality, which routes to
		// the peer. The local node refuses with NOT_LEADER.
		mgr.install_version_for_route_test(
			tenant_id,
			key_identity,
			dangling(5, PlacementSemantics::RawLocality, None),
		);
		assert_eq!(
			dispatch_point_read(&server, &node).await,
			ReadStatus::NOT_LEADER,
			"a legacy record stays on its raw locality"
		);

		// 2. A newer partitioned entry whose partition routes to the LOCAL
		// node: primacy now passes, and with no lease filed the read stops at
		// LEASE_EXPIRED — after routing, before any slab byte.
		mgr.install_version_for_route_test(
			tenant_id,
			key_identity,
			dangling(
				6,
				PlacementSemantics::Partitioned,
				Some(PartitionId::new(0x4000 + local_parity)),
			),
		);
		assert_eq!(
			dispatch_point_read(&server, &node).await,
			ReadStatus::LEASE_EXPIRED,
			"a partitioned record is owned by its partition's replica set"
		);

		// 3. An even newer partitioned entry whose partition routes to the
		// peer: NOT_LEADER again, so the partition value is genuinely
		// consulted rather than partitioned records short-circuiting to
		// local primacy.
		mgr.install_version_for_route_test(
			tenant_id,
			key_identity,
			dangling(
				7,
				PlacementSemantics::Partitioned,
				Some(PartitionId::new(0x4000 + (1 - local_parity))),
			),
		);
		assert_eq!(
			dispatch_point_read(&server, &node).await,
			ReadStatus::NOT_LEADER,
			"the partition value governs, not merely the partitioned stamp"
		);

		shutdown_lease_node(node).await;
	}

	/// ADR 0090 step 3, real route: an in-flight partition transfer overrides
	/// ring primacy on the production READ_REQ path. The ring says the peer owns
	/// this partition; the ledger says the transfer owner is the local node — so
	/// the local node serves (stopping at the lease gate), and completing the
	/// transfer hands primacy back to the ring, which refuses with NOT_LEADER.
	#[tokio::test]
	async fn adr0090_an_in_flight_transfer_overrides_ring_primacy_on_the_read_route() {
		use crate::consensus::{MigrationLedgerEntry, MigrationState, partition_span};

		let dir = bootstrap_tempdir("adr0090-transfer-route");
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			79,
			dir.to_path_buf(),
			"127.0.0.1:0".into(),
		))
		.await
		.unwrap();
		let (_mgr_dir, mgr) = make_manager().await;
		let local = node.local_node;
		let key_identity = simple_read_meta(1).key_identity;
		let raw = Locality::from(key_identity).get();

		// A partition whose parity routes to the PEER on the two-node static
		// ring, so the ledger override — not the ring — is what lets the local
		// node serve. The peer id is chosen so BOTH the raw locality and the
		// partition route away from the local node.
		let (peer, peer_parity) = if raw % 2 == 0 {
			(
				NodeId {
					id: local.id.checked_sub(1).expect("dev node id is nonzero"),
					_reserved: 0,
				},
				0u64,
			)
		} else {
			(
				NodeId {
					id: local.id + 1,
					_reserved: 0,
				},
				1u64,
			)
		};
		let partition = PartitionId::new(0x4000 + peer_parity);
		let server = routed_server(&node, mgr.clone(), vec![local, peer]).await;
		mgr.install_version_for_route_test(
			simple_read_meta(1).tenant_id,
			key_identity,
			dangling(5, PlacementSemantics::Partitioned, Some(partition)),
		);

		// Ring alone: the peer owns the partition, the local node refuses.
		assert_eq!(
			dispatch_point_read(&server, &node).await,
			ReadStatus::NOT_LEADER,
			"without a transfer the ring's owner governs"
		);

		// An in-flight transfer to the local node: primacy passes and the read
		// stops at the lease gate — served here, and only here.
		let (start, end) = partition_span(partition, 16).unwrap();
		node.state_machine
			.seed_migration_for_test(MigrationLedgerEntry {
				migration_id: 31,
				range_start: start,
				range_end: end,
				from_epoch: 0,
				to_epoch: 1,
				logical_owner: local,
				upstream_migration_id: None,
				source_replicas: vec![peer],
				migration_hlc_floor: hlc(0, 0, 0),
				state: MigrationState::BackfillInProgress,
				completed_by: None,
				completed_at_hlc: None,
			})
			.await;
		assert_eq!(
			dispatch_point_read(&server, &node).await,
			ReadStatus::LEASE_EXPIRED,
			"an in-flight transfer makes its owner — and only its owner — serve"
		);

		// Completed: serving reverts to the ring, which still names the peer.
		node.state_machine
			.seed_migration_for_test(MigrationLedgerEntry {
				migration_id: 31,
				range_start: start,
				range_end: end,
				from_epoch: 0,
				to_epoch: 1,
				logical_owner: local,
				upstream_migration_id: None,
				source_replicas: vec![peer],
				migration_hlc_floor: hlc(0, 0, 0),
				state: MigrationState::Completed,
				completed_by: Some(local),
				completed_at_hlc: Some(hlc(9, 0, 1)),
			})
			.await;
		assert_eq!(
			dispatch_point_read(&server, &node).await,
			ReadStatus::NOT_LEADER,
			"a completed transfer hands primacy back to the ring"
		);

		shutdown_lease_node(node).await;
	}

	/// ADR 0090 ingress control: a REAL partitioned table write, through the
	/// production `table_write` path, lands on the node that owns its
	/// PARTITION — while the same row's raw locality is owned by the peer. The
	/// received owner is proven from durable local state before any read.
	#[tokio::test]
	async fn adr0090_a_partitioned_write_lands_on_the_partition_owner() {
		let dir = bootstrap_tempdir("adr0090-write-owner");
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			78,
			dir.to_path_buf(),
			"127.0.0.1:0".into(),
		))
		.await
		.unwrap();
		let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
		loop {
			if node.raft.current_leader().await == Some(node.local_node.id) {
				break;
			}
			assert!(
				tokio::time::Instant::now() < deadline,
				"timed out waiting for leader"
			);
			tokio::time::sleep(Duration::from_millis(20)).await;
		}
		node.data_server
			.refresh_leases_once(&node.ring, &*node.clock, 60_000)
			.await;
		let descriptor = multirow_typed_table_descriptor();
		node.raft
			.client_write(WendelonRequest::SchemaUpdate(typed_table_schema_update(
				&descriptor,
			)))
			.await
			.unwrap();
		let local = node.local_node;

		// Find a row whose RAW locality is odd: under a two-node static router
		// its raw locality routes to nodes[1] while its partition — the top 63
		// bits, always even — routes to nodes[0]. The locality of a PK row is
		// deterministic, so it can be computed before writing.
		let probe_hlc = hlc(1, 0, 1);
		let mut chosen = None;
		for id in 1..512i64 {
			let batch = typed_table_batch(vec![id]);
			let meta = typed_table_meta(&descriptor, batch.num_rows());
			let admission = validate_table_write_request_batch(
				&meta,
				&batch,
				&descriptor,
				WriterNodeId::from_node_id(local),
				probe_hlc,
			)
			.unwrap();
			if admission.localities[0].get() % 2 == 1 {
				chosen = Some((id, admission.key_identities[0], admission.localities[0]));
				break;
			}
		}
		let (id, key_identity, locality) =
			chosen.expect("some row in 1..512 has an odd raw locality");
		let partition = crate::schema::table::partition_for_locality(locality, 63).unwrap();
		assert_eq!(
			partition.get() % 2,
			0,
			"premise: a 63-bit prefix partition is always even"
		);

		// Premise: with nodes [local, peer] (peer id higher, so local sorts
		// first) the partition's owner is the local node and the raw locality's
		// owner is the peer. Routing by the wrong value is observable.
		let peer = NodeId {
			id: local.id + 1,
			_reserved: 0,
		};
		let router = StaticReplicaRouter::new(vec![local, peer], 1);
		assert_eq!(
			router.replicas_for_placement(
				crate::schema::table::PlacementKey::from_parts(
					PlacementSemantics::Partitioned,
					Some(partition),
					locality,
				)
				.unwrap()
			),
			vec![local],
			"premise: the partition is owned locally"
		);
		assert_eq!(
			router.replicas_for_locality(locality),
			vec![peer],
			"premise: the raw locality is owned by the peer"
		);

		let (_mgr_dir, mgr) = make_manager().await;
		let server =
			routed_server_with_lease_gate(&node, mgr.clone(), vec![local, peer], false).await;
		let batch = typed_table_batch(vec![id]);
		let meta = typed_table_meta(&descriptor, batch.num_rows());
		let (resp, _) = tokio::time::timeout(
			Duration::from_secs(10),
			server.table_write(meta, batch.clone()),
		)
		.await
		.expect("partitioned write must not hang")
		.unwrap();
		assert_eq!(
			resp.write.status,
			WriteStatus::OK,
			"a write owned by the local partition succeeds locally: {resp:?}"
		);

		// The received owner, proven from durable local state before any read:
		// the local node holds the row, stamped partitioned, under the V5 chain.
		let held = mgr
			.lookup_as_of(tenant(), key_identity, hlc(0, 0, 0))
			.expect("the partition owner holds the written row");
		assert_eq!(
			held.partition,
			Some(partition),
			"the stored location carries the derived partition"
		);
		assert_eq!(
			held.placement,
			crate::schema::table::PlacementSemantics::Partitioned,
			"the stored location is stamped partitioned"
		);

		// No inverse arm: fan-out to a remote owner blocks awaiting transport in
		// this harness, so "routed away" is not cheaply observable here. The
		// positive arm IS the discriminator — under raw-locality fan-out this
		// row's owner is the peer, so the write above could not have returned OK
		// with the row landing locally. Verified by injection: reverting the
		// fan-out to replicas_for_locality makes this test time out.
		shutdown_lease_node(node).await;
	}
}

/// ADR 0090 decision 6: mixed legacy/partitioned state is a supported steady
/// state, not a transition window. One table, one scan interval, records under
/// both placements — every row returns exactly once, each resolved by its own
/// record's placement.
mod adr0090_mixed_scan {
	use super::*;
	use crate::schema::table::PlacementSemantics;

	#[tokio::test]
	async fn adr0090_one_interval_scans_both_placements_exactly_once() {
		let (_mgr_dir, mgr) = make_manager().await;
		let descriptor = append_only_table_descriptor();

		// Partitioned rows: written through the real builder, which derives the
		// partition at the descriptor-aware boundary.
		let partitioned_ids = vec![1_i64, 2, 3, 4];
		let partitioned_keys = append_append_only_rows_for_query(
			&mgr,
			&descriptor,
			partitioned_ids.clone(),
			hlc(10, 0, 1),
		)
		.await;

		// Legacy rows in the SAME table: the pre-0090 writer shape — source rows
		// with no partition, so the frame carries no `__row_partition__` column
		// and every index entry decodes as raw-locality.
		let legacy_ids = vec![101_i64, 102, 103, 104];
		let legacy_batch = typed_table_batch(legacy_ids.clone());
		let mut legacy_meta = typed_table_meta(&descriptor, legacy_batch.num_rows());
		legacy_meta.client_hlc = hlc(11, 0, 1);
		let admission = validate_table_write_request_batch(
			&legacy_meta,
			&legacy_batch,
			&descriptor,
			WriterNodeId::ZERO,
			hlc(11, 0, 1),
		)
		.unwrap();
		let legacy_keys = admission.key_identities.clone();
		let legacy_rows = (0..legacy_batch.num_rows())
			.map(|row| MultiRowSourceRow {
				source_row_index: row,
				key_identity: admission.key_identities[row],
				routing_locality: admission.localities[row],
				row_hlc: hlc(11, 0, 1),
				is_tombstone: false,
				partition: None,
				table_object_id: Some(descriptor.table_object_id),
			})
			.collect::<Vec<_>>();
		let encoded = crate::storage::multirow::encode_multirow_from_survivors_with_policy(
			&legacy_batch,
			&legacy_rows,
			descriptor.record_body_compression_policy,
		)
		.unwrap();
		let decoded = decode_batch(&encoded.frame).unwrap();
		assert!(
			decoded
				.column_by_name(crate::schema::STORAGE_ROW_PARTITION_COLUMN)
				.is_none(),
			"premise: the legacy frame carries no partition column"
		);
		let entries = storage_index_entries_from_batch(&decoded).unwrap();
		mgr.append_durable_multirow_with_locality(tenant(), &encoded.frame, &entries)
			.await
			.unwrap();

		// Premise: both placements are genuinely present in the same table.
		for key in &partitioned_keys {
			let held = mgr.lookup_as_of(tenant(), *key, hlc(0, 0, 0)).unwrap();
			assert_eq!(held.placement, PlacementSemantics::Partitioned);
			assert!(held.partition.is_some());
		}
		for key in &legacy_keys {
			let held = mgr.lookup_as_of(tenant(), *key, hlc(0, 0, 0)).unwrap();
			assert_eq!(held.placement, PlacementSemantics::RawLocality);
			assert_eq!(held.partition, None);
		}

		// One scan interval covering both placements, through the production
		// whole-record query path — which cross-checks every record's index
		// placement against its frame before serving it.
		let program = pbc::Program::compile(&[]).unwrap();
		let mut meta = base_meta();
		meta.as_of_hlc = hlc(99, 0, 1);
		let ranges = [QueryScanRange {
			start: Locality::ZERO,
			end: Locality::MAX,
		}];
		let plan = plan_query_execution_for_ranges_with_program(
			&mgr,
			n(1),
			&meta,
			&program,
			&ranges,
			QueryExecutionBudget::default(),
			None,
			Some(&descriptor),
			QueryProjectedExecution::Disabled,
			RowIdentityProjection::None,
			None,
		)
		.await
		.unwrap();
		assert_eq!(plan.response.status, ReadStatus::OK);

		let mut seen = Vec::new();
		for batch in &plan.batches {
			let ids = batch
				.column_by_name("id")
				.and_then(|array| array.as_any().downcast_ref::<Int64Array>())
				.expect("scan output carries the user id column");
			seen.extend(ids.iter().map(|value| value.expect("id is non-null")));
		}
		seen.sort_unstable();
		let mut expected = [partitioned_ids, legacy_ids].concat();
		expected.sort_unstable();
		assert_eq!(
			seen, expected,
			"one interval returns every row of both placements exactly once"
		);
	}
}

/// ADR 0090 decision 5: partition-granular transfer state. An in-flight
/// Migration Ledger entry covering a partition makes exactly its owner the
/// serving node — never both sides of the transfer, never neither — and a
/// completed or absent entry hands serving back to the ring.
mod adr0090_partition_transfer {
	use super::*;
	use crate::consensus::{
		MigrationLedgerEntry, MigrationState, partition_span, partition_transfer_for,
	};
	use crate::schema::PartitionId;
	use crate::schema::table::PlacementKey;
	use std::collections::HashMap;

	fn span_entry(
		id: u64,
		partition: PartitionId,
		owner: NodeId,
		state: MigrationState,
	) -> MigrationLedgerEntry {
		let (start, end) = partition_span(partition, 16).unwrap();
		MigrationLedgerEntry {
			migration_id: id,
			range_start: start,
			range_end: end,
			from_epoch: 1,
			to_epoch: 2,
			logical_owner: owner,
			upstream_migration_id: None,
			source_replicas: vec![n(1)],
			migration_hlc_floor: hlc(0, 0, 0),
			state,
			completed_by: None,
			completed_at_hlc: None,
		}
	}

	#[test]
	fn adr0090_partition_span_covers_prefix_and_every_suffix() {
		let partition = PartitionId::new(0x00AB_0000_0000_0000);
		let (start, end) = partition_span(partition, 16).unwrap();
		assert_eq!(start, 0x00AB_0000_0000_0000);
		assert_eq!(end, 0x00AB_FFFF_FFFF_FFFF);
		assert!(partition_span(partition, 0).is_err());
		assert!(partition_span(partition, 64).is_err());
	}

	#[test]
	fn adr0090_an_in_flight_transfer_names_exactly_one_serving_node() {
		let partition = PartitionId::new(0x00AB_0000_0000_0000);
		let mut migrations = HashMap::new();
		migrations.insert(9, span_entry(9, partition, n(5), MigrationState::Pending));
		let transfer = partition_transfer_for(partition, &migrations)
			.unwrap()
			.expect("an in-flight entry covering the partition yields a transfer");
		assert_eq!(transfer.serving, n(5), "the entry owner serves, alone");

		// A row deep inside the span is covered by the same entry — the whole
		// partition transfers together, never row by row.
		let interior = PartitionId::new(0x00AB_0000_DEAD_BEEF);
		let same = partition_transfer_for(interior, &migrations)
			.unwrap()
			.unwrap();
		assert_eq!(same.serving, n(5));
	}

	#[test]
	fn adr0090_conflicting_in_flight_claims_are_refused_not_arbitrated() {
		let partition = PartitionId::new(0x00AB_0000_0000_0000);
		let mut migrations = HashMap::new();
		migrations.insert(1, span_entry(1, partition, n(5), MigrationState::Pending));
		migrations.insert(
			2,
			span_entry(2, partition, n(6), MigrationState::BackfillInProgress),
		);
		let err = partition_transfer_for(partition, &migrations)
			.expect_err("two owners claiming one partition is the both-owners hazard");
		assert!(err.to_string().contains("different owners"), "{err}");
	}

	/// The transfer owner is the serving PRIMARY, not the whole replica set: a
	/// literal single-owner transfer would silently collapse replicated
	/// durability to one node. The remainder of the set comes from the ring's
	/// own walk at the configured RF.
	#[test]
	fn adr0090_a_transfer_keeps_the_configured_replication_factor() {
		let partition = PartitionId::new(0x00AB_0000_0000_0000);
		let router = StaticReplicaRouter::new(vec![n(1), n(2), n(3)], 2);
		let mut in_flight = HashMap::new();
		in_flight.insert(8, span_entry(8, partition, n(9), MigrationState::Pending));
		let serving = serving_replicas_for_placement(
			&router,
			&in_flight,
			PlacementKey::from_parts(
				crate::schema::table::PlacementSemantics::Partitioned,
				Some(partition),
				Locality::new(partition.get()),
			)
			.unwrap(),
		)
		.unwrap();
		assert_eq!(
			serving[0],
			n(9),
			"the transfer owner anchors the set as serving primary"
		);
		assert_eq!(
			serving.len(),
			2,
			"the configured RF survives the transfer: {serving:?}"
		);
		assert!(
			serving[1].id != n(9).id,
			"the second replica comes from the ring, not a duplicate of the owner"
		);
	}

	#[test]
	fn adr0090_completed_or_absent_transfers_hand_serving_back_to_the_ring() {
		let partition = PartitionId::new(0x00AB_0000_0000_0000);
		let mut migrations = HashMap::new();
		migrations.insert(3, span_entry(3, partition, n(5), MigrationState::Completed));
		assert!(
			partition_transfer_for(partition, &migrations)
				.unwrap()
				.is_none(),
			"a completed transfer no longer overrides the ring"
		);
		assert!(
			partition_transfer_for(partition, &HashMap::new())
				.unwrap()
				.is_none(),
			"no entry, no override"
		);

		// And the combined serving authority never yields neither: with no
		// transfer the ring answers, with one the owner answers.
		let router = StaticReplicaRouter::new(vec![n(1), n(2)], 1);
		let ring_serving = serving_replicas_for_placement(
			&router,
			&migrations,
			PlacementKey::from_parts(
				crate::schema::table::PlacementSemantics::Partitioned,
				Some(partition),
				Locality::new(partition.get()),
			)
			.unwrap(),
		)
		.unwrap();
		assert_eq!(ring_serving.len(), 1, "the ring serves a settled partition");
		let mut in_flight = HashMap::new();
		in_flight.insert(4, span_entry(4, partition, n(9), MigrationState::Pending));
		let transfer_serving = serving_replicas_for_placement(
			&router,
			&in_flight,
			PlacementKey::from_parts(
				crate::schema::table::PlacementSemantics::Partitioned,
				Some(partition),
				Locality::new(partition.get()),
			)
			.unwrap(),
		)
		.unwrap();
		assert_eq!(
			transfer_serving,
			vec![n(9)],
			"an in-flight transfer is served by exactly its owner"
		);
		assert!(
			!transfer_serving.contains(&ring_serving[0]) || ring_serving[0] == n(9),
			"the displaced ring owner does not also serve"
		);
	}
}

/// ADR 0090 step 3: a membership change over a table holding BOTH placements,
/// with a partition transfer in flight across the boundary. Every row resolves
/// to exactly one serving owner throughout — the in-flight partition to its
/// transfer owner regardless of the new ring, everything else to the new ring —
/// and the mixed scan still returns every row exactly once.
mod adr0090_membership_transition {
	use super::*;
	use crate::consensus::{
		MigrationLedgerEntry, MigrationState, partition_span, partition_transfer_for,
	};
	use crate::network::ring::{RingRouter, RingState};
	use crate::network::transport::{
		MembershipEpoch, NodeCapabilitySet, NodeRole, NodeRoleSet, PeerCapabilities,
		TransportTierSet,
	};
	use crate::schema::table::{PlacementKey, PlacementSemantics};
	use std::collections::HashMap;

	fn membership(ids: &[u32], epoch: u64) -> MembershipEpoch {
		let mut peers = HashMap::new();
		for id in ids {
			peers.insert(
				n(*id),
				PeerCapabilities {
					address: format!("loopback://{id}"),
					tiers: TransportTierSet::empty(),
					roles: NodeRoleSet::empty().with(NodeRole::Data),
					capabilities: NodeCapabilitySet::empty(),
				},
			);
		}
		MembershipEpoch { epoch, peers }
	}

	#[tokio::test]
	async fn adr0090_membership_change_with_an_in_flight_transfer_over_mixed_state() {
		// The mixed table from the decision-6 control: partitioned rows through
		// the real builder, legacy rows through the pre-0090 writer shape.
		let (_mgr_dir, mgr) = make_manager().await;
		let descriptor = append_only_table_descriptor();
		let partitioned_keys = append_append_only_rows_for_query(
			&mgr,
			&descriptor,
			vec![1_i64, 2, 3, 4],
			hlc(10, 0, 1),
		)
		.await;
		let legacy_batch = typed_table_batch(vec![101_i64, 102, 103, 104]);
		let mut legacy_meta = typed_table_meta(&descriptor, legacy_batch.num_rows());
		legacy_meta.client_hlc = hlc(11, 0, 1);
		let admission = validate_table_write_request_batch(
			&legacy_meta,
			&legacy_batch,
			&descriptor,
			WriterNodeId::ZERO,
			hlc(11, 0, 1),
		)
		.unwrap();
		let legacy_keys = admission.key_identities.clone();
		let legacy_rows = (0..legacy_batch.num_rows())
			.map(|row| MultiRowSourceRow {
				source_row_index: row,
				key_identity: admission.key_identities[row],
				routing_locality: admission.localities[row],
				row_hlc: hlc(11, 0, 1),
				is_tombstone: false,
				partition: None,
				table_object_id: Some(descriptor.table_object_id),
			})
			.collect::<Vec<_>>();
		let encoded = crate::storage::multirow::encode_multirow_from_survivors_with_policy(
			&legacy_batch,
			&legacy_rows,
			descriptor.record_body_compression_policy,
		)
		.unwrap();
		let entries =
			storage_index_entries_from_batch(&decode_batch(&encoded.frame).unwrap()).unwrap();
		mgr.append_durable_multirow_with_locality(tenant(), &encoded.frame, &entries)
			.await
			.unwrap();

		// The placement key of every row, read from durable state.
		let mut keys = Vec::new();
		for key in partitioned_keys.iter().chain(legacy_keys.iter()) {
			let held = mgr.lookup_as_of(tenant(), *key, hlc(0, 0, 0)).unwrap();
			keys.push(
				PlacementKey::from_parts(
					held.placement,
					held.partition,
					held.partition
						.map_or(Locality::from(*key), |_| Locality::from(*key)),
				)
				.unwrap(),
			);
		}

		// A real membership change: the router's state is atomically swapped
		// from epoch 1 to epoch 2 with four new nodes.
		let router = RingRouter::new(RingState::build(&membership(&[1, 2, 3], 1), 1));
		router.install(RingState::build(&membership(&[1, 2, 3, 4, 5, 6, 7], 2), 1));

		// An in-flight transfer for one partitioned row's partition, owned by a
		// node the new ring does NOT pick for it — the override must be
		// load-bearing, so the premise finds such a node.
		let transferring = keys
			.iter()
			.find_map(|key| match key {
				PlacementKey::Partition(partition) => Some(*partition),
				PlacementKey::RawLocality(_) => None,
			})
			.expect("the mixed table holds at least one partitioned row");
		let ring_owner = router.replicas_for_placement(
			PlacementKey::from_parts(
				PlacementSemantics::Partitioned,
				Some(transferring),
				Locality::new(transferring.get()),
			)
			.unwrap(),
		)[0];
		let transfer_owner = (1..=7)
			.map(n)
			.find(|node| node.id != ring_owner.id)
			.expect("seven nodes offer an owner besides the ring's");
		let (start, end) = partition_span(transferring, 16).unwrap();
		let mut migrations = HashMap::new();
		migrations.insert(
			21,
			MigrationLedgerEntry {
				migration_id: 21,
				range_start: start,
				range_end: end,
				from_epoch: 1,
				to_epoch: 2,
				logical_owner: transfer_owner,
				upstream_migration_id: None,
				source_replicas: vec![ring_owner],
				migration_hlc_floor: hlc(0, 0, 0),
				state: MigrationState::BackfillInProgress,
				completed_by: None,
				completed_at_hlc: None,
			},
		);

		// Every row resolves to exactly one serving owner across the change.
		for key in &keys {
			let serving = serving_replicas_for_placement(&router, &migrations, *key).unwrap();
			assert_eq!(
				serving.len(),
				1,
				"every row has exactly one serving owner during the transition: {key:?}"
			);
			match key {
				PlacementKey::Partition(partition)
					if partition_transfer_for(*partition, &migrations)
						.unwrap()
						.is_some() =>
				{
					assert_eq!(
						serving,
						vec![transfer_owner],
						"the in-flight partition serves at its transfer owner"
					);
					assert_ne!(
						serving[0].id, ring_owner.id,
						"premise: the override is load-bearing, not coincidental"
					);
				}
				other => {
					assert_eq!(
						serving,
						router.replicas_for_placement(*other),
						"rows outside the transfer follow the new ring"
					);
				}
			}
		}

		// Completing the transfer hands the partition back to the new ring.
		migrations.get_mut(&21).unwrap().state = MigrationState::Completed;
		let settled = serving_replicas_for_placement(
			&router,
			&migrations,
			PlacementKey::from_parts(
				PlacementSemantics::Partitioned,
				Some(transferring),
				Locality::new(transferring.get()),
			)
			.unwrap(),
		)
		.unwrap();
		assert_eq!(
			settled,
			vec![ring_owner],
			"a completed transfer follows the ring"
		);

		// And the mixed scan is unmoved by any of it: local data serves every
		// row exactly once, both placements, transfer in flight or not.
		let program = pbc::Program::compile(&[]).unwrap();
		let mut meta = base_meta();
		meta.as_of_hlc = hlc(99, 0, 1);
		let ranges = [QueryScanRange {
			start: Locality::ZERO,
			end: Locality::MAX,
		}];
		let plan = plan_query_execution_for_ranges_with_program(
			&mgr,
			n(1),
			&meta,
			&program,
			&ranges,
			QueryExecutionBudget::default(),
			None,
			Some(&descriptor),
			QueryProjectedExecution::Disabled,
			RowIdentityProjection::None,
			None,
		)
		.await
		.unwrap();
		let mut seen = Vec::new();
		for batch in &plan.batches {
			let ids = batch
				.column_by_name("id")
				.and_then(|array| array.as_any().downcast_ref::<Int64Array>())
				.expect("scan output carries the user id column");
			seen.extend(ids.iter().map(|value| value.expect("id is non-null")));
		}
		seen.sort_unstable();
		assert_eq!(
			seen,
			vec![1, 2, 3, 4, 101, 102, 103, 104],
			"the mixed scan returns every row exactly once across the transition"
		);
	}
}

/// Reproduces the CTAS-replacement sequence at the wendelon layer: two
/// partitioned tables, delete every row of the first, scan the second.
#[tokio::test]
async fn adr0090_probe_delete_of_one_table_then_scan_of_another() {
	let (_mgr_dir, mgr) = make_manager().await;
	let old_table = append_only_table_descriptor();
	let mut new_table = append_only_table_descriptor();
	new_table.table_object_id = TableObjectId::new([0x48; 16]);
	new_table.refresh_descriptor_fingerprints().unwrap();

	let old_keys =
		append_append_only_rows_for_query(&mgr, &old_table, vec![1, 2, 3, 4], hlc(10, 0, 1)).await;
	append_append_only_rows_for_query(&mgr, &new_table, vec![3, 4], hlc(11, 0, 1)).await;

	// Delete the old table's rows (the retirement shape).
	append_append_only_tombstones_for_query(&mgr, &old_table, &old_keys, hlc(12, 0, 1)).await;

	// Scan the new table.
	let program = pbc::Program::compile(&[]).unwrap();
	let mut meta = base_meta();
	meta.as_of_hlc = hlc(99, 0, 1);
	let ranges = [QueryScanRange {
		start: Locality::ZERO,
		end: Locality::MAX,
	}];
	let plan = plan_query_execution_for_ranges_with_program(
		&mgr,
		n(1),
		&meta,
		&program,
		&ranges,
		QueryExecutionBudget::default(),
		None,
		Some(&new_table),
		QueryProjectedExecution::Disabled,
		RowIdentityProjection::None,
		None,
	)
	.await
	.unwrap();
	assert_eq!(
		plan.response.status,
		ReadStatus::OK,
		"{:?}",
		plan.response.status
	);
	let rows: usize = plan.batches.iter().map(|batch| batch.num_rows()).sum();
	assert_eq!(
		rows, 2,
		"the second table's rows survive the first's deletion"
	);
}

/// ADR 0091: a server-side execution failure after dispatch reaches the client
/// as a typed terminal frame — never as a bare stream close the client can only
/// report as `early eof`. The poison is a real failure: a version whose index
/// entry reads legacy over a partitioned frame, which the placement cross-check
/// refuses mid-scan.
mod adr0091_typed_stream_errors {
	use super::*;
	use crate::schema::TableQueryRequestId;
	use crate::schema::table::PlacementSemantics;
	use tokio::io::AsyncWriteExt;

	async fn poisoned_fixture(
		rows: Vec<i64>,
	) -> (
		crate::test_support::TestDir,
		WendelonNode,
		WendelonDataServer,
		Arc<SlabManager>,
		TableSchemaDescriptor,
	) {
		poisoned_fixture_with(rows, true).await
	}

	async fn poisoned_fixture_with(
		rows: Vec<i64>,
		poison: bool,
	) -> (
		crate::test_support::TestDir,
		WendelonNode,
		WendelonDataServer,
		Arc<SlabManager>,
		TableSchemaDescriptor,
	) {
		let dir = bootstrap_tempdir(&format!("adr0091-{}", rows.len()));
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			81,
			dir.to_path_buf(),
			"127.0.0.1:0".into(),
		))
		.await
		.unwrap();
		let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
		loop {
			if node.raft.current_leader().await == Some(node.local_node.id) {
				break;
			}
			assert!(tokio::time::Instant::now() < deadline, "leader wait");
			tokio::time::sleep(Duration::from_millis(20)).await;
		}
		let descriptor = append_only_table_descriptor();
		node.raft
			.client_write(WendelonRequest::SchemaUpdate(typed_table_schema_update(
				&descriptor,
			)))
			.await
			.unwrap();
		let (_mgr_dir, mgr) = make_manager().await;
		let keys = append_append_only_rows_for_query(&mgr, &descriptor, rows, hlc(10, 0, 1)).await;

		// Poison the row with the HIGHEST locality: scanned last, so larger
		// fixtures emit data frames before the failure. Its newest version is a
		// legacy-reading twin of the real partitioned location — exactly what
		// the cross-check exists to refuse.
		// The scan's own order is the truth about which row is reached last.
		let poisoned = mgr
			.scan_as_of_with_identity_and_locality(
				tenant(),
				Locality::ZERO,
				Locality::MAX,
				hlc(0, 0, 0),
			)
			.last()
			.map(|(key_identity, _, _)| *key_identity)
			.expect("fixture has rows");
		let _ = &keys;
		let held = mgr.lookup_as_of(tenant(), poisoned, hlc(0, 0, 0)).unwrap();
		assert_eq!(
			held.placement,
			PlacementSemantics::Partitioned,
			"premise: the poisoned row is genuinely partitioned"
		);
		if poison {
			let mut twin = held;
			twin.hlc.physical_ms += 1;
			twin.placement = PlacementSemantics::RawLocality;
			twin.partition = None;
			mgr.install_version_for_route_test(tenant(), poisoned, twin);
		}

		let server = super::adr0090_route_selection::routed_server_with_lease_gate(
			&node,
			mgr.clone(),
			vec![node.local_node],
			false,
		)
		.await;
		(dir, node, server, mgr, descriptor)
	}

	fn query_frame(descriptor: &TableSchemaDescriptor, node: &WendelonNode) -> HrftFrame {
		let query_meta = TableQueryRequestMeta {
			min_hlc: hlc(0, 0, 0),
			expected_epoch: 1,
			tenant_id: tenant(),
			table_object_id: descriptor.table_object_id,
			schema_generation: descriptor.schema_generation,
			physical_descriptor_fingerprint: *descriptor.physical_descriptor_fingerprint.as_bytes(),
			consistency: Consistency::SERIALIZABLE,
			row_identity_projection: RowIdentityProjection::None.as_u8(),
			_pad: [0; 6],
			bytecode_len: 0,
			runtime_filter_len: 0,
			as_of_hlc: hlc(99, 0, 1),
			row_limit_rows: 0,
			deadline_remaining_millis: 0,
			request_id: TableQueryRequestId::from_u128(91),
			trace_id: [0; 16],
			trace_parent_span_id: [0; 8],
			trace_flags: 0,
			_trace_pad: [0; 7],
		};
		let mut meta_buf = node.pool.acquire();
		meta_buf
			.as_mut_vec()
			.extend_from_slice(bytemuck::bytes_of(&query_meta));
		HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: MsgType::TABLE_QUERY_REQ.0,
				flags: 0,
				stream_id: 91,
				meta_len: meta_buf.len() as u32,
				arrow_len: 0,
				trailing_len: 0,
			},
			meta: meta_buf,
			arrow_payload: None,
			trailing: None,
		}
	}

	/// Drive the streaming handler concurrently with a client-side reader and
	/// return every frame until the stream ends.
	async fn dispatch_and_read(
		node: &WendelonNode,
		server: WendelonDataServer,
		frame: HrftFrame,
	) -> (std::io::Result<()>, Vec<(u8, bool, Vec<u8>)>) {
		let (mut client_side, server_side) = tokio::io::duplex(64 * 1024);
		let header = frame.header;
		let mut body = Vec::new();
		body.extend_from_slice(&frame.meta[..]);
		client_side.write_all(&body).await.unwrap();
		client_side.shutdown().await.unwrap();
		let context = RequestContext {
			remote: Some(node.local_node),
			actor: actor(ActorRole::Operator, None),
		};
		let pool = node.pool.clone();
		let server_task = tokio::spawn(async move {
			DataRingServer::handle_data_stream(
				&server,
				context,
				header,
				Box::new(DuplexTransportStream { inner: server_side }),
				pool,
			)
			.await
		});
		let mut frames = Vec::new();
		loop {
			match read_frame(&mut client_side, &node.pool).await {
				Ok(response) => {
					let is_end = (response.header.flags & FRAME_FLAG_STREAM_END) != 0;
					frames.push((response.header.msg_type, is_end, response.meta[..].to_vec()));
					if is_end {
						break;
					}
				}
				Err(_) => break,
			}
		}
		let outcome = server_task.await.unwrap();
		(outcome, frames)
	}

	fn terminal_status(frames: &[(u8, bool, Vec<u8>)]) -> Option<ReadStatus> {
		frames.last().and_then(|(_, is_end, meta)| {
			(*is_end && meta.len() >= std::mem::size_of::<QueryResponseMeta>()).then(|| {
				let response: &QueryResponseMeta =
					bytemuck::from_bytes(&meta[..std::mem::size_of::<QueryResponseMeta>()]);
				response.status
			})
		})
	}

	#[tokio::test]
	async fn adr0091_a_pre_frame_execution_error_is_a_typed_terminal() {
		let (_node_dir, node, server, _mgr, descriptor) = poisoned_fixture(vec![1, 2, 3, 4]).await;
		let before = metrics().table_query_execution_error_count("framed");
		let (outcome, frames) =
			dispatch_and_read(&node, server, query_frame(&descriptor, &node)).await;
		outcome.expect("a framed error is a handled request, not a transport failure");
		assert_eq!(
			terminal_status(&frames),
			Some(ReadStatus::QUERY_EXECUTION_FAILED),
			"the failure names itself instead of closing unframed: {frames:?}"
		);
		assert!(
			metrics().table_query_execution_error_count("framed") > before,
			"the dispatch seam records the framed error"
		);
		shutdown_lease_node(node).await;
	}

	#[tokio::test]
	async fn adr0091_a_mid_stream_execution_error_is_a_typed_terminal() {
		let (_node_dir, node, server, _mgr, descriptor) =
			poisoned_fixture((1..=5000).collect()).await;
		let (outcome, frames) =
			dispatch_and_read(&node, server, query_frame(&descriptor, &node)).await;
		outcome.expect("a framed error is a handled request, not a transport failure");
		assert!(
			frames.len() >= 2,
			"premise: at least one data frame precedes the failure ({} frames)",
			frames.len()
		);
		assert_eq!(
			terminal_status(&frames),
			Some(ReadStatus::QUERY_EXECUTION_FAILED),
			"a mid-stream failure still names itself"
		);
		shutdown_lease_node(node).await;
	}

	#[tokio::test]
	async fn adr0091_a_dead_transport_still_surfaces_as_an_io_error() {
		let (_node_dir, node, server, _mgr, descriptor) = poisoned_fixture(vec![1, 2, 3, 4]).await;
		let before = metrics().table_query_execution_error_count("unframed");
		let frame = query_frame(&descriptor, &node);
		let (mut client_side, server_side) = tokio::io::duplex(64 * 1024);
		let header = frame.header;
		client_side.write_all(&frame.meta[..]).await.unwrap();
		client_side.shutdown().await.unwrap();
		// Kill the reading half before the server can write anything: the
		// typed frame cannot be delivered, so the bare close remains — as an
		// io error, distinguishable from a query failure.
		drop(client_side);
		let outcome = DataRingServer::handle_data_stream(
			&server,
			RequestContext {
				remote: Some(node.local_node),
				actor: actor(ActorRole::Operator, None),
			},
			header,
			Box::new(DuplexTransportStream { inner: server_side }),
			node.pool.clone(),
		)
		.await;
		assert!(
			outcome.is_err(),
			"a dead transport is a transport failure, not a silently-dropped query"
		);
		assert!(
			metrics().table_query_execution_error_count("unframed") > before,
			"the unframed outcome is still recorded at the seam"
		);
		shutdown_lease_node(node).await;
	}
}

/// ADR 0090 follow-up control: a PARTITIONED row read through a secondary
/// index. The V1 segment format carries no placement, so a segment-built
/// location reads as legacy — over a partitioned frame, the placement
/// cross-check must still let the row serve, because the placement authority
/// is the key chain, never the segment.
#[tokio::test]
async fn secondary_index_read_serves_partitioned_rows() {
	let dir = bootstrap_tempdir("secondary-index-partitioned-read");
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		56,
		dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();
	stop_secondary_index_lifecycle(&node).await;
	let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
	loop {
		if node.raft.current_leader().await == Some(node.local_node.id) {
			break;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for leader"
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
	node.data_server
		.refresh_leases_once(&node.ring, &*node.clock, 60_000)
		.await;

	let descriptor = multirow_typed_table_descriptor();
	node.raft
		.client_write(WendelonRequest::SchemaUpdate(typed_table_schema_update(
			&descriptor,
		)))
		.await
		.unwrap();

	let batch = typed_table_batch_with_names(vec![(1, "target"), (2, "other")]);
	let mut write_meta = typed_table_meta(&descriptor, batch.num_rows());
	write_meta.parent_write_id.sequence_no = 560;
	let (write_resp, _) = node
		.data_server
		.table_write(write_meta, batch)
		.await
		.unwrap();
	assert_eq!(write_resp.write.status, WriteStatus::OK);

	// Premise: the rows really are partitioned — otherwise this control would
	// pass on any build and prove nothing.
	let admission = validate_table_write_request_batch(
		&write_meta,
		&typed_table_batch_with_names(vec![(1, "target"), (2, "other")]),
		&descriptor,
		WriterNodeId::from_node_id(node.local_node),
		write_resp.write.commit_hlc,
	)
	.unwrap();
	let held = node
		.slab
		.lookup_as_of(tenant(), admission.key_identities[0], hlc(0, 0, 0))
		.expect("written row resolves");
	assert_eq!(
		held.placement,
		crate::schema::table::PlacementSemantics::Partitioned,
		"premise: the indexed table writes partitioned rows"
	);

	let sealed_slab_id = node.slab.force_seal_active_for_test().await;
	let index = name_secondary_index_descriptor(&descriptor);
	node.raft
		.client_write(WendelonRequest::SchemaUpdate(
			secondary_index_schema_update(&index),
		))
		.await
		.unwrap();
	let backfill = node
		.slab
		.run_secondary_index_backfill_once(tenant(), &descriptor, &index, 8, 4096)
		.await
		.unwrap();
	assert_eq!(backfill.completed.len(), 1);
	assert_eq!(
		backfill.completed[0].candidate.source_slab_id(),
		sealed_slab_id
	);
	node.raft
		.client_write(WendelonRequest::SchemaUpdate(
			secondary_index_backfill_complete_schema_update(&index, node.local_node),
		))
		.await
		.unwrap();

	let program = name_eq_program("target");
	let query_meta =
		table_query_meta_for_test(&descriptor, write_resp.write.commit_hlc, program.len());
	let plan = node
		.data_server
		.prepare_table_query_execution_plan_with_migration(
			&query_meta,
			&program,
			DecodedRuntimeFilter::Absent,
		)
		.await
		.expect("a partitioned row behind a secondary index must serve, not refuse");
	assert_eq!(plan.response.status, ReadStatus::OK);
	assert_eq!(plan.response.rows, 1);
	assert!(
		plan.secondary_index_access_plan.is_some(),
		"premise: the read went through the secondary index"
	);

	shutdown_lease_node(node).await;
}

/// ADR 0090 follow-up: the durable secondary-index round-trip. The V1 segment
/// format carries no placement, so a segment-decoded location reads as legacy —
/// exactly what `into_entry` reconstructs after a reload. The key chain is the
/// placement authority, so the read must serve the partitioned row anyway.
#[tokio::test]
async fn adr0090_segment_decoded_locations_resolve_placement_from_the_chain() {
	use crate::schema::table::PlacementSemantics;
	use crate::storage::secondary_index::SecondaryIndexLookupRow;

	let (_mgr_dir, mgr) = make_manager().await;
	let descriptor = append_only_table_descriptor();
	let keys = append_append_only_rows_for_query(&mgr, &descriptor, vec![7], hlc(10, 0, 1)).await;
	let key = keys[0];
	let held = mgr.lookup_as_of(tenant(), key, hlc(0, 0, 0)).unwrap();
	assert_eq!(
		held.placement,
		PlacementSemantics::Partitioned,
		"premise: the indexed row is partitioned"
	);

	// The segment-decoded shape: same physical location, placement lost to the
	// V1 format. Physical identity ignores placement, so this IS the same
	// location as far as reads are concerned.
	let mut segment_decoded = held;
	segment_decoded.placement = PlacementSemantics::RawLocality;
	segment_decoded.partition = None;
	let row = SecondaryIndexLookupRow {
		row_key_id: key,
		row_hlc: held.hlc,
		record_location: segment_decoded,
	};

	let program = pbc::Program::compile(&[]).unwrap();
	let read = read_secondary_index_query_location(
		&mgr,
		tenant(),
		&row,
		&descriptor,
		&program,
		&QueryProjectedExecution::Disabled,
	)
	.await
	.expect("a partitioned row behind a reloaded segment must serve, not refuse")
	.expect("the row materializes");
	assert_eq!(read.batch.num_rows(), 1);
}

/// ADR 0089: the decisive control — one read and one decode per DISTINCT
/// physical record. Locations examined stay equal to rows by design (the index
/// is row-granular), so counting them would show nothing; the audit counts
/// decodes, which packing plus coalescing amortize to ceil(rows / target).
mod adr0089_scan_coalescing {
	use super::*;

	async fn packed_scan(rows: usize, row_limit: Option<usize>) -> (QueryExecutionPlan, usize) {
		let (_mgr_dir, mgr) = make_manager().await;
		let descriptor = append_only_table_descriptor();
		append_append_only_rows_for_query(
			&mgr,
			&descriptor,
			(1..=rows as i64).collect(),
			hlc(10, 0, 1),
		)
		.await;
		let program = pbc::Program::compile(&[]).unwrap();
		let mut meta = base_meta();
		meta.as_of_hlc = hlc(99, 0, 1);
		let ranges = [QueryScanRange {
			start: Locality::ZERO,
			end: Locality::MAX,
		}];
		let plan = plan_query_execution_for_ranges_with_program(
			&mgr,
			n(1),
			&meta,
			&program,
			&ranges,
			QueryExecutionBudget::default(),
			row_limit,
			Some(&descriptor),
			QueryProjectedExecution::Disabled,
			RowIdentityProjection::None,
			None,
		)
		.await
		.unwrap();
		(plan, rows)
	}

	#[tokio::test]
	async fn adr0089_one_decode_per_distinct_record() {
		// append-only test layout: target_rows = 4, max_rows = 8. Ten rows in a
		// shared partition pack to ceil-shaped records; the scan must decode
		// each record exactly once while emitting every row.
		let (plan, rows) = packed_scan(10, None).await;
		assert_eq!(plan.response.status, ReadStatus::OK);
		let emitted: usize = plan.batches.iter().map(|batch| batch.num_rows()).sum();
		assert_eq!(emitted, rows, "every row is emitted exactly once");

		let audit = plan.scan_decode_audit;
		let locations = plan.clocks.progress_marks;
		assert!(
			audit.distinct_records_decoded < rows as u64,
			"packing must beat one record per row: {} decodes for {rows} rows",
			audit.distinct_records_decoded
		);
		assert_eq!(
			audit.distinct_records_decoded + audit.coalesced_hits,
			rows as u64,
			"every row resolves through exactly one decode or one hit"
		);
		// The spread splits rows across at most `spread_factor` partitions
		// (4 by default); with target 4 the record count is bounded by the
		// per-partition ceilings.
		assert!(
			audit.distinct_records_decoded <= 8,
			"10 rows across <=4 partitions at target 4 cannot need more than 8 \
			 records: {audit:?}"
		);
		assert!(
			locations >= rows as u64,
			"the index stays row-granular: locations tracked per row"
		);
	}

	#[tokio::test]
	async fn adr0089_bounded_cache_and_order_survive_large_scans() {
		let (plan, rows) = packed_scan(200, None).await;
		assert_eq!(plan.response.status, ReadStatus::OK);
		let audit = plan.scan_decode_audit;
		assert!(
			audit.max_resident_records <= 8,
			"the decoded-record cache is bounded: {audit:?}"
		);
		assert_eq!(
			audit.distinct_records_decoded + audit.coalesced_hits,
			rows as u64
		);
		// Order: the emitted ids are exactly the written set; index order is
		// ascending locality, so re-sorting by value must be a permutation.
		let mut seen = Vec::new();
		for batch in &plan.batches {
			let ids = batch
				.column_by_name("id")
				.and_then(|array| array.as_any().downcast_ref::<Int64Array>())
				.expect("scan output carries the id column");
			seen.extend(ids.iter().map(|value| value.expect("id non-null")));
		}
		seen.sort_unstable();
		assert_eq!(seen, (1..=rows as i64).collect::<Vec<_>>());
	}

	#[tokio::test]
	async fn adr0089_limits_still_bound_rows_not_records() {
		let (plan, _) = packed_scan(10, Some(3)).await;
		assert_eq!(plan.response.status, ReadStatus::OK);
		assert_eq!(
			plan.response.rows, 3,
			"the limit's row semantics are untouched"
		);
		let emitted: usize = plan.batches.iter().map(|batch| batch.num_rows()).sum();
		assert_eq!(emitted, 3);
	}
}

#[tokio::test]
async fn mutation_dedup_entry_counts_grow_for_new_operations_not_replays() {
	let dir = bootstrap_tempdir("mutation-cache-entry-growth");
	let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
		48,
		dir.to_path_buf(),
		"127.0.0.1:0".into(),
	))
	.await
	.unwrap();
	let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
	loop {
		if node.raft.current_leader().await == Some(node.local_node.id) {
			break;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"timed out waiting for leader"
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
	node.data_server
		.refresh_leases_once(&node.ring, &*node.clock, 60_000)
		.await;

	let descriptor = append_only_table_descriptor();
	node.raft
		.client_write(WendelonRequest::SchemaUpdate(typed_table_schema_update(
			&descriptor,
		)))
		.await
		.unwrap();

	let mut points = Vec::new();
	assert_eq!(node.data_server.idempotency.len(), 0);
	assert_eq!(node.data_server.table_delete_idempotency.len(), 0);
	for operation in 0..4 {
		let batch = typed_table_batch(vec![100 + operation]);
		let mut meta = typed_table_meta(&descriptor, batch.num_rows());
		meta.parent_write_id.sequence_no = 62900 + operation as u64;
		let before = node.data_server.idempotency.len();
		let (response, _) = node
			.data_server
			.table_write(meta, batch.clone())
			.await
			.unwrap();
		assert_eq!(response.write.status, WriteStatus::OK);
		let after = node.data_server.idempotency.len();
		assert!(after > before, "new operation retains an entry");
		let (retry, _) = node
			.data_server
			.table_write(meta, batch.clone())
			.await
			.unwrap();
		assert_eq!(retry.write.status, WriteStatus::DUPLICATE);
		assert_eq!(
			retry.write.mutation_stop_reason(),
			response.write.mutation_stop_reason()
		);
		assert_eq!(
			retry.write.mutation_commit_knowledge(),
			response.write.mutation_commit_knowledge()
		);
		println!(
			"NATIVE_REPLAY_PIN first_status={} replay_status={} stop_reason={} commit_knowledge={}",
			response.write.status.0,
			retry.write.status.0,
			retry.write.mutation_stop_reason().0,
			retry.write.mutation_commit_knowledge().0
		);
		assert_eq!(
			node.data_server.idempotency.len(),
			after,
			"replay adds no entries"
		);
		let admission = validate_table_write_request_batch(
			&meta,
			&batch,
			&descriptor,
			WriterNodeId::from_node_id(node.local_node),
			response.write.commit_hlc,
		)
		.unwrap();
		let mut delete_meta: TableDeleteRowsRequestMeta = bytemuck::Zeroable::zeroed();
		delete_meta.parent_write_id = crate::schema::ClientWriteId {
			client_uuid: [0x62; 16],
			sequence_no: operation as u64,
			idempotency_ttl_ms: 30_000,
			_pad: 0,
		};
		delete_meta.client_hlc = hlc(10, 0, 48);
		delete_meta.expected_epoch = 1;
		delete_meta.tenant_id = tenant();
		delete_meta.table_object_id = descriptor.table_object_id;
		delete_meta.schema_generation = descriptor.schema_generation;
		delete_meta.row_count = 1;
		delete_meta.durability = DurabilityLevel::QUORUM;
		let key = admission.key_identities[0];
		let (deleted, _) = node
			.data_server
			.table_delete(delete_meta, vec![key])
			.await
			.unwrap();
		assert_eq!(deleted.write.status, WriteStatus::OK);
		assert_eq!(deleted.deleted_count, 1);
		assert_eq!(
			node.data_server.table_delete_idempotency.len(),
			operation as usize + 1
		);
		let retained = node.data_server.idempotency.len();
		let (retry, _) = node
			.data_server
			.table_delete(delete_meta, vec![key])
			.await
			.unwrap();
		assert_eq!(retry.deleted_count, 1);
		assert_eq!(
			node.data_server.table_delete_idempotency.len(),
			operation as usize + 1
		);
		assert_eq!(node.data_server.idempotency.len(), retained);
		points.push((
			operation + 1,
			retained,
			node.data_server.table_delete_idempotency.len(),
		));
	}
	println!("CACHE_PIN (distinct_write_delete_pairs, write_entries, delete_entries)={points:?}");
	shutdown_lease_node(node).await;
}

#[tokio::test]
async fn expired_table_outcome_keeps_warm_cache_replay_but_cold_replay_is_indeterminate() {
	let cfg_dir = bootstrap_tempdir("expired-table-warm-cold-replay");
	let cfg = NodeBootstrapConfig::dev(6298, cfg_dir.to_path_buf(), "127.0.0.1:0".into());
	let node = WendelonNode::bootstrap_single_node(cfg.clone())
		.await
		.unwrap();
	let leader_deadline = tokio::time::Instant::now() + Duration::from_secs(3);
	while node.raft.current_leader().await != Some(node.local_node.id) {
		assert!(
			tokio::time::Instant::now() < leader_deadline,
			"leader timeout"
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
	node.data_server
		.refresh_leases_once(&node.ring, &*node.clock, 60_000)
		.await;
	let descriptor = typed_table_descriptor();
	node.raft
		.client_write(WendelonRequest::SchemaUpdate(typed_table_schema_update(
			&descriptor,
		)))
		.await
		.unwrap();
	let batch = typed_table_batch(vec![629]);
	let mut meta = typed_table_meta(&descriptor, batch.num_rows());
	// The longest TTL the store admits, so no stall between the write and the
	// next resolve can expire it early; expiry is then applied explicitly.
	const TTL_MS: u32 = 300_000;
	meta.parent_write_id.idempotency_ttl_ms = TTL_MS;
	let before_write = crate::network::mutation_outcomes::now_ms_for_test();
	let (first, _) = node
		.data_server
		.table_write(meta, batch.clone())
		.await
		.unwrap();
	let after_write = crate::network::mutation_outcomes::now_ms_for_test();
	assert_eq!(first.write.status, WriteStatus::OK);
	assert_eq!(
		first.write.mutation_commit_knowledge(),
		MutationCommitKnowledge::COMMITTED
	);
	let request = MutationOutcomeRequestMeta {
		operation_id: meta.parent_write_id,
		tenant_id: meta.tenant_id,
	};
	let initial = node
		.data_server
		.resolve_mutation_outcome(request)
		.await
		.unwrap();
	assert_eq!(initial.resolution, MutationResolutionStatus::PRESENT);
	assert_eq!(
		initial.write.mutation_commit_knowledge(),
		MutationCommitKnowledge::COMMITTED
	);
	let retained = node.data_server.idempotency.len();
	let durable_bytes = node.slab.local_l1_bytes().await;
	// The write recorded its deadline TTL after its own commit, on the store's
	// SystemTime clock. Check that deadline, then let it pass, instead of
	// waiting out a wall-clock TTL that a stalled host can overrun between the
	// write and the first resolve.
	let deadline = node
		.data_server
		.mutation_outcomes
		.expires_at_ms_for_test(meta.parent_write_id, meta.tenant_id)
		.expect("the write recorded its outcome");
	let ttl = u64::from(TTL_MS);
	assert!(
		(before_write + ttl..=after_write + ttl).contains(&deadline),
		"deadline {deadline} is not the write time plus the TTL"
	);
	node.data_server
		.mutation_outcomes
		.expire_for_test(meta.parent_write_id, meta.tenant_id)
		.await;
	let expired = node
		.data_server
		.resolve_mutation_outcome(request)
		.await
		.unwrap();
	assert_eq!(expired.resolution, MutationResolutionStatus::EXPIRED);
	assert_eq!(expired.write.status, WriteStatus::WRITE_OPERATION_EXPIRED);
	assert_eq!(
		expired.write.mutation_stop_reason(),
		MutationStopReason::NONE
	);
	assert_eq!(
		expired.write.mutation_commit_knowledge(),
		MutationCommitKnowledge::INDETERMINATE
	);
	assert_eq!(node.data_server.idempotency.len(), retained);
	let (warm, _) = node
		.data_server
		.table_write(meta, batch.clone())
		.await
		.unwrap();
	assert_eq!(warm.write.status, WriteStatus::DUPLICATE);
	assert_eq!(
		warm.write.mutation_commit_knowledge(),
		MutationCommitKnowledge::COMMITTED
	);
	assert_eq!(node.data_server.idempotency.len(), retained);
	assert_eq!(node.slab.local_l1_bytes().await, durable_bytes);
	let after_warm = node
		.data_server
		.resolve_mutation_outcome(request)
		.await
		.unwrap();
	assert_eq!(after_warm.resolution, MutationResolutionStatus::EXPIRED);
	println!(
		"TTL_PIN ttl_ms={TTL_MS} deadline_checked=true retained_entries={} expired_status={} expired_stop={} expired_commit={} warm_status={} warm_stop={} warm_commit={} resolution_after_warm=Expired",
		retained,
		expired.write.status.0,
		expired.write.mutation_stop_reason().0,
		expired.write.mutation_commit_knowledge().0,
		warm.write.status.0,
		warm.write.mutation_stop_reason().0,
		warm.write.mutation_commit_knowledge().0
	);
	shutdown_lease_node(node).await;
	// Object/server reopen, not a power-loss or process-kill experiment. This bounded
	// handoff matches the existing durable-outcome reopen fixture in this module.
	tokio::time::sleep(Duration::from_millis(25)).await;
	let reopened = WendelonNode::bootstrap_single_node(cfg).await.unwrap();
	let leader_deadline = tokio::time::Instant::now() + Duration::from_secs(3);
	while reopened.raft.current_leader().await != Some(reopened.local_node.id) {
		assert!(
			tokio::time::Instant::now() < leader_deadline,
			"reopened leader timeout"
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
	reopened
		.data_server
		.refresh_leases_once(&reopened.ring, &*reopened.clock, 60_000)
		.await;
	assert_eq!(reopened.data_server.idempotency.len(), 0);
	let cold_resolution = reopened
		.data_server
		.resolve_mutation_outcome(request)
		.await
		.unwrap();
	assert_eq!(
		cold_resolution.resolution,
		MutationResolutionStatus::EXPIRED
	);
	let (cold, _) = reopened
		.data_server
		.table_write(meta, batch.clone())
		.await
		.unwrap();
	assert_eq!(cold.write.status, WriteStatus::WRITE_OPERATION_EXPIRED);
	assert_eq!(cold.write.mutation_stop_reason(), MutationStopReason::NONE);
	assert_eq!(
		cold.write.mutation_commit_knowledge(),
		MutationCommitKnowledge::INDETERMINATE
	);
	assert_eq!(
		reopened.slab.local_l1_bytes().await,
		durable_bytes,
		"expired replay appends no additional data"
	);
	let key = table_row_identity(tenant(), descriptor.table_object_id, &629_i64.to_le_bytes());
	let location = reopened
		.slab
		.lookup(tenant(), key)
		.expect("previous committed row remains indexed");
	let stored = decode_batch(&reopened.slab.read(location).await.unwrap()).unwrap();
	assert_eq!(
		stored, batch,
		"exact committed payload survives expiry and server reopen"
	);
	assert_eq!(
		stored
			.column_by_name("id")
			.unwrap()
			.as_any()
			.downcast_ref::<Int64Array>()
			.unwrap()
			.value(0),
		629
	);
	println!(
		"COLD_TTL_PIN resolution=Expired cache_before=0 replay_status={} replay_stop={} replay_commit={} durable_bytes_before={} durable_bytes_after={} previous_row_readable=true",
		cold.write.status.0,
		cold.write.mutation_stop_reason().0,
		cold.write.mutation_commit_knowledge().0,
		durable_bytes,
		reopened.slab.local_l1_bytes().await
	);
	shutdown_lease_node(reopened).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn adr629_p2b_raw_and_table_local_jobs_remain_observed_after_caller_abort() {
	for table in [false, true] {
		let node_dir = bootstrap_tempdir("p2b-owned-local");
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			62941,
			node_dir.to_path_buf(),
			"127.0.0.1:0".into(),
		))
		.await
		.unwrap();
		tokio::time::timeout(Duration::from_secs(5), async {
			while node.raft.current_leader().await != Some(node.local_node.id) {
				tokio::task::yield_now().await;
			}
		})
		.await
		.unwrap();
		node.data_server
			.refresh_leases_once(&node.ring, &*node.clock, 60000)
			.await;
		if table {
			node.raft
				.client_write(WendelonRequest::SchemaUpdate(typed_table_schema_update(
					&typed_table_descriptor(),
				)))
				.await
				.unwrap();
		}
		let entered = Arc::new(Notify::new());
		let release = Arc::new(Notify::new());
		*node.data_server.local_observation_gate.lock().unwrap() =
			Some((entered.clone(), release.clone()));
		// Ownership fixture only. A cloned server disables the unrelated lease
		// precondition; the production node/policy is not changed.
		let server = Arc::new(node.data_server.as_ref().clone().with_lease_gate(false));
		let epoch = node.data_server.admission_snapshot().current_epoch;
		let mut job = tokio::spawn(async move {
			if table {
				let batch = typed_table_batch(vec![629]);
				let mut meta = typed_table_meta(&typed_table_descriptor(), batch.num_rows());
				meta.expected_epoch = epoch;
				server
					.table_write(meta, batch)
					.await
					.map(|(reply, _)| reply.write.status)
			} else {
				server
					.write(simple_write_meta(epoch), single_kv_batch())
					.await
					.map(|(reply, _)| reply.status)
			}
		});
		tokio::time::timeout(Duration::from_secs(10), async {
			tokio::select! {
				_ = entered.notified() => {},
				result = &mut job => panic!("primary returned before local gate: table={table} result={result:?}"),
			}
		}).await.unwrap();
		let authority = node.slab.finality_observation_registry();
		assert!(authority.test_state().0 > 0);
		job.abort();
		let _ = job.await;
		assert!(
			authority.test_state().0 > 0,
			"detached append owns its clone"
		);
		release.notify_one();
		tokio::time::timeout(Duration::from_secs(10), async {
			while authority.test_state().0 != 0 {
				tokio::task::yield_now().await;
			}
		})
		.await
		.unwrap();
		assert!(
			node.slab.local_l1_bytes().await > 0,
			"the orphaned job really published local bytes"
		);
		assert_eq!(authority.test_state().2, None);
		shutdown_lease_node(node).await;
	}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn adr629_p2b_raw_and_table_followers_keep_the_actual_pending_commit_after_abort() {
	for table in [false, true] {
		let node_dir = bootstrap_tempdir("p2b-follower");
		let node = WendelonNode::bootstrap_single_node(NodeBootstrapConfig::dev(
			62942,
			node_dir.to_path_buf(),
			"127.0.0.1:0".into(),
		))
		.await
		.unwrap();
		tokio::time::timeout(Duration::from_secs(5), async {
			while node.raft.current_leader().await != Some(node.local_node.id) {
				tokio::task::yield_now().await;
			}
		})
		.await
		.unwrap();
		let descriptor = typed_table_descriptor();
		if table {
			node.raft
				.client_write(WendelonRequest::SchemaUpdate(typed_table_schema_update(
					&descriptor,
				)))
				.await
				.unwrap();
		}
		node.handles.pending_finalization_reconciler.abort();
		let (ready, release) = node.data_server.pending_finalizations.test_commit_gate(0);
		let commit = hlc(6280, 0, node.local_node.id);
		let (msg_type, meta, arrow) = if table {
			let batch = typed_table_batch(vec![628]);
			let meta = typed_table_meta(&descriptor, batch.num_rows());
			let bifurcated =
				bifurcate_table_write(&meta, &batch, &descriptor, WriterNodeId::ZERO, commit)
					.unwrap();
			let replica = table_replica_meta(&meta, &bifurcated, &bifurcated.children, commit);
			(
				MsgType::TABLE_REPLICATE_REQ,
				bytemuck::bytes_of(&replica).to_vec(),
				encode_table_replica_children(&bifurcated.children).unwrap(),
			)
		} else {
			let meta = simple_write_meta(node.data_server.admission_snapshot().current_epoch);
			let bifurcated = bifurcate_client_write(&meta, &single_kv_batch()).unwrap();
			let replica = ReplicaWriteMeta {
				commit_hlc: commit,
				write_id: meta.write_id,
				epoch: meta.expected_epoch,
				tenant_id: meta.tenant_id,
				charged_bytes: bifurcated.charged_bytes,
				key_identity: bifurcated.key_identity,
				payload_hash: bifurcated.payload_hash,
				durability: meta.durability,
				await_finalization: meta.await_finalization,
				_pad: [0; 6],
			};
			(
				MsgType::REPLICATE_REQ,
				bytemuck::bytes_of(&replica).to_vec(),
				bifurcated.frame_bytes.as_ref().clone(),
			)
		};
		let frame = HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: msg_type.0,
				flags: 0,
				stream_id: 628,
				meta_len: meta.len() as u32,
				arrow_len: arrow.len() as u32,
				trailing_len: 0,
			},
			meta: crate::network::pool::PooledBuf::orphan(meta),
			arrow_payload: Some(crate::network::pool::PooledBuf::orphan(arrow)),
			trailing: None,
		};
		let server = node.data_server.clone();
		let job = tokio::spawn(async move {
			if table {
				server.handle_table_replicate(frame).await
			} else {
				server.handle_replicate(frame).await
			}
		});
		tokio::time::timeout(
			Duration::from_secs(10),
			tokio::task::spawn_blocking(move || ready.recv().unwrap()),
		)
		.await
		.unwrap()
		.unwrap();
		let authority = node.slab.finality_observation_registry();
		assert!(authority.test_state().0 > 0);
		job.abort();
		let _ = job.await;
		assert!(authority.test_state().0 > 0);
		release.send(()).unwrap();
		tokio::time::timeout(Duration::from_secs(10), async {
			while authority.test_state().0 != 0 {
				tokio::task::yield_now().await;
			}
		})
		.await
		.unwrap();
		assert_eq!(
			node.data_server
				.pending_finalizations
				.load_all()
				.await
				.unwrap()
				.len(),
			1
		);
		assert_eq!(authority.test_state(), (0, 1, None));
		shutdown_lease_node(node).await;
	}
}

#[path = "tests/tenant_cache/tests.rs"]
mod tenant_cache;

#[path = "tests/pending_tenant/tests.rs"]
mod pending_tenant;

#[path = "counter_visibility/tests.rs"]
mod counter_visibility_tests;

#[path = "counter_write_key/tests.rs"]
mod counter_write_key_tests;
