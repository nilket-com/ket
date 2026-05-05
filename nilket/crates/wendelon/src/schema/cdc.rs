use crate::hash::state_digest::{StateDigest, StateDigestAccumulator, visible_row_element};
use crate::schema::{
	HybridTimestamp, Identity, Locality, LogicalSchemaFingerprint, RecordRowIndex, RowCount,
	STORAGE_ROW_HLC_COLUMN, STORAGE_ROW_KEY_ID_COLUMN, STORAGE_ROW_ROUTING_LOCALITY_COLUMN,
	STORAGE_ROW_TABLE_OBJECT_ID_COLUMN, STORAGE_ROW_TOMBSTONE_COLUMN, TableObjectId, TenantId,
};
use arrow::array::{
	ArrayRef, BooleanArray, FixedSizeBinaryBuilder, UInt32Array, UInt64Array, new_null_array,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use bytemuck::{Pod, Zeroable};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::{Arc, RwLock};

pub const CDC_ROW_INDEX_COLUMN: &str = "__row_idx__";
pub const CDC_SUPERSEDES_HLC_COLUMN: &str = "__supersedes_hlc__";

pub type Hlc = HybridTimestamp;
pub type RowKeyId = Identity;

#[derive(
	Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Pod, Zeroable, Serialize, Deserialize,
)]
#[repr(C)]
pub struct CdcCursor {
	pub row_hlc: Hlc,
	pub row_key_id: RowKeyId,
	pub record_row_index: RecordRowIndex,
	pub _pad: u32,
}

impl CdcCursor {
	pub const MIN: Self = Self {
		row_hlc: HybridTimestamp {
			physical_ms: 0,
			logical: 0,
			node_id: 0,
		},
		row_key_id: Identity::ZERO,
		record_row_index: RecordRowIndex::ZERO,
		_pad: 0,
	};

	pub const MAX: Self = Self {
		row_hlc: HybridTimestamp {
			physical_ms: u64::MAX,
			logical: u32::MAX,
			node_id: u32::MAX,
		},
		row_key_id: Identity::MAX,
		record_row_index: RecordRowIndex::MAX,
		_pad: 0,
	};

	pub const fn new(row_hlc: Hlc, row_key_id: RowKeyId, record_row_index: RecordRowIndex) -> Self {
		Self {
			row_hlc,
			row_key_id,
			record_row_index,
			_pad: 0,
		}
	}

	pub const fn ceiling_for_hlc(row_hlc: Hlc) -> Self {
		Self {
			row_hlc,
			row_key_id: Identity::MAX,
			record_row_index: RecordRowIndex::MAX,
			_pad: 0,
		}
	}
}

impl Default for CdcCursor {
	fn default() -> Self {
		Self::MIN
	}
}

#[derive(
	Clone,
	Copy,
	Debug,
	Default,
	PartialEq,
	Eq,
	PartialOrd,
	Ord,
	Hash,
	Pod,
	Zeroable,
	Serialize,
	Deserialize,
)]
#[repr(transparent)]
pub struct CdcSubscriptionId(pub u128);

impl CdcSubscriptionId {
	pub const ZERO: Self = Self(0);

	pub const fn new(value: u128) -> Self {
		Self(value)
	}

	pub const fn get(self) -> u128 {
		self.0
	}
}

impl fmt::Display for CdcSubscriptionId {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{:032x}", self.0)
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum CdcSubscribeBound {
	Live = 0,
	SnapshotAtOpen = 1,
}

impl CdcSubscribeBound {
	pub const fn as_raw(self) -> u8 {
		self as u8
	}

	pub fn from_raw(raw: u8) -> Result<Self, CdcDecodeError> {
		match raw {
			0 => Ok(Self::Live),
			1 => Ok(Self::SnapshotAtOpen),
			_ => Err(CdcDecodeError::InvalidBound(raw)),
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum CdcFrameSource {
	Ring = 0,
	Catchup = 1,
}

impl CdcFrameSource {
	pub const fn as_raw(self) -> u8 {
		self as u8
	}

	pub const fn as_str(self) -> &'static str {
		match self {
			Self::Ring => "ring",
			Self::Catchup => "catchup",
		}
	}

	pub fn from_raw(raw: u8) -> Result<Self, CdcDecodeError> {
		match raw {
			0 => Ok(Self::Ring),
			1 => Ok(Self::Catchup),
			_ => Err(CdcDecodeError::InvalidFrameSource(raw)),
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum CdcDigestDomain {
	TableRowVisible = 1,
}

impl CdcDigestDomain {
	pub const fn as_raw(self) -> u32 {
		self as u32
	}

	pub const fn as_str(self) -> &'static str {
		match self {
			Self::TableRowVisible => "digest.table.row.visible.v1",
		}
	}

	pub fn from_raw(raw: u32) -> Result<Self, CdcDecodeError> {
		match raw {
			1 => Ok(Self::TableRowVisible),
			_ => Err(CdcDecodeError::InvalidDigestDomain(raw)),
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum CdcRejectCode {
	BelowRetentionFloor = 1,
	RingOverflowDuringCatchup = 2,
	SchemaFingerprintIncompatible = 3,
	CheckpointDigestMismatch = 4,
	UnknownTable = 5,
	TenantAuthDenied = 6,
	TableNotReplicatedHere = 7,
	InternalEngineError = 8,
}

impl CdcRejectCode {
	pub const ALL: [Self; 8] = [
		Self::BelowRetentionFloor,
		Self::RingOverflowDuringCatchup,
		Self::SchemaFingerprintIncompatible,
		Self::CheckpointDigestMismatch,
		Self::UnknownTable,
		Self::TenantAuthDenied,
		Self::TableNotReplicatedHere,
		Self::InternalEngineError,
	];

	pub const fn as_raw(self) -> u32 {
		self as u32
	}

	pub const fn as_str(self) -> &'static str {
		match self {
			Self::BelowRetentionFloor => "below_retention_floor",
			Self::RingOverflowDuringCatchup => "ring_overflow_during_catchup",
			Self::SchemaFingerprintIncompatible => "schema_fingerprint_incompatible",
			Self::CheckpointDigestMismatch => "checkpoint_digest_mismatch",
			Self::UnknownTable => "unknown_table",
			Self::TenantAuthDenied => "tenant_auth_denied",
			Self::TableNotReplicatedHere => "table_not_replicated_here",
			Self::InternalEngineError => "internal_engine_error",
		}
	}

	pub fn from_raw(raw: u32) -> Result<Self, CdcDecodeError> {
		match raw {
			1 => Ok(Self::BelowRetentionFloor),
			2 => Ok(Self::RingOverflowDuringCatchup),
			3 => Ok(Self::SchemaFingerprintIncompatible),
			4 => Ok(Self::CheckpointDigestMismatch),
			5 => Ok(Self::UnknownTable),
			6 => Ok(Self::TenantAuthDenied),
			7 => Ok(Self::TableNotReplicatedHere),
			8 => Ok(Self::InternalEngineError),
			_ => Err(CdcDecodeError::InvalidRejectCode(raw)),
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum CdcDecodeError {
	#[error("invalid CDC subscribe bound discriminant {0}")]
	InvalidBound(u8),
	#[error("invalid CDC frame source discriminant {0}")]
	InvalidFrameSource(u8),
	#[error("invalid CDC digest domain discriminant {0}")]
	InvalidDigestDomain(u32),
	#[error("invalid CDC reject code discriminant {0}")]
	InvalidRejectCode(u32),
}

#[derive(Clone, Copy, Debug, Pod, Zeroable)]
#[repr(C)]
pub struct CdcSubscribeRequestMeta {
	pub tenant_id: TenantId,
	pub table_object_id: TableObjectId,
	pub start_cursor: CdcCursor,
	pub bound_raw: u8,
	pub resume_digest_present: u8,
	pub _pad: [u8; 6],
	pub logical_schema_fingerprint: [u8; 32],
	pub resume_visible_state_digest: StateDigest,
}

impl CdcSubscribeRequestMeta {
	pub fn bound(self) -> Result<CdcSubscribeBound, CdcDecodeError> {
		CdcSubscribeBound::from_raw(self.bound_raw)
	}

	pub fn resume_digest(self) -> Option<StateDigest> {
		(self.resume_digest_present != 0).then_some(self.resume_visible_state_digest)
	}

	pub fn decoded_logical_schema_fingerprint(self) -> LogicalSchemaFingerprint {
		LogicalSchemaFingerprint::from_bytes(self.logical_schema_fingerprint)
	}
}

#[derive(Clone, Copy, Debug, Pod, Zeroable)]
#[repr(C)]
pub struct CdcFrameMeta {
	pub subscription_id: CdcSubscriptionId,
	pub frame_low_cursor: CdcCursor,
	pub frame_high_cursor: CdcCursor,
	pub logical_schema_fingerprint: [u8; 32],
	pub event_count: RowCount,
	pub source_raw: u8,
	pub _pad: [u8; 7],
}

impl CdcFrameMeta {
	pub fn source(self) -> Result<CdcFrameSource, CdcDecodeError> {
		CdcFrameSource::from_raw(self.source_raw)
	}

	pub fn decoded_logical_schema_fingerprint(self) -> LogicalSchemaFingerprint {
		LogicalSchemaFingerprint::from_bytes(self.logical_schema_fingerprint)
	}
}

#[derive(Clone, Copy, Debug, Pod, Zeroable)]
#[repr(C)]
pub struct CdcCheckpointMeta {
	pub subscription_id: CdcSubscriptionId,
	pub cursor: CdcCursor,
	pub visible_state_digest: StateDigest,
	pub digest_domain_raw: u32,
	pub _pad: [u8; 4],
}

impl CdcCheckpointMeta {
	pub fn digest_domain(self) -> Result<CdcDigestDomain, CdcDecodeError> {
		CdcDigestDomain::from_raw(self.digest_domain_raw)
	}
}

#[derive(Clone, Copy, Debug, Pod, Zeroable)]
#[repr(C)]
pub struct CdcRejectMeta {
	pub subscription_id: CdcSubscriptionId,
	pub code_raw: u32,
	pub _pad: [u8; 12],
}

impl CdcRejectMeta {
	pub const fn new(subscription_id: CdcSubscriptionId, code: CdcRejectCode) -> Self {
		Self {
			subscription_id,
			code_raw: code.as_raw(),
			_pad: [0; 12],
		}
	}

	pub fn code(self) -> Result<CdcRejectCode, CdcDecodeError> {
		CdcRejectCode::from_raw(self.code_raw)
	}
}

pub struct AtomicCdcCursor {
	inner: RwLock<CdcCursor>,
}

impl AtomicCdcCursor {
	pub fn new(cursor: CdcCursor) -> Self {
		Self {
			inner: RwLock::new(cursor),
		}
	}

	pub fn load(&self) -> CdcCursor {
		*self.inner.read().expect("cdc cursor rwlock poisoned")
	}

	pub fn store(&self, cursor: CdcCursor) {
		*self.inner.write().expect("cdc cursor rwlock poisoned") = cursor;
	}

	pub fn advance_max(&self, cursor: CdcCursor) -> CdcCursor {
		let mut guard = self.inner.write().expect("cdc cursor rwlock poisoned");
		if cursor > *guard {
			*guard = cursor;
		}
		*guard
	}
}

impl fmt::Debug for AtomicCdcCursor {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_tuple("AtomicCdcCursor")
			.field(&self.load())
			.finish()
	}
}

pub fn cdc_event_schema(
	logical_schema_fingerprint: LogicalSchemaFingerprint,
	source_schema: &Schema,
) -> SchemaRef {
	let mut fields = Vec::with_capacity(source_schema.fields().len() + 7);
	fields.push(Arc::new(Field::new(
		STORAGE_ROW_KEY_ID_COLUMN,
		DataType::FixedSizeBinary(32),
		false,
	)));
	fields.push(Arc::new(Field::new(
		STORAGE_ROW_HLC_COLUMN,
		DataType::FixedSizeBinary(16),
		false,
	)));
	fields.push(Arc::new(Field::new(
		STORAGE_ROW_TOMBSTONE_COLUMN,
		DataType::Boolean,
		false,
	)));
	fields.push(Arc::new(Field::new(
		STORAGE_ROW_TABLE_OBJECT_ID_COLUMN,
		DataType::FixedSizeBinary(16),
		false,
	)));
	fields.push(Arc::new(Field::new(
		STORAGE_ROW_ROUTING_LOCALITY_COLUMN,
		DataType::UInt64,
		true,
	)));
	fields.push(Arc::new(Field::new(
		CDC_ROW_INDEX_COLUMN,
		DataType::UInt32,
		false,
	)));
	fields.push(Arc::new(Field::new(
		CDC_SUPERSEDES_HLC_COLUMN,
		DataType::FixedSizeBinary(16),
		true,
	)));

	for field in source_schema.fields() {
		if !is_cdc_synthetic_column(field.name()) {
			let mut cdc_field = Field::new(field.name(), field.data_type().clone(), true);
			if !field.metadata().is_empty() {
				cdc_field = cdc_field.with_metadata(field.metadata().clone());
			}
			fields.push(Arc::new(cdc_field));
		}
	}

	let mut metadata = source_schema.metadata().clone();
	metadata.insert(
		"wendelon.cdc.logical_schema_fingerprint".to_string(),
		hex_lower(logical_schema_fingerprint.as_bytes()),
	);
	Arc::new(Schema::new_with_metadata(fields, metadata))
}

pub fn cdc_event_batch(
	logical_schema_fingerprint: LogicalSchemaFingerprint,
	row_hlc: HybridTimestamp,
	row_key_id: Identity,
	table_object_id: TableObjectId,
	routing_locality: Option<Locality>,
	record_row_index: RecordRowIndex,
	supersedes_hlc: Option<HybridTimestamp>,
	source_batch: &RecordBatch,
) -> arrow::error::Result<RecordBatch> {
	if source_batch.num_rows() != 1 {
		return Err(arrow::error::ArrowError::InvalidArgumentError(format!(
			"CDC event batch source must contain exactly one row, got {}",
			source_batch.num_rows()
		)));
	}
	cdc_event_batch_from_user_columns(
		logical_schema_fingerprint,
		row_hlc,
		row_key_id,
		table_object_id,
		routing_locality,
		record_row_index,
		false,
		supersedes_hlc,
		source_batch.schema().as_ref(),
		cdc_user_columns_from_source_batch(source_batch),
	)
}

pub fn cdc_tombstone_event_batch(
	logical_schema_fingerprint: LogicalSchemaFingerprint,
	row_hlc: HybridTimestamp,
	row_key_id: Identity,
	table_object_id: TableObjectId,
	routing_locality: Option<Locality>,
	record_row_index: RecordRowIndex,
	supersedes_hlc: Option<HybridTimestamp>,
	source_schema: &Schema,
) -> arrow::error::Result<RecordBatch> {
	let user_columns = source_schema
		.fields()
		.iter()
		.filter(|field| !is_cdc_synthetic_column(field.name()))
		.map(|field| new_null_array(field.data_type(), 1))
		.collect();
	cdc_event_batch_from_user_columns(
		logical_schema_fingerprint,
		row_hlc,
		row_key_id,
		table_object_id,
		routing_locality,
		record_row_index,
		true,
		supersedes_hlc,
		source_schema,
		user_columns,
	)
}

fn cdc_event_batch_from_user_columns(
	logical_schema_fingerprint: LogicalSchemaFingerprint,
	row_hlc: HybridTimestamp,
	row_key_id: Identity,
	table_object_id: TableObjectId,
	routing_locality: Option<Locality>,
	record_row_index: RecordRowIndex,
	row_tombstone: bool,
	supersedes_hlc: Option<HybridTimestamp>,
	source_schema: &Schema,
	user_columns: Vec<ArrayRef>,
) -> arrow::error::Result<RecordBatch> {
	let mut row_key = FixedSizeBinaryBuilder::with_capacity(1, 32);
	row_key.append_value(row_key_id.as_bytes())?;
	let mut row_hlc_builder = FixedSizeBinaryBuilder::with_capacity(1, 16);
	row_hlc_builder.append_value(cdc_hlc_bytes(row_hlc))?;
	let tombstone = BooleanArray::from(vec![row_tombstone]);
	let mut table = FixedSizeBinaryBuilder::with_capacity(1, 16);
	table.append_value(table_object_id.as_bytes())?;
	let routing = UInt64Array::from(vec![routing_locality.map(Locality::get)]);
	let row_index = UInt32Array::from(vec![record_row_index.get()]);
	let mut supersedes = FixedSizeBinaryBuilder::with_capacity(1, 16);
	if let Some(hlc) = supersedes_hlc {
		supersedes.append_value(cdc_hlc_bytes(hlc))?;
	} else {
		supersedes.append_null();
	}

	let mut columns: Vec<ArrayRef> = vec![
		Arc::new(row_key.finish()),
		Arc::new(row_hlc_builder.finish()),
		Arc::new(tombstone),
		Arc::new(table.finish()),
		Arc::new(routing),
		Arc::new(row_index),
		Arc::new(supersedes.finish()),
	];
	columns.extend(user_columns);
	RecordBatch::try_new(
		cdc_event_schema(logical_schema_fingerprint, source_schema),
		columns,
	)
}

fn cdc_user_columns_from_source_batch(source_batch: &RecordBatch) -> Vec<ArrayRef> {
	source_batch
		.schema()
		.fields()
		.iter()
		.enumerate()
		.filter_map(|(index, field)| {
			(!is_cdc_synthetic_column(field.name())).then(|| source_batch.column(index).clone())
		})
		.collect()
}

pub fn cdc_visible_rows_digest(
	tenant_id: TenantId,
	table_object_id: TableObjectId,
	row_key_ids: impl IntoIterator<Item = Identity>,
) -> StateDigest {
	let mut accumulator = StateDigestAccumulator::new();
	for row_key_id in row_key_ids {
		accumulator.add_element(visible_row_element(tenant_id, table_object_id, row_key_id));
	}
	accumulator.finish()
}

fn is_cdc_synthetic_column(name: &str) -> bool {
	matches!(
		name,
		STORAGE_ROW_KEY_ID_COLUMN
			| STORAGE_ROW_HLC_COLUMN
			| STORAGE_ROW_TOMBSTONE_COLUMN
			| STORAGE_ROW_TABLE_OBJECT_ID_COLUMN
			| STORAGE_ROW_ROUTING_LOCALITY_COLUMN
			| CDC_ROW_INDEX_COLUMN
			| CDC_SUPERSEDES_HLC_COLUMN
	)
}

fn hex_lower(bytes: &[u8]) -> String {
	let mut out = String::with_capacity(bytes.len() * 2);
	for byte in bytes {
		use std::fmt::Write as _;
		let _ = write!(&mut out, "{byte:02x}");
	}
	out
}

pub fn cdc_cursor_in_catchup_window(
	cursor: CdcCursor,
	start_exclusive: CdcCursor,
	upper_inclusive: CdcCursor,
) -> bool {
	cursor > start_exclusive && cursor <= upper_inclusive
}

pub const fn cdc_hlc_bytes(hlc: HybridTimestamp) -> [u8; 16] {
	let physical = hlc.physical_ms.to_le_bytes();
	let logical = hlc.logical.to_le_bytes();
	let node = hlc.node_id.to_le_bytes();
	[
		physical[0],
		physical[1],
		physical[2],
		physical[3],
		physical[4],
		physical[5],
		physical[6],
		physical[7],
		logical[0],
		logical[1],
		logical[2],
		logical[3],
		node[0],
		node[1],
		node[2],
		node[3],
	]
}

pub const fn cdc_cursor_for_location(
	row_hlc: HybridTimestamp,
	row_key_id: Identity,
	record_row_index: RecordRowIndex,
) -> CdcCursor {
	CdcCursor::new(row_hlc, row_key_id, record_row_index)
}

pub const fn cdc_cursor_ceiling_for_hlc(row_hlc: HybridTimestamp) -> CdcCursor {
	CdcCursor::ceiling_for_hlc(row_hlc)
}

pub const fn cdc_locality_for_row(locality: Locality) -> Locality {
	locality
}
