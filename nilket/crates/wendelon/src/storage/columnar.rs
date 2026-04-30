use crate::crypto::{CryptoManager, TieredMultiRangeReadPlan, TieredPlaintextRange};
use crate::metrics::MetricsRegistry;
use crate::schema::aggregate::AggregateExpression;
use crate::schema::pbc::{self, Program};
use crate::schema::{
	EncryptedChunkCount, IpcBufferIndex, IpcBufferLength, IpcBufferOffset,
	LogicalSchemaFingerprint, RecordBodyByteLen, RecordBodyByteOffset, RecordBodyCompressionPolicy,
	RecordLayoutPolicy, STORAGE_ROW_HLC_COLUMN, STORAGE_ROW_KEY_ID_COLUMN,
	STORAGE_ROW_ROUTING_LOCALITY_COLUMN, STORAGE_ROW_TABLE_OBJECT_ID_COLUMN,
	STORAGE_ROW_TOMBSTONE_COLUMN, SlabId, TableSchemaDescriptorV1,
};
use crate::storage::core::RecordLocation;
use arrow::array::{ArrayData, ArrayRef, NullArray, make_array};
use arrow::buffer::Buffer;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::error::ArrowError;
use arrow::ipc::convert::fb_to_schema;
use arrow::ipc::{BodyCompressionMethod, CompressionType, MessageHeader, root_as_message};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::io::{Error, ErrorKind, Read};
use std::sync::Arc;
use std::time::Duration;

pub const L2_COLUMN_FETCH_INITIAL_WINDOW_BYTES: u64 = 64 * 1024;
pub const L2_COLUMN_FETCH_MAX_PROBE_BYTES: u64 = 256 * 1024;
pub const L2_COLUMN_FETCH_MIN_RECORD_BYTES: u64 = 256 * 1024;
pub const L2_COLUMN_FETCH_DEFAULT_SELECTED_FRACTION_MICROS: u32 = 670_000;
pub const L2_COLUMN_FETCH_DEFAULT_MAX_RANGES_PER_RECORD: u32 = 8;
pub const L2_COLUMN_FETCH_DEFAULT_MAX_PARALLEL_RANGE_GETS: u32 = 4;
pub const L2_COLUMN_FETCH_FRACTION_DENOMINATOR: u32 = 1_000_000;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct L2ColumnFetchConfig {
	pub enabled: bool,
	pub ipc_metadata_initial_window_bytes: RecordBodyByteLen,
	pub max_ipc_metadata_probe_bytes: RecordBodyByteLen,
	pub min_record_bytes: RecordBodyByteLen,
	pub max_selected_fraction_micros: u32,
	pub max_ranges_per_record: EncryptedChunkCount,
	pub max_parallel_range_gets: EncryptedChunkCount,
}

impl Default for L2ColumnFetchConfig {
	fn default() -> Self {
		Self {
			enabled: false,
			ipc_metadata_initial_window_bytes: RecordBodyByteLen::new(
				L2_COLUMN_FETCH_INITIAL_WINDOW_BYTES,
			),
			max_ipc_metadata_probe_bytes: RecordBodyByteLen::new(L2_COLUMN_FETCH_MAX_PROBE_BYTES),
			min_record_bytes: RecordBodyByteLen::new(L2_COLUMN_FETCH_MIN_RECORD_BYTES),
			max_selected_fraction_micros: L2_COLUMN_FETCH_DEFAULT_SELECTED_FRACTION_MICROS,
			max_ranges_per_record: EncryptedChunkCount::new(
				L2_COLUMN_FETCH_DEFAULT_MAX_RANGES_PER_RECORD,
			),
			max_parallel_range_gets: EncryptedChunkCount::new(
				L2_COLUMN_FETCH_DEFAULT_MAX_PARALLEL_RANGE_GETS,
			),
		}
	}
}

impl L2ColumnFetchConfig {
	pub fn validate(&self) -> std::io::Result<()> {
		if self.ipc_metadata_initial_window_bytes.get() == 0 {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"l2 column fetch metadata initial window must be greater than zero",
			));
		}
		if self.max_ipc_metadata_probe_bytes.get() == 0 {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"l2 column fetch metadata probe cap must be greater than zero",
			));
		}
		if self.ipc_metadata_initial_window_bytes > self.max_ipc_metadata_probe_bytes {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"l2 column fetch metadata initial window must not exceed probe cap",
			));
		}
		if self.min_record_bytes.get() == 0 {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"l2 column fetch min_record_bytes must be greater than zero",
			));
		}
		if self.max_selected_fraction_micros >= L2_COLUMN_FETCH_FRACTION_DENOMINATOR {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"l2 column fetch selected fraction must be < 1_000_000 micros",
			));
		}
		if self.max_ranges_per_record.get() == 0 {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"l2 column fetch max_ranges_per_record must be greater than zero",
			));
		}
		if self.max_parallel_range_gets.get() == 0 {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"l2 column fetch max_parallel_range_gets must be greater than zero",
			));
		}
		Ok(())
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TieredRecordId {
	pub slab_id: SlabId,
	pub record_offset: RecordBodyByteOffset,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PlaintextByteRange {
	pub start: u64,
	pub len: u64,
}

impl PlaintextByteRange {
	pub fn end(self) -> std::io::Result<u64> {
		self.start.checked_add(self.len).ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidInput,
				"plaintext byte range end offset overflow",
			)
		})
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct L2ColumnFetchMetadataProbe {
	pub window_bytes: RecordBodyByteLen,
	pub record_plaintext_range: PlaintextByteRange,
	pub record_relative_range: TieredPlaintextRange,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum L2ColumnFetchPreparation {
	NeedMoreMetadata(L2ColumnFetchMetadataProbe),
	Strategy(PreparedL2ColumnFetchStrategy),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreparedL2ColumnFetchStrategy {
	pub directory: Option<RecordBufferDirectory>,
	pub strategy: L2FetchStrategy,
}

pub fn initial_metadata_probe_plan(
	config: &L2ColumnFetchConfig,
	record_location: &RecordLocation,
) -> Result<L2ColumnFetchMetadataProbe, ColumnFetchError> {
	let record_len = record_len_for_probe(record_location)?;
	let window = config
		.ipc_metadata_initial_window_bytes
		.get()
		.min(config.max_ipc_metadata_probe_bytes.get())
		.min(record_len);
	metadata_probe_plan(record_location, window)
}

pub fn next_metadata_probe_plan(
	config: &L2ColumnFetchConfig,
	record_location: &RecordLocation,
	current_window: RecordBodyByteLen,
	needed_bytes: u64,
) -> Result<Option<L2ColumnFetchMetadataProbe>, ColumnFetchError> {
	let record_len = record_len_for_probe(record_location)?;
	let cap = config.max_ipc_metadata_probe_bytes.get().min(record_len);
	if needed_bytes > cap || current_window.get() >= cap {
		return Ok(None);
	}
	let doubled = current_window.get().saturating_mul(2).max(1);
	let next = doubled.max(needed_bytes).min(cap);
	if next <= current_window.get() {
		return Ok(None);
	}
	metadata_probe_plan(record_location, next).map(Some)
}

fn metadata_probe_plan(
	record_location: &RecordLocation,
	window: u64,
) -> Result<L2ColumnFetchMetadataProbe, ColumnFetchError> {
	let record_len = record_len_for_probe(record_location)?;
	if window > record_len {
		return Err(ColumnFetchError::InvalidRange(format!(
			"metadata probe window {window} exceeds record length {record_len}"
		)));
	}
	let window_bytes = RecordBodyByteLen::try_new(window).map_err(|err| {
		ColumnFetchError::InvalidRange(format!("metadata probe length rejected: {err}"))
	})?;
	Ok(L2ColumnFetchMetadataProbe {
		window_bytes,
		record_plaintext_range: PlaintextByteRange {
			start: record_location.offset,
			len: record_len,
		},
		record_relative_range: TieredPlaintextRange::new(
			RecordBodyByteOffset::new(0),
			window_bytes,
		),
	})
}

fn record_len_for_probe(record_location: &RecordLocation) -> Result<u64, ColumnFetchError> {
	let record_len = u64::from(record_location.length);
	if record_len == 0 {
		return Err(ColumnFetchError::InvalidRange(
			"metadata probe requires non-empty record".into(),
		));
	}
	Ok(record_len)
}

pub fn prepare_l2_column_fetch_strategy(
	crypto: &CryptoManager,
	config: &L2ColumnFetchConfig,
	descriptor: &TableSchemaDescriptorV1,
	record_location: &RecordLocation,
	record_id: TieredRecordId,
	requirement: &ColumnFetchRequirement,
	metadata_window_bytes: &[u8],
	current_probe_window: RecordBodyByteLen,
	object_size_bytes: u64,
	tail_window: &[u8],
	additional_metadata: Option<&[u8]>,
	rehydration_available: bool,
) -> std::io::Result<L2ColumnFetchPreparation> {
	let pre_probe = pick_l2_fetch_strategy(
		config,
		descriptor,
		record_location,
		requirement,
		None,
		None,
		rehydration_available,
	);
	if !matches!(
		pre_probe,
		L2FetchStrategy::Fallback(ColumnarFallbackReason::MetadataProbeCap)
	) {
		return Ok(L2ColumnFetchPreparation::Strategy(
			PreparedL2ColumnFetchStrategy {
				directory: None,
				strategy: pre_probe,
			},
		));
	}

	let probe = metadata_probe_plan(record_location, current_probe_window.get())
		.map_err(column_fetch_io_error)?;
	let directory = match parse_record_buffer_directory(
		record_id,
		probe.record_plaintext_range,
		metadata_window_bytes,
		descriptor,
	) {
		Ok(directory) => directory,
		Err(ColumnFetchError::MetadataTruncated { needed, .. }) => {
			let next =
				next_metadata_probe_plan(config, record_location, current_probe_window, needed)
					.map_err(column_fetch_io_error)?;
			return Ok(match next {
				Some(probe) => L2ColumnFetchPreparation::NeedMoreMetadata(probe),
				None => L2ColumnFetchPreparation::Strategy(PreparedL2ColumnFetchStrategy {
					directory: None,
					strategy: L2FetchStrategy::Fallback(ColumnarFallbackReason::MetadataProbeCap),
				}),
			});
		}
		Err(ColumnFetchError::UnsupportedArrowLayout) => {
			return Ok(L2ColumnFetchPreparation::Strategy(
				PreparedL2ColumnFetchStrategy {
					directory: None,
					strategy: L2FetchStrategy::Fallback(
						ColumnarFallbackReason::UnsupportedArrowLayout,
					),
				},
			));
		}
		Err(err) => return Err(column_fetch_io_error(err)),
	};

	let selected_ranges = match selected_record_body_ranges(&directory, requirement) {
		Ok(ranges) => ranges,
		Err(ColumnFetchError::UnsupportedArrowLayout) => {
			return Ok(L2ColumnFetchPreparation::Strategy(
				PreparedL2ColumnFetchStrategy {
					directory: Some(directory),
					strategy: L2FetchStrategy::Fallback(
						ColumnarFallbackReason::UnsupportedArrowLayout,
					),
				},
			));
		}
		Err(err) => return Err(column_fetch_io_error(err)),
	};
	let record_plaintext_range = record_plaintext_range_for_crypto(record_location)?;
	let tiered_plan = crypto.plan_tiered_multi_range_read(
		record_location.slab_id,
		object_size_bytes,
		tail_window,
		additional_metadata,
		record_plaintext_range,
		&selected_ranges,
	)?;
	let candidate_plan = match build_column_fetch_plan(&directory, requirement, &tiered_plan) {
		Ok(plan) => Some(plan),
		Err(ColumnFetchError::UnsupportedArrowLayout) => None,
		Err(err) => return Err(column_fetch_io_error(err)),
	};
	let strategy = pick_l2_fetch_strategy(
		config,
		descriptor,
		record_location,
		requirement,
		Some(&directory),
		candidate_plan,
		rehydration_available,
	);
	Ok(L2ColumnFetchPreparation::Strategy(
		PreparedL2ColumnFetchStrategy {
			directory: Some(directory),
			strategy,
		},
	))
}

fn record_plaintext_range_for_crypto(
	record_location: &RecordLocation,
) -> std::io::Result<std::ops::Range<usize>> {
	let start = usize::try_from(record_location.offset).map_err(|_| {
		Error::new(
			ErrorKind::InvalidData,
			format!(
				"record offset {} does not fit into usize",
				record_location.offset
			),
		)
	})?;
	let end_u64 = record_location
		.offset
		.checked_add(u64::from(record_location.length))
		.ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				format!(
					"record range overflows u64 for slab {} at offset {}",
					record_location.slab_id, record_location.offset
				),
			)
		})?;
	let end = usize::try_from(end_u64).map_err(|_| {
		Error::new(
			ErrorKind::InvalidData,
			format!("record end {end_u64} does not fit into usize"),
		)
	})?;
	Ok(start..end)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordBufferDirectory {
	pub record_id: TieredRecordId,
	pub record_plaintext_range: PlaintextByteRange,
	pub row_count: usize,
	pub record_body_compression: RecordBodyCompressionPolicy,
	pub ipc_schema: Schema,
	pub buffers: Vec<IpcBufferRange>,
	pub column_to_buffers: Vec<ColumnBufferRange>,
	pub column_null_counts: Vec<usize>,
	pub storage_column_ordinals: BTreeMap<StorageColumn, u32>,
	pub logical_schema_fingerprint: LogicalSchemaFingerprint,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IpcBufferRange {
	pub offset_in_record: IpcBufferOffset,
	pub length: IpcBufferLength,
}

impl IpcBufferRange {
	pub fn end(self) -> std::io::Result<u64> {
		self.offset_in_record
			.get()
			.checked_add(self.length.get())
			.ok_or_else(|| Error::new(ErrorKind::InvalidInput, "IPC buffer span overflow"))
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ColumnBufferRange {
	pub first_buffer_ordinal: IpcBufferIndex,
	pub buffer_count: u32,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ColumnProjection {
	columns: Vec<u32>,
}

impl ColumnProjection {
	pub fn new(columns: impl Into<Vec<u32>>) -> Self {
		let mut columns = columns.into();
		columns.sort_unstable();
		columns.dedup();
		Self { columns }
	}

	pub fn columns(&self) -> &[u32] {
		self.columns.as_slice()
	}
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ColumnOrdinalSet {
	columns: BTreeSet<u32>,
}

impl ColumnOrdinalSet {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn from_columns(columns: impl IntoIterator<Item = u32>) -> Self {
		Self {
			columns: columns.into_iter().collect(),
		}
	}

	pub fn insert(&mut self, column: u32) -> bool {
		self.columns.insert(column)
	}

	pub fn contains(&self, column: u32) -> bool {
		self.columns.contains(&column)
	}

	pub fn is_empty(&self) -> bool {
		self.columns.is_empty()
	}

	pub fn len(&self) -> usize {
		self.columns.len()
	}

	pub fn iter(&self) -> impl Iterator<Item = u32> + '_ {
		self.columns.iter().copied()
	}

	pub fn extend(&mut self, other: &ColumnOrdinalSet) {
		self.columns.extend(other.iter());
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum StorageColumn {
	RowHlc,
	RowKeyId,
	RowRoutingLocality,
	RowTombstone,
	/// ADR 045 Phase 8 — per-row binding back to the
	/// originating descriptor's `table_object_id`. Stamped on
	/// every multi-row record (PK + AppendOnly). Descriptor-
	/// filtered query paths fetch this column and verify it
	/// against `descriptor.table_object_id` to isolate rows
	/// across cross-table table-spread prefix collisions and
	/// to avoid PK re-derivation on the projected fast path.
	RowTableObjectId,
}

impl StorageColumn {
	pub fn from_name(name: &str) -> Option<Self> {
		match name {
			STORAGE_ROW_HLC_COLUMN => Some(Self::RowHlc),
			STORAGE_ROW_KEY_ID_COLUMN => Some(Self::RowKeyId),
			STORAGE_ROW_ROUTING_LOCALITY_COLUMN => Some(Self::RowRoutingLocality),
			STORAGE_ROW_TOMBSTONE_COLUMN => Some(Self::RowTombstone),
			STORAGE_ROW_TABLE_OBJECT_ID_COLUMN => Some(Self::RowTableObjectId),
			_ => None,
		}
	}
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StorageColumnSet {
	columns: BTreeSet<StorageColumn>,
}

impl StorageColumnSet {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn insert(&mut self, column: StorageColumn) -> bool {
		self.columns.insert(column)
	}

	pub fn contains(&self, column: StorageColumn) -> bool {
		self.columns.contains(&column)
	}

	pub fn iter(&self) -> impl Iterator<Item = StorageColumn> + '_ {
		self.columns.iter().copied()
	}

	pub fn is_empty(&self) -> bool {
		self.columns.is_empty()
	}

	pub fn extend(&mut self, other: &StorageColumnSet) {
		self.columns.extend(other.iter());
	}
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ColumnFetchRequirement {
	pub predicate_columns: ColumnOrdinalSet,
	pub output_columns: ColumnOrdinalSet,
	pub aggregate_columns: ColumnOrdinalSet,
	pub storage_columns: StorageColumnSet,
}

impl ColumnFetchRequirement {
	pub fn extend(&mut self, other: &ColumnFetchRequirement) {
		self.predicate_columns.extend(&other.predicate_columns);
		self.output_columns.extend(&other.output_columns);
		self.aggregate_columns.extend(&other.aggregate_columns);
		self.storage_columns.extend(&other.storage_columns);
	}

	pub fn is_full(&self, schema: &Schema) -> bool {
		let mut required = self.user_visible();
		for (ordinal, field) in schema.fields().iter().enumerate() {
			if StorageColumn::from_name(field.name()).is_some() {
				continue;
			}
			let Ok(ordinal) = u32::try_from(ordinal) else {
				return false;
			};
			if !required.contains(ordinal) {
				return false;
			}
			required.insert(ordinal);
		}
		true
	}

	pub fn user_visible(&self) -> ColumnOrdinalSet {
		let mut out = self.predicate_columns.clone();
		out.extend(&self.output_columns);
		out.extend(&self.aggregate_columns);
		out
	}
}

pub fn analyze_pbc_column_requirement(
	program: &Program,
	user_column_count: usize,
) -> Result<ColumnFetchRequirement, ColumnFetchError> {
	let mut requirement = ColumnFetchRequirement::default();
	insert_pbc_predicate_columns(&mut requirement, program, user_column_count)?;
	match program.project_columns() {
		Some(columns) => {
			for column in columns {
				let column = u32::try_from(*column).map_err(|_| {
					ColumnFetchError::InvalidRange(format!(
						"PBC PROJECT column ordinal {column} exceeds u32::MAX"
					))
				})?;
				insert_required_column(
					&mut requirement,
					RequiredColumnAxis::Output,
					column,
					user_column_count,
				)?;
			}
		}
		None => {
			let user_column_count_u32 = u32::try_from(user_column_count).map_err(|_| {
				ColumnFetchError::InvalidRange(format!(
					"user column count {user_column_count} exceeds u32::MAX"
				))
			})?;
			for column in 0..user_column_count_u32 {
				requirement.output_columns.insert(column);
			}
		}
	}
	Ok(requirement)
}

pub fn analyze_pbc_predicate_column_requirement(
	program: &Program,
	user_column_count: usize,
) -> Result<ColumnFetchRequirement, ColumnFetchError> {
	let mut requirement = ColumnFetchRequirement::default();
	insert_pbc_predicate_columns(&mut requirement, program, user_column_count)?;
	Ok(requirement)
}

pub fn analyze_aggregate_column_requirement(
	expressions: &[AggregateExpression],
	user_column_count: usize,
) -> Result<ColumnFetchRequirement, ColumnFetchError> {
	let mut requirement = ColumnFetchRequirement::default();
	for expression in expressions {
		let function = expression.function().map_err(|err| {
			ColumnFetchError::InvalidRange(format!("aggregate expression rejected: {err:?}"))
		})?;
		let Some(column) = function.column() else {
			continue;
		};
		insert_required_column(
			&mut requirement,
			RequiredColumnAxis::Aggregate,
			u32::from(column),
			user_column_count,
		)?;
	}
	Ok(requirement)
}

pub fn analyze_aggregate_query_column_requirement(
	program: &Program,
	aggregate_expressions: &[AggregateExpression],
	user_column_count: usize,
) -> Result<ColumnFetchRequirement, ColumnFetchError> {
	let mut requirement = analyze_pbc_predicate_column_requirement(program, user_column_count)?;
	requirement.extend(&analyze_aggregate_column_requirement(
		aggregate_expressions,
		user_column_count,
	)?);
	Ok(requirement)
}

fn insert_pbc_predicate_columns(
	requirement: &mut ColumnFetchRequirement,
	program: &Program,
	user_column_count: usize,
) -> Result<(), ColumnFetchError> {
	for column in program.referenced_columns() {
		insert_required_column(
			requirement,
			RequiredColumnAxis::Predicate,
			column,
			user_column_count,
		)?;
	}
	Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RequiredColumnAxis {
	Predicate,
	Output,
	Aggregate,
}

fn insert_required_column(
	requirement: &mut ColumnFetchRequirement,
	axis: RequiredColumnAxis,
	column: u32,
	user_column_count: usize,
) -> Result<(), ColumnFetchError> {
	match classify_requirement_column(column, user_column_count)? {
		RequirementColumn::User(ordinal) => match axis {
			RequiredColumnAxis::Predicate => {
				requirement.predicate_columns.insert(ordinal);
			}
			RequiredColumnAxis::Output => {
				requirement.output_columns.insert(ordinal);
			}
			RequiredColumnAxis::Aggregate => {
				requirement.aggregate_columns.insert(ordinal);
			}
		},
		RequirementColumn::Storage(storage_column) => {
			if axis == RequiredColumnAxis::Output {
				return Err(ColumnFetchError::InvalidRange(format!(
					"storage column {storage_column:?} cannot appear in PBC PROJECT output"
				)));
			}
			requirement.storage_columns.insert(storage_column);
		}
	}
	Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RequirementColumn {
	User(u32),
	Storage(StorageColumn),
}

fn classify_requirement_column(
	column: u32,
	user_column_count: usize,
) -> Result<RequirementColumn, ColumnFetchError> {
	let user_column_count = u32::try_from(user_column_count).map_err(|_| {
		ColumnFetchError::InvalidRange(format!(
			"user column count {user_column_count} exceeds u32::MAX"
		))
	})?;
	if column < user_column_count {
		return Ok(RequirementColumn::User(column));
	}
	let storage_offset = column.saturating_sub(user_column_count);
	let storage_column = match storage_offset {
		0 => StorageColumn::RowHlc,
		1 => StorageColumn::RowKeyId,
		2 => StorageColumn::RowRoutingLocality,
		3 => StorageColumn::RowTombstone,
		_ => {
			return Err(ColumnFetchError::InvalidRange(format!(
				"column ordinal {column} out of range for {user_column_count} user columns"
			)));
		}
	};
	Ok(RequirementColumn::Storage(storage_column))
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum L2FetchStrategy {
	WholeRecord,
	Columnar(ColumnarRecordFetchPlan),
	Fallback(ColumnarFallbackReason),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ColumnarFallbackReason {
	Disabled,
	SingleRow,
	FullProjection,
	SmallRecord,
	TooManyBytes,
	TooManyRanges,
	MetadataProbeCap,
	UnsupportedArrowLayout,
	RehydrationAvailable,
}

impl ColumnarFallbackReason {
	pub fn as_str(self) -> &'static str {
		match self {
			Self::Disabled => "disabled",
			Self::SingleRow => "single_row",
			Self::FullProjection => "full_projection",
			Self::SmallRecord => "small_record",
			Self::TooManyBytes => "too_many_bytes",
			Self::TooManyRanges => "too_many_ranges",
			Self::MetadataProbeCap => "metadata_probe_cap",
			Self::UnsupportedArrowLayout => "unsupported_arrow_layout",
			Self::RehydrationAvailable => "rehydration_available",
		}
	}
}

impl L2FetchStrategy {
	pub fn fallback_reason(&self) -> Option<ColumnarFallbackReason> {
		match self {
			Self::Fallback(reason) => Some(*reason),
			Self::WholeRecord | Self::Columnar(_) => None,
		}
	}

	pub fn selected_chunk_runs(&self) -> Option<u64> {
		match self {
			Self::Columnar(plan) => Some(plan.chunk_runs.len() as u64),
			Self::WholeRecord | Self::Fallback(_) => None,
		}
	}
}

pub fn observe_l2_fetch_strategy(
	metrics: &MetricsRegistry,
	strategy: &L2FetchStrategy,
	planning_duration: Duration,
) {
	metrics.observe_storage_l2_column_fetch_plan(planning_duration);
	if let Some(reason) = strategy.fallback_reason() {
		metrics.observe_storage_l2_column_fetch_fallback(reason.as_str());
	}
	if let Some(chunk_runs) = strategy.selected_chunk_runs() {
		metrics.observe_storage_l2_column_fetch_selected_chunk_runs(chunk_runs);
	}
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ColumnarRecordFetchPlan {
	pub chunk_runs: Vec<ColumnarChunkRun>,
	pub selected_plaintext_ranges: Vec<PlaintextByteRange>,
	pub fetched_ciphertext_bytes: u64,
	pub avoided_record_body_bytes: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ColumnarChunkRun {
	pub first_chunk: crate::schema::EncryptedChunkIndex,
	pub chunk_count: EncryptedChunkCount,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ProjectedRecordBufferSet {
	buffers: BTreeMap<u32, Vec<u8>>,
}

impl ProjectedRecordBufferSet {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn insert(&mut self, buffer_ordinal: IpcBufferIndex, bytes: Vec<u8>) -> Option<Vec<u8>> {
		self.buffers.insert(buffer_ordinal.get(), bytes)
	}

	pub fn get(&self, buffer_ordinal: IpcBufferIndex) -> Option<&[u8]> {
		self.buffers.get(&buffer_ordinal.get()).map(Vec::as_slice)
	}

	pub fn len(&self) -> usize {
		self.buffers.len()
	}

	pub fn is_empty(&self) -> bool {
		self.buffers.is_empty()
	}
}

#[derive(Clone, Debug)]
pub struct ProjectedRecordBatch {
	pub batch: RecordBatch,
	pub ordinal_mapping: ColumnOrdinalMapping,
	pub storage_mapping: BTreeMap<StorageColumn, usize>,
}

impl ProjectedRecordBatch {
	pub fn physical_for(&self, descriptor_ordinal: u32) -> Option<usize> {
		self.ordinal_mapping.physical_for(descriptor_ordinal)
	}

	pub fn storage_physical_for(&self, storage_column: StorageColumn) -> Option<usize> {
		self.storage_mapping.get(&storage_column).copied()
	}

	pub fn user_visible(&self) -> Result<RecordBatch, ArrowError> {
		let indices = self
			.ordinal_mapping
			.descriptor_to_physical
			.iter()
			.filter_map(|physical| *physical)
			.collect::<Vec<_>>();
		self.batch.project(&indices)
	}

	pub fn slice(&self, offset: usize, length: usize) -> Self {
		Self {
			batch: self.batch.slice(offset, length),
			ordinal_mapping: self.ordinal_mapping.clone(),
			storage_mapping: self.storage_mapping.clone(),
		}
	}
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ColumnOrdinalMapping {
	descriptor_to_physical: Vec<Option<usize>>,
}

impl ColumnOrdinalMapping {
	pub fn new(descriptor_to_physical: Vec<Option<usize>>) -> Self {
		Self {
			descriptor_to_physical,
		}
	}

	pub fn physical_for(&self, descriptor_ordinal: u32) -> Option<usize> {
		let index = usize::try_from(descriptor_ordinal).ok()?;
		self.descriptor_to_physical.get(index).copied().flatten()
	}
}

#[derive(Debug, thiserror::Error)]
pub enum ColumnFetchError {
	#[error("columnar metadata truncated: needed {needed} bytes, have {have}")]
	MetadataTruncated { needed: u64, have: u64 },
	#[error("columnar flatbuffer invalid: {0}")]
	FlatBufferInvalid(String),
	#[error("columnar schema mismatch for record {record_id:?}")]
	SchemaMismatch { record_id: TieredRecordId },
	#[error("columnar range invalid: {0}")]
	InvalidRange(String),
	#[error("unsupported arrow layout for columnar materialization")]
	UnsupportedArrowLayout,
}

pub fn parse_record_buffer_directory(
	record_id: TieredRecordId,
	record_plaintext_range: PlaintextByteRange,
	metadata_window_bytes: &[u8],
	descriptor: &TableSchemaDescriptorV1,
) -> Result<RecordBufferDirectory, ColumnFetchError> {
	let mut offset = 0_usize;
	let schema_message = read_ipc_message_metadata(metadata_window_bytes, &mut offset)?;
	if schema_message.header_type != MessageHeader::Schema {
		return Err(ColumnFetchError::FlatBufferInvalid(
			"arrow IPC stream did not start with a schema message".to_string(),
		));
	}
	let ipc_schema = fb_to_schema(
		schema_message
			.message
			.header_as_schema()
			.ok_or_else(|| ColumnFetchError::FlatBufferInvalid("missing schema header".into()))?,
	);
	let descriptor_schema = descriptor.arrow_schema().map_err(|err| {
		ColumnFetchError::FlatBufferInvalid(format!("descriptor schema decode failed: {err}"))
	})?;
	validate_ipc_schema_matches_descriptor(record_id, &ipc_schema, &descriptor_schema)?;

	let batch_message = read_ipc_message_metadata(metadata_window_bytes, &mut offset)?;
	if batch_message.header_type != MessageHeader::RecordBatch {
		return Err(ColumnFetchError::UnsupportedArrowLayout);
	}
	let batch = batch_message
		.message
		.header_as_record_batch()
		.ok_or_else(|| ColumnFetchError::FlatBufferInvalid("missing record batch header".into()))?;
	let row_count = usize::try_from(batch.length()).map_err(|_| {
		ColumnFetchError::FlatBufferInvalid("negative record batch row count".into())
	})?;
	let record_body_compression = match batch.compression() {
		None => RecordBodyCompressionPolicy::Uncompressed,
		Some(compression) => {
			if compression.method() != BodyCompressionMethod::BUFFER {
				return Err(ColumnFetchError::UnsupportedArrowLayout);
			}
			match compression.codec() {
				CompressionType::LZ4_FRAME => RecordBodyCompressionPolicy::Lz4Frame,
				_ => return Err(ColumnFetchError::UnsupportedArrowLayout),
			}
		}
	};
	let flat_nodes = batch.nodes().ok_or_else(|| {
		ColumnFetchError::FlatBufferInvalid("record batch missing field nodes".into())
	})?;
	let column_null_counts = flat_nodes
		.iter()
		.map(|node| {
			usize::try_from(node.null_count()).map_err(|_| {
				ColumnFetchError::FlatBufferInvalid("negative record batch null count".into())
			})
		})
		.collect::<Result<Vec<_>, _>>()?;
	if column_null_counts.len() != ipc_schema.fields().len() {
		return Err(ColumnFetchError::UnsupportedArrowLayout);
	}
	let flat_buffers = batch.buffers().ok_or_else(|| {
		ColumnFetchError::FlatBufferInvalid("record batch missing buffers".into())
	})?;
	let mut buffers = Vec::with_capacity(flat_buffers.len());
	let record_len = record_plaintext_range.len;
	for buffer in flat_buffers {
		let body_offset = u64::try_from(buffer.offset()).map_err(|_| {
			ColumnFetchError::FlatBufferInvalid("negative Arrow IPC buffer offset".into())
		})?;
		let length = u64::try_from(buffer.length()).map_err(|_| {
			ColumnFetchError::FlatBufferInvalid("negative Arrow IPC buffer length".into())
		})?;
		let offset_in_record = (batch_message.body_start as u64)
			.checked_add(body_offset)
			.ok_or_else(|| ColumnFetchError::InvalidRange("IPC buffer offset overflow".into()))?;
		let end = offset_in_record
			.checked_add(length)
			.ok_or_else(|| ColumnFetchError::InvalidRange("IPC buffer end overflow".into()))?;
		if end > record_len {
			return Err(ColumnFetchError::InvalidRange(format!(
				"IPC buffer range {offset_in_record}..{end} exceeds record length {record_len}"
			)));
		}
		buffers.push(IpcBufferRange {
			offset_in_record: IpcBufferOffset::try_new(offset_in_record).map_err(|err| {
				ColumnFetchError::InvalidRange(format!("IPC buffer offset rejected: {err}"))
			})?,
			length: IpcBufferLength::try_new(length).map_err(|err| {
				ColumnFetchError::InvalidRange(format!("IPC buffer length rejected: {err}"))
			})?,
		});
	}

	let mut next_buffer = 0_u32;
	let mut column_to_buffers = Vec::with_capacity(ipc_schema.fields().len());
	let mut storage_column_ordinals = BTreeMap::new();
	for (ordinal, field) in ipc_schema.fields().iter().enumerate() {
		let ordinal = u32::try_from(ordinal).map_err(|_| {
			ColumnFetchError::InvalidRange("column ordinal exceeds u32::MAX".into())
		})?;
		if let Some(storage_column) = StorageColumn::from_name(field.name()) {
			storage_column_ordinals.insert(storage_column, ordinal);
		}
		let buffer_count = buffer_count_for_field(field)?;
		column_to_buffers.push(ColumnBufferRange {
			first_buffer_ordinal: IpcBufferIndex::new(next_buffer),
			buffer_count,
		});
		next_buffer = next_buffer
			.checked_add(buffer_count)
			.ok_or_else(|| ColumnFetchError::InvalidRange("IPC buffer ordinal overflow".into()))?;
	}
	if next_buffer as usize != buffers.len() {
		return Err(ColumnFetchError::FlatBufferInvalid(format!(
			"IPC schema expects {next_buffer} buffers but metadata lists {}",
			buffers.len()
		)));
	}

	Ok(RecordBufferDirectory {
		record_id,
		record_plaintext_range,
		row_count,
		record_body_compression,
		ipc_schema,
		buffers,
		column_to_buffers,
		column_null_counts,
		storage_column_ordinals,
		logical_schema_fingerprint: descriptor.logical_schema_fingerprint,
	})
}

pub fn selected_record_body_ranges(
	directory: &RecordBufferDirectory,
	requirement: &ColumnFetchRequirement,
) -> Result<Vec<TieredPlaintextRange>, ColumnFetchError> {
	let buffer_ordinals = selected_buffer_ordinals(directory, requirement)?;
	let mut ranges = Vec::new();
	for buffer_ordinal in buffer_ordinals {
		let buffer = directory
			.buffers
			.get(buffer_ordinal as usize)
			.ok_or_else(|| ColumnFetchError::InvalidRange("buffer ordinal out of range".into()))?;
		if buffer.length.get() == 0 {
			continue;
		}
		ranges.push(PlaintextByteRange {
			start: buffer.offset_in_record.get(),
			len: buffer.length.get(),
		});
	}
	coalesce_plaintext_ranges(&mut ranges)?;
	ranges
		.into_iter()
		.map(|range| {
			Ok(TieredPlaintextRange::new(
				RecordBodyByteOffset::try_new(range.start).map_err(|err| {
					ColumnFetchError::InvalidRange(format!("record body offset rejected: {err}"))
				})?,
				RecordBodyByteLen::try_new(range.len).map_err(|err| {
					ColumnFetchError::InvalidRange(format!("record body length rejected: {err}"))
				})?,
			))
		})
		.collect()
}

fn selected_buffer_ordinals(
	directory: &RecordBufferDirectory,
	requirement: &ColumnFetchRequirement,
) -> Result<BTreeSet<u32>, ColumnFetchError> {
	let mut buffer_ordinals = BTreeSet::new();
	for ordinal in requirement.user_visible().iter() {
		insert_column_buffers(directory, ordinal, &mut buffer_ordinals)?;
	}
	for storage_column in requirement.storage_columns.iter() {
		let ordinal = directory
			.storage_column_ordinals
			.get(&storage_column)
			.copied()
			.ok_or(ColumnFetchError::UnsupportedArrowLayout)?;
		insert_column_buffers(directory, ordinal, &mut buffer_ordinals)?;
	}
	Ok(buffer_ordinals)
}

pub fn build_column_fetch_plan(
	directory: &RecordBufferDirectory,
	requirement: &ColumnFetchRequirement,
	tiered_plan: &TieredMultiRangeReadPlan,
) -> Result<ColumnarRecordFetchPlan, ColumnFetchError> {
	let selected_ranges = selected_record_body_ranges(directory, requirement)?;
	let selected_plaintext_ranges = selected_ranges
		.iter()
		.map(|range| PlaintextByteRange {
			start: range.start.get(),
			len: range.len.get(),
		})
		.collect::<Vec<_>>();
	let fetched_ciphertext_bytes = tiered_plan
		.ciphertext_ranges_in_object()
		.into_iter()
		.map(|range| range.end.saturating_sub(range.start))
		.sum();
	let selected_bytes = selected_plaintext_ranges
		.iter()
		.map(|range| range.len)
		.sum::<u64>();
	Ok(ColumnarRecordFetchPlan {
		chunk_runs: tiered_plan
			.chunk_runs()
			.iter()
			.map(|run| ColumnarChunkRun {
				first_chunk: run.first_chunk(),
				chunk_count: run.chunk_count(),
			})
			.collect(),
		selected_plaintext_ranges,
		fetched_ciphertext_bytes,
		avoided_record_body_bytes: directory
			.record_plaintext_range
			.len
			.saturating_sub(selected_bytes),
	})
}

pub fn pick_l2_fetch_strategy(
	config: &L2ColumnFetchConfig,
	descriptor: &TableSchemaDescriptorV1,
	record_location: &RecordLocation,
	requirement: &ColumnFetchRequirement,
	directory: Option<&RecordBufferDirectory>,
	candidate_plan: Option<ColumnarRecordFetchPlan>,
	rehydration_available: bool,
) -> L2FetchStrategy {
	if !config.enabled {
		return L2FetchStrategy::Fallback(ColumnarFallbackReason::Disabled);
	}
	if matches!(
		descriptor.record_layout_policy,
		RecordLayoutPolicy::SingleRow
	) {
		return L2FetchStrategy::Fallback(ColumnarFallbackReason::SingleRow);
	}
	let Ok(schema) = descriptor.arrow_schema() else {
		return L2FetchStrategy::Fallback(ColumnarFallbackReason::UnsupportedArrowLayout);
	};
	if requirement.is_full(&schema) {
		return L2FetchStrategy::Fallback(ColumnarFallbackReason::FullProjection);
	}
	if u64::from(record_location.length) < config.min_record_bytes.get() {
		return L2FetchStrategy::Fallback(ColumnarFallbackReason::SmallRecord);
	}
	if rehydration_available {
		return L2FetchStrategy::Fallback(ColumnarFallbackReason::RehydrationAvailable);
	}
	if directory.is_none() {
		return L2FetchStrategy::Fallback(ColumnarFallbackReason::MetadataProbeCap);
	}
	let Some(plan) = candidate_plan else {
		return L2FetchStrategy::Fallback(ColumnarFallbackReason::UnsupportedArrowLayout);
	};
	let selected_scaled = plan
		.fetched_ciphertext_bytes
		.saturating_mul(u64::from(L2_COLUMN_FETCH_FRACTION_DENOMINATOR));
	let whole_scaled = u64::from(record_location.length)
		.saturating_mul(u64::from(config.max_selected_fraction_micros));
	if selected_scaled > whole_scaled {
		return L2FetchStrategy::Fallback(ColumnarFallbackReason::TooManyBytes);
	}
	if plan.chunk_runs.len() > config.max_ranges_per_record.get() as usize {
		return L2FetchStrategy::Fallback(ColumnarFallbackReason::TooManyRanges);
	}
	L2FetchStrategy::Columnar(plan)
}

pub fn decode_projected_record_batch(
	ipc_bytes: &[u8],
	directory: &RecordBufferDirectory,
	requirement: &ColumnFetchRequirement,
) -> Result<ProjectedRecordBatch, ArrowError> {
	let buffers = record_buffer_set_from_ipc_bytes(ipc_bytes, directory, requirement)?;
	decode_projected_record_batch_from_buffers(directory, requirement, &buffers)
}

pub fn decode_projected_record_batch_from_plaintext_ranges<B: AsRef<[u8]>>(
	directory: &RecordBufferDirectory,
	requirement: &ColumnFetchRequirement,
	plaintext_ranges: &[TieredPlaintextRange],
	plaintext_extracts: &[B],
) -> Result<ProjectedRecordBatch, ArrowError> {
	let buffers = record_buffer_set_from_plaintext_ranges(
		directory,
		requirement,
		plaintext_ranges,
		plaintext_extracts,
	)?;
	decode_projected_record_batch_from_buffers(directory, requirement, &buffers)
}

pub fn record_buffer_set_from_ipc_bytes(
	ipc_bytes: &[u8],
	directory: &RecordBufferDirectory,
	requirement: &ColumnFetchRequirement,
) -> Result<ProjectedRecordBufferSet, ArrowError> {
	let required_buffers =
		selected_buffer_ordinals(directory, requirement).map_err(column_fetch_arrow_error)?;
	let mut buffers = ProjectedRecordBufferSet::new();
	for buffer_ordinal in required_buffers {
		let buffer_index = IpcBufferIndex::new(buffer_ordinal);
		let buffer = directory
			.buffers
			.get(buffer_ordinal as usize)
			.ok_or_else(|| {
				ArrowError::SchemaError(format!("buffer ordinal {buffer_ordinal} out of range"))
			})?;
		let start = usize::try_from(buffer.offset_in_record.get()).map_err(|_| {
			ArrowError::SchemaError(format!(
				"buffer ordinal {buffer_ordinal} offset exceeds usize::MAX"
			))
		})?;
		let len = usize::try_from(buffer.length.get()).map_err(|_| {
			ArrowError::SchemaError(format!(
				"buffer ordinal {buffer_ordinal} length exceeds usize::MAX"
			))
		})?;
		let end = start.checked_add(len).ok_or_else(|| {
			ArrowError::SchemaError(format!("buffer ordinal {buffer_ordinal} range overflows"))
		})?;
		if end > ipc_bytes.len() {
			return Err(ArrowError::SchemaError(format!(
				"buffer ordinal {buffer_ordinal} range {start}..{end} exceeds IPC record bytes {}",
				ipc_bytes.len()
			)));
		}
		buffers.insert(buffer_index, ipc_bytes[start..end].to_vec());
	}
	Ok(buffers)
}

pub fn record_buffer_set_from_plaintext_ranges<B: AsRef<[u8]>>(
	directory: &RecordBufferDirectory,
	requirement: &ColumnFetchRequirement,
	plaintext_ranges: &[TieredPlaintextRange],
	plaintext_extracts: &[B],
) -> Result<ProjectedRecordBufferSet, ArrowError> {
	if plaintext_ranges.len() != plaintext_extracts.len() {
		return Err(ArrowError::InvalidArgumentError(format!(
			"projected decode expected {} plaintext extracts, got {}",
			plaintext_ranges.len(),
			plaintext_extracts.len()
		)));
	}

	let required_buffers =
		selected_buffer_ordinals(directory, requirement).map_err(column_fetch_arrow_error)?;
	let mut buffers = ProjectedRecordBufferSet::new();
	for buffer_ordinal in required_buffers {
		let buffer_index = IpcBufferIndex::new(buffer_ordinal);
		let buffer = directory
			.buffers
			.get(buffer_ordinal as usize)
			.ok_or_else(|| {
				ArrowError::SchemaError(format!("buffer ordinal {buffer_ordinal} out of range"))
			})?;
		if buffer.length.get() == 0 {
			buffers.insert(buffer_index, Vec::new());
			continue;
		}
		let start = buffer.offset_in_record.get();
		let end = buffer.end().map_err(|err| {
			ArrowError::SchemaError(format!("buffer ordinal {buffer_ordinal} range: {err}"))
		})?;
		let mut matched = None;
		for (range, extract) in plaintext_ranges.iter().zip(plaintext_extracts.iter()) {
			let range_start = range.start.get();
			let range_end = range_start.checked_add(range.len.get()).ok_or_else(|| {
				ArrowError::SchemaError("plaintext extract range overflows".into())
			})?;
			if start >= range_start && end <= range_end {
				let relative_start = usize::try_from(start - range_start).map_err(|_| {
					ArrowError::SchemaError(format!(
						"buffer ordinal {buffer_ordinal} relative start exceeds usize::MAX"
					))
				})?;
				let relative_end = usize::try_from(end - range_start).map_err(|_| {
					ArrowError::SchemaError(format!(
						"buffer ordinal {buffer_ordinal} relative end exceeds usize::MAX"
					))
				})?;
				let extract = extract.as_ref();
				if relative_end > extract.len() {
					return Err(ArrowError::SchemaError(format!(
						"buffer ordinal {buffer_ordinal} range exceeds plaintext extract"
					)));
				}
				matched = Some(extract[relative_start..relative_end].to_vec());
				break;
			}
		}
		let bytes = matched.ok_or_else(|| {
			ArrowError::SchemaError(format!(
				"buffer ordinal {buffer_ordinal} was not covered by plaintext extracts"
			))
		})?;
		buffers.insert(buffer_index, bytes);
	}
	Ok(buffers)
}

pub fn decode_projected_record_batch_from_buffers(
	directory: &RecordBufferDirectory,
	requirement: &ColumnFetchRequirement,
	buffers: &ProjectedRecordBufferSet,
) -> Result<ProjectedRecordBatch, ArrowError> {
	if directory.ipc_schema.fields().len() != directory.column_to_buffers.len() {
		return Err(ArrowError::SchemaError(format!(
			"projected decode directory has {} columns but IPC schema has {}",
			directory.column_to_buffers.len(),
			directory.ipc_schema.fields().len()
		)));
	}

	let schema = &directory.ipc_schema;
	let user_physical_ordinals = schema
		.fields()
		.iter()
		.enumerate()
		.filter_map(|(physical, field)| {
			StorageColumn::from_name(field.name())
				.is_none()
				.then_some(physical)
		})
		.collect::<Vec<_>>();

	let mut descriptor_to_physical = vec![None; user_physical_ordinals.len()];
	let mut fields = Vec::new();
	let mut arrays = Vec::new();
	for descriptor_ordinal in requirement.user_visible().iter() {
		let descriptor_index = usize::try_from(descriptor_ordinal).map_err(|_| {
			ArrowError::SchemaError(format!(
				"descriptor column ordinal {descriptor_ordinal} exceeds usize::MAX"
			))
		})?;
		let physical = *user_physical_ordinals
			.get(descriptor_index)
			.ok_or_else(|| {
				ArrowError::SchemaError(format!(
					"descriptor column ordinal {descriptor_ordinal} out of range"
				))
			})?;
		let field = schema.field(physical);
		validate_projected_field(directory, field, physical)?;
		descriptor_to_physical[descriptor_index] = Some(arrays.len());
		fields.push(field.clone());
		arrays.push(materialize_projected_array(
			directory, field, physical, buffers,
		)?);
	}

	let mut storage_mapping = BTreeMap::new();
	for storage_column in requirement.storage_columns.iter() {
		let physical = directory
			.storage_column_ordinals
			.get(&storage_column)
			.copied()
			.ok_or_else(|| {
				ArrowError::SchemaError(format!(
					"storage column {storage_column:?} missing from record directory"
				))
			})?;
		let physical = usize::try_from(physical).map_err(|_| {
			ArrowError::SchemaError(format!(
				"storage column {storage_column:?} ordinal exceeds usize::MAX"
			))
		})?;
		let field = schema.field(physical);
		if StorageColumn::from_name(field.name()) != Some(storage_column) {
			return Err(ArrowError::SchemaError(format!(
				"storage column {storage_column:?} mapped to incompatible field {}",
				field.name()
			)));
		}
		validate_projected_field(directory, field, physical)?;
		storage_mapping.insert(storage_column, arrays.len());
		fields.push(field.clone());
		arrays.push(materialize_projected_array(
			directory, field, physical, buffers,
		)?);
	}

	let options = RecordBatchOptions::new().with_row_count(Some(directory.row_count));
	let batch = RecordBatch::try_new_with_options(Arc::new(Schema::new(fields)), arrays, &options)?;
	Ok(ProjectedRecordBatch {
		batch,
		ordinal_mapping: ColumnOrdinalMapping::new(descriptor_to_physical),
		storage_mapping,
	})
}

fn materialize_projected_array(
	directory: &RecordBufferDirectory,
	field: &Field,
	physical_ordinal: usize,
	buffers: &ProjectedRecordBufferSet,
) -> Result<ArrayRef, ArrowError> {
	if matches!(field.data_type(), DataType::Null) {
		return Ok(Arc::new(NullArray::new(directory.row_count)) as ArrayRef);
	}
	let body_buffers = materialize_projected_array_buffers(directory, physical_ordinal, buffers)?;
	let null_count = *directory
		.column_null_counts
		.get(physical_ordinal)
		.ok_or_else(|| {
			ArrowError::SchemaError(format!(
				"directory missing null count for physical ordinal {physical_ordinal}"
			))
		})?;
	let null_buffer = (null_count > 0).then_some(body_buffers[0].clone());
	let mut builder = match field.data_type() {
		DataType::Utf8 | DataType::Binary | DataType::LargeBinary | DataType::LargeUtf8 => {
			if body_buffers.len() != 3 {
				return Err(ArrowError::SchemaError(format!(
					"variable-width column {} expected 3 IPC buffers, got {}",
					field.name(),
					body_buffers.len()
				)));
			}
			ArrayData::builder(field.data_type().clone())
				.len(directory.row_count)
				.buffers(body_buffers[1..3].to_vec())
				.null_bit_buffer(null_buffer)
		}
		data_type
			if data_type.is_primitive()
				|| matches!(data_type, DataType::Boolean | DataType::FixedSizeBinary(_)) =>
		{
			if body_buffers.len() != 2 {
				return Err(ArrowError::SchemaError(format!(
					"fixed-width column {} expected 2 IPC buffers, got {}",
					field.name(),
					body_buffers.len()
				)));
			}
			ArrayData::builder(field.data_type().clone())
				.len(directory.row_count)
				.add_buffer(body_buffers[1].clone())
				.null_bit_buffer(null_buffer)
		}
		_ => {
			return Err(ArrowError::SchemaError(format!(
				"unsupported Arrow layout for projected column {}",
				field.name()
			)));
		}
	};
	builder = builder.null_count(null_count).align_buffers(true);
	Ok(make_array(builder.build()?))
}

fn materialize_projected_array_buffers(
	directory: &RecordBufferDirectory,
	physical_ordinal: usize,
	buffers: &ProjectedRecordBufferSet,
) -> Result<Vec<Buffer>, ArrowError> {
	let mapping = directory
		.column_to_buffers
		.get(physical_ordinal)
		.ok_or_else(|| {
			ArrowError::SchemaError(format!(
				"directory missing column buffer mapping for physical ordinal {physical_ordinal}"
			))
		})?;
	let first = mapping.first_buffer_ordinal.get();
	let end = first
		.checked_add(mapping.buffer_count)
		.ok_or_else(|| ArrowError::SchemaError("column buffer ordinal range overflows".into()))?;
	let mut out = Vec::with_capacity(mapping.buffer_count as usize);
	for buffer_ordinal in first..end {
		out.push(materialize_ipc_body_buffer(
			directory,
			IpcBufferIndex::new(buffer_ordinal),
			buffers,
		)?);
	}
	Ok(out)
}

fn materialize_ipc_body_buffer(
	directory: &RecordBufferDirectory,
	buffer_ordinal: IpcBufferIndex,
	buffers: &ProjectedRecordBufferSet,
) -> Result<Buffer, ArrowError> {
	let ipc_range = directory
		.buffers
		.get(buffer_ordinal.get() as usize)
		.ok_or_else(|| {
			ArrowError::SchemaError(format!(
				"buffer ordinal {} out of range",
				buffer_ordinal.get()
			))
		})?;
	if ipc_range.length.get() == 0 {
		return Ok(Buffer::from_vec(Vec::<u8>::new()));
	}
	let bytes = buffers.get(buffer_ordinal).ok_or_else(|| {
		ArrowError::SchemaError(format!(
			"buffer ordinal {} was not fetched into projected buffer set",
			buffer_ordinal.get()
		))
	})?;
	let body_bytes = decode_ipc_body_buffer(bytes, directory.record_body_compression)?;
	Ok(Buffer::from_vec(body_bytes))
}

fn decode_ipc_body_buffer(
	bytes: &[u8],
	policy: RecordBodyCompressionPolicy,
) -> Result<Vec<u8>, ArrowError> {
	match policy {
		RecordBodyCompressionPolicy::Uncompressed => Ok(bytes.to_vec()),
		RecordBodyCompressionPolicy::Lz4Frame => decode_lz4_ipc_body_buffer(bytes),
	}
}

fn decode_lz4_ipc_body_buffer(bytes: &[u8]) -> Result<Vec<u8>, ArrowError> {
	if bytes.is_empty() {
		return Ok(Vec::new());
	}
	if bytes.len() < 8 {
		return Err(ArrowError::IpcError(
			"compressed IPC buffer missing uncompressed-length prefix".into(),
		));
	}
	let uncompressed_len = i64::from_le_bytes(bytes[..8].try_into().expect("fixed width"));
	if uncompressed_len == 0 {
		return Ok(Vec::new());
	}
	if uncompressed_len == -1 {
		return Ok(bytes[8..].to_vec());
	}
	if uncompressed_len < 0 {
		return Err(ArrowError::IpcError(format!(
			"invalid compressed IPC buffer length prefix {uncompressed_len}"
		)));
	}
	let expected_len = usize::try_from(uncompressed_len).map_err(|_| {
		ArrowError::IpcError(format!(
			"compressed IPC buffer length prefix {uncompressed_len} exceeds usize::MAX"
		))
	})?;
	let mut out = Vec::with_capacity(expected_len);
	lz4_flex::frame::FrameDecoder::new(&bytes[8..])
		.read_to_end(&mut out)
		.map_err(|err| ArrowError::IpcError(format!("lz4 IPC buffer decompress: {err}")))?;
	if out.len() != expected_len {
		return Err(ArrowError::IpcError(format!(
			"lz4 IPC buffer decoded {} bytes, expected {expected_len}",
			out.len()
		)));
	}
	Ok(out)
}

pub fn execute_pbc_projected_record_batch(
	projected: &ProjectedRecordBatch,
	program: &Program,
	requirement: &ColumnFetchRequirement,
	user_column_count: usize,
) -> std::io::Result<RecordBatch> {
	execute_projected_record_batch_with_outputs(
		projected,
		program,
		&requirement.output_columns,
		user_column_count,
	)
}

pub fn execute_aggregate_projected_record_batch(
	projected: &ProjectedRecordBatch,
	program: &Program,
	requirement: &ColumnFetchRequirement,
	user_column_count: usize,
) -> std::io::Result<RecordBatch> {
	execute_projected_record_batch_with_outputs(
		projected,
		program,
		&requirement.aggregate_columns,
		user_column_count,
	)
}

fn execute_projected_record_batch_with_outputs(
	projected: &ProjectedRecordBatch,
	program: &Program,
	output_columns: &ColumnOrdinalSet,
	user_column_count: usize,
) -> std::io::Result<RecordBatch> {
	let output_columns = output_columns.iter().collect::<Vec<_>>();
	let remapped = program.remap_columns_with_project(
		|column| map_projected_pbc_column(projected, column, user_column_count),
		Some(&output_columns),
	)?;
	pbc::execute_program(&remapped, &projected.batch)
}

fn map_projected_pbc_column(
	projected: &ProjectedRecordBatch,
	column: u32,
	user_column_count: usize,
) -> std::io::Result<u32> {
	let physical = match classify_requirement_column(column, user_column_count)
		.map_err(column_fetch_io_error)?
	{
		RequirementColumn::User(ordinal) => projected.physical_for(ordinal).ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				format!("PBC column {ordinal} was not fetched into projected batch"),
			)
		})?,
		RequirementColumn::Storage(storage_column) => projected
			.storage_physical_for(storage_column)
			.ok_or_else(|| {
				Error::new(
					ErrorKind::InvalidData,
					format!("PBC storage column {storage_column:?} was not fetched"),
				)
			})?,
	};
	u32::try_from(physical).map_err(|_| {
		Error::new(
			ErrorKind::InvalidData,
			format!("projected physical column {physical} exceeds u32::MAX"),
		)
	})
}

fn column_fetch_io_error(error: ColumnFetchError) -> Error {
	Error::new(ErrorKind::InvalidData, error.to_string())
}

fn column_fetch_arrow_error(error: ColumnFetchError) -> ArrowError {
	ArrowError::SchemaError(error.to_string())
}

fn validate_projected_field(
	directory: &RecordBufferDirectory,
	field: &Field,
	physical_ordinal: usize,
) -> Result<(), ArrowError> {
	let expected_buffers = buffer_count_for_field(field).map_err(|err| {
		ArrowError::SchemaError(format!(
			"selected column {} has unsupported Arrow layout: {err}",
			field.name()
		))
	})?;
	let directory_buffers = directory
		.column_to_buffers
		.get(physical_ordinal)
		.ok_or_else(|| {
			ArrowError::SchemaError(format!(
				"directory missing column buffer mapping for physical ordinal {physical_ordinal}"
			))
		})?;
	if directory_buffers.buffer_count != expected_buffers {
		return Err(ArrowError::SchemaError(format!(
			"directory buffer count mismatch for column {}: directory={} schema={expected_buffers}",
			field.name(),
			directory_buffers.buffer_count
		)));
	}
	Ok(())
}

struct IpcMessageMetadata<'a> {
	message: arrow::ipc::Message<'a>,
	header_type: MessageHeader,
	body_start: usize,
}

fn read_ipc_message_metadata<'a>(
	bytes: &'a [u8],
	offset: &mut usize,
) -> Result<IpcMessageMetadata<'a>, ColumnFetchError> {
	let prefix_start = *offset;
	if bytes.len().saturating_sub(*offset) < 4 {
		return Err(ColumnFetchError::MetadataTruncated {
			needed: (*offset as u64).saturating_add(4),
			have: bytes.len() as u64,
		});
	}
	let first = u32::from_le_bytes(bytes[*offset..*offset + 4].try_into().expect("fixed width"));
	*offset += 4;
	let metadata_len = if first == u32::MAX {
		if bytes.len().saturating_sub(*offset) < 4 {
			return Err(ColumnFetchError::MetadataTruncated {
				needed: (*offset as u64).saturating_add(4),
				have: bytes.len() as u64,
			});
		}
		let len = u32::from_le_bytes(bytes[*offset..*offset + 4].try_into().expect("fixed width"));
		*offset += 4;
		len as usize
	} else {
		first as usize
	};
	if metadata_len == 0 {
		return Err(ColumnFetchError::FlatBufferInvalid(
			"arrow IPC stream ended before record batch metadata".to_string(),
		));
	}
	let metadata_start = *offset;
	let metadata_end = metadata_start.checked_add(metadata_len).ok_or_else(|| {
		ColumnFetchError::InvalidRange("arrow IPC metadata length overflow".into())
	})?;
	if metadata_end > bytes.len() {
		return Err(ColumnFetchError::MetadataTruncated {
			needed: metadata_end as u64,
			have: bytes.len() as u64,
		});
	}
	let message = root_as_message(&bytes[metadata_start..metadata_end])
		.map_err(|err| ColumnFetchError::FlatBufferInvalid(format!("{err}")))?;
	let body_len = usize::try_from(message.bodyLength())
		.map_err(|_| ColumnFetchError::FlatBufferInvalid("negative body length".into()))?;
	let body_start = metadata_end;
	*offset = body_start
		.checked_add(body_len)
		.ok_or_else(|| ColumnFetchError::InvalidRange("arrow IPC body length overflow".into()))?;
	if *offset < prefix_start {
		return Err(ColumnFetchError::InvalidRange(
			"arrow IPC message offset moved backwards".into(),
		));
	}
	Ok(IpcMessageMetadata {
		header_type: message.header_type(),
		message,
		body_start,
	})
}

fn validate_ipc_schema_matches_descriptor(
	record_id: TieredRecordId,
	ipc_schema: &Schema,
	descriptor_schema: &Schema,
) -> Result<(), ColumnFetchError> {
	let user_fields = ipc_schema
		.fields()
		.iter()
		.filter(|field| StorageColumn::from_name(field.name()).is_none())
		.cloned()
		.collect::<Vec<_>>();
	let expected = descriptor_schema
		.fields()
		.iter()
		.cloned()
		.collect::<Vec<_>>();
	if user_fields != expected {
		return Err(ColumnFetchError::SchemaMismatch { record_id });
	}
	Ok(())
}

fn buffer_count_for_field(field: &Field) -> Result<u32, ColumnFetchError> {
	buffer_count_for_data_type(field.data_type())
}

fn buffer_count_for_data_type(data_type: &DataType) -> Result<u32, ColumnFetchError> {
	let count = match data_type {
		DataType::Null => 0,
		DataType::Utf8 | DataType::Binary | DataType::LargeBinary | DataType::LargeUtf8 => 3,
		DataType::Boolean | DataType::FixedSizeBinary(_) => 2,
		data_type if data_type.is_primitive() => 2,
		_ => return Err(ColumnFetchError::UnsupportedArrowLayout),
	};
	Ok(count)
}

fn insert_column_buffers(
	directory: &RecordBufferDirectory,
	column_ordinal: u32,
	buffer_ordinals: &mut BTreeSet<u32>,
) -> Result<(), ColumnFetchError> {
	let mapping = directory
		.column_to_buffers
		.get(column_ordinal as usize)
		.ok_or_else(|| ColumnFetchError::InvalidRange("column ordinal out of range".into()))?;
	let first = mapping.first_buffer_ordinal.get();
	let end = first.checked_add(mapping.buffer_count).ok_or_else(|| {
		ColumnFetchError::InvalidRange("column buffer ordinal range overflow".into())
	})?;
	for ordinal in first..end {
		buffer_ordinals.insert(ordinal);
	}
	Ok(())
}

fn coalesce_plaintext_ranges(ranges: &mut Vec<PlaintextByteRange>) -> Result<(), ColumnFetchError> {
	ranges.sort_by_key(|range| range.start);
	let mut out: Vec<PlaintextByteRange> = Vec::with_capacity(ranges.len());
	for range in ranges.drain(..) {
		let range_end = range
			.start
			.checked_add(range.len)
			.ok_or_else(|| ColumnFetchError::InvalidRange("plaintext range overflow".into()))?;
		if let Some(last) = out.last_mut() {
			let last_end = last
				.start
				.checked_add(last.len)
				.ok_or_else(|| ColumnFetchError::InvalidRange("plaintext range overflow".into()))?;
			if range.start <= last_end {
				last.len = range_end.saturating_sub(last.start).max(last.len);
				continue;
			}
		}
		out.push(range);
	}
	*ranges = out;
	Ok(())
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::crypto::{CryptoManager, TieredOffsetIndexEntry, TieredSlabObjectMetadata};
	use crate::schema::pbc::{OP_GTE, OP_PROJECT, OP_PUSH_COL, OP_PUSH_INT, Program};
	use crate::schema::{
		AggregateExpression, AggregateFunctionTag, EncryptedChunkIndex, HybridTimestamp,
		IdentityPrefix, LocalityPolicyV1, MultiRowLayoutDescriptor, PrimaryKeyDescriptorV1,
		RecordBodyCompressionPolicy, RecordLayoutPolicy, RecordRowIndex, RowIdentityPolicy,
		SchemaEvolutionPolicyV1, TableObjectId, TenantId,
	};
	use crate::storage::ipc::encode_record_batch_bytes;
	use arrow::array::{ArrayRef, FixedSizeBinaryBuilder, Int64Array, StringArray, UInt64Array};
	use arrow::datatypes::{DataType, Field};
	use std::sync::Arc;

	fn descriptor_for(schema: &Schema) -> TableSchemaDescriptorV1 {
		TableSchemaDescriptorV1::new(
			TableObjectId::new([0x44; 16]),
			1,
			"test",
			"columnar",
			schema,
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("id")),
			LocalityPolicyV1::IdentityProjection,
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.unwrap()
	}

	fn multirow_descriptor_for(schema: &Schema) -> TableSchemaDescriptorV1 {
		TableSchemaDescriptorV1::new_with_layout(
			TableObjectId::new([0x45; 16]),
			1,
			"test",
			"columnar",
			schema,
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("id")),
			LocalityPolicyV1::IdentityProjection,
			RecordLayoutPolicy::MultiRow(
				MultiRowLayoutDescriptor::new(2, 64, 8 * 1024 * 1024).unwrap(),
			),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.unwrap()
	}

	fn test_record_id() -> TieredRecordId {
		TieredRecordId {
			slab_id: SlabId::new(44),
			record_offset: RecordBodyByteOffset::new(0),
		}
	}

	fn record_location(length: u32) -> RecordLocation {
		RecordLocation {
			slab_id: SlabId::new(44),
			offset: 0,
			length,
			hlc: HybridTimestamp {
				physical_ms: 0,
				logical: 0,
				node_id: 0,
			},
			record_row_index: RecordRowIndex::new(0),
			is_tombstone: false,
		}
	}

	fn candidate_plan(fetched_ciphertext_bytes: u64, chunk_runs: usize) -> ColumnarRecordFetchPlan {
		ColumnarRecordFetchPlan {
			chunk_runs: (0..chunk_runs)
				.map(|index| ColumnarChunkRun {
					first_chunk: EncryptedChunkIndex::new(index as u64),
					chunk_count: EncryptedChunkCount::new(1),
				})
				.collect(),
			selected_plaintext_ranges: vec![PlaintextByteRange { start: 0, len: 1 }],
			fetched_ciphertext_bytes,
			avoided_record_body_bytes: 1,
		}
	}

	fn parse_directory_for_batch(
		batch: &RecordBatch,
		descriptor: &TableSchemaDescriptorV1,
	) -> (Vec<u8>, RecordBufferDirectory) {
		parse_directory_for_batch_with_policy(
			batch,
			descriptor,
			RecordBodyCompressionPolicy::Uncompressed,
		)
	}

	fn parse_directory_for_batch_with_policy(
		batch: &RecordBatch,
		descriptor: &TableSchemaDescriptorV1,
		policy: RecordBodyCompressionPolicy,
	) -> (Vec<u8>, RecordBufferDirectory) {
		let bytes = encode_record_batch_bytes(batch, policy).expect("encode test IPC");
		let directory = parse_record_buffer_directory(
			test_record_id(),
			PlaintextByteRange {
				start: 0,
				len: bytes.len() as u64,
			},
			&bytes,
			descriptor,
		)
		.expect("parse IPC buffer directory");
		(bytes, directory)
	}

	fn pbc_push_col(bytecode: &mut Vec<u8>, column: u32) {
		bytecode.push(OP_PUSH_COL);
		bytecode.extend_from_slice(&column.to_le_bytes());
	}

	fn pbc_push_int(bytecode: &mut Vec<u8>, value: i64) {
		bytecode.push(OP_PUSH_INT);
		bytecode.extend_from_slice(&value.to_le_bytes());
	}

	fn pbc_project(bytecode: &mut Vec<u8>, columns: &[u32]) {
		bytecode.push(OP_PROJECT);
		bytecode.extend_from_slice(&(columns.len() as u16).to_le_bytes());
		for column in columns {
			bytecode.extend_from_slice(&column.to_le_bytes());
		}
	}

	fn compile_pbc(bytecode: Vec<u8>) -> Program {
		Program::compile(&bytecode).expect("compile test PBC")
	}

	fn test_metric(segment: &str) -> String {
		format!("{}{}", concat!("wendelo", "n_"), segment)
	}

	#[test]
	fn l2_column_fetch_config_validates_bounds() {
		L2ColumnFetchConfig::default().validate().unwrap();

		let mut cfg = L2ColumnFetchConfig {
			max_ipc_metadata_probe_bytes: RecordBodyByteLen::new(0),
			..L2ColumnFetchConfig::default()
		};
		assert!(cfg.validate().is_err());

		cfg = L2ColumnFetchConfig {
			ipc_metadata_initial_window_bytes: RecordBodyByteLen::new(512),
			max_ipc_metadata_probe_bytes: RecordBodyByteLen::new(256),
			..L2ColumnFetchConfig::default()
		};
		assert!(cfg.validate().is_err());

		cfg = L2ColumnFetchConfig {
			max_selected_fraction_micros: L2_COLUMN_FETCH_FRACTION_DENOMINATOR,
			..L2ColumnFetchConfig::default()
		};
		assert!(cfg.validate().is_err());

		cfg = L2ColumnFetchConfig {
			max_ranges_per_record: EncryptedChunkCount::new(0),
			..L2ColumnFetchConfig::default()
		};
		assert!(cfg.validate().is_err());
	}

	#[test]
	fn metadata_probe_plan_uses_initial_window_and_record_cap() {
		let cfg = L2ColumnFetchConfig {
			ipc_metadata_initial_window_bytes: RecordBodyByteLen::new(64),
			max_ipc_metadata_probe_bytes: RecordBodyByteLen::new(256),
			..L2ColumnFetchConfig::default()
		};

		let probe = initial_metadata_probe_plan(&cfg, &record_location(1_000)).unwrap();
		assert_eq!(probe.window_bytes.get(), 64);
		assert_eq!(probe.record_plaintext_range.start, 0);
		assert_eq!(probe.record_plaintext_range.len, 1_000);
		assert_eq!(probe.record_relative_range.start.get(), 0);
		assert_eq!(probe.record_relative_range.len.get(), 64);

		let small = initial_metadata_probe_plan(&cfg, &record_location(32)).unwrap();
		assert_eq!(small.window_bytes.get(), 32);
		assert_eq!(small.record_relative_range.len.get(), 32);
	}

	#[test]
	fn metadata_probe_plan_grows_until_probe_cap() {
		let cfg = L2ColumnFetchConfig {
			ipc_metadata_initial_window_bytes: RecordBodyByteLen::new(64),
			max_ipc_metadata_probe_bytes: RecordBodyByteLen::new(256),
			..L2ColumnFetchConfig::default()
		};
		let location = record_location(1_000);

		let next =
			next_metadata_probe_plan(&cfg, &location, RecordBodyByteLen::new(64), 96).unwrap();
		assert_eq!(next.unwrap().window_bytes.get(), 128);

		let capped =
			next_metadata_probe_plan(&cfg, &location, RecordBodyByteLen::new(128), 240).unwrap();
		assert_eq!(capped.unwrap().window_bytes.get(), 256);

		assert!(
			next_metadata_probe_plan(&cfg, &location, RecordBodyByteLen::new(128), 300)
				.unwrap()
				.is_none()
		);
		assert!(
			next_metadata_probe_plan(&cfg, &location, RecordBodyByteLen::new(256), 256)
				.unwrap()
				.is_none()
		);
	}

	#[test]
	fn strategy_metric_observer_records_fallbacks_and_chunk_runs() {
		let registry = MetricsRegistry::default();

		observe_l2_fetch_strategy(
			&registry,
			&L2FetchStrategy::Fallback(ColumnarFallbackReason::TooManyRanges),
			Duration::from_micros(9),
		);
		observe_l2_fetch_strategy(
			&registry,
			&L2FetchStrategy::Columnar(candidate_plan(10, 3)),
			Duration::from_micros(11),
		);

		let rendered = registry.render_prometheus();
		assert!(rendered.contains(&format!(
			"{}_sum 20",
			test_metric("storage_l2_column_fetch_plan_micros")
		)));
		assert!(rendered.contains(&format!(
			"{}_sum 3",
			test_metric("storage_l2_column_fetch_selected_chunk_runs")
		)));
		assert!(rendered.contains(&format!(
			"{}{{reason=\"too_many_ranges\"}} 1",
			test_metric("storage_l2_column_fetch_fallback_total")
		)));
	}

	#[test]
	fn column_requirement_full_ignores_storage_columns() {
		let schema = Schema::new(vec![
			Field::new("a", DataType::Int64, false),
			Field::new("b", DataType::Utf8, true),
			Field::new(STORAGE_ROW_HLC_COLUMN, DataType::FixedSizeBinary(16), false),
		]);
		let requirement = ColumnFetchRequirement {
			output_columns: ColumnOrdinalSet::from_columns([0, 1]),
			..ColumnFetchRequirement::default()
		};
		assert!(requirement.is_full(&schema));
		assert_eq!(requirement.user_visible().len(), 2);
	}

	#[test]
	fn user_visible_requirement_excludes_storage_columns() {
		let mut storage_columns = StorageColumnSet::new();
		storage_columns.insert(StorageColumn::RowHlc);
		let requirement = ColumnFetchRequirement {
			predicate_columns: ColumnOrdinalSet::from_columns([2]),
			output_columns: ColumnOrdinalSet::from_columns([0]),
			aggregate_columns: ColumnOrdinalSet::from_columns([1]),
			storage_columns,
		};
		let visible = requirement.user_visible();
		assert!(visible.contains(0));
		assert!(visible.contains(1));
		assert!(visible.contains(2));
		assert!(requirement.storage_columns.contains(StorageColumn::RowHlc));
	}

	#[test]
	fn pbc_requirement_derives_predicates_outputs_and_storage() {
		let mut bytecode = Vec::new();
		pbc_push_col(&mut bytecode, 0);
		pbc_push_col(&mut bytecode, 1);
		pbc_push_col(&mut bytecode, 3);
		pbc_project(&mut bytecode, &[0, 2]);
		let program = compile_pbc(bytecode);

		let requirement = analyze_pbc_column_requirement(&program, 3).unwrap();

		assert!(requirement.predicate_columns.contains(0));
		assert!(requirement.predicate_columns.contains(1));
		assert!(!requirement.predicate_columns.contains(3));
		assert!(requirement.output_columns.contains(0));
		assert!(requirement.output_columns.contains(2));
		assert!(requirement.storage_columns.contains(StorageColumn::RowHlc));
		let visible = requirement.user_visible();
		assert!(visible.contains(0));
		assert!(visible.contains(1));
		assert!(visible.contains(2));
	}

	#[test]
	fn pbc_requirement_without_project_outputs_full_user_schema() {
		let mut bytecode = Vec::new();
		pbc_push_col(&mut bytecode, 1);
		let program = compile_pbc(bytecode);
		let schema = Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("name", DataType::Utf8, true),
		]);

		let requirement = analyze_pbc_column_requirement(&program, 2).unwrap();

		assert!(requirement.predicate_columns.contains(1));
		assert!(requirement.output_columns.contains(0));
		assert!(requirement.output_columns.contains(1));
		assert!(requirement.is_full(&schema));
	}

	#[test]
	fn aggregate_requirement_skips_count_star_and_tracks_inputs() {
		let count_star =
			analyze_aggregate_column_requirement(&[AggregateExpression::count_star()], 3).unwrap();
		assert!(count_star.user_visible().is_empty());

		let requirement = analyze_aggregate_column_requirement(
			&[
				AggregateExpression {
					tag: AggregateFunctionTag::MIN,
					column_ordinal: 1,
					parameter_block: Vec::new(),
				},
				AggregateExpression {
					tag: AggregateFunctionTag::MAX,
					column_ordinal: 2,
					parameter_block: Vec::new(),
				},
			],
			3,
		)
		.unwrap();

		assert!(requirement.aggregate_columns.contains(1));
		assert!(requirement.aggregate_columns.contains(2));
		assert!(!requirement.output_columns.contains(1));
	}

	#[test]
	fn aggregate_query_requirement_uses_pbc_as_predicate_only() {
		let mut bytecode = Vec::new();
		pbc_push_col(&mut bytecode, 1);
		let program = compile_pbc(bytecode);

		let requirement = analyze_aggregate_query_column_requirement(
			&program,
			&[AggregateExpression::count_star()],
			3,
		)
		.unwrap();

		assert!(requirement.predicate_columns.contains(1));
		assert!(requirement.output_columns.is_empty());
		assert!(requirement.aggregate_columns.is_empty());
		assert_eq!(requirement.user_visible().len(), 1);
	}

	#[test]
	fn pbc_requirement_rejects_unknown_column_ordinal() {
		let mut bytecode = Vec::new();
		pbc_push_col(&mut bytecode, 6);
		let program = compile_pbc(bytecode);

		let err = analyze_pbc_column_requirement(&program, 2).unwrap_err();

		assert!(matches!(err, ColumnFetchError::InvalidRange(_)));
	}

	#[test]
	fn pbc_requirement_rejects_storage_columns_in_project_output() {
		let mut bytecode = Vec::new();
		pbc_project(&mut bytecode, &[2]);
		let program = compile_pbc(bytecode);

		let err = analyze_pbc_column_requirement(&program, 2).unwrap_err();

		assert!(format!("{err}").contains("PBC PROJECT output"));
	}

	#[test]
	fn projected_pbc_filter_and_project_matches_whole_record() {
		let schema = Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("name", DataType::Utf8, true),
		]);
		let descriptor = descriptor_for(&schema);
		let batch = RecordBatch::try_new(
			Arc::new(schema),
			vec![
				Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
				Arc::new(StringArray::from(vec![
					Some("alpha"),
					Some("beta"),
					Some("gamma"),
				])) as ArrayRef,
			],
		)
		.unwrap();
		let (bytes, directory) = parse_directory_for_batch(&batch, &descriptor);
		let mut bytecode = Vec::new();
		pbc_push_col(&mut bytecode, 0);
		pbc_push_int(&mut bytecode, 2);
		bytecode.push(OP_GTE);
		pbc_project(&mut bytecode, &[1]);
		let program = compile_pbc(bytecode);
		let requirement = analyze_pbc_column_requirement(&program, 2).unwrap();
		let projected = decode_projected_record_batch(&bytes, &directory, &requirement).unwrap();

		let projected_out =
			execute_pbc_projected_record_batch(&projected, &program, &requirement, 2).unwrap();
		let whole_out = pbc::execute_program(&program, &batch).unwrap();

		assert_eq!(projected_out, whole_out);
	}

	#[test]
	fn projected_pbc_terminal_project_uses_descriptor_order() {
		let schema = Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("name", DataType::Utf8, true),
		]);
		let descriptor = descriptor_for(&schema);
		let batch = RecordBatch::try_new(
			Arc::new(schema),
			vec![
				Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
				Arc::new(StringArray::from(vec![Some("alpha"), Some("beta")])) as ArrayRef,
			],
		)
		.unwrap();
		let (bytes, directory) = parse_directory_for_batch(&batch, &descriptor);
		let mut bytecode = Vec::new();
		pbc_project(&mut bytecode, &[1, 0]);
		let program = compile_pbc(bytecode);
		let requirement = analyze_pbc_column_requirement(&program, 2).unwrap();
		let projected = decode_projected_record_batch(&bytes, &directory, &requirement).unwrap();

		let out =
			execute_pbc_projected_record_batch(&projected, &program, &requirement, 2).unwrap();

		assert_eq!(out.schema().field(0).name(), "id");
		assert_eq!(out.schema().field(1).name(), "name");
	}

	#[test]
	fn projected_pbc_strips_storage_columns_when_project_is_absent() {
		let user_schema = Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("name", DataType::Utf8, true),
		]);
		let descriptor = descriptor_for(&user_schema);
		let physical_schema = Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("name", DataType::Utf8, true),
			Field::new(STORAGE_ROW_HLC_COLUMN, DataType::FixedSizeBinary(16), false),
		]);
		let mut hlc = FixedSizeBinaryBuilder::with_capacity(2, 16);
		hlc.append_value([1_u8; 16]).unwrap();
		hlc.append_value([2_u8; 16]).unwrap();
		let batch = RecordBatch::try_new(
			Arc::new(physical_schema),
			vec![
				Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
				Arc::new(StringArray::from(vec![Some("alpha"), Some("beta")])) as ArrayRef,
				Arc::new(hlc.finish()) as ArrayRef,
			],
		)
		.unwrap();
		let (bytes, directory) = parse_directory_for_batch(&batch, &descriptor);
		let program = compile_pbc(Vec::new());
		let mut requirement = analyze_pbc_column_requirement(&program, 2).unwrap();
		requirement.storage_columns.insert(StorageColumn::RowHlc);
		let projected = decode_projected_record_batch(&bytes, &directory, &requirement).unwrap();

		let out =
			execute_pbc_projected_record_batch(&projected, &program, &requirement, 2).unwrap();

		assert_eq!(out.num_columns(), 2);
		assert_eq!(out.schema().field(0).name(), "id");
		assert_eq!(out.schema().field(1).name(), "name");
	}

	#[test]
	fn projected_aggregate_filter_returns_accumulator_inputs() {
		let schema = Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("name", DataType::Utf8, true),
		]);
		let descriptor = descriptor_for(&schema);
		let batch = RecordBatch::try_new(
			Arc::new(schema),
			vec![
				Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
				Arc::new(StringArray::from(vec![
					Some("alpha"),
					Some("beta"),
					Some("gamma"),
				])) as ArrayRef,
			],
		)
		.unwrap();
		let (bytes, directory) = parse_directory_for_batch(&batch, &descriptor);
		let mut bytecode = Vec::new();
		pbc_push_col(&mut bytecode, 0);
		pbc_push_int(&mut bytecode, 2);
		bytecode.push(OP_GTE);
		let program = compile_pbc(bytecode);
		let requirement = analyze_aggregate_query_column_requirement(
			&program,
			&[AggregateExpression {
				tag: AggregateFunctionTag::MIN,
				column_ordinal: 1,
				parameter_block: Vec::new(),
			}],
			2,
		)
		.unwrap();
		let projected = decode_projected_record_batch(&bytes, &directory, &requirement).unwrap();

		let out = execute_aggregate_projected_record_batch(&projected, &program, &requirement, 2)
			.unwrap();

		assert_eq!(out.num_rows(), 2);
		assert_eq!(out.num_columns(), 1);
		assert_eq!(out.schema().field(0).name(), "name");
	}

	#[test]
	fn projected_aggregate_count_star_preserves_filtered_row_count() {
		let schema = Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("name", DataType::Utf8, true),
		]);
		let descriptor = descriptor_for(&schema);
		let batch = RecordBatch::try_new(
			Arc::new(schema),
			vec![
				Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
				Arc::new(StringArray::from(vec![
					Some("alpha"),
					Some("beta"),
					Some("gamma"),
				])) as ArrayRef,
			],
		)
		.unwrap();
		let (bytes, directory) = parse_directory_for_batch(&batch, &descriptor);
		let mut bytecode = Vec::new();
		pbc_push_col(&mut bytecode, 0);
		pbc_push_int(&mut bytecode, 2);
		bytecode.push(OP_GTE);
		let program = compile_pbc(bytecode);
		let requirement = analyze_aggregate_query_column_requirement(
			&program,
			&[AggregateExpression::count_star()],
			2,
		)
		.unwrap();
		let projected = decode_projected_record_batch(&bytes, &directory, &requirement).unwrap();

		let out = execute_aggregate_projected_record_batch(&projected, &program, &requirement, 2)
			.unwrap();

		assert_eq!(out.num_rows(), 2);
		assert_eq!(out.num_columns(), 0);
	}

	#[test]
	fn parses_record_buffer_directory_from_metadata_only_window() {
		let schema = Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("name", DataType::Utf8, true),
		]);
		let descriptor = descriptor_for(&schema);
		let batch = RecordBatch::try_new(
			Arc::new(schema),
			vec![
				Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
				Arc::new(StringArray::from(vec![Some("alpha"), None, Some("gamma")])) as ArrayRef,
			],
		)
		.unwrap();
		let bytes = encode_record_batch_bytes(&batch, RecordBodyCompressionPolicy::Uncompressed)
			.expect("encode test IPC");
		let mut offset = 0;
		read_ipc_message_metadata(&bytes, &mut offset).unwrap();
		let batch_message = read_ipc_message_metadata(&bytes, &mut offset).unwrap();
		let metadata_only = &bytes[..batch_message.body_start];

		let directory = parse_record_buffer_directory(
			test_record_id(),
			PlaintextByteRange {
				start: 0,
				len: bytes.len() as u64,
			},
			metadata_only,
			&descriptor,
		)
		.unwrap();

		assert_eq!(directory.column_to_buffers.len(), 2);
		assert_eq!(directory.column_to_buffers[0].buffer_count, 2);
		assert_eq!(directory.column_to_buffers[1].buffer_count, 3);
		assert_eq!(directory.buffers.len(), 5);
		assert_eq!(
			directory.logical_schema_fingerprint,
			descriptor.logical_schema_fingerprint
		);
	}

	#[test]
	fn selected_record_body_ranges_include_utf8_and_storage_buffers() {
		let user_schema = Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("name", DataType::Utf8, true),
		]);
		let descriptor = descriptor_for(&user_schema);
		let physical_schema = Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("name", DataType::Utf8, true),
			Field::new(STORAGE_ROW_HLC_COLUMN, DataType::FixedSizeBinary(16), false),
			Field::new(
				STORAGE_ROW_KEY_ID_COLUMN,
				DataType::FixedSizeBinary(32),
				false,
			),
			Field::new(STORAGE_ROW_ROUTING_LOCALITY_COLUMN, DataType::UInt64, false),
		]);
		let mut hlc = FixedSizeBinaryBuilder::with_capacity(2, 16);
		hlc.append_value([1_u8; 16]).unwrap();
		hlc.append_value([2_u8; 16]).unwrap();
		let mut key_id = FixedSizeBinaryBuilder::with_capacity(2, 32);
		key_id.append_value([3_u8; 32]).unwrap();
		key_id.append_value([4_u8; 32]).unwrap();
		let batch = RecordBatch::try_new(
			Arc::new(physical_schema),
			vec![
				Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
				Arc::new(StringArray::from(vec![Some("alpha"), Some("beta")])) as ArrayRef,
				Arc::new(hlc.finish()) as ArrayRef,
				Arc::new(key_id.finish()) as ArrayRef,
				Arc::new(UInt64Array::from(vec![10, 20])) as ArrayRef,
			],
		)
		.unwrap();
		let (_bytes, directory) = parse_directory_for_batch(&batch, &descriptor);
		let mut storage_columns = StorageColumnSet::new();
		storage_columns.insert(StorageColumn::RowHlc);
		let requirement = ColumnFetchRequirement {
			output_columns: ColumnOrdinalSet::from_columns([1]),
			storage_columns,
			..ColumnFetchRequirement::default()
		};

		let ranges = selected_record_body_ranges(&directory, &requirement).unwrap();

		assert_eq!(
			directory
				.storage_column_ordinals
				.get(&StorageColumn::RowHlc),
			Some(&2)
		);
		assert!(!ranges.is_empty());
		assert!(ranges.windows(2).all(|pair| pair[0].start <= pair[1].start));
		assert!(ranges.iter().all(|range| range.len.get() > 0));
	}

	#[test]
	fn decode_projected_record_batch_returns_narrow_batch_and_mappings() {
		let user_schema = Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("name", DataType::Utf8, true),
		]);
		let descriptor = descriptor_for(&user_schema);
		let physical_schema = Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("name", DataType::Utf8, true),
			Field::new(STORAGE_ROW_HLC_COLUMN, DataType::FixedSizeBinary(16), false),
		]);
		let mut hlc = FixedSizeBinaryBuilder::with_capacity(2, 16);
		hlc.append_value([1_u8; 16]).unwrap();
		hlc.append_value([2_u8; 16]).unwrap();
		let batch = RecordBatch::try_new(
			Arc::new(physical_schema),
			vec![
				Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
				Arc::new(StringArray::from(vec![Some("alpha"), Some("beta")])) as ArrayRef,
				Arc::new(hlc.finish()) as ArrayRef,
			],
		)
		.unwrap();
		let (bytes, directory) = parse_directory_for_batch(&batch, &descriptor);
		let mut storage_columns = StorageColumnSet::new();
		storage_columns.insert(StorageColumn::RowHlc);
		let requirement = ColumnFetchRequirement {
			output_columns: ColumnOrdinalSet::from_columns([1]),
			storage_columns,
			..ColumnFetchRequirement::default()
		};

		let projected = decode_projected_record_batch(&bytes, &directory, &requirement).unwrap();

		assert_eq!(projected.batch.num_rows(), 2);
		assert_eq!(projected.batch.num_columns(), 2);
		assert_eq!(projected.batch.schema().field(0).name(), "name");
		assert_eq!(
			projected.batch.schema().field(1).name(),
			STORAGE_ROW_HLC_COLUMN
		);
		assert_eq!(projected.physical_for(0), None);
		assert_eq!(projected.physical_for(1), Some(0));
		assert_eq!(
			projected.storage_physical_for(StorageColumn::RowHlc),
			Some(1)
		);
		let user_visible = projected.user_visible().unwrap();
		assert_eq!(user_visible.num_rows(), 2);
		assert_eq!(user_visible.num_columns(), 1);
		assert_eq!(user_visible.schema().field(0).name(), "name");
	}

	#[test]
	fn decode_projected_record_batch_from_plaintext_ranges_uses_selected_buffers() {
		let schema = Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("name", DataType::Utf8, true),
		]);
		let descriptor = descriptor_for(&schema);
		let batch = RecordBatch::try_new(
			Arc::new(schema),
			vec![
				Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
				Arc::new(StringArray::from(vec![Some("alpha"), None, Some("gamma")])) as ArrayRef,
			],
		)
		.unwrap();
		let (bytes, directory) = parse_directory_for_batch(&batch, &descriptor);
		let requirement = ColumnFetchRequirement {
			output_columns: ColumnOrdinalSet::from_columns([1]),
			..ColumnFetchRequirement::default()
		};
		let selected_ranges = selected_record_body_ranges(&directory, &requirement).unwrap();
		let extracts = selected_ranges
			.iter()
			.map(|range| {
				let start = range.start.get() as usize;
				let end = start + range.len.get() as usize;
				bytes[start..end].to_vec()
			})
			.collect::<Vec<_>>();

		let buffers = record_buffer_set_from_plaintext_ranges(
			&directory,
			&requirement,
			&selected_ranges,
			&extracts,
		)
		.unwrap();
		let projected = decode_projected_record_batch_from_plaintext_ranges(
			&directory,
			&requirement,
			&selected_ranges,
			&extracts,
		)
		.unwrap();

		assert_eq!(buffers.len(), 3);
		assert_eq!(projected.batch, batch.project(&[1]).unwrap());
	}

	#[test]
	fn decode_projected_record_batch_decompresses_lz4_selected_buffers() {
		let schema = Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("name", DataType::Utf8, true),
		]);
		let descriptor = descriptor_for(&schema);
		let names = (0..128)
			.map(|row| Some(format!("region-{row:03}-payload-payload")))
			.collect::<Vec<_>>();
		let batch = RecordBatch::try_new(
			Arc::new(schema),
			vec![
				Arc::new(Int64Array::from_iter_values(0..128)) as ArrayRef,
				Arc::new(StringArray::from(names)) as ArrayRef,
			],
		)
		.unwrap();
		let (bytes, directory) = parse_directory_for_batch_with_policy(
			&batch,
			&descriptor,
			RecordBodyCompressionPolicy::Lz4Frame,
		);
		let requirement = ColumnFetchRequirement {
			output_columns: ColumnOrdinalSet::from_columns([1]),
			..ColumnFetchRequirement::default()
		};

		let projected = decode_projected_record_batch(&bytes, &directory, &requirement).unwrap();
		let whole = crate::storage::multirow::decode_batch(&bytes).unwrap();

		assert_eq!(
			directory.record_body_compression,
			RecordBodyCompressionPolicy::Lz4Frame
		);
		assert_eq!(projected.batch, whole.project(&[1]).unwrap());
	}

	#[test]
	fn decode_projected_record_batch_preserves_descriptor_order() {
		let schema = Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("name", DataType::Utf8, true),
		]);
		let descriptor = descriptor_for(&schema);
		let batch = RecordBatch::try_new(
			Arc::new(schema),
			vec![
				Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
				Arc::new(StringArray::from(vec![Some("alpha"), Some("beta")])) as ArrayRef,
			],
		)
		.unwrap();
		let (bytes, directory) = parse_directory_for_batch(&batch, &descriptor);
		let requirement = ColumnFetchRequirement {
			predicate_columns: ColumnOrdinalSet::from_columns([1]),
			output_columns: ColumnOrdinalSet::from_columns([0]),
			..ColumnFetchRequirement::default()
		};

		let projected = decode_projected_record_batch(&bytes, &directory, &requirement).unwrap();

		assert_eq!(projected.batch.num_columns(), 2);
		assert_eq!(projected.batch.schema().field(0).name(), "id");
		assert_eq!(projected.batch.schema().field(1).name(), "name");
		assert_eq!(projected.physical_for(0), Some(0));
		assert_eq!(projected.physical_for(1), Some(1));
	}

	#[test]
	fn decode_projected_record_batch_accepts_count_star_shape() {
		let schema = Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("name", DataType::Utf8, true),
		]);
		let descriptor = descriptor_for(&schema);
		let batch = RecordBatch::try_new(
			Arc::new(schema),
			vec![
				Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
				Arc::new(StringArray::from(vec![Some("alpha"), Some("beta")])) as ArrayRef,
			],
		)
		.unwrap();
		let (bytes, directory) = parse_directory_for_batch(&batch, &descriptor);

		let projected =
			decode_projected_record_batch(&bytes, &directory, &ColumnFetchRequirement::default())
				.unwrap();

		assert_eq!(projected.batch.num_rows(), 2);
		assert_eq!(projected.batch.num_columns(), 0);
		assert_eq!(projected.user_visible().unwrap().num_rows(), 2);
	}

	#[test]
	fn decode_projected_record_batch_rejects_missing_descriptor_column() {
		let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
		let descriptor = descriptor_for(&schema);
		let batch = RecordBatch::try_new(
			Arc::new(schema),
			vec![Arc::new(Int64Array::from(vec![1])) as ArrayRef],
		)
		.unwrap();
		let (bytes, directory) = parse_directory_for_batch(&batch, &descriptor);
		let requirement = ColumnFetchRequirement {
			output_columns: ColumnOrdinalSet::from_columns([1]),
			..ColumnFetchRequirement::default()
		};

		let err = decode_projected_record_batch(&bytes, &directory, &requirement).unwrap_err();

		assert!(format!("{err}").contains("out of range"));
	}

	#[test]
	fn parser_reports_needed_bytes_for_truncated_metadata() {
		let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
		let descriptor = descriptor_for(&schema);
		let batch = RecordBatch::try_new(
			Arc::new(schema),
			vec![Arc::new(Int64Array::from(vec![1])) as ArrayRef],
		)
		.unwrap();
		let bytes = encode_record_batch_bytes(&batch, RecordBodyCompressionPolicy::Uncompressed)
			.expect("encode test IPC");

		let err = parse_record_buffer_directory(
			test_record_id(),
			PlaintextByteRange {
				start: 0,
				len: bytes.len() as u64,
			},
			&bytes[..2],
			&descriptor,
		)
		.unwrap_err();

		assert!(matches!(
			err,
			ColumnFetchError::MetadataTruncated { needed: 4, have: 2 }
		));
	}

	#[test]
	fn prepare_strategy_requests_larger_metadata_probe() {
		let schema = Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("name", DataType::Utf8, true),
		]);
		let descriptor = multirow_descriptor_for(&schema);
		let batch = RecordBatch::try_new(
			Arc::new(schema),
			vec![
				Arc::new(Int64Array::from(vec![1])) as ArrayRef,
				Arc::new(StringArray::from(vec![Some("alpha")])) as ArrayRef,
			],
		)
		.unwrap();
		let bytes = encode_record_batch_bytes(&batch, RecordBodyCompressionPolicy::Uncompressed)
			.expect("encode test IPC");
		let cfg = L2ColumnFetchConfig {
			enabled: true,
			ipc_metadata_initial_window_bytes: RecordBodyByteLen::new(2),
			max_ipc_metadata_probe_bytes: RecordBodyByteLen::new(64),
			min_record_bytes: RecordBodyByteLen::new(1),
			..L2ColumnFetchConfig::default()
		};
		let requirement = ColumnFetchRequirement {
			output_columns: ColumnOrdinalSet::from_columns([1]),
			..ColumnFetchRequirement::default()
		};
		let crypto = CryptoManager::new_dev();

		let preparation = prepare_l2_column_fetch_strategy(
			&crypto,
			&cfg,
			&descriptor,
			&record_location(bytes.len() as u32),
			test_record_id(),
			&requirement,
			&bytes[..2],
			RecordBodyByteLen::new(2),
			0,
			&[],
			None,
			false,
		)
		.unwrap();

		assert!(matches!(
			preparation,
			L2ColumnFetchPreparation::NeedMoreMetadata(L2ColumnFetchMetadataProbe {
				window_bytes,
				..
			}) if window_bytes.get() == 4
		));
	}

	#[test]
	fn prepare_strategy_returns_probe_cap_fallback() {
		let schema = Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("name", DataType::Utf8, true),
		]);
		let descriptor = multirow_descriptor_for(&schema);
		let batch = RecordBatch::try_new(
			Arc::new(schema),
			vec![
				Arc::new(Int64Array::from(vec![1])) as ArrayRef,
				Arc::new(StringArray::from(vec![Some("alpha")])) as ArrayRef,
			],
		)
		.unwrap();
		let bytes = encode_record_batch_bytes(&batch, RecordBodyCompressionPolicy::Uncompressed)
			.expect("encode test IPC");
		let cfg = L2ColumnFetchConfig {
			enabled: true,
			ipc_metadata_initial_window_bytes: RecordBodyByteLen::new(2),
			max_ipc_metadata_probe_bytes: RecordBodyByteLen::new(2),
			min_record_bytes: RecordBodyByteLen::new(1),
			..L2ColumnFetchConfig::default()
		};
		let requirement = ColumnFetchRequirement {
			output_columns: ColumnOrdinalSet::from_columns([1]),
			..ColumnFetchRequirement::default()
		};
		let crypto = CryptoManager::new_dev();

		let preparation = prepare_l2_column_fetch_strategy(
			&crypto,
			&cfg,
			&descriptor,
			&record_location(bytes.len() as u32),
			test_record_id(),
			&requirement,
			&bytes[..2],
			RecordBodyByteLen::new(2),
			0,
			&[],
			None,
			false,
		)
		.unwrap();

		assert!(matches!(
			preparation,
			L2ColumnFetchPreparation::Strategy(PreparedL2ColumnFetchStrategy {
				directory: None,
				strategy: L2FetchStrategy::Fallback(ColumnarFallbackReason::MetadataProbeCap),
			})
		));
	}

	#[test]
	fn prepare_strategy_builds_columnar_plan_from_metadata_and_tail() {
		let mut fields = vec![Field::new("id", DataType::Int64, false)];
		let mut columns = vec![Arc::new(Int64Array::from_iter_values(0..20_000)) as ArrayRef];
		for column in 1..10 {
			fields.push(Field::new(format!("c{column}"), DataType::Int64, false));
			columns.push(Arc::new(Int64Array::from_iter_values(
				(0..20_000).map(|row| i64::from(column * 100_000 + row)),
			)) as ArrayRef);
		}
		let schema = Schema::new(fields);
		let descriptor = multirow_descriptor_for(&schema);
		let batch = RecordBatch::try_new(Arc::new(schema), columns).unwrap();
		let bytes = encode_record_batch_bytes(&batch, RecordBodyCompressionPolicy::Uncompressed)
			.expect("encode test IPC");
		let requirement = ColumnFetchRequirement {
			output_columns: ColumnOrdinalSet::from_columns([9]),
			..ColumnFetchRequirement::default()
		};
		let cfg = L2ColumnFetchConfig {
			enabled: true,
			ipc_metadata_initial_window_bytes: RecordBodyByteLen::try_new(bytes.len() as u64)
				.unwrap(),
			max_ipc_metadata_probe_bytes: RecordBodyByteLen::try_new(bytes.len() as u64).unwrap(),
			min_record_bytes: RecordBodyByteLen::new(1),
			..L2ColumnFetchConfig::default()
		};
		let crypto = CryptoManager::new_dev();
		crypto.install_local_only_for_tests(TenantId::ZERO).unwrap();
		let slab_id = SlabId::new(446);
		let encrypted = crypto
			.encrypt_tiered_slab_object(
				slab_id,
				&TieredSlabObjectMetadata {
					tenant_id: TenantId::ZERO,
					created_hlc: 0,
					offset_index: vec![TieredOffsetIndexEntry {
						key_locator: IdentityPrefix::ZERO,
						byte_offset: 0,
						byte_length: bytes.len().try_into().unwrap(),
						flags: 0,
					}],
					column_stats_block: None,
					has_row_tombstone_records: false,
				},
				&bytes,
			)
			.unwrap();
		let object_size = encrypted.bytes.len() as u64;
		let tail_start = object_size.saturating_sub(64 * 1024) as usize;

		let preparation = prepare_l2_column_fetch_strategy(
			&crypto,
			&cfg,
			&descriptor,
			&RecordLocation {
				slab_id,
				..record_location(bytes.len() as u32)
			},
			TieredRecordId {
				slab_id,
				record_offset: RecordBodyByteOffset::new(0),
			},
			&requirement,
			&bytes,
			RecordBodyByteLen::try_new(bytes.len() as u64).unwrap(),
			object_size,
			&encrypted.bytes[tail_start..],
			None,
			false,
		)
		.unwrap();

		let L2ColumnFetchPreparation::Strategy(prepared) = preparation else {
			panic!("expected final strategy");
		};
		assert!(prepared.directory.is_some());
		assert!(matches!(prepared.strategy, L2FetchStrategy::Columnar(_)));
		assert!(prepared.strategy.selected_chunk_runs().unwrap() > 0);
	}

	#[test]
	fn prepare_strategy_surfaces_crypto_plan_errors_without_fallback() {
		let schema = Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("name", DataType::Utf8, true),
		]);
		let descriptor = multirow_descriptor_for(&schema);
		let batch = RecordBatch::try_new(
			Arc::new(schema),
			vec![
				Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
				Arc::new(StringArray::from(vec![Some("alpha"), Some("beta")])) as ArrayRef,
			],
		)
		.unwrap();
		let bytes = encode_record_batch_bytes(&batch, RecordBodyCompressionPolicy::Uncompressed)
			.expect("encode test IPC");
		let requirement = ColumnFetchRequirement {
			output_columns: ColumnOrdinalSet::from_columns([1]),
			..ColumnFetchRequirement::default()
		};
		let cfg = L2ColumnFetchConfig {
			enabled: true,
			ipc_metadata_initial_window_bytes: RecordBodyByteLen::try_new(bytes.len() as u64)
				.unwrap(),
			max_ipc_metadata_probe_bytes: RecordBodyByteLen::try_new(bytes.len() as u64).unwrap(),
			min_record_bytes: RecordBodyByteLen::new(1),
			..L2ColumnFetchConfig::default()
		};
		let crypto = CryptoManager::new_dev();
		crypto.install_local_only_for_tests(TenantId::ZERO).unwrap();

		let err = prepare_l2_column_fetch_strategy(
			&crypto,
			&cfg,
			&descriptor,
			&record_location(bytes.len() as u32),
			test_record_id(),
			&requirement,
			&bytes,
			RecordBodyByteLen::try_new(bytes.len() as u64).unwrap(),
			512,
			&[0_u8; 128],
			None,
			false,
		)
		.unwrap_err();

		assert!(format!("{err}").contains("tiered slab"));
	}

	#[test]
	fn build_column_fetch_plan_uses_crypto_multi_range_runs() {
		let schema = Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("name", DataType::Utf8, true),
		]);
		let descriptor = descriptor_for(&schema);
		let batch = RecordBatch::try_new(
			Arc::new(schema),
			vec![
				Arc::new(Int64Array::from(vec![1, 2, 3, 4])) as ArrayRef,
				Arc::new(StringArray::from(vec![
					Some("alpha"),
					Some("beta"),
					Some("gamma"),
					Some("delta"),
				])) as ArrayRef,
			],
		)
		.unwrap();
		let (bytes, directory) = parse_directory_for_batch(&batch, &descriptor);
		let requirement = ColumnFetchRequirement {
			output_columns: ColumnOrdinalSet::from_columns([1]),
			..ColumnFetchRequirement::default()
		};
		let selected_ranges = selected_record_body_ranges(&directory, &requirement).unwrap();
		let crypto = CryptoManager::new_dev();
		crypto.install_local_only_for_tests(TenantId::ZERO).unwrap();
		let encrypted = crypto
			.encrypt_tiered_slab_object(
				SlabId::new(445),
				&TieredSlabObjectMetadata {
					tenant_id: TenantId::ZERO,
					created_hlc: 0,
					offset_index: vec![TieredOffsetIndexEntry {
						key_locator: IdentityPrefix::ZERO,
						byte_offset: 0,
						byte_length: bytes.len().try_into().unwrap(),
						flags: 0,
					}],
					column_stats_block: None,
					has_row_tombstone_records: false,
				},
				&bytes,
			)
			.unwrap();
		let object_size = encrypted.bytes.len() as u64;
		let tail_start = object_size.saturating_sub(64 * 1024) as usize;
		let tiered_plan = crypto
			.plan_tiered_multi_range_read(
				SlabId::new(445),
				object_size,
				&encrypted.bytes[tail_start..],
				None,
				0..bytes.len(),
				&selected_ranges,
			)
			.unwrap();

		let plan = build_column_fetch_plan(&directory, &requirement, &tiered_plan).unwrap();

		assert!(!plan.chunk_runs.is_empty());
		assert!(!plan.selected_plaintext_ranges.is_empty());
		assert!(plan.fetched_ciphertext_bytes > 0);
		assert!(plan.avoided_record_body_bytes > 0);
	}

	#[test]
	fn strategy_picker_applies_fallback_order_and_accepts_sparse_plan() {
		let schema = Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("name", DataType::Utf8, true),
		]);
		let single_descriptor = descriptor_for(&schema);
		let descriptor = multirow_descriptor_for(&schema);
		let batch = RecordBatch::try_new(
			Arc::new(schema),
			vec![
				Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
				Arc::new(StringArray::from(vec![Some("alpha"), Some("beta")])) as ArrayRef,
			],
		)
		.unwrap();
		let (_bytes, directory) = parse_directory_for_batch(&batch, &descriptor);
		let sparse_requirement = ColumnFetchRequirement {
			output_columns: ColumnOrdinalSet::from_columns([1]),
			..ColumnFetchRequirement::default()
		};
		let mut cfg = L2ColumnFetchConfig {
			enabled: true,
			min_record_bytes: RecordBodyByteLen::new(10),
			..L2ColumnFetchConfig::default()
		};

		assert!(matches!(
			pick_l2_fetch_strategy(
				&L2ColumnFetchConfig::default(),
				&descriptor,
				&record_location(1_000),
				&sparse_requirement,
				Some(&directory),
				Some(candidate_plan(10, 1)),
				false,
			),
			L2FetchStrategy::Fallback(ColumnarFallbackReason::Disabled)
		));
		assert!(matches!(
			pick_l2_fetch_strategy(
				&cfg,
				&single_descriptor,
				&record_location(1_000),
				&sparse_requirement,
				Some(&directory),
				Some(candidate_plan(10, 1)),
				false,
			),
			L2FetchStrategy::Fallback(ColumnarFallbackReason::SingleRow)
		));
		assert!(matches!(
			pick_l2_fetch_strategy(
				&cfg,
				&descriptor,
				&record_location(5),
				&sparse_requirement,
				Some(&directory),
				Some(candidate_plan(10, 1)),
				false,
			),
			L2FetchStrategy::Fallback(ColumnarFallbackReason::SmallRecord)
		));
		assert!(matches!(
			pick_l2_fetch_strategy(
				&cfg,
				&descriptor,
				&record_location(1_000),
				&sparse_requirement,
				None,
				Some(candidate_plan(10, 1)),
				false,
			),
			L2FetchStrategy::Fallback(ColumnarFallbackReason::MetadataProbeCap)
		));
		assert!(matches!(
			pick_l2_fetch_strategy(
				&cfg,
				&descriptor,
				&record_location(1_000),
				&sparse_requirement,
				Some(&directory),
				Some(candidate_plan(800, 1)),
				false,
			),
			L2FetchStrategy::Fallback(ColumnarFallbackReason::TooManyBytes)
		));
		cfg.max_ranges_per_record = EncryptedChunkCount::new(1);
		assert!(matches!(
			pick_l2_fetch_strategy(
				&cfg,
				&descriptor,
				&record_location(1_000),
				&sparse_requirement,
				Some(&directory),
				Some(candidate_plan(10, 2)),
				false,
			),
			L2FetchStrategy::Fallback(ColumnarFallbackReason::TooManyRanges)
		));
		cfg.max_ranges_per_record = EncryptedChunkCount::new(8);
		assert!(matches!(
			pick_l2_fetch_strategy(
				&cfg,
				&descriptor,
				&record_location(1_000),
				&sparse_requirement,
				Some(&directory),
				Some(candidate_plan(10, 1)),
				false,
			),
			L2FetchStrategy::Columnar(_)
		));
		let full_requirement = ColumnFetchRequirement {
			output_columns: ColumnOrdinalSet::from_columns([0, 1]),
			..ColumnFetchRequirement::default()
		};
		assert!(matches!(
			pick_l2_fetch_strategy(
				&cfg,
				&descriptor,
				&record_location(1_000),
				&full_requirement,
				Some(&directory),
				Some(candidate_plan(10, 1)),
				false,
			),
			L2FetchStrategy::Fallback(ColumnarFallbackReason::FullProjection)
		));
	}
}
