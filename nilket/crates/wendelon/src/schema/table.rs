use crate::schema::wpbc::{
	ColumnPredicateComparisonV1, ColumnPredicateLiteralV1, Program as WpbcProgram,
};
use crate::schema::{
	ClientWriteId, DurabilityLevel, HybridTimestamp, Identity, Locality, NodeId, TenantId,
	WriteResponseMeta, write_payload_schema,
};
use arrow::array::{
	Array, BinaryArray, Int8Array, Int16Array, Int32Array, Int64Array, LargeBinaryArray,
	LargeStringArray, StringArray, TimestampMillisecondArray, UInt8Array, UInt16Array, UInt32Array,
	UInt64Array,
};
use arrow::datatypes::{DataType, Schema, TimeUnit};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use bytemuck::{Pod, Zeroable};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::io::{Cursor, Error, ErrorKind};

const TABLE_ROW_IDENTITY_DOMAIN: &[u8] = b"wendelon.table-row.v1";
const TABLE_LOCALITY_DOMAIN: &[u8] = b"wendelon.table-locality.v1";
const TABLE_COLUMN_LOCALITY_HASH_DOMAIN: &[u8] = b"wendelon.table-column-locality.v1";

pub const TABLE_SCHEMA_DESCRIPTOR_VERSION: u16 = 1;
pub const TABLE_SPREAD_DEFAULT_FACTOR: u16 = 4;
pub const TABLE_SPREAD_MAX_FACTOR: u16 = 256;
pub const TABLE_SPREAD_DEFAULT_PREFIX_BITS: u8 = 16;
pub const TABLE_WRITE_MAX_ROWS: usize = 65_536;
pub const TABLE_FIELD_DEFAULT_METADATA_KEY: &str = "wendelon.default.v1";
pub const TABLE_CLUSTERING_MAX_COLUMNS: usize = 6;
pub const COLUMN_PROJECTION_RANGE_ARC_BUDGET: usize = 1024;
pub const COLUMN_STATS_BLOOM_DEFAULT_BITS_PER_ROW: u8 = 10;
pub const COLUMN_STATS_BLOOM_MIN_BITS_PER_ROW: u8 = 1;
pub const COLUMN_STATS_BLOOM_MAX_BITS_PER_ROW: u8 = 32;
pub const COLUMN_STATS_HLL_DEFAULT_PRECISION: u8 = 12;
pub const COLUMN_STATS_HLL_MIN_PRECISION: u8 = 10;
pub const COLUMN_STATS_HLL_MAX_PRECISION: u8 = 14;
pub const COLUMN_STATS_SMALL_SET_DEFAULT_MAX_DISTINCT_VALUES: u32 = 4096;
pub const COLUMN_STATS_SMALL_SET_MAX_DISTINCT_VALUES: u32 = 4096;
pub const COLUMN_STATS_SMALL_SET_DEFAULT_MAX_BYTES: u32 = 32 * 1024;
pub const COLUMN_STATS_SMALL_SET_MAX_BYTES: u32 = 32 * 1024;
pub const COLUMN_STATS_CIPHERTEXT_BUDGET_DIVISOR: u64 = 256;
pub const COLUMN_STATS_DESCRIPTOR_SLAB_PAYLOAD_BYTES: u64 = 64 * 1024 * 1024;
pub const COLUMN_STATS_CSB_AEAD_TAG_BYTES: u64 = 16;
pub const COLUMN_STATS_CSB_HEADER_BYTES: u64 = 84;
pub const COLUMN_STATS_CSB_COLUMN_HEADER_BYTES: u64 = 9;
pub const COLUMN_STATS_DEFAULT_STRING_PREFIX_BYTES: u64 = 64;

#[derive(
	Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Pod, Zeroable, Serialize, Deserialize,
)]
#[repr(transparent)]
#[serde(transparent)]
pub struct TableObjectId([u8; 16]);

impl TableObjectId {
	pub const DEFAULT_KV: Self = Self([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);

	pub const fn new(bytes: [u8; 16]) -> Self {
		Self(bytes)
	}

	pub const fn as_bytes(&self) -> &[u8; 16] {
		&self.0
	}

	pub const fn into_bytes(self) -> [u8; 16] {
		self.0
	}
}

impl From<[u8; 16]> for TableObjectId {
	fn from(value: [u8; 16]) -> Self {
		Self::new(value)
	}
}

impl From<TableObjectId> for [u8; 16] {
	fn from(value: TableObjectId) -> Self {
		value.0
	}
}

// ============================================================
//  TABLE WRITE WIRE METADATA AND ADMISSION
// ============================================================

/// Fixed metadata for typed table writes.
///
/// The existing [`crate::schema::WriteRequestMeta`] stays byte-stable
/// for legacy KV writes. Typed table writes use this parallel frame so
/// table identity and schema generation are explicit on the hot path.
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
#[repr(C)]
pub struct TableWriteRequestMeta {
	/// Parent idempotency key for the whole client-submitted batch.
	pub parent_write_id: ClientWriteId, // 32 bytes
	/// Client's current HLC; leader merges max(leader_hlc, client_hlc)+1.
	pub client_hlc: HybridTimestamp, // 16 bytes
	/// Client's believed epoch; leader rejects on mismatch.
	pub expected_epoch: u64, //  8 bytes
	/// Tenant the table belongs to.
	pub tenant_id: TenantId, //  8 bytes
	/// Durable tenant-scoped table identity from the descriptor.
	pub table_object_id: TableObjectId, // 16 bytes
	/// Descriptor generation the write was compiled against.
	pub schema_generation: u64, //  8 bytes
	/// Material used when deriving child write ids during split append.
	pub child_ordinal_base: u64, //  8 bytes
	/// Set by proxy follower for response routing; zeroed on direct writes.
	pub proxy_source: NodeId, //  8 bytes
	/// Wire row count. `0` encodes 65,536; empty batches are invalid.
	pub row_count: u16, //  2 bytes
	/// 0 = direct to leader; incremented by each proxy; max 2.
	pub proxy_hop: u8, //  1 byte
	/// Durability level; same values as legacy write metadata.
	pub durability: DurabilityLevel, //  1 byte
	/// If 1, leader streams back FinalizationMarker in the response.
	pub await_finalization: u8, //  1 byte
	pub _pad: [u8; 3], //  3 bytes alignment
}

const _: () = assert!(std::mem::size_of::<TableWriteRequestMeta>() == 112);

impl TableWriteRequestMeta {
	pub fn decoded_row_count(self) -> usize {
		decode_table_write_row_count(self.row_count)
	}
}

#[derive(Clone, Copy, Debug, Pod, Zeroable)]
#[repr(C)]
pub struct TableWriteResponseMeta {
	pub write: WriteResponseMeta, // 96 bytes
	/// Zero on non-table-specific outcomes. Non-zero values are
	/// [`WpbcRejectCode`] discriminants for schema/admission rejects.
	pub reject_code: u16, //  2 bytes
	pub _pad: [u8; 6],            //  6 bytes alignment
}

const _: () = assert!(std::mem::size_of::<TableWriteResponseMeta>() == 104);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
#[repr(C)]
pub struct TableReplicaWriteMeta {
	/// HLC chosen by the accepting node. Every child row in this
	/// parent table write is indexed under the same commit timestamp.
	pub commit_hlc: HybridTimestamp, // 16 bytes
	/// Parent idempotency key for the whole table batch.
	pub parent_write_id: ClientWriteId, // 32 bytes
	/// Epoch of the accepting node.
	pub epoch: u64, //  8 bytes
	/// Tenant the replicated rows belong to.
	pub tenant_id: TenantId, //  8 bytes
	/// Durable tenant-scoped table identity.
	pub table_object_id: TableObjectId, // 16 bytes
	/// Authoritative quota charge for all child rows in the batch.
	pub charged_bytes: u64, //  8 bytes
	/// Quota charge for the child rows carried in this replica frame.
	pub group_charged_bytes: u64, //  8 bytes
	/// Synthetic parent identity used by the aggregate finalization.
	pub parent_key_identity: Identity, // 32 bytes
	/// Digest over the ordered child identities and payload digests.
	pub payload_hash: [u8; 32], // 32 bytes
	/// Digest over the ordered child rows carried in this replica frame.
	pub group_payload_hash: [u8; 32], // 32 bytes
	/// Number of child slab records represented by this parent write.
	pub record_count: u32, //  4 bytes
	/// Number of child slab records carried in this replica frame.
	pub group_record_count: u32, //  4 bytes
	/// Durability class echoed into the eventual finalization marker.
	pub durability: DurabilityLevel, //  1 byte
	/// Mirrors the caller's receipt preference.
	pub await_finalization: u8, //  1 byte
	pub _pad: [u8; 6], //  6 bytes alignment
}

const _: () = assert!(std::mem::size_of::<TableReplicaWriteMeta>() == 208);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable)]
#[repr(C)]
pub struct TableQueryRequestMeta {
	/// Causal read-after-write bound.
	pub min_hlc: HybridTimestamp, // 16 bytes
	pub expected_epoch: u64,                     //  8 bytes
	pub tenant_id: TenantId,                     //  8 bytes
	pub table_object_id: TableObjectId,          // 16 bytes
	pub schema_generation: u64,                  //  8 bytes
	pub consistency: crate::schema::Consistency, //  1 byte
	pub _pad: [u8; 7],                           //  7 bytes
	/// Length of the WPBC bytecode in the trailing section.
	pub bytecode_len: u32, //  4 bytes
	pub _pad2: [u8; 4],                          //  4 bytes
	/// Time-travel upper bound (zero = latest).
	pub as_of_hlc: HybridTimestamp, // 16 bytes
}

const _: () = assert!(std::mem::size_of::<TableQueryRequestMeta>() == 88);

pub const fn decode_table_write_row_count(row_count: u16) -> usize {
	// `u16::MAX` is one row short of ADR 037's 65,536-row cap, so
	// the wire sentinel `0` means max-sized batch. Empty batches are
	// still invalid and are rejected against the actual RecordBatch.
	if row_count == 0 {
		TABLE_WRITE_MAX_ROWS
	} else {
		row_count as usize
	}
}

pub fn encode_table_write_row_count(row_count: usize) -> std::io::Result<u16> {
	match row_count {
		0 => Err(Error::new(
			ErrorKind::InvalidInput,
			"table writes require at least one row",
		)),
		TABLE_WRITE_MAX_ROWS => Ok(0),
		1..=65_535 => Ok(row_count as u16),
		_ => Err(Error::new(
			ErrorKind::InvalidInput,
			format!("table write row_count exceeds {TABLE_WRITE_MAX_ROWS}"),
		)),
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Pod, Zeroable, Serialize, Deserialize)]
#[repr(transparent)]
#[serde(transparent)]
pub struct WpbcRejectCode(pub u16);

impl WpbcRejectCode {
	pub const NONE: Self = Self(0);
	pub const WPBC_REJECT_SCHEMA_STALE: Self = Self(0x3701);
	pub const WPBC_REJECT_SCHEMA_UNKNOWN: Self = Self(0x3702);
	pub const WPBC_REJECT_COLUMN_TYPE_MISMATCH: Self = Self(0x3703);
	pub const WPBC_REJECT_PK_MISSING: Self = Self(0x3704);
	pub const WPBC_REJECT_PK_NULL: Self = Self(0x3705);
	pub const WPBC_REJECT_PK_TYPE_UNSUPPORTED: Self = Self(0x3706);
	pub const WPBC_REJECT_DUPLICATE_PK_IN_BATCH: Self = Self(0x3707);
	pub const WPBC_REJECT_LOCALITY_UNSUPPORTED: Self = Self(0x3708);
	pub const WPBC_REJECT_BATCH_TOO_LARGE: Self = Self(0x3709);
	pub const WPBC_REJECT_CLUSTERING_TOO_WIDE: Self = Self(0x3801);
	pub const WPBC_REJECT_CLUSTERING_EMPTY: Self = Self(0x3802);
	pub const WPBC_REJECT_CLUSTERING_COLUMN_UNKNOWN: Self = Self(0x3803);
	pub const WPBC_REJECT_CLUSTERING_TYPE_UNSUPPORTED: Self = Self(0x3804);
	pub const WPBC_REJECT_CLUSTERING_NULLABLE: Self = Self(0x3805);
	pub const WPBC_REJECT_CLUSTERING_WIDTH_OVERFLOW: Self = Self(0x3806);
	pub const WPBC_REJECT_CLUSTERING_WIDTH_INVALID: Self = Self(0x3807);
	pub const WPBC_REJECT_PROJECTION_NOT_YET_SUPPORTED: Self = Self(0x3808);
	pub const WPBC_REJECT_RANGE_DECOMPOSITION_OVERFLOW: Self = Self(0x3809);
	pub const WPBC_REJECT_CLUSTERING_DUPLICATE_COLUMN: Self = Self(0x380A);
	pub const WPBC_REJECT_CLUSTERING_VALUE_NULL: Self = Self(0x380B);
	pub const WPBC_REJECT_STATS_UNSUPPORTED_TYPE: Self = Self(0x3901);
	pub const WPBC_REJECT_STATS_DUPLICATE_COLUMN: Self = Self(0x3902);
	pub const WPBC_REJECT_STATS_COLUMN_UNKNOWN: Self = Self(0x3903);
	pub const WPBC_REJECT_STATS_BUDGET_EXCEEDED: Self = Self(0x3904);
	pub const WPBC_REJECT_STATS_PRECISION_INVALID: Self = Self(0x3905);
	pub const WPBC_REJECT_STATS_BITS_INVALID: Self = Self(0x3906);
	pub const WPBC_REJECT_STATS_SMALLSET_INVALID: Self = Self(0x3907);
	pub const WPBC_REJECT_STATS_FORMAT_VERSION: Self = Self(0x3908);
	pub const WPBC_REJECT_STATS_FINGERPRINT_MISMATCH: Self = Self(0x3909);
	pub const WPBC_REJECT_STATS_DECRYPT_FAILED: Self = Self(0x390A);

	pub const fn name(self) -> &'static str {
		match self.0 {
			0 => "NONE",
			0x3701 => "WPBC_REJECT_SCHEMA_STALE",
			0x3702 => "WPBC_REJECT_SCHEMA_UNKNOWN",
			0x3703 => "WPBC_REJECT_COLUMN_TYPE_MISMATCH",
			0x3704 => "WPBC_REJECT_PK_MISSING",
			0x3705 => "WPBC_REJECT_PK_NULL",
			0x3706 => "WPBC_REJECT_PK_TYPE_UNSUPPORTED",
			0x3707 => "WPBC_REJECT_DUPLICATE_PK_IN_BATCH",
			0x3708 => "WPBC_REJECT_LOCALITY_UNSUPPORTED",
			0x3709 => "WPBC_REJECT_BATCH_TOO_LARGE",
			0x3801 => "WPBC_REJECT_CLUSTERING_TOO_WIDE",
			0x3802 => "WPBC_REJECT_CLUSTERING_EMPTY",
			0x3803 => "WPBC_REJECT_CLUSTERING_COLUMN_UNKNOWN",
			0x3804 => "WPBC_REJECT_CLUSTERING_TYPE_UNSUPPORTED",
			0x3805 => "WPBC_REJECT_CLUSTERING_NULLABLE",
			0x3806 => "WPBC_REJECT_CLUSTERING_WIDTH_OVERFLOW",
			0x3807 => "WPBC_REJECT_CLUSTERING_WIDTH_INVALID",
			0x3808 => "WPBC_REJECT_PROJECTION_NOT_YET_SUPPORTED",
			0x3809 => "WPBC_REJECT_RANGE_DECOMPOSITION_OVERFLOW",
			0x380A => "WPBC_REJECT_CLUSTERING_DUPLICATE_COLUMN",
			0x380B => "WPBC_REJECT_CLUSTERING_VALUE_NULL",
			0x3901 => "WPBC_REJECT_STATS_UNSUPPORTED_TYPE",
			0x3902 => "WPBC_REJECT_STATS_DUPLICATE_COLUMN",
			0x3903 => "WPBC_REJECT_STATS_COLUMN_UNKNOWN",
			0x3904 => "WPBC_REJECT_STATS_BUDGET_EXCEEDED",
			0x3905 => "WPBC_REJECT_STATS_PRECISION_INVALID",
			0x3906 => "WPBC_REJECT_STATS_BITS_INVALID",
			0x3907 => "WPBC_REJECT_STATS_SMALLSET_INVALID",
			0x3908 => "WPBC_REJECT_STATS_FORMAT_VERSION",
			0x3909 => "WPBC_REJECT_STATS_FINGERPRINT_MISMATCH",
			0x390A => "WPBC_REJECT_STATS_DECRYPT_FAILED",
			_ => "WPBC_REJECT_UNKNOWN",
		}
	}
}

impl fmt::Display for WpbcRejectCode {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str(self.name())
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableWriteValidationError {
	pub code: WpbcRejectCode,
	pub message: String,
}

impl TableWriteValidationError {
	fn new(code: WpbcRejectCode, message: impl Into<String>) -> Self {
		Self {
			code,
			message: message.into(),
		}
	}
}

impl fmt::Display for TableWriteValidationError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{}: {}", self.code, self.message)
	}
}

impl std::error::Error for TableWriteValidationError {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableWriteAdmission {
	pub canonical_primary_keys: Vec<Vec<u8>>,
	pub key_identities: Vec<Identity>,
	pub localities: Vec<Locality>,
}

pub fn validate_table_write_request_batch(
	meta: &TableWriteRequestMeta,
	batch: &RecordBatch,
	descriptor: &TableSchemaDescriptorV1,
) -> Result<TableWriteAdmission, TableWriteValidationError> {
	descriptor.validate().map_err(|err| {
		TableWriteValidationError::new(
			WpbcRejectCode::WPBC_REJECT_SCHEMA_UNKNOWN,
			format!("invalid table schema descriptor: {err}"),
		)
	})?;
	if descriptor.table_object_id != meta.table_object_id {
		return Err(TableWriteValidationError::new(
			WpbcRejectCode::WPBC_REJECT_SCHEMA_UNKNOWN,
			"table write references a different table_object_id than the descriptor",
		));
	}
	if descriptor.schema_generation != meta.schema_generation {
		return Err(TableWriteValidationError::new(
			WpbcRejectCode::WPBC_REJECT_SCHEMA_STALE,
			format!(
				"table write schema_generation {} does not match descriptor generation {}",
				meta.schema_generation, descriptor.schema_generation
			),
		));
	}
	let expected_rows = meta.decoded_row_count();
	if batch.num_rows() == 0 || batch.num_rows() > TABLE_WRITE_MAX_ROWS {
		return Err(TableWriteValidationError::new(
			WpbcRejectCode::WPBC_REJECT_BATCH_TOO_LARGE,
			format!(
				"table write row_count {} is outside 1..={TABLE_WRITE_MAX_ROWS}",
				batch.num_rows()
			),
		));
	}
	if batch.num_rows() != expected_rows {
		return Err(TableWriteValidationError::new(
			WpbcRejectCode::WPBC_REJECT_BATCH_TOO_LARGE,
			format!(
				"table write row_count mismatch: meta={} actual={}",
				expected_rows,
				batch.num_rows()
			),
		));
	}

	let schema = descriptor.arrow_schema().map_err(|err| {
		TableWriteValidationError::new(
			WpbcRejectCode::WPBC_REJECT_COLUMN_TYPE_MISMATCH,
			format!("descriptor Arrow schema cannot be decoded: {err}"),
		)
	})?;
	let primary_key_column = descriptor.primary_key.columns.first().ok_or_else(|| {
		TableWriteValidationError::new(
			WpbcRejectCode::WPBC_REJECT_PK_MISSING,
			"table descriptor does not declare a primary key column",
		)
	})?;
	if batch.column_by_name(primary_key_column).is_none() {
		return Err(TableWriteValidationError::new(
			WpbcRejectCode::WPBC_REJECT_PK_MISSING,
			format!("table write is missing primary key column {primary_key_column:?}"),
		));
	}
	if batch.schema().as_ref() != &schema {
		return Err(TableWriteValidationError::new(
			WpbcRejectCode::WPBC_REJECT_COLUMN_TYPE_MISMATCH,
			"table write Arrow schema does not match descriptor schema",
		));
	}

	let canonical_primary_keys = canonicalize_primary_key_column(batch, descriptor)?;
	let mut seen = BTreeSet::new();
	for pk in &canonical_primary_keys {
		if !seen.insert(pk.clone()) {
			return Err(TableWriteValidationError::new(
				WpbcRejectCode::WPBC_REJECT_DUPLICATE_PK_IN_BATCH,
				"table write contains duplicate primary key values",
			));
		}
	}

	let mut key_identities = Vec::with_capacity(canonical_primary_keys.len());
	let mut localities = Vec::with_capacity(canonical_primary_keys.len());
	for (row, pk) in canonical_primary_keys.iter().enumerate() {
		let key_identity = table_row_identity(meta.tenant_id, meta.table_object_id, pk);
		let locality = match descriptor.locality_policy {
			LocalityPolicyV1::IdentityProjection => Locality::from(key_identity),
			LocalityPolicyV1::TableSpreadProjection(spread) => {
				table_spread_locality(meta.tenant_id, meta.table_object_id, key_identity, spread)
					.map_err(|err| {
						TableWriteValidationError::new(
							WpbcRejectCode::WPBC_REJECT_LOCALITY_UNSUPPORTED,
							format!("table spread locality failed: {err}"),
						)
					})?
			}
			LocalityPolicyV1::ColumnProjection(ref projection) => column_projection_locality(
				meta.tenant_id,
				meta.table_object_id,
				key_identity,
				projection,
				batch,
				row,
			)?,
		};
		key_identities.push(key_identity);
		localities.push(locality);
	}

	Ok(TableWriteAdmission {
		canonical_primary_keys,
		key_identities,
		localities,
	})
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableSchemaDescriptorV1 {
	pub contract_version: u16,
	pub table_object_id: TableObjectId,
	pub schema_generation: u64,
	pub namespace: String,
	pub table_name: String,
	#[serde(with = "serde_bytes")]
	pub arrow_schema_ipc: Vec<u8>,
	pub primary_key: PrimaryKeyDescriptorV1,
	pub locality_policy: LocalityPolicyV1,
	#[serde(default)]
	pub stats_policy: ColumnStatsPolicyV1,
	pub schema_fingerprint: [u8; 32],
	pub evolution_policy: SchemaEvolutionPolicyV1,
}

impl TableSchemaDescriptorV1 {
	pub fn new(
		table_object_id: TableObjectId,
		schema_generation: u64,
		namespace: impl Into<String>,
		table_name: impl Into<String>,
		arrow_schema: &Schema,
		primary_key: PrimaryKeyDescriptorV1,
		locality_policy: LocalityPolicyV1,
		evolution_policy: SchemaEvolutionPolicyV1,
	) -> std::io::Result<Self> {
		let mut descriptor = Self {
			contract_version: TABLE_SCHEMA_DESCRIPTOR_VERSION,
			table_object_id,
			schema_generation,
			namespace: namespace.into(),
			table_name: table_name.into(),
			arrow_schema_ipc: encode_arrow_schema_ipc(arrow_schema)?,
			primary_key,
			locality_policy,
			stats_policy: ColumnStatsPolicyV1::default(),
			schema_fingerprint: [0; 32],
			evolution_policy,
		};
		descriptor.schema_fingerprint = descriptor.compute_schema_fingerprint()?;
		descriptor.validate()?;
		Ok(descriptor)
	}

	pub fn arrow_schema(&self) -> std::io::Result<Schema> {
		decode_arrow_schema_ipc(&self.arrow_schema_ipc)
	}

	pub fn compute_schema_fingerprint(&self) -> std::io::Result<[u8; 32]> {
		let mut canonical = self.clone();
		canonical.schema_fingerprint = [0; 32];
		let bytes = postcard::to_allocvec(&canonical).map_err(|err| {
			Error::new(
				ErrorKind::InvalidData,
				format!("failed to encode table schema descriptor: {err}"),
			)
		})?;
		Ok(crate::hash::blake3::digest(&bytes))
	}

	pub fn validate(&self) -> std::io::Result<()> {
		if self.contract_version != TABLE_SCHEMA_DESCRIPTOR_VERSION {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"unsupported table schema descriptor version",
			));
		}
		validate_descriptor_name("namespace", self.namespace.as_str())?;
		validate_descriptor_name("table name", self.table_name.as_str())?;
		let expected = self.compute_schema_fingerprint()?;
		if self.schema_fingerprint != expected {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"table schema descriptor fingerprint mismatch",
			));
		}
		let schema = self.arrow_schema()?;
		validate_primary_key(&schema, &self.primary_key)?;
		validate_locality_policy(&schema, &self.locality_policy)?;
		validate_stats_policy(&schema, &self.stats_policy)?;
		Ok(())
	}
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrimaryKeyDescriptorV1 {
	pub columns: Vec<String>,
	pub canonical_encoding: PrimaryKeyEncodingV1,
}

impl PrimaryKeyDescriptorV1 {
	pub fn single(column: impl Into<String>) -> Self {
		Self {
			columns: vec![column.into()],
			canonical_encoding: PrimaryKeyEncodingV1::ArrowV1,
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrimaryKeyEncodingV1 {
	ArrowV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LocalityPolicyV1 {
	TableSpreadProjection(TableSpreadLocalityDescriptorV1),
	IdentityProjection,
	ColumnProjection(ColumnLocalityDescriptorV1),
}

impl LocalityPolicyV1 {
	pub const fn default_table_spread() -> Self {
		Self::TableSpreadProjection(TableSpreadLocalityDescriptorV1 {
			spread_factor: TABLE_SPREAD_DEFAULT_FACTOR,
			prefix_bits: TABLE_SPREAD_DEFAULT_PREFIX_BITS,
		})
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableSpreadLocalityDescriptorV1 {
	pub spread_factor: u16,
	pub prefix_bits: u8,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnLocalityDescriptorV1 {
	pub columns: Vec<ClusteringColumnSpecV1>,
	pub projection: ProjectionFunctionV1,
	pub spread_factor: u16,
	pub prefix_bits: u8,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusteringColumnSpecV1 {
	pub name: String,
	pub width_bits: u8,
	pub string_prefix_bytes: Option<u8>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnStatsPolicyV1 {
	pub bloom_filters: Vec<ColumnBloomSpecV1>,
	pub hll_distincts: Vec<ColumnHllSpecV1>,
	pub small_sets: Vec<ColumnSmallSetSpecV1>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnBloomSpecV1 {
	pub column_name: String,
	pub bits_per_row: BloomBitsPerRow,
}

impl ColumnBloomSpecV1 {
	pub fn new(column_name: impl Into<String>) -> Self {
		Self {
			column_name: column_name.into(),
			bits_per_row: BloomBitsPerRow::DEFAULT,
		}
	}

	pub fn with_bits_per_row(
		column_name: impl Into<String>,
		bits_per_row: u8,
	) -> std::io::Result<Self> {
		Ok(Self {
			column_name: column_name.into(),
			bits_per_row: BloomBitsPerRow::new(bits_per_row)?,
		})
	}
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnHllSpecV1 {
	pub column_name: String,
	pub precision: HllPrecision,
}

impl ColumnHllSpecV1 {
	pub fn new(column_name: impl Into<String>) -> Self {
		Self {
			column_name: column_name.into(),
			precision: HllPrecision::DEFAULT,
		}
	}

	pub fn with_precision(column_name: impl Into<String>, precision: u8) -> std::io::Result<Self> {
		Ok(Self {
			column_name: column_name.into(),
			precision: HllPrecision::new(precision)?,
		})
	}
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnSmallSetSpecV1 {
	pub column_name: String,
	pub max_distinct_values: SmallSetValueCap,
	pub max_bytes: SmallSetByteCap,
}

impl ColumnSmallSetSpecV1 {
	pub fn new(column_name: impl Into<String>) -> Self {
		Self {
			column_name: column_name.into(),
			max_distinct_values: SmallSetValueCap::DEFAULT,
			max_bytes: SmallSetByteCap::DEFAULT,
		}
	}

	pub fn with_caps(
		column_name: impl Into<String>,
		max_distinct_values: u32,
		max_bytes: u32,
	) -> std::io::Result<Self> {
		Ok(Self {
			column_name: column_name.into(),
			max_distinct_values: SmallSetValueCap::new(max_distinct_values)?,
			max_bytes: SmallSetByteCap::new(max_bytes)?,
		})
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[repr(transparent)]
#[serde(transparent)]
pub struct BloomBitsPerRow(u8);

impl BloomBitsPerRow {
	pub const DEFAULT: Self = Self(COLUMN_STATS_BLOOM_DEFAULT_BITS_PER_ROW);

	pub fn new(bits_per_row: u8) -> std::io::Result<Self> {
		let value = Self(bits_per_row);
		value.validate()?;
		Ok(value)
	}

	pub const fn get(self) -> u8 {
		self.0
	}

	fn validate(self) -> std::io::Result<()> {
		if !(COLUMN_STATS_BLOOM_MIN_BITS_PER_ROW..=COLUMN_STATS_BLOOM_MAX_BITS_PER_ROW)
			.contains(&self.0)
		{
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"bloom bits_per_row must be in {}..={}",
					COLUMN_STATS_BLOOM_MIN_BITS_PER_ROW, COLUMN_STATS_BLOOM_MAX_BITS_PER_ROW
				),
			));
		}
		Ok(())
	}
}

impl Default for BloomBitsPerRow {
	fn default() -> Self {
		Self::DEFAULT
	}
}

impl From<BloomBitsPerRow> for u8 {
	fn from(value: BloomBitsPerRow) -> Self {
		value.get()
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[repr(transparent)]
#[serde(transparent)]
pub struct HllPrecision(u8);

impl HllPrecision {
	pub const DEFAULT: Self = Self(COLUMN_STATS_HLL_DEFAULT_PRECISION);

	pub fn new(precision: u8) -> std::io::Result<Self> {
		let value = Self(precision);
		value.validate()?;
		Ok(value)
	}

	pub const fn get(self) -> u8 {
		self.0
	}

	fn validate(self) -> std::io::Result<()> {
		if !(COLUMN_STATS_HLL_MIN_PRECISION..=COLUMN_STATS_HLL_MAX_PRECISION).contains(&self.0) {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"HLL precision must be in {}..={}",
					COLUMN_STATS_HLL_MIN_PRECISION, COLUMN_STATS_HLL_MAX_PRECISION
				),
			));
		}
		Ok(())
	}
}

impl Default for HllPrecision {
	fn default() -> Self {
		Self::DEFAULT
	}
}

impl From<HllPrecision> for u8 {
	fn from(value: HllPrecision) -> Self {
		value.get()
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[repr(transparent)]
#[serde(transparent)]
pub struct SmallSetValueCap(u32);

impl SmallSetValueCap {
	pub const DEFAULT: Self = Self(COLUMN_STATS_SMALL_SET_DEFAULT_MAX_DISTINCT_VALUES);

	pub fn new(max_distinct_values: u32) -> std::io::Result<Self> {
		let value = Self(max_distinct_values);
		value.validate()?;
		Ok(value)
	}

	pub const fn get(self) -> u32 {
		self.0
	}

	fn validate(self) -> std::io::Result<()> {
		if self.0 == 0 || self.0 > COLUMN_STATS_SMALL_SET_MAX_DISTINCT_VALUES {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"small-set max_distinct_values must be in 1..={}",
					COLUMN_STATS_SMALL_SET_MAX_DISTINCT_VALUES
				),
			));
		}
		Ok(())
	}
}

impl Default for SmallSetValueCap {
	fn default() -> Self {
		Self::DEFAULT
	}
}

impl From<SmallSetValueCap> for u32 {
	fn from(value: SmallSetValueCap) -> Self {
		value.get()
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[repr(transparent)]
#[serde(transparent)]
pub struct SmallSetByteCap(u32);

impl SmallSetByteCap {
	pub const DEFAULT: Self = Self(COLUMN_STATS_SMALL_SET_DEFAULT_MAX_BYTES);

	pub fn new(max_bytes: u32) -> std::io::Result<Self> {
		let value = Self(max_bytes);
		value.validate()?;
		Ok(value)
	}

	pub const fn get(self) -> u32 {
		self.0
	}

	fn validate(self) -> std::io::Result<()> {
		if self.0 == 0 || self.0 > COLUMN_STATS_SMALL_SET_MAX_BYTES {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"small-set max_bytes must be in 1..={}",
					COLUMN_STATS_SMALL_SET_MAX_BYTES
				),
			));
		}
		Ok(())
	}
}

impl Default for SmallSetByteCap {
	fn default() -> Self {
		Self::DEFAULT
	}
}

impl From<SmallSetByteCap> for u32 {
	fn from(value: SmallSetByteCap) -> Self {
		value.get()
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionFunctionV1 {
	RandomHash,
	ZOrder,
	Hilbert,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchemaEvolutionPolicyV1 {
	AdditiveOnly,
}

pub fn default_kv_table_descriptor_v1() -> std::io::Result<TableSchemaDescriptorV1> {
	TableSchemaDescriptorV1::new(
		TableObjectId::DEFAULT_KV,
		0,
		"default",
		"kv",
		&write_payload_schema(),
		PrimaryKeyDescriptorV1::single("key"),
		LocalityPolicyV1::IdentityProjection,
		SchemaEvolutionPolicyV1::AdditiveOnly,
	)
}

pub fn encode_table_schema_descriptor_v1(
	descriptor: &TableSchemaDescriptorV1,
) -> std::io::Result<Vec<u8>> {
	descriptor.validate()?;
	postcard::to_allocvec(descriptor).map_err(|err| {
		Error::new(
			ErrorKind::InvalidData,
			format!("failed to encode table schema descriptor: {err}"),
		)
	})
}

pub fn decode_table_schema_descriptor_v1(bytes: &[u8]) -> std::io::Result<TableSchemaDescriptorV1> {
	let descriptor: TableSchemaDescriptorV1 = postcard::from_bytes(bytes).map_err(|err| {
		Error::new(
			ErrorKind::InvalidData,
			format!("failed to decode table schema descriptor: {err}"),
		)
	})?;
	descriptor.validate()?;
	Ok(descriptor)
}

pub fn validate_table_schema_evolution(
	previous: &TableSchemaDescriptorV1,
	next: &TableSchemaDescriptorV1,
) -> std::io::Result<()> {
	previous.validate()?;
	next.validate()?;
	if previous.table_object_id != next.table_object_id {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"table schema alter cannot change table_object_id",
		));
	}
	if previous.namespace != next.namespace || previous.table_name != next.table_name {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"table schema alter cannot rename the table in V1",
		));
	}
	if next.schema_generation <= previous.schema_generation {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"table schema alter must advance schema_generation",
		));
	}
	if previous.primary_key != next.primary_key {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"table schema alter cannot change the primary key in V1",
		));
	}
	if previous.locality_policy != next.locality_policy {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"table schema alter cannot change locality_policy in V1",
		));
	}
	if previous.evolution_policy != next.evolution_policy {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"table schema alter cannot change evolution_policy in V1",
		));
	}

	let previous_schema = previous.arrow_schema()?;
	let next_schema = next.arrow_schema()?;
	if previous_schema.metadata() != next_schema.metadata() {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"table schema alter cannot change schema metadata in V1",
		));
	}
	let previous_fields = previous_schema.fields();
	let next_fields = next_schema.fields();
	if next_fields.len() < previous_fields.len() {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"table schema alter cannot drop columns in V1",
		));
	}
	for (idx, previous_field) in previous_fields.iter().enumerate() {
		let next_field = next_fields.get(idx).ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				"table schema alter cannot drop columns in V1",
			)
		})?;
		if previous_field.name() != next_field.name() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"table schema alter cannot reorder or rename column {:?}",
					previous_field.name()
				),
			));
		}
		if previous_field.data_type() != next_field.data_type() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"table schema alter cannot change type of column {:?}",
					previous_field.name()
				),
			));
		}
		if previous_field.metadata() != next_field.metadata() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"table schema alter cannot change metadata for column {:?}",
					previous_field.name()
				),
			));
		}
		if previous_field.is_nullable() && !next_field.is_nullable() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"table schema alter cannot tighten nullability for column {:?}",
					previous_field.name()
				),
			));
		}
	}
	for field in next_fields.iter().skip(previous_fields.len()) {
		if !field.is_nullable() && !field_has_default(field.as_ref()) {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"new non-null column {:?} requires {TABLE_FIELD_DEFAULT_METADATA_KEY} metadata",
					field.name()
				),
			));
		}
		if let Some(default) = field.metadata().get(TABLE_FIELD_DEFAULT_METADATA_KEY) {
			if default.is_empty() {
				return Err(Error::new(
					ErrorKind::InvalidData,
					format!(
						"new column {:?} has empty {TABLE_FIELD_DEFAULT_METADATA_KEY} metadata",
						field.name()
					),
				));
			}
		}
	}
	Ok(())
}

fn field_has_default(field: &arrow::datatypes::Field) -> bool {
	field
		.metadata()
		.contains_key(TABLE_FIELD_DEFAULT_METADATA_KEY)
}

pub fn encode_arrow_schema_ipc(schema: &Schema) -> std::io::Result<Vec<u8>> {
	let mut bytes = Vec::new();
	{
		let mut writer = StreamWriter::try_new(&mut bytes, schema).map_err(|err| {
			Error::new(
				ErrorKind::InvalidData,
				format!("arrow schema stream init: {err}"),
			)
		})?;
		writer.finish().map_err(|err| {
			Error::new(
				ErrorKind::InvalidData,
				format!("arrow schema stream finish: {err}"),
			)
		})?;
	}
	Ok(bytes)
}

pub fn decode_arrow_schema_ipc(bytes: &[u8]) -> std::io::Result<Schema> {
	let reader = StreamReader::try_new(Cursor::new(bytes), None).map_err(|err| {
		Error::new(
			ErrorKind::InvalidData,
			format!("arrow schema stream open: {err}"),
		)
	})?;
	Ok(reader.schema().as_ref().clone())
}

pub fn table_row_identity(
	tenant_id: TenantId,
	table_object_id: TableObjectId,
	canonical_pk_value: &[u8],
) -> Identity {
	let mut hasher = blake3::Hasher::new();
	hasher.update(TABLE_ROW_IDENTITY_DOMAIN);
	hasher.update(&tenant_id.get().to_le_bytes());
	hasher.update(table_object_id.as_bytes());
	hasher.update(canonical_pk_value);
	Identity::new(*hasher.finalize().as_bytes())
}

pub fn table_spread_locality(
	tenant_id: TenantId,
	table_object_id: TableObjectId,
	key_identity: Identity,
	spread: TableSpreadLocalityDescriptorV1,
) -> std::io::Result<Locality> {
	validate_table_spread(spread)?;
	let bucket = jump_consistent_hash(identity_u64(key_identity), spread.spread_factor);
	let table_hash = table_spread_prefix_hash(tenant_id, table_object_id, bucket);
	let row_hash = identity_u64(key_identity);
	let (prefix_mask, suffix_mask) = locality_masks(spread.prefix_bits)?;
	Ok(Locality::new(
		(table_hash & prefix_mask) | (row_hash & suffix_mask),
	))
}

pub fn table_spread_prefix_ranges(
	tenant_id: TenantId,
	table_object_id: TableObjectId,
	spread: TableSpreadLocalityDescriptorV1,
) -> std::io::Result<Vec<(Locality, Locality)>> {
	validate_table_spread(spread)?;
	let (prefix_mask, suffix_mask) = locality_masks(spread.prefix_bits)?;
	let mut prefixes = BTreeSet::new();
	for bucket in 0..spread.spread_factor {
		prefixes.insert(table_spread_prefix_hash(tenant_id, table_object_id, bucket) & prefix_mask);
	}
	Ok(prefixes
		.into_iter()
		.map(|prefix| (Locality::new(prefix), Locality::new(prefix | suffix_mask)))
		.collect())
}

pub fn column_projection_prefix_ranges(
	tenant_id: TenantId,
	table_object_id: TableObjectId,
	projection: &ColumnLocalityDescriptorV1,
) -> std::io::Result<Vec<(Locality, Locality)>> {
	table_spread_prefix_ranges(
		tenant_id,
		table_object_id,
		TableSpreadLocalityDescriptorV1 {
			spread_factor: projection.spread_factor,
			prefix_bits: projection.prefix_bits,
		},
	)
}

pub fn column_projection_predicate_ranges(
	tenant_id: TenantId,
	table_object_id: TableObjectId,
	schema: &Schema,
	projection: &ColumnLocalityDescriptorV1,
	program: &WpbcProgram,
	arc_budget: usize,
) -> std::io::Result<ColumnProjectionRangePruning> {
	validate_column_projection(schema, projection)?;
	if projection.projection != ProjectionFunctionV1::ZOrder {
		return Ok(ColumnProjectionRangePruning::Fallback);
	}
	let Some(predicates) = program.conjunctive_column_predicates() else {
		return Ok(ColumnProjectionRangePruning::Fallback);
	};
	let widths: Vec<u8> = projection
		.columns
		.iter()
		.map(|column| column.width_bits)
		.collect();
	let clustering_columns: Vec<(usize, &ClusteringColumnSpecV1, usize)> = projection
		.columns
		.iter()
		.enumerate()
		.map(|(dim, column)| {
			let column_index = schema.index_of(&column.name).map_err(|err| {
				Error::new(
					ErrorKind::InvalidData,
					format!("locality column {:?} cannot be indexed: {err}", column.name),
				)
			})?;
			Ok((dim, column, column_index))
		})
		.collect::<std::io::Result<_>>()?;
	let mut lower = vec![0_u64; projection.columns.len()];
	let mut upper: Vec<u64> = widths
		.iter()
		.map(|width| low_mask(u32::from(*width)))
		.collect();
	let mut allowed_values: Vec<Option<BTreeSet<u64>>> =
		projection.columns.iter().map(|_| None).collect();
	let mut constrained = false;
	for predicate in predicates {
		for (dim, column, column_index) in &clustering_columns {
			if *column_index != predicate.column_index {
				continue;
			}
			let field = schema.field(*column_index);
			match predicate.comparison {
				ColumnPredicateComparisonV1::Eq => {
					let Some(value) =
						normalize_clustering_literal(field.data_type(), &predicate.literal, column)
					else {
						return Ok(ColumnProjectionRangePruning::Fallback);
					};
					constrained = true;
					lower[*dim] = lower[*dim].max(value);
					upper[*dim] = upper[*dim].min(value);
					let mut values = BTreeSet::new();
					values.insert(value);
					restrict_allowed_values(&mut allowed_values[*dim], values);
				}
				ColumnPredicateComparisonV1::Gt | ColumnPredicateComparisonV1::Gte => {
					let Some(value) =
						normalize_clustering_literal(field.data_type(), &predicate.literal, column)
					else {
						return Ok(ColumnProjectionRangePruning::Fallback);
					};
					constrained = true;
					lower[*dim] = lower[*dim].max(value);
				}
				ColumnPredicateComparisonV1::Lt | ColumnPredicateComparisonV1::Lte => {
					let Some(value) =
						normalize_clustering_literal(field.data_type(), &predicate.literal, column)
					else {
						return Ok(ColumnProjectionRangePruning::Fallback);
					};
					constrained = true;
					upper[*dim] = upper[*dim].min(value);
				}
				ColumnPredicateComparisonV1::InList => {
					let ColumnPredicateLiteralV1::List(literals) = &predicate.literal else {
						return Ok(ColumnProjectionRangePruning::Fallback);
					};
					constrained = true;
					let mut values = BTreeSet::new();
					for literal in literals {
						let Some(value) =
							normalize_clustering_literal(field.data_type(), literal, column)
						else {
							return Ok(ColumnProjectionRangePruning::Fallback);
						};
						values.insert(value);
					}
					restrict_allowed_values(&mut allowed_values[*dim], values);
				}
			}
		}
	}
	if !constrained {
		return Ok(ColumnProjectionRangePruning::Fallback);
	}
	if lower.iter().zip(&upper).any(|(lo, hi)| lo > hi) {
		return Ok(ColumnProjectionRangePruning::Pruned(Vec::new()));
	}
	for (dim, values) in allowed_values.iter_mut().enumerate() {
		if let Some(values) = values {
			values.retain(|value| lower[dim] <= *value && *value <= upper[dim]);
			if values.is_empty() {
				return Ok(ColumnProjectionRangePruning::Pruned(Vec::new()));
			}
		}
	}
	let table_prefixes = column_projection_table_prefixes(tenant_id, table_object_id, projection)?;
	let prefix_count = table_prefixes.len().max(1);
	let suffix_budget = (arc_budget / prefix_count).max(1);
	let suffix_arcs = match decompose_z_order_ranges_with_allowed_values(
		&lower,
		&upper,
		&widths,
		&allowed_values,
		suffix_budget,
	) {
		Ok(arcs) => arcs,
		Err(RangeDecompositionOverflow) => return Ok(ColumnProjectionRangePruning::Overflow),
	};
	let suffix_width = clustering_suffix_width(&widths);
	let entropy_bits = 64 - u32::from(projection.prefix_bits) - u32::from(suffix_width);
	let entropy_mask = low_mask(entropy_bits);
	let mut ranges = Vec::new();
	for table_prefix in table_prefixes {
		for (suffix_start, suffix_end) in &suffix_arcs {
			if ranges.len() >= arc_budget {
				return Ok(ColumnProjectionRangePruning::Overflow);
			}
			ranges.push((
				Locality::new(table_prefix | (suffix_start << entropy_bits)),
				Locality::new(table_prefix | (suffix_end << entropy_bits) | entropy_mask),
			));
		}
	}
	Ok(ColumnProjectionRangePruning::Pruned(ranges))
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ColumnProjectionRangePruning {
	Pruned(Vec<(Locality, Locality)>),
	Fallback,
	Overflow,
}

fn restrict_allowed_values(slot: &mut Option<BTreeSet<u64>>, values: BTreeSet<u64>) {
	if let Some(existing) = slot {
		existing.retain(|value| values.contains(value));
	} else {
		*slot = Some(values);
	}
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableSchemaCatalogV1 {
	#[serde(default)]
	by_id: BTreeMap<TableSchemaCatalogKey, TableSchemaDescriptorV1>,
	#[serde(default)]
	latest_by_name: BTreeMap<TableNameCatalogKey, TableSchemaCatalogKey>,
}

impl TableSchemaCatalogV1 {
	pub fn upsert(
		&mut self,
		tenant_id: TenantId,
		descriptor: TableSchemaDescriptorV1,
	) -> std::io::Result<()> {
		descriptor.validate()?;
		let key = TableSchemaCatalogKey {
			tenant_id,
			table_object_id: descriptor.table_object_id,
			schema_generation: descriptor.schema_generation,
		};
		let name_key = TableNameCatalogKey {
			tenant_id,
			namespace: descriptor.namespace.clone(),
			table_name: descriptor.table_name.clone(),
		};
		if let Some(existing) = self.by_id.get(&key) {
			if existing == &descriptor {
				self.latest_by_name.insert(name_key, key);
				return Ok(());
			}
			return Err(Error::new(
				ErrorKind::InvalidData,
				"table schema catalog key already exists with a different descriptor",
			));
		}
		if let Some(existing) =
			self.latest_by_name(tenant_id, &descriptor.namespace, &descriptor.table_name)
		{
			if existing.table_object_id != descriptor.table_object_id {
				return Err(Error::new(
					ErrorKind::InvalidData,
					"table schema name is already bound to a different table_object_id",
				));
			}
			if existing.schema_generation > descriptor.schema_generation {
				return Err(Error::new(
					ErrorKind::InvalidData,
					"table schema catalog cannot regress schema_generation for a name",
				));
			}
		}
		if let Some(existing) = self.latest_by_id(tenant_id, descriptor.table_object_id) {
			if existing.namespace != descriptor.namespace
				|| existing.table_name != descriptor.table_name
			{
				return Err(Error::new(
					ErrorKind::InvalidData,
					"table_object_id is already bound to a different table name",
				));
			}
			if existing.schema_generation > descriptor.schema_generation {
				return Err(Error::new(
					ErrorKind::InvalidData,
					"table schema catalog cannot regress schema_generation for a table_object_id",
				));
			}
		}
		self.latest_by_name.insert(name_key, key.clone());
		self.by_id.insert(key, descriptor);
		Ok(())
	}

	pub fn get(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
		schema_generation: u64,
	) -> Option<&TableSchemaDescriptorV1> {
		self.by_id.get(&TableSchemaCatalogKey {
			tenant_id,
			table_object_id,
			schema_generation,
		})
	}

	pub fn latest_by_name(
		&self,
		tenant_id: TenantId,
		namespace: &str,
		table_name: &str,
	) -> Option<&TableSchemaDescriptorV1> {
		let key = self.latest_by_name.get(&TableNameCatalogKey {
			tenant_id,
			namespace: namespace.to_owned(),
			table_name: table_name.to_owned(),
		})?;
		self.by_id.get(key)
	}

	pub fn latest_by_id(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
	) -> Option<&TableSchemaDescriptorV1> {
		let start = TableSchemaCatalogKey {
			tenant_id,
			table_object_id,
			schema_generation: 0,
		};
		let end = TableSchemaCatalogKey {
			tenant_id,
			table_object_id,
			schema_generation: u64::MAX,
		};
		self.by_id
			.range(start..=end)
			.next_back()
			.map(|(_, descriptor)| descriptor)
	}

	pub fn descriptors_for_tenant(&self, tenant_id: TenantId) -> Vec<TableSchemaDescriptorV1> {
		let start = TableSchemaCatalogKey {
			tenant_id,
			table_object_id: TableObjectId::new([0; 16]),
			schema_generation: 0,
		};
		let end = TableSchemaCatalogKey {
			tenant_id,
			table_object_id: TableObjectId::new([0xFF; 16]),
			schema_generation: u64::MAX,
		};
		self.by_id
			.range(start..=end)
			.map(|(_, descriptor)| descriptor.clone())
			.collect()
	}

	pub fn len(&self) -> usize {
		self.by_id.len()
	}

	pub fn is_empty(&self) -> bool {
		self.by_id.is_empty()
	}
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
struct TableSchemaCatalogKey {
	tenant_id: TenantId,
	table_object_id: TableObjectId,
	schema_generation: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
struct TableNameCatalogKey {
	tenant_id: TenantId,
	namespace: String,
	table_name: String,
}

fn validate_descriptor_name(label: &str, value: &str) -> std::io::Result<()> {
	if value.is_empty() || value.len() > 128 {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!("{label} length is out of bounds"),
		));
	}
	if !value
		.bytes()
		.all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
	{
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!("{label} contains unsupported characters"),
		));
	}
	Ok(())
}

fn validate_primary_key(
	schema: &Schema,
	primary_key: &PrimaryKeyDescriptorV1,
) -> std::io::Result<()> {
	if primary_key.columns.len() != 1 {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"table schema V1 requires exactly one primary key column",
		));
	}
	let column = primary_key.columns[0].as_str();
	validate_descriptor_name("primary key column", column)?;
	let field = schema.field_with_name(column).map_err(|_| {
		Error::new(
			ErrorKind::InvalidData,
			format!("primary key column {column:?} is not present in Arrow schema"),
		)
	})?;
	if field.is_nullable() {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!("primary key column {column:?} must be non-nullable"),
		));
	}
	if !is_supported_primary_key_type(field.data_type()) {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"primary key column {column:?} uses unsupported V1 type {:?}",
				field.data_type()
			),
		));
	}
	Ok(())
}

fn validate_locality_policy(schema: &Schema, policy: &LocalityPolicyV1) -> std::io::Result<()> {
	match policy {
		LocalityPolicyV1::TableSpreadProjection(spread) => validate_table_spread(*spread),
		LocalityPolicyV1::IdentityProjection => Ok(()),
		LocalityPolicyV1::ColumnProjection(projection) => {
			validate_table_spread(TableSpreadLocalityDescriptorV1 {
				spread_factor: projection.spread_factor,
				prefix_bits: projection.prefix_bits,
			})?;
			validate_column_projection(schema, projection)
		}
	}
}

fn validate_column_projection(
	schema: &Schema,
	projection: &ColumnLocalityDescriptorV1,
) -> std::io::Result<()> {
	match projection.projection {
		ProjectionFunctionV1::RandomHash | ProjectionFunctionV1::ZOrder => {}
		ProjectionFunctionV1::Hilbert => {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"{}: Hilbert column locality is reserved for a future ADR",
					WpbcRejectCode::WPBC_REJECT_PROJECTION_NOT_YET_SUPPORTED
				),
			));
		}
	}
	if projection.columns.is_empty() {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"{}: column locality projection requires at least one column",
				WpbcRejectCode::WPBC_REJECT_CLUSTERING_EMPTY
			),
		));
	}
	if projection.columns.len() > TABLE_CLUSTERING_MAX_COLUMNS {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"{}: column locality projection supports at most {TABLE_CLUSTERING_MAX_COLUMNS} columns",
				WpbcRejectCode::WPBC_REJECT_CLUSTERING_TOO_WIDE
			),
		));
	}

	let mut seen = BTreeSet::new();
	let mut width_sum = u16::from(projection.prefix_bits);
	for column in &projection.columns {
		validate_descriptor_name("locality column", &column.name)?;
		if !seen.insert(column.name.clone()) {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"{}: duplicate locality column {:?}",
					WpbcRejectCode::WPBC_REJECT_CLUSTERING_DUPLICATE_COLUMN,
					column.name
				),
			));
		}
		if column.width_bits == 0 || column.width_bits > 64 {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"{}: locality column {:?} width_bits must be in 1..=64",
					WpbcRejectCode::WPBC_REJECT_CLUSTERING_WIDTH_INVALID,
					column.name
				),
			));
		}
		width_sum = width_sum.saturating_add(u16::from(column.width_bits));
		if width_sum > 64 {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"{}: sum(width_bits) + prefix_bits exceeds 64",
					WpbcRejectCode::WPBC_REJECT_CLUSTERING_WIDTH_OVERFLOW
				),
			));
		}
		let field = schema.field_with_name(&column.name).map_err(|_| {
			Error::new(
				ErrorKind::InvalidData,
				format!(
					"{}: locality column {:?} is not present in Arrow schema",
					WpbcRejectCode::WPBC_REJECT_CLUSTERING_COLUMN_UNKNOWN,
					column.name
				),
			)
		})?;
		if field.is_nullable() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"{}: locality column {:?} must be non-nullable",
					WpbcRejectCode::WPBC_REJECT_CLUSTERING_NULLABLE,
					column.name
				),
			));
		}
		validate_clustering_type_and_width(field.data_type(), column)?;
	}
	Ok(())
}

fn validate_stats_policy(schema: &Schema, policy: &ColumnStatsPolicyV1) -> std::io::Result<()> {
	validate_bloom_stats_policy(schema, &policy.bloom_filters)?;
	validate_hll_stats_policy(schema, &policy.hll_distincts)?;
	validate_small_set_stats_policy(schema, &policy.small_sets)?;
	validate_stats_policy_budget(schema, policy)?;
	Ok(())
}

pub const fn max_column_stats_block_ciphertext_bytes_for_slab_payload(
	slab_payload_bytes: u64,
) -> u64 {
	slab_payload_bytes / COLUMN_STATS_CIPHERTEXT_BUDGET_DIVISOR
}

fn validate_stats_policy_budget(
	schema: &Schema,
	policy: &ColumnStatsPolicyV1,
) -> std::io::Result<()> {
	let budget = max_column_stats_block_ciphertext_bytes_for_slab_payload(
		COLUMN_STATS_DESCRIPTOR_SLAB_PAYLOAD_BYTES,
	);
	let estimated =
		estimate_stats_policy_csb_ciphertext_bytes(schema, policy, TABLE_WRITE_MAX_ROWS as u64)?;
	if estimated > budget {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"{}: estimated CSB ciphertext size {estimated} exceeds descriptor budget {budget}",
				WpbcRejectCode::WPBC_REJECT_STATS_BUDGET_EXCEEDED
			),
		));
	}
	Ok(())
}

fn estimate_stats_policy_csb_ciphertext_bytes(
	schema: &Schema,
	policy: &ColumnStatsPolicyV1,
	row_capacity: u64,
) -> std::io::Result<u64> {
	let column_count = u64::try_from(schema.fields().len()).map_err(|_| {
		Error::new(
			ErrorKind::InvalidData,
			"stats schema field count exceeds u64::MAX",
		)
	})?;
	let mut bytes = COLUMN_STATS_CSB_AEAD_TAG_BYTES
		.checked_add(COLUMN_STATS_CSB_HEADER_BYTES)
		.and_then(|bytes| {
			bytes.checked_add(column_count.checked_mul(COLUMN_STATS_CSB_COLUMN_HEADER_BYTES)?)
		})
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "stats budget estimate overflow"))?;

	for field in schema.fields() {
		bytes = bytes
			.checked_add(estimated_always_on_column_stats_payload_bytes(
				field.data_type(),
			))
			.ok_or_else(|| Error::new(ErrorKind::InvalidData, "stats budget estimate overflow"))?;
	}
	for spec in &policy.bloom_filters {
		let bit_count = row_capacity
			.checked_mul(u64::from(spec.bits_per_row.get()))
			.ok_or_else(|| Error::new(ErrorKind::InvalidData, "stats bloom estimate overflow"))?;
		let bloom_bytes = 4_u64
			.checked_add(bit_count.div_ceil(8))
			.ok_or_else(|| Error::new(ErrorKind::InvalidData, "stats bloom estimate overflow"))?;
		bytes = bytes
			.checked_add(bloom_bytes)
			.ok_or_else(|| Error::new(ErrorKind::InvalidData, "stats budget estimate overflow"))?;
	}
	for spec in &policy.hll_distincts {
		let registers = 1_u64
			.checked_shl(u32::from(spec.precision.get()))
			.ok_or_else(|| Error::new(ErrorKind::InvalidData, "stats HLL estimate overflow"))?;
		bytes = bytes
			.checked_add(1_u64 + registers)
			.ok_or_else(|| Error::new(ErrorKind::InvalidData, "stats budget estimate overflow"))?;
	}
	for spec in &policy.small_sets {
		bytes = bytes
			.checked_add(u64::from(spec.max_bytes.get()))
			.ok_or_else(|| Error::new(ErrorKind::InvalidData, "stats budget estimate overflow"))?;
	}
	Ok(bytes)
}

fn estimated_always_on_column_stats_payload_bytes(data_type: &DataType) -> u64 {
	let Some(value_bytes) = estimated_canonical_stats_value_bytes(data_type) else {
		return 16;
	};
	2 * (4 + value_bytes) + 16
}

fn estimated_canonical_stats_value_bytes(data_type: &DataType) -> Option<u64> {
	match data_type {
		DataType::Int8 | DataType::UInt8 | DataType::Boolean => Some(1),
		DataType::Int16 | DataType::UInt16 => Some(2),
		DataType::Int32 | DataType::UInt32 | DataType::Date32 => Some(4),
		DataType::Int64 | DataType::UInt64 | DataType::Timestamp(_, _) | DataType::Date64 => {
			Some(8)
		}
		DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary => {
			Some(COLUMN_STATS_DEFAULT_STRING_PREFIX_BYTES)
		}
		_ => None,
	}
}

fn validate_bloom_stats_policy(
	schema: &Schema,
	specs: &[ColumnBloomSpecV1],
) -> std::io::Result<()> {
	let mut seen = BTreeSet::new();
	for spec in specs {
		validate_descriptor_name("stats bloom column", &spec.column_name)?;
		if !seen.insert(spec.column_name.clone()) {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"{}: duplicate bloom-filter stats column {:?}",
					WpbcRejectCode::WPBC_REJECT_STATS_DUPLICATE_COLUMN,
					spec.column_name
				),
			));
		}
		if let Err(err) = spec.bits_per_row.validate() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"{}: bloom bits_per_row for {:?} is invalid: {}",
					WpbcRejectCode::WPBC_REJECT_STATS_BITS_INVALID,
					spec.column_name,
					err
				),
			));
		}
		validate_stats_policy_column_type(schema, &spec.column_name, "bloom-filter")?;
	}
	Ok(())
}

fn validate_hll_stats_policy(schema: &Schema, specs: &[ColumnHllSpecV1]) -> std::io::Result<()> {
	let mut seen = BTreeSet::new();
	for spec in specs {
		validate_descriptor_name("stats HLL column", &spec.column_name)?;
		if !seen.insert(spec.column_name.clone()) {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"{}: duplicate HLL stats column {:?}",
					WpbcRejectCode::WPBC_REJECT_STATS_DUPLICATE_COLUMN,
					spec.column_name
				),
			));
		}
		if let Err(err) = spec.precision.validate() {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"{}: HLL precision for {:?} is invalid: {}",
					WpbcRejectCode::WPBC_REJECT_STATS_PRECISION_INVALID,
					spec.column_name,
					err
				),
			));
		}
		validate_stats_policy_column_type(schema, &spec.column_name, "HLL")?;
	}
	Ok(())
}

fn validate_small_set_stats_policy(
	schema: &Schema,
	specs: &[ColumnSmallSetSpecV1],
) -> std::io::Result<()> {
	let mut seen = BTreeSet::new();
	for spec in specs {
		validate_descriptor_name("stats small-set column", &spec.column_name)?;
		if !seen.insert(spec.column_name.clone()) {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"{}: duplicate small-set stats column {:?}",
					WpbcRejectCode::WPBC_REJECT_STATS_DUPLICATE_COLUMN,
					spec.column_name
				),
			));
		}
		if let Err(err) = spec
			.max_distinct_values
			.validate()
			.and_then(|()| spec.max_bytes.validate())
		{
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"{}: small-set stats limits for {:?} are invalid: {}",
					WpbcRejectCode::WPBC_REJECT_STATS_SMALLSET_INVALID,
					spec.column_name,
					err
				),
			));
		}
		validate_stats_policy_column_type(schema, &spec.column_name, "small-set")?;
	}
	Ok(())
}

fn validate_stats_policy_column_type(
	schema: &Schema,
	column_name: &str,
	stat_kind: &str,
) -> std::io::Result<()> {
	let field = schema.field_with_name(column_name).map_err(|_| {
		Error::new(
			ErrorKind::InvalidData,
			format!(
				"{}: {stat_kind} stats column {column_name:?} is not present in Arrow schema",
				WpbcRejectCode::WPBC_REJECT_STATS_COLUMN_UNKNOWN
			),
		)
	})?;
	if !is_supported_column_stats_type(field.data_type()) {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"{}: {stat_kind} stats column {column_name:?} uses unsupported V1 type {:?}",
				WpbcRejectCode::WPBC_REJECT_STATS_UNSUPPORTED_TYPE,
				field.data_type()
			),
		));
	}
	Ok(())
}

pub fn is_supported_column_stats_type(data_type: &DataType) -> bool {
	matches!(
		data_type,
		DataType::Int8
			| DataType::Int16
			| DataType::Int32
			| DataType::Int64
			| DataType::UInt8
			| DataType::UInt16
			| DataType::UInt32
			| DataType::UInt64
			| DataType::Boolean
			| DataType::Timestamp(_, _)
			| DataType::Date32
			| DataType::Date64
			| DataType::Utf8
			| DataType::LargeUtf8
			| DataType::Binary
			| DataType::LargeBinary
	)
}

fn validate_clustering_type_and_width(
	data_type: &DataType,
	column: &ClusteringColumnSpecV1,
) -> std::io::Result<()> {
	match data_type {
		DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary => {
			let Some(prefix_bytes) = column.string_prefix_bytes else {
				return Err(Error::new(
					ErrorKind::InvalidData,
					format!(
						"{}: locality column {:?} requires string_prefix_bytes",
						WpbcRejectCode::WPBC_REJECT_CLUSTERING_WIDTH_INVALID,
						column.name
					),
				));
			};
			if prefix_bytes == 0
				|| prefix_bytes > 8
				|| prefix_bytes.saturating_mul(8) != column.width_bits
			{
				return Err(Error::new(
					ErrorKind::InvalidData,
					format!(
						"{}: locality column {:?} string_prefix_bytes must match width_bits",
						WpbcRejectCode::WPBC_REJECT_CLUSTERING_WIDTH_INVALID,
						column.name
					),
				));
			}
			Ok(())
		}
		DataType::Int8
		| DataType::Int16
		| DataType::Int32
		| DataType::Int64
		| DataType::UInt8
		| DataType::UInt16
		| DataType::UInt32
		| DataType::UInt64
		| DataType::Timestamp(TimeUnit::Millisecond, _) => {
			let natural_bits = match data_type {
				DataType::Int8 | DataType::UInt8 => 8,
				DataType::Int16 | DataType::UInt16 => 16,
				DataType::Int32 | DataType::UInt32 => 32,
				DataType::Int64
				| DataType::UInt64
				| DataType::Timestamp(TimeUnit::Millisecond, _) => 64,
				_ => unreachable!("numeric locality type match drifted"),
			};
			if column.width_bits > natural_bits {
				return Err(Error::new(
					ErrorKind::InvalidData,
					format!(
						"{}: locality column {:?} width_bits exceeds natural type width",
						WpbcRejectCode::WPBC_REJECT_CLUSTERING_WIDTH_INVALID,
						column.name
					),
				));
			}
			if column.string_prefix_bytes.is_some() {
				return Err(Error::new(
					ErrorKind::InvalidData,
					format!(
						"{}: numeric locality column {:?} cannot declare string_prefix_bytes",
						WpbcRejectCode::WPBC_REJECT_CLUSTERING_WIDTH_INVALID,
						column.name
					),
				));
			}
			Ok(())
		}
		other => Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"{}: locality column {:?} uses unsupported V1 type {other:?}",
				WpbcRejectCode::WPBC_REJECT_CLUSTERING_TYPE_UNSUPPORTED,
				column.name
			),
		)),
	}
}

fn validate_table_spread(spread: TableSpreadLocalityDescriptorV1) -> std::io::Result<()> {
	if spread.spread_factor == 0 || spread.spread_factor > TABLE_SPREAD_MAX_FACTOR {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"table spread_factor must be in 1..={TABLE_SPREAD_MAX_FACTOR}, got {}",
				spread.spread_factor
			),
		));
	}
	if spread.prefix_bits == 0 || spread.prefix_bits >= 64 {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"table spread prefix_bits must be in 1..=63, got {}",
				spread.prefix_bits
			),
		));
	}
	Ok(())
}

fn is_supported_primary_key_type(data_type: &DataType) -> bool {
	matches!(
		data_type,
		DataType::Int8
			| DataType::Int16
			| DataType::Int32
			| DataType::Int64
			| DataType::UInt8
			| DataType::UInt16
			| DataType::UInt32
			| DataType::UInt64
			| DataType::Utf8
			| DataType::LargeUtf8
			| DataType::Binary
			| DataType::LargeBinary
			| DataType::Timestamp(TimeUnit::Millisecond, _)
	)
}

fn canonicalize_primary_key_column(
	batch: &RecordBatch,
	descriptor: &TableSchemaDescriptorV1,
) -> Result<Vec<Vec<u8>>, TableWriteValidationError> {
	if descriptor.primary_key.canonical_encoding != PrimaryKeyEncodingV1::ArrowV1 {
		return Err(TableWriteValidationError::new(
			WpbcRejectCode::WPBC_REJECT_PK_TYPE_UNSUPPORTED,
			"unsupported primary key canonical encoding",
		));
	}
	let column_name = descriptor
		.primary_key
		.columns
		.first()
		.ok_or_else(|| {
			TableWriteValidationError::new(
				WpbcRejectCode::WPBC_REJECT_PK_MISSING,
				"table descriptor does not declare a primary key column",
			)
		})?
		.as_str();
	let array = batch.column_by_name(column_name).ok_or_else(|| {
		TableWriteValidationError::new(
			WpbcRejectCode::WPBC_REJECT_PK_MISSING,
			format!("table write is missing primary key column {column_name:?}"),
		)
	})?;
	let mut out = Vec::with_capacity(batch.num_rows());
	for row in 0..batch.num_rows() {
		if array.is_null(row) {
			return Err(TableWriteValidationError::new(
				WpbcRejectCode::WPBC_REJECT_PK_NULL,
				format!("primary key column {column_name:?} is null at row {row}"),
			));
		}
		out.push(canonicalize_primary_key_value(array.as_ref(), row)?);
	}
	Ok(out)
}

fn canonicalize_primary_key_value(
	array: &dyn Array,
	row: usize,
) -> Result<Vec<u8>, TableWriteValidationError> {
	macro_rules! fixed_width {
		($array:expr, $ty:ty) => {{
			let array = $array.as_any().downcast_ref::<$ty>().ok_or_else(|| {
				TableWriteValidationError::new(
					WpbcRejectCode::WPBC_REJECT_PK_TYPE_UNSUPPORTED,
					format!(
						"primary key array downcast failed for {:?}",
						$array.data_type()
					),
				)
			})?;
			Ok(array.value(row).to_le_bytes().to_vec())
		}};
	}

	match array.data_type() {
		DataType::Int8 => fixed_width!(array, Int8Array),
		DataType::Int16 => fixed_width!(array, Int16Array),
		DataType::Int32 => fixed_width!(array, Int32Array),
		DataType::Int64 => fixed_width!(array, Int64Array),
		DataType::UInt8 => fixed_width!(array, UInt8Array),
		DataType::UInt16 => fixed_width!(array, UInt16Array),
		DataType::UInt32 => fixed_width!(array, UInt32Array),
		DataType::UInt64 => fixed_width!(array, UInt64Array),
		DataType::Utf8 => {
			let array = array
				.as_any()
				.downcast_ref::<StringArray>()
				.ok_or_else(|| {
					TableWriteValidationError::new(
						WpbcRejectCode::WPBC_REJECT_PK_TYPE_UNSUPPORTED,
						"Utf8 primary key array downcast failed",
					)
				})?;
			Ok(array.value(row).as_bytes().to_vec())
		}
		DataType::LargeUtf8 => {
			let array = array
				.as_any()
				.downcast_ref::<LargeStringArray>()
				.ok_or_else(|| {
					TableWriteValidationError::new(
						WpbcRejectCode::WPBC_REJECT_PK_TYPE_UNSUPPORTED,
						"LargeUtf8 primary key array downcast failed",
					)
				})?;
			Ok(array.value(row).as_bytes().to_vec())
		}
		DataType::Binary => {
			let array = array
				.as_any()
				.downcast_ref::<BinaryArray>()
				.ok_or_else(|| {
					TableWriteValidationError::new(
						WpbcRejectCode::WPBC_REJECT_PK_TYPE_UNSUPPORTED,
						"Binary primary key array downcast failed",
					)
				})?;
			Ok(array.value(row).to_vec())
		}
		DataType::LargeBinary => {
			let array = array
				.as_any()
				.downcast_ref::<LargeBinaryArray>()
				.ok_or_else(|| {
					TableWriteValidationError::new(
						WpbcRejectCode::WPBC_REJECT_PK_TYPE_UNSUPPORTED,
						"LargeBinary primary key array downcast failed",
					)
				})?;
			Ok(array.value(row).to_vec())
		}
		DataType::Timestamp(TimeUnit::Millisecond, _) => {
			let array = array
				.as_any()
				.downcast_ref::<TimestampMillisecondArray>()
				.ok_or_else(|| {
					TableWriteValidationError::new(
						WpbcRejectCode::WPBC_REJECT_PK_TYPE_UNSUPPORTED,
						"Timestamp(Millisecond) primary key array downcast failed",
					)
				})?;
			Ok(array.value(row).to_le_bytes().to_vec())
		}
		other => Err(TableWriteValidationError::new(
			WpbcRejectCode::WPBC_REJECT_PK_TYPE_UNSUPPORTED,
			format!("unsupported primary key type {other:?}"),
		)),
	}
}

fn identity_u64(identity: Identity) -> u64 {
	let mut bytes = [0; 8];
	bytes.copy_from_slice(&identity.as_bytes()[..8]);
	u64::from_le_bytes(bytes)
}

fn table_spread_prefix_hash(
	tenant_id: TenantId,
	table_object_id: TableObjectId,
	bucket: u16,
) -> u64 {
	let mut hasher = blake3::Hasher::new();
	hasher.update(TABLE_LOCALITY_DOMAIN);
	hasher.update(&tenant_id.get().to_le_bytes());
	hasher.update(table_object_id.as_bytes());
	hasher.update(&bucket.to_le_bytes());
	let digest = hasher.finalize();
	let mut bytes = [0; 8];
	bytes.copy_from_slice(&digest.as_bytes()[..8]);
	u64::from_le_bytes(bytes)
}

fn column_projection_locality(
	tenant_id: TenantId,
	table_object_id: TableObjectId,
	key_identity: Identity,
	projection: &ColumnLocalityDescriptorV1,
	batch: &RecordBatch,
	row: usize,
) -> Result<Locality, TableWriteValidationError> {
	let coords = normalize_clustering_row(batch, projection, row)?;
	let widths: Vec<u8> = projection
		.columns
		.iter()
		.map(|column| column.width_bits)
		.collect();
	let suffix_width = clustering_suffix_width(&widths);
	let z_suffix = match projection.projection {
		ProjectionFunctionV1::ZOrder => z_order_encode(&coords, &widths),
		ProjectionFunctionV1::RandomHash => random_hash_suffix(&coords, &widths, suffix_width),
		ProjectionFunctionV1::Hilbert => {
			return Err(TableWriteValidationError::new(
				WpbcRejectCode::WPBC_REJECT_PROJECTION_NOT_YET_SUPPORTED,
				"Hilbert column locality is reserved for a future ADR",
			));
		}
	};
	let bucket = jump_consistent_hash(identity_u64(key_identity), projection.spread_factor);
	let table_hash = table_spread_prefix_hash(tenant_id, table_object_id, bucket);
	let prefix_mask = u64::MAX << (64 - projection.prefix_bits);
	let entropy_bits = 64 - u32::from(projection.prefix_bits) - u32::from(suffix_width);
	let entropy_tail = identity_u64(key_identity) & low_mask(entropy_bits);
	let locality = (table_hash & prefix_mask) | (z_suffix << entropy_bits) | entropy_tail;
	Ok(Locality::new(locality))
}

fn column_projection_table_prefixes(
	tenant_id: TenantId,
	table_object_id: TableObjectId,
	projection: &ColumnLocalityDescriptorV1,
) -> std::io::Result<Vec<u64>> {
	validate_table_spread(TableSpreadLocalityDescriptorV1 {
		spread_factor: projection.spread_factor,
		prefix_bits: projection.prefix_bits,
	})?;
	let (prefix_mask, _) = locality_masks(projection.prefix_bits)?;
	let mut prefixes = BTreeSet::new();
	for bucket in 0..projection.spread_factor {
		prefixes.insert(table_spread_prefix_hash(tenant_id, table_object_id, bucket) & prefix_mask);
	}
	Ok(prefixes.into_iter().collect())
}

fn normalize_clustering_row(
	batch: &RecordBatch,
	projection: &ColumnLocalityDescriptorV1,
	row: usize,
) -> Result<Vec<u64>, TableWriteValidationError> {
	let mut coords = Vec::with_capacity(projection.columns.len());
	for column in &projection.columns {
		let array = batch.column_by_name(&column.name).ok_or_else(|| {
			TableWriteValidationError::new(
				WpbcRejectCode::WPBC_REJECT_CLUSTERING_COLUMN_UNKNOWN,
				format!("table write is missing clustering column {:?}", column.name),
			)
		})?;
		if array.is_null(row) {
			return Err(TableWriteValidationError::new(
				WpbcRejectCode::WPBC_REJECT_CLUSTERING_VALUE_NULL,
				format!("clustering column {:?} is null at row {row}", column.name),
			));
		}
		coords.push(normalize_clustering_value(array.as_ref(), row, column)?);
	}
	Ok(coords)
}

fn normalize_clustering_value(
	array: &dyn Array,
	row: usize,
	column: &ClusteringColumnSpecV1,
) -> Result<u64, TableWriteValidationError> {
	macro_rules! signed_width {
		($array:expr, $ty:ty, $bits:expr) => {{
			let array = $array.as_any().downcast_ref::<$ty>().ok_or_else(|| {
				TableWriteValidationError::new(
					WpbcRejectCode::WPBC_REJECT_CLUSTERING_TYPE_UNSUPPORTED,
					format!(
						"clustering array downcast failed for {:?}",
						$array.data_type()
					),
				)
			})?;
			Ok(normalize_signed_int(
				array.value(row) as i64,
				$bits,
				column.width_bits,
			))
		}};
	}
	macro_rules! unsigned_width {
		($array:expr, $ty:ty, $bits:expr) => {{
			let array = $array.as_any().downcast_ref::<$ty>().ok_or_else(|| {
				TableWriteValidationError::new(
					WpbcRejectCode::WPBC_REJECT_CLUSTERING_TYPE_UNSUPPORTED,
					format!(
						"clustering array downcast failed for {:?}",
						$array.data_type()
					),
				)
			})?;
			Ok(take_high_bits(
				array.value(row) as u64,
				$bits,
				column.width_bits,
			))
		}};
	}

	match array.data_type() {
		DataType::Int8 => signed_width!(array, Int8Array, 8),
		DataType::Int16 => signed_width!(array, Int16Array, 16),
		DataType::Int32 => signed_width!(array, Int32Array, 32),
		DataType::Int64 => signed_width!(array, Int64Array, 64),
		DataType::UInt8 => unsigned_width!(array, UInt8Array, 8),
		DataType::UInt16 => unsigned_width!(array, UInt16Array, 16),
		DataType::UInt32 => unsigned_width!(array, UInt32Array, 32),
		DataType::UInt64 => unsigned_width!(array, UInt64Array, 64),
		DataType::Timestamp(TimeUnit::Millisecond, _) => {
			signed_width!(array, TimestampMillisecondArray, 64)
		}
		DataType::Utf8 => {
			let array = array
				.as_any()
				.downcast_ref::<StringArray>()
				.ok_or_else(|| {
					TableWriteValidationError::new(
						WpbcRejectCode::WPBC_REJECT_CLUSTERING_TYPE_UNSUPPORTED,
						"Utf8 clustering array downcast failed",
					)
				})?;
			Ok(normalize_prefix_bytes(
				array.value(row).as_bytes(),
				column.string_prefix_bytes.unwrap_or(0),
			))
		}
		DataType::LargeUtf8 => {
			let array = array
				.as_any()
				.downcast_ref::<LargeStringArray>()
				.ok_or_else(|| {
					TableWriteValidationError::new(
						WpbcRejectCode::WPBC_REJECT_CLUSTERING_TYPE_UNSUPPORTED,
						"LargeUtf8 clustering array downcast failed",
					)
				})?;
			Ok(normalize_prefix_bytes(
				array.value(row).as_bytes(),
				column.string_prefix_bytes.unwrap_or(0),
			))
		}
		DataType::Binary => {
			let array = array
				.as_any()
				.downcast_ref::<BinaryArray>()
				.ok_or_else(|| {
					TableWriteValidationError::new(
						WpbcRejectCode::WPBC_REJECT_CLUSTERING_TYPE_UNSUPPORTED,
						"Binary clustering array downcast failed",
					)
				})?;
			Ok(normalize_prefix_bytes(
				array.value(row),
				column.string_prefix_bytes.unwrap_or(0),
			))
		}
		DataType::LargeBinary => {
			let array = array
				.as_any()
				.downcast_ref::<LargeBinaryArray>()
				.ok_or_else(|| {
					TableWriteValidationError::new(
						WpbcRejectCode::WPBC_REJECT_CLUSTERING_TYPE_UNSUPPORTED,
						"LargeBinary clustering array downcast failed",
					)
				})?;
			Ok(normalize_prefix_bytes(
				array.value(row),
				column.string_prefix_bytes.unwrap_or(0),
			))
		}
		other => Err(TableWriteValidationError::new(
			WpbcRejectCode::WPBC_REJECT_CLUSTERING_TYPE_UNSUPPORTED,
			format!("unsupported clustering column type {other:?}"),
		)),
	}
}

fn normalize_clustering_literal(
	data_type: &DataType,
	literal: &ColumnPredicateLiteralV1,
	column: &ClusteringColumnSpecV1,
) -> Option<u64> {
	match (data_type, literal) {
		(DataType::Int8, ColumnPredicateLiteralV1::Int(value))
			if (i8::MIN as i64..=i8::MAX as i64).contains(value) =>
		{
			Some(normalize_signed_int(*value, 8, column.width_bits))
		}
		(DataType::Int16, ColumnPredicateLiteralV1::Int(value))
			if (i16::MIN as i64..=i16::MAX as i64).contains(value) =>
		{
			Some(normalize_signed_int(*value, 16, column.width_bits))
		}
		(DataType::Int32, ColumnPredicateLiteralV1::Int(value))
			if (i32::MIN as i64..=i32::MAX as i64).contains(value) =>
		{
			Some(normalize_signed_int(*value, 32, column.width_bits))
		}
		(DataType::Int64, ColumnPredicateLiteralV1::Int(value)) => {
			Some(normalize_signed_int(*value, 64, column.width_bits))
		}
		(DataType::UInt8, ColumnPredicateLiteralV1::Int(value))
			if (0..=u8::MAX as i64).contains(value) =>
		{
			Some(take_high_bits(*value as u64, 8, column.width_bits))
		}
		(DataType::UInt16, ColumnPredicateLiteralV1::Int(value))
			if (0..=u16::MAX as i64).contains(value) =>
		{
			Some(take_high_bits(*value as u64, 16, column.width_bits))
		}
		(DataType::UInt32, ColumnPredicateLiteralV1::Int(value))
			if (0..=u32::MAX as i64).contains(value) =>
		{
			Some(take_high_bits(*value as u64, 32, column.width_bits))
		}
		(DataType::UInt64, ColumnPredicateLiteralV1::Int(value)) if *value >= 0 => {
			Some(take_high_bits(*value as u64, 64, column.width_bits))
		}
		(DataType::Timestamp(TimeUnit::Millisecond, _), ColumnPredicateLiteralV1::Ts(value))
		| (DataType::Timestamp(TimeUnit::Millisecond, _), ColumnPredicateLiteralV1::Int(value)) => {
			Some(normalize_signed_int(*value, 64, column.width_bits))
		}
		(DataType::Utf8 | DataType::LargeUtf8, ColumnPredicateLiteralV1::Str(bytes)) => Some(
			normalize_prefix_bytes(bytes, column.string_prefix_bytes.unwrap_or(0)),
		),
		_ => None,
	}
}

fn normalize_signed_int(value: i64, natural_bits: u8, width_bits: u8) -> u64 {
	let sign_bit = 1_u64 << (natural_bits - 1);
	let normalized = if natural_bits == 64 {
		(value as u64) ^ sign_bit
	} else {
		((value as u64) & low_mask(u32::from(natural_bits))) ^ sign_bit
	};
	take_high_bits(normalized, natural_bits, width_bits)
}

fn take_high_bits(value: u64, natural_bits: u8, width_bits: u8) -> u64 {
	if width_bits == 64 {
		if natural_bits == 64 {
			value
		} else {
			value << (64 - natural_bits)
		}
	} else if width_bits <= natural_bits {
		(value >> (natural_bits - width_bits)) & low_mask(u32::from(width_bits))
	} else {
		(value << (width_bits - natural_bits)) & low_mask(u32::from(width_bits))
	}
}

fn normalize_prefix_bytes(bytes: &[u8], prefix_bytes: u8) -> u64 {
	let mut out = 0_u64;
	for idx in 0..prefix_bytes as usize {
		out <<= 8;
		out |= u64::from(bytes.get(idx).copied().unwrap_or(0));
	}
	out
}

fn z_order_encode(coords: &[u64], widths: &[u8]) -> u64 {
	if let Some(encoded) = z_order_encode_bmi2(coords, widths) {
		return encoded;
	}
	z_order_encode_portable(coords, widths)
}

fn z_order_encode_portable(coords: &[u64], widths: &[u8]) -> u64 {
	debug_assert_eq!(coords.len(), widths.len());
	if coords.len() == 1 {
		return coords[0] & low_mask(u32::from(widths[0]));
	}
	let max_width = widths.iter().copied().max().unwrap_or(0);
	let mut out = 0_u64;
	for level in 0..max_width {
		for (coord, width) in coords.iter().zip(widths.iter().copied()) {
			if level < width {
				let bit_index = width - 1 - level;
				out = (out << 1) | ((coord >> bit_index) & 1);
			}
		}
	}
	out
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
fn z_order_encode_bmi2(coords: &[u64], widths: &[u8]) -> Option<u64> {
	if !std::arch::is_x86_feature_detected!("bmi2") {
		return None;
	}
	Some(unsafe { z_order_encode_bmi2_unchecked(coords, widths) })
}

#[cfg(not(any(target_arch = "x86", target_arch = "x86_64")))]
fn z_order_encode_bmi2(_coords: &[u64], _widths: &[u8]) -> Option<u64> {
	None
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "bmi2")]
unsafe fn z_order_encode_bmi2_unchecked(coords: &[u64], widths: &[u8]) -> u64 {
	debug_assert_eq!(coords.len(), widths.len());
	if coords.len() == 1 {
		return coords[0] & low_mask(u32::from(widths[0]));
	}
	let masks = z_order_deposit_masks(widths);
	let mut out = 0_u64;
	for ((coord, width), mask) in coords.iter().zip(widths.iter().copied()).zip(masks) {
		out |= unsafe { pdep_u64(*coord & low_mask(u32::from(width)), mask) };
	}
	out
}

#[cfg(target_arch = "x86_64")]
unsafe fn pdep_u64(value: u64, mask: u64) -> u64 {
	unsafe { std::arch::x86_64::_pdep_u64(value, mask) }
}

#[cfg(target_arch = "x86")]
unsafe fn pdep_u64(value: u64, mask: u64) -> u64 {
	unsafe { std::arch::x86::_pdep_u64(value, mask) }
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
fn z_order_deposit_masks(widths: &[u8]) -> Vec<u64> {
	let mut masks = vec![0_u64; widths.len()];
	let max_width = widths.iter().copied().max().unwrap_or(0);
	let mut out_bit = widths.iter().map(|width| u32::from(*width)).sum::<u32>();
	for level in 0..max_width {
		for (idx, width) in widths.iter().copied().enumerate() {
			if level < width {
				out_bit -= 1;
				masks[idx] |= 1_u64 << out_bit;
			}
		}
	}
	debug_assert_eq!(out_bit, 0);
	masks
}

fn random_hash_suffix(coords: &[u64], widths: &[u8], suffix_width: u8) -> u64 {
	let mut hasher = blake3::Hasher::new();
	hasher.update(TABLE_COLUMN_LOCALITY_HASH_DOMAIN);
	for (coord, width) in coords.iter().zip(widths.iter().copied()) {
		hasher.update(&[width]);
		hasher.update(&coord.to_le_bytes());
	}
	let digest = hasher.finalize();
	let mut bytes = [0; 8];
	bytes.copy_from_slice(&digest.as_bytes()[..8]);
	u64::from_le_bytes(bytes) & low_mask(u32::from(suffix_width))
}

fn clustering_suffix_width(widths: &[u8]) -> u8 {
	widths.iter().copied().sum()
}

fn low_mask(bits: u32) -> u64 {
	match bits {
		0 => 0,
		64.. => u64::MAX,
		_ => (1_u64 << bits) - 1,
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RangeDecompositionOverflow;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ZCellRelation {
	Disjoint,
	Inside,
	Partial,
}

fn decompose_z_order_ranges(
	lower: &[u64],
	upper: &[u64],
	widths: &[u8],
	arc_budget: usize,
) -> Result<Vec<(u64, u64)>, RangeDecompositionOverflow> {
	debug_assert_eq!(lower.len(), widths.len());
	debug_assert_eq!(upper.len(), widths.len());
	let suffix_width = widths.iter().map(|width| u32::from(*width)).sum::<u32>();
	if suffix_width == 0 {
		return Ok(Vec::new());
	}
	let sequence = z_order_dimension_sequence(widths);
	let mut state = ZRangeDecomposer {
		lower,
		upper,
		widths,
		sequence,
		arc_budget: arc_budget.max(1),
		node_budget: arc_budget
			.max(1)
			.saturating_mul(suffix_width as usize + 1)
			.saturating_mul(4),
		nodes_visited: 0,
		suffix_width,
		prefixes: vec![0; widths.len()],
		fixed_bits: vec![0; widths.len()],
		arcs: Vec::new(),
	};
	state.visit(0, 0)?;
	Ok(state.arcs)
}

fn decompose_z_order_ranges_with_allowed_values(
	lower: &[u64],
	upper: &[u64],
	widths: &[u8],
	allowed_values: &[Option<BTreeSet<u64>>],
	arc_budget: usize,
) -> Result<Vec<(u64, u64)>, RangeDecompositionOverflow> {
	if allowed_values.iter().all(Option::is_none) {
		return decompose_z_order_ranges(lower, upper, widths, arc_budget);
	}
	let mut arcs = Vec::new();
	let mut combo_lower = lower.to_vec();
	let mut combo_upper = upper.to_vec();
	collect_allowed_z_order_ranges(
		0,
		lower,
		upper,
		widths,
		allowed_values,
		&mut combo_lower,
		&mut combo_upper,
		arc_budget.max(1),
		&mut arcs,
	)?;
	Ok(arcs)
}

fn collect_allowed_z_order_ranges(
	dim: usize,
	lower: &[u64],
	upper: &[u64],
	widths: &[u8],
	allowed_values: &[Option<BTreeSet<u64>>],
	combo_lower: &mut [u64],
	combo_upper: &mut [u64],
	arc_budget: usize,
	arcs: &mut Vec<(u64, u64)>,
) -> Result<(), RangeDecompositionOverflow> {
	if dim == allowed_values.len() {
		let remaining_budget = arc_budget.saturating_sub(arcs.len()).max(1);
		let next = decompose_z_order_ranges(combo_lower, combo_upper, widths, remaining_budget)?;
		merge_z_order_arcs(arcs, next, arc_budget)?;
		return Ok(());
	}
	if let Some(values) = &allowed_values[dim] {
		for value in values {
			combo_lower[dim] = *value;
			combo_upper[dim] = *value;
			collect_allowed_z_order_ranges(
				dim + 1,
				lower,
				upper,
				widths,
				allowed_values,
				combo_lower,
				combo_upper,
				arc_budget,
				arcs,
			)?;
		}
		combo_lower[dim] = lower[dim];
		combo_upper[dim] = upper[dim];
	} else {
		combo_lower[dim] = lower[dim];
		combo_upper[dim] = upper[dim];
		collect_allowed_z_order_ranges(
			dim + 1,
			lower,
			upper,
			widths,
			allowed_values,
			combo_lower,
			combo_upper,
			arc_budget,
			arcs,
		)?;
	}
	Ok(())
}

fn merge_z_order_arcs(
	arcs: &mut Vec<(u64, u64)>,
	mut next: Vec<(u64, u64)>,
	arc_budget: usize,
) -> Result<(), RangeDecompositionOverflow> {
	arcs.append(&mut next);
	arcs.sort_unstable_by_key(|arc| arc.0);
	let mut merged: Vec<(u64, u64)> = Vec::with_capacity(arcs.len());
	for (start, end) in arcs.drain(..) {
		if let Some(last) = merged.last_mut() {
			let adjacent_or_overlap = last.1 == u64::MAX || start <= last.1 + 1;
			if adjacent_or_overlap {
				last.1 = last.1.max(end);
				continue;
			}
		}
		merged.push((start, end));
	}
	if merged.len() > arc_budget {
		return Err(RangeDecompositionOverflow);
	}
	*arcs = merged;
	Ok(())
}

struct ZRangeDecomposer<'a> {
	lower: &'a [u64],
	upper: &'a [u64],
	widths: &'a [u8],
	sequence: Vec<usize>,
	arc_budget: usize,
	node_budget: usize,
	nodes_visited: usize,
	suffix_width: u32,
	prefixes: Vec<u64>,
	fixed_bits: Vec<u8>,
	arcs: Vec<(u64, u64)>,
}

impl ZRangeDecomposer<'_> {
	fn visit(&mut self, pos: usize, z_prefix: u64) -> Result<(), RangeDecompositionOverflow> {
		self.nodes_visited = self.nodes_visited.saturating_add(1);
		if self.nodes_visited > self.node_budget {
			return Err(RangeDecompositionOverflow);
		}
		match self.cell_relation() {
			ZCellRelation::Disjoint => return Ok(()),
			ZCellRelation::Inside => {
				let remaining = self.suffix_width - pos as u32;
				let start = shift_prefix(z_prefix, remaining);
				let end = start | low_mask(remaining);
				self.push_arc(start, end)?;
				return Ok(());
			}
			ZCellRelation::Partial => {}
		}
		if pos == self.sequence.len() {
			self.push_arc(z_prefix, z_prefix)?;
			return Ok(());
		}
		let dim = self.sequence[pos];
		for bit in 0..=1 {
			self.prefixes[dim] = (self.prefixes[dim] << 1) | bit;
			self.fixed_bits[dim] += 1;
			self.visit(pos + 1, (z_prefix << 1) | bit)?;
			self.fixed_bits[dim] -= 1;
			self.prefixes[dim] >>= 1;
		}
		Ok(())
	}

	fn cell_relation(&self) -> ZCellRelation {
		let mut inside = true;
		for dim in 0..self.widths.len() {
			let remaining = u32::from(self.widths[dim] - self.fixed_bits[dim]);
			let cell_low = shift_prefix(self.prefixes[dim], remaining);
			let cell_high = cell_low | low_mask(remaining);
			if cell_high < self.lower[dim] || cell_low > self.upper[dim] {
				return ZCellRelation::Disjoint;
			}
			if cell_low < self.lower[dim] || cell_high > self.upper[dim] {
				inside = false;
			}
		}
		if inside {
			ZCellRelation::Inside
		} else {
			ZCellRelation::Partial
		}
	}

	fn push_arc(&mut self, start: u64, end: u64) -> Result<(), RangeDecompositionOverflow> {
		if let Some(last) = self.arcs.last_mut() {
			let adjacent_or_overlap = last.1 == u64::MAX || start <= last.1 + 1;
			if adjacent_or_overlap {
				last.1 = last.1.max(end);
				return Ok(());
			}
		}
		if self.arcs.len() >= self.arc_budget {
			return Err(RangeDecompositionOverflow);
		}
		self.arcs.push((start, end));
		Ok(())
	}
}

fn shift_prefix(prefix: u64, remaining_bits: u32) -> u64 {
	if remaining_bits >= 64 {
		0
	} else {
		prefix << remaining_bits
	}
}

fn z_order_dimension_sequence(widths: &[u8]) -> Vec<usize> {
	let suffix_width = widths.iter().map(|width| usize::from(*width)).sum();
	let mut sequence = Vec::with_capacity(suffix_width);
	let max_width = widths.iter().copied().max().unwrap_or(0);
	for level in 0..max_width {
		for (idx, width) in widths.iter().copied().enumerate() {
			if level < width {
				sequence.push(idx);
			}
		}
	}
	sequence
}

fn locality_masks(prefix_bits: u8) -> std::io::Result<(u64, u64)> {
	if prefix_bits == 0 || prefix_bits >= 64 {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!("table spread prefix_bits must be in 1..=63, got {prefix_bits}"),
		));
	}
	let prefix_mask = u64::MAX << (64 - prefix_bits);
	Ok((prefix_mask, !prefix_mask))
}

fn jump_consistent_hash(key: u64, buckets: u16) -> u16 {
	debug_assert!(buckets > 0);
	let buckets = i64::from(buckets);
	let mut key = key;
	let mut b = -1_i64;
	let mut j = 0_i64;
	while j < buckets {
		b = j;
		key = key.wrapping_mul(2_862_933_555_777_941_757).wrapping_add(1);
		j = (((b + 1) as f64) * (((1_u64 << 31) as f64) / (((key >> 33) + 1) as f64))) as i64;
	}
	b as u16
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::schema::wpbc::{
		OP_AND, OP_EQ, OP_GT, OP_GTE, OP_IN_LIST, OP_LTE, OP_OR, OP_PUSH_COL, OP_PUSH_INT,
		OP_PUSH_STR,
	};
	use arrow::array::{Int64Array, LargeStringArray, UInt32Array};
	use arrow::datatypes::{Field, Schema};
	use arrow::record_batch::RecordBatch;
	use std::collections::{BTreeSet, HashMap};
	use std::sync::Arc;

	fn table_schema(data_type: DataType, nullable_pk: bool) -> Schema {
		Schema::new(vec![
			Field::new("id", data_type, nullable_pk),
			Field::new("name", DataType::LargeUtf8, true),
		])
	}

	fn table_batch(ids: Vec<Option<i64>>) -> RecordBatch {
		let rows = ids.len();
		let has_null_pk = ids.iter().any(Option::is_none);
		let names: Vec<Option<&str>> = (0..rows)
			.map(|idx| match idx {
				0 => Some("codex"),
				1 => Some("claude"),
				_ => Some("gemini"),
			})
			.collect();
		let schema = Arc::new(table_schema(DataType::Int64, false));
		let columns = vec![
			Arc::new(Int64Array::from(ids)) as Arc<dyn Array>,
			Arc::new(LargeStringArray::from(names)) as Arc<dyn Array>,
		];
		if has_null_pk {
			// The validator must fail closed even if malformed Arrow
			// arrives below RecordBatch's normal nullability checks.
			unsafe { RecordBatch::new_unchecked(schema, columns, rows) }
		} else {
			RecordBatch::try_new(schema, columns).unwrap()
		}
	}

	fn descriptor_for_generation(
		schema: &Schema,
		schema_generation: u64,
	) -> TableSchemaDescriptorV1 {
		TableSchemaDescriptorV1::new(
			TableObjectId::new([7; 16]),
			schema_generation,
			"analytics",
			"events",
			schema,
			PrimaryKeyDescriptorV1::single("id"),
			LocalityPolicyV1::default_table_spread(),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.unwrap()
	}

	fn descriptor_for(schema: &Schema) -> TableSchemaDescriptorV1 {
		descriptor_for_generation(schema, 1)
	}

	fn descriptor_with_stats(
		schema: &Schema,
		stats_policy: ColumnStatsPolicyV1,
	) -> std::io::Result<TableSchemaDescriptorV1> {
		let mut descriptor = descriptor_for(schema);
		descriptor.stats_policy = stats_policy;
		descriptor.schema_fingerprint = descriptor.compute_schema_fingerprint()?;
		descriptor.validate()?;
		Ok(descriptor)
	}

	fn cluster_column(name: &str, width_bits: u8) -> ClusteringColumnSpecV1 {
		ClusteringColumnSpecV1 {
			name: name.to_owned(),
			width_bits,
			string_prefix_bytes: None,
		}
	}

	fn cluster_string_column(name: &str, string_prefix_bytes: u8) -> ClusteringColumnSpecV1 {
		ClusteringColumnSpecV1 {
			name: name.to_owned(),
			width_bits: string_prefix_bytes * 8,
			string_prefix_bytes: Some(string_prefix_bytes),
		}
	}

	fn clustered_schema() -> Schema {
		Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("event_hour", DataType::UInt32, false),
			Field::new("customer_id", DataType::UInt32, false),
			Field::new("region", DataType::LargeUtf8, false),
		])
	}

	fn schema_with_clustering_candidate(data_type: DataType) -> Schema {
		Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("candidate", data_type, false),
		])
	}

	fn clustered_batch() -> RecordBatch {
		RecordBatch::try_new(
			Arc::new(clustered_schema()),
			vec![
				Arc::new(Int64Array::from(vec![1, 2, 3])) as Arc<dyn Array>,
				Arc::new(UInt32Array::from(vec![10_u32, 11, 12])) as Arc<dyn Array>,
				Arc::new(UInt32Array::from(vec![100_u32, 101, 102])) as Arc<dyn Array>,
				Arc::new(LargeStringArray::from(vec!["us", "eu", "ap"])) as Arc<dyn Array>,
			],
		)
		.unwrap()
	}

	fn clustered_descriptor(
		projection: ProjectionFunctionV1,
		columns: Vec<ClusteringColumnSpecV1>,
	) -> TableSchemaDescriptorV1 {
		TableSchemaDescriptorV1::new(
			TableObjectId::new([7; 16]),
			1,
			"analytics",
			"events",
			&clustered_schema(),
			PrimaryKeyDescriptorV1::single("id"),
			LocalityPolicyV1::ColumnProjection(ColumnLocalityDescriptorV1 {
				columns,
				projection,
				spread_factor: TABLE_SPREAD_DEFAULT_FACTOR,
				prefix_bits: TABLE_SPREAD_DEFAULT_PREFIX_BITS,
			}),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.unwrap()
	}

	fn push_col(bytes: &mut Vec<u8>, column: u32) {
		bytes.push(OP_PUSH_COL);
		bytes.extend_from_slice(&column.to_le_bytes());
	}

	fn push_int(bytes: &mut Vec<u8>, value: i64) {
		bytes.push(OP_PUSH_INT);
		bytes.extend_from_slice(&value.to_le_bytes());
	}

	fn push_str(bytes: &mut Vec<u8>, value: &str) {
		bytes.push(OP_PUSH_STR);
		bytes.extend_from_slice(&(value.len() as u16).to_le_bytes());
		bytes.extend_from_slice(value.as_bytes());
	}

	fn in_list(bytes: &mut Vec<u8>, count: u16) {
		bytes.push(OP_IN_LIST);
		bytes.extend_from_slice(&count.to_le_bytes());
	}

	fn table_meta(descriptor: &TableSchemaDescriptorV1, row_count: usize) -> TableWriteRequestMeta {
		TableWriteRequestMeta {
			parent_write_id: ClientWriteId {
				client_uuid: [8; 16],
				sequence_no: 11,
				idempotency_ttl_ms: 30_000,
				_pad: 0,
			},
			client_hlc: HybridTimestamp {
				physical_ms: 10,
				logical: 0,
				node_id: 1,
			},
			expected_epoch: 2,
			tenant_id: TenantId::new(9),
			table_object_id: descriptor.table_object_id,
			schema_generation: descriptor.schema_generation,
			child_ordinal_base: 0,
			proxy_source: NodeId {
				id: 0,
				_reserved: 0,
			},
			row_count: encode_table_write_row_count(row_count).unwrap(),
			proxy_hop: 0,
			durability: DurabilityLevel::QUORUM,
			await_finalization: 0,
			_pad: [0; 3],
		}
	}

	#[test]
	fn default_kv_descriptor_matches_legacy_write_schema() {
		let descriptor = default_kv_table_descriptor_v1().unwrap();
		assert_eq!(descriptor.table_object_id, TableObjectId::DEFAULT_KV);
		assert_eq!(descriptor.arrow_schema().unwrap(), write_payload_schema());
		assert_eq!(
			descriptor.primary_key,
			PrimaryKeyDescriptorV1::single("key")
		);
		assert_eq!(
			descriptor.locality_policy,
			LocalityPolicyV1::IdentityProjection
		);
	}

	#[test]
	fn descriptor_roundtrips_and_checks_fingerprint() {
		let descriptor = descriptor_for(&table_schema(DataType::Int64, false));
		let encoded = encode_table_schema_descriptor_v1(&descriptor).unwrap();
		let decoded = decode_table_schema_descriptor_v1(&encoded).unwrap();
		assert_eq!(decoded, descriptor);

		let mut tampered = descriptor;
		tampered.table_name = "other".to_owned();
		assert!(tampered.validate().is_err());
	}

	#[test]
	fn table_query_wire_contract_remains_descriptor_and_wpbc_only() {
		assert_eq!(std::mem::size_of::<TableQueryRequestMeta>(), 88);
		assert_eq!(std::mem::offset_of!(TableQueryRequestMeta, min_hlc), 0);
		assert_eq!(
			std::mem::offset_of!(TableQueryRequestMeta, expected_epoch),
			16
		);
		assert_eq!(std::mem::offset_of!(TableQueryRequestMeta, tenant_id), 24);
		assert_eq!(
			std::mem::offset_of!(TableQueryRequestMeta, table_object_id),
			32
		);
		assert_eq!(
			std::mem::offset_of!(TableQueryRequestMeta, schema_generation),
			48
		);
		assert_eq!(std::mem::offset_of!(TableQueryRequestMeta, consistency), 56);
		assert_eq!(std::mem::offset_of!(TableQueryRequestMeta, _pad), 57);
		assert_eq!(
			std::mem::offset_of!(TableQueryRequestMeta, bytecode_len),
			64
		);
		assert_eq!(std::mem::offset_of!(TableQueryRequestMeta, _pad2), 68);
		assert_eq!(std::mem::offset_of!(TableQueryRequestMeta, as_of_hlc), 72);
	}

	#[test]
	fn stats_policy_defaults_empty_and_roundtrips_in_fingerprint() {
		let descriptor = descriptor_for(&table_schema(DataType::Int64, false));
		assert_eq!(descriptor.stats_policy, ColumnStatsPolicyV1::default());

		let schema = Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("region", DataType::LargeUtf8, true),
			Field::new("active", DataType::Boolean, true),
		]);
		let descriptor = descriptor_with_stats(
			&schema,
			ColumnStatsPolicyV1 {
				bloom_filters: vec![ColumnBloomSpecV1::new("region")],
				hll_distincts: vec![ColumnHllSpecV1::new("id")],
				small_sets: vec![ColumnSmallSetSpecV1::new("active")],
			},
		)
		.unwrap();
		let encoded = encode_table_schema_descriptor_v1(&descriptor).unwrap();
		let decoded = decode_table_schema_descriptor_v1(&encoded).unwrap();
		assert_eq!(decoded, descriptor);
	}

	#[test]
	fn stats_policy_validation_uses_stable_reject_codes() {
		let schema = Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("score", DataType::Float64, true),
		]);

		let duplicate = descriptor_with_stats(
			&schema,
			ColumnStatsPolicyV1 {
				bloom_filters: vec![ColumnBloomSpecV1::new("id"), ColumnBloomSpecV1::new("id")],
				..ColumnStatsPolicyV1::default()
			},
		)
		.unwrap_err();
		assert!(
			duplicate
				.to_string()
				.contains("WPBC_REJECT_STATS_DUPLICATE_COLUMN")
		);

		let unknown = descriptor_with_stats(
			&schema,
			ColumnStatsPolicyV1 {
				hll_distincts: vec![ColumnHllSpecV1::new("missing")],
				..ColumnStatsPolicyV1::default()
			},
		)
		.unwrap_err();
		assert!(
			unknown
				.to_string()
				.contains("WPBC_REJECT_STATS_COLUMN_UNKNOWN")
		);

		let unsupported = descriptor_with_stats(
			&schema,
			ColumnStatsPolicyV1 {
				small_sets: vec![ColumnSmallSetSpecV1::new("score")],
				..ColumnStatsPolicyV1::default()
			},
		)
		.unwrap_err();
		assert!(
			unsupported
				.to_string()
				.contains("WPBC_REJECT_STATS_UNSUPPORTED_TYPE")
		);

		let invalid_bits = descriptor_with_stats(
			&schema,
			ColumnStatsPolicyV1 {
				bloom_filters: vec![ColumnBloomSpecV1 {
					column_name: "id".into(),
					bits_per_row: BloomBitsPerRow(0),
				}],
				..ColumnStatsPolicyV1::default()
			},
		)
		.unwrap_err();
		assert!(
			invalid_bits
				.to_string()
				.contains("WPBC_REJECT_STATS_BITS_INVALID")
		);

		let invalid_precision = descriptor_with_stats(
			&schema,
			ColumnStatsPolicyV1 {
				hll_distincts: vec![ColumnHllSpecV1 {
					column_name: "id".into(),
					precision: HllPrecision(COLUMN_STATS_HLL_MIN_PRECISION - 1),
				}],
				..ColumnStatsPolicyV1::default()
			},
		)
		.unwrap_err();
		assert!(
			invalid_precision
				.to_string()
				.contains("WPBC_REJECT_STATS_PRECISION_INVALID")
		);

		let invalid_small_set = descriptor_with_stats(
			&schema,
			ColumnStatsPolicyV1 {
				small_sets: vec![ColumnSmallSetSpecV1 {
					column_name: "id".into(),
					max_distinct_values: SmallSetValueCap(0),
					max_bytes: SmallSetByteCap::DEFAULT,
				}],
				..ColumnStatsPolicyV1::default()
			},
		)
		.unwrap_err();
		assert!(
			invalid_small_set
				.to_string()
				.contains("WPBC_REJECT_STATS_SMALLSET_INVALID")
		);

		let budget = descriptor_with_stats(
			&schema,
			ColumnStatsPolicyV1 {
				bloom_filters: vec![ColumnBloomSpecV1 {
					column_name: "id".into(),
					bits_per_row: BloomBitsPerRow::new(COLUMN_STATS_BLOOM_MAX_BITS_PER_ROW)
						.unwrap(),
				}],
				..ColumnStatsPolicyV1::default()
			},
		)
		.unwrap_err();
		assert!(
			budget
				.to_string()
				.contains("WPBC_REJECT_STATS_BUDGET_EXCEEDED")
		);
	}

	#[test]
	fn stats_policy_newtypes_validate_units_at_construction() {
		assert_eq!(
			BloomBitsPerRow::new(COLUMN_STATS_BLOOM_DEFAULT_BITS_PER_ROW)
				.unwrap()
				.get(),
			COLUMN_STATS_BLOOM_DEFAULT_BITS_PER_ROW
		);
		assert!(BloomBitsPerRow::new(0).is_err());
		assert_eq!(
			HllPrecision::new(COLUMN_STATS_HLL_DEFAULT_PRECISION)
				.unwrap()
				.get(),
			COLUMN_STATS_HLL_DEFAULT_PRECISION
		);
		assert!(HllPrecision::new(COLUMN_STATS_HLL_MAX_PRECISION + 1).is_err());
		assert_eq!(
			SmallSetValueCap::new(COLUMN_STATS_SMALL_SET_DEFAULT_MAX_DISTINCT_VALUES)
				.unwrap()
				.get(),
			COLUMN_STATS_SMALL_SET_DEFAULT_MAX_DISTINCT_VALUES
		);
		assert!(SmallSetValueCap::new(0).is_err());
		assert_eq!(
			SmallSetByteCap::new(COLUMN_STATS_SMALL_SET_DEFAULT_MAX_BYTES)
				.unwrap()
				.get(),
			COLUMN_STATS_SMALL_SET_DEFAULT_MAX_BYTES
		);
		assert!(SmallSetByteCap::new(COLUMN_STATS_SMALL_SET_MAX_BYTES + 1).is_err());
	}

	#[test]
	fn additive_schema_evolution_allows_nullable_append_and_relaxed_nullability() {
		let previous_schema = Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("name", DataType::LargeUtf8, false),
		]);
		let next_schema = Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("name", DataType::LargeUtf8, true),
			Field::new("email", DataType::LargeUtf8, true),
		]);
		let previous = descriptor_for_generation(&previous_schema, 1);
		let next = descriptor_for_generation(&next_schema, 2);
		validate_table_schema_evolution(&previous, &next).unwrap();
	}

	#[test]
	fn additive_schema_evolution_allows_non_null_append_with_default_metadata() {
		let previous_schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
		let next_schema = Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("country", DataType::LargeUtf8, false).with_metadata(HashMap::from([(
				TABLE_FIELD_DEFAULT_METADATA_KEY.to_owned(),
				"US".to_owned(),
			)])),
		]);
		let previous = descriptor_for_generation(&previous_schema, 1);
		let next = descriptor_for_generation(&next_schema, 2);
		validate_table_schema_evolution(&previous, &next).unwrap();
	}

	#[test]
	fn column_projection_schema_evolution_requires_stable_descriptor() {
		let previous_schema = Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("event_hour", DataType::UInt32, false),
		]);
		let next_schema = Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("event_hour", DataType::UInt32, false),
			Field::new("region", DataType::LargeUtf8, true),
		]);
		let locality = LocalityPolicyV1::ColumnProjection(ColumnLocalityDescriptorV1 {
			columns: vec![cluster_column("event_hour", 16)],
			projection: ProjectionFunctionV1::ZOrder,
			spread_factor: TABLE_SPREAD_DEFAULT_FACTOR,
			prefix_bits: TABLE_SPREAD_DEFAULT_PREFIX_BITS,
		});
		let previous = TableSchemaDescriptorV1::new(
			TableObjectId::new([7; 16]),
			1,
			"analytics",
			"events",
			&previous_schema,
			PrimaryKeyDescriptorV1::single("id"),
			locality.clone(),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.unwrap();
		let next = TableSchemaDescriptorV1::new(
			previous.table_object_id,
			2,
			"analytics",
			"events",
			&next_schema,
			PrimaryKeyDescriptorV1::single("id"),
			locality,
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.unwrap();
		validate_table_schema_evolution(&previous, &next).unwrap();

		let changed_locality = TableSchemaDescriptorV1::new(
			previous.table_object_id,
			3,
			"analytics",
			"events",
			&next_schema,
			PrimaryKeyDescriptorV1::single("id"),
			LocalityPolicyV1::ColumnProjection(ColumnLocalityDescriptorV1 {
				columns: vec![cluster_column("event_hour", 20)],
				projection: ProjectionFunctionV1::ZOrder,
				spread_factor: TABLE_SPREAD_DEFAULT_FACTOR,
				prefix_bits: TABLE_SPREAD_DEFAULT_PREFIX_BITS,
			}),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.unwrap();
		let err = validate_table_schema_evolution(&previous, &changed_locality).unwrap_err();
		assert!(err.to_string().contains("cannot change locality_policy"));
	}

	#[test]
	fn additive_schema_evolution_rejects_non_additive_changes() {
		let previous_schema = Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("age", DataType::Int64, true),
		]);
		let previous = descriptor_for_generation(&previous_schema, 1);

		let type_change = descriptor_for_generation(
			&Schema::new(vec![
				Field::new("id", DataType::Int64, false),
				Field::new("age", DataType::LargeUtf8, true),
			]),
			2,
		);
		let err = validate_table_schema_evolution(&previous, &type_change).unwrap_err();
		assert!(err.to_string().contains("change type"));

		let dropped = descriptor_for_generation(
			&Schema::new(vec![Field::new("id", DataType::Int64, false)]),
			2,
		);
		let err = validate_table_schema_evolution(&previous, &dropped).unwrap_err();
		assert!(err.to_string().contains("drop columns"));

		let non_null_append = descriptor_for_generation(
			&Schema::new(vec![
				Field::new("id", DataType::Int64, false),
				Field::new("age", DataType::Int64, true),
				Field::new("region", DataType::LargeUtf8, false),
			]),
			2,
		);
		let err = validate_table_schema_evolution(&previous, &non_null_append).unwrap_err();
		assert!(err.to_string().contains(TABLE_FIELD_DEFAULT_METADATA_KEY));
	}

	#[test]
	fn primary_key_validation_rejects_nullable_or_unsupported_types() {
		let nullable = TableSchemaDescriptorV1::new(
			TableObjectId::new([7; 16]),
			1,
			"analytics",
			"events",
			&table_schema(DataType::Int64, true),
			PrimaryKeyDescriptorV1::single("id"),
			LocalityPolicyV1::default_table_spread(),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		);
		assert!(nullable.is_err());

		let float_pk = TableSchemaDescriptorV1::new(
			TableObjectId::new([7; 16]),
			1,
			"analytics",
			"events",
			&table_schema(DataType::Float64, false),
			PrimaryKeyDescriptorV1::single("id"),
			LocalityPolicyV1::default_table_spread(),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		);
		assert!(float_pk.is_err());
	}

	#[test]
	fn primary_key_validation_rejects_composite_v1_shape() {
		let descriptor = TableSchemaDescriptorV1::new(
			TableObjectId::new([7; 16]),
			1,
			"analytics",
			"events",
			&table_schema(DataType::Int64, false),
			PrimaryKeyDescriptorV1 {
				columns: vec!["id".to_owned(), "name".to_owned()],
				canonical_encoding: PrimaryKeyEncodingV1::ArrowV1,
			},
			LocalityPolicyV1::default_table_spread(),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		);
		assert!(descriptor.is_err());
	}

	#[test]
	fn table_identity_is_domain_separated_by_table_object() {
		let tenant = TenantId::new(9);
		let pk = 42_i64.to_le_bytes();
		let left = table_row_identity(tenant, TableObjectId::new([1; 16]), &pk);
		let right = table_row_identity(tenant, TableObjectId::new([2; 16]), &pk);
		assert_ne!(left, right);
	}

	#[test]
	fn table_spread_ranges_and_locality_share_prefix() {
		let tenant = TenantId::new(9);
		let table = TableObjectId::new([3; 16]);
		let spread = TableSpreadLocalityDescriptorV1 {
			spread_factor: 4,
			prefix_bits: 16,
		};
		let ranges = table_spread_prefix_ranges(tenant, table, spread).unwrap();
		assert!(!ranges.is_empty());
		assert!(ranges.len() <= 4);

		let identity = table_row_identity(tenant, table, b"pk-1");
		let locality = table_spread_locality(tenant, table, identity, spread).unwrap();
		assert!(
			ranges
				.iter()
				.any(|(start, end)| start.get() <= locality.get() && locality.get() <= end.get())
		);
	}

	#[test]
	fn column_projection_descriptor_validation_rejects_invalid_shapes() {
		let empty = TableSchemaDescriptorV1::new(
			TableObjectId::new([7; 16]),
			1,
			"analytics",
			"events",
			&clustered_schema(),
			PrimaryKeyDescriptorV1::single("id"),
			LocalityPolicyV1::ColumnProjection(ColumnLocalityDescriptorV1 {
				columns: vec![],
				projection: ProjectionFunctionV1::ZOrder,
				spread_factor: TABLE_SPREAD_DEFAULT_FACTOR,
				prefix_bits: TABLE_SPREAD_DEFAULT_PREFIX_BITS,
			}),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.unwrap_err();
		assert!(empty.to_string().contains("WPBC_REJECT_CLUSTERING_EMPTY"));

		let too_wide = TableSchemaDescriptorV1::new(
			TableObjectId::new([7; 16]),
			1,
			"analytics",
			"events",
			&clustered_schema(),
			PrimaryKeyDescriptorV1::single("id"),
			LocalityPolicyV1::ColumnProjection(ColumnLocalityDescriptorV1 {
				columns: vec![
					cluster_column("id", 1),
					cluster_column("event_hour", 1),
					cluster_column("customer_id", 1),
					cluster_string_column("region", 1),
					cluster_column("id", 1),
					cluster_column("event_hour", 1),
					cluster_column("customer_id", 1),
				],
				projection: ProjectionFunctionV1::ZOrder,
				spread_factor: TABLE_SPREAD_DEFAULT_FACTOR,
				prefix_bits: TABLE_SPREAD_DEFAULT_PREFIX_BITS,
			}),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.unwrap_err();
		assert!(
			too_wide
				.to_string()
				.contains("WPBC_REJECT_CLUSTERING_TOO_WIDE")
		);

		let duplicate = TableSchemaDescriptorV1::new(
			TableObjectId::new([7; 16]),
			1,
			"analytics",
			"events",
			&clustered_schema(),
			PrimaryKeyDescriptorV1::single("id"),
			LocalityPolicyV1::ColumnProjection(ColumnLocalityDescriptorV1 {
				columns: vec![
					cluster_column("event_hour", 4),
					cluster_column("event_hour", 4),
				],
				projection: ProjectionFunctionV1::ZOrder,
				spread_factor: TABLE_SPREAD_DEFAULT_FACTOR,
				prefix_bits: TABLE_SPREAD_DEFAULT_PREFIX_BITS,
			}),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.unwrap_err();
		assert!(
			duplicate
				.to_string()
				.contains("WPBC_REJECT_CLUSTERING_DUPLICATE_COLUMN")
		);

		let nullable = TableSchemaDescriptorV1::new(
			TableObjectId::new([7; 16]),
			1,
			"analytics",
			"events",
			&table_schema(DataType::Int64, false),
			PrimaryKeyDescriptorV1::single("id"),
			LocalityPolicyV1::ColumnProjection(ColumnLocalityDescriptorV1 {
				columns: vec![cluster_string_column("name", 2)],
				projection: ProjectionFunctionV1::ZOrder,
				spread_factor: TABLE_SPREAD_DEFAULT_FACTOR,
				prefix_bits: TABLE_SPREAD_DEFAULT_PREFIX_BITS,
			}),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.unwrap_err();
		assert!(
			nullable
				.to_string()
				.contains("WPBC_REJECT_CLUSTERING_NULLABLE")
		);

		let hilbert = TableSchemaDescriptorV1::new(
			TableObjectId::new([7; 16]),
			1,
			"analytics",
			"events",
			&clustered_schema(),
			PrimaryKeyDescriptorV1::single("id"),
			LocalityPolicyV1::ColumnProjection(ColumnLocalityDescriptorV1 {
				columns: vec![cluster_column("event_hour", 16)],
				projection: ProjectionFunctionV1::Hilbert,
				spread_factor: TABLE_SPREAD_DEFAULT_FACTOR,
				prefix_bits: TABLE_SPREAD_DEFAULT_PREFIX_BITS,
			}),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.unwrap_err();
		assert!(
			hilbert
				.to_string()
				.contains("WPBC_REJECT_PROJECTION_NOT_YET_SUPPORTED")
		);

		let overflow = TableSchemaDescriptorV1::new(
			TableObjectId::new([7; 16]),
			1,
			"analytics",
			"events",
			&clustered_schema(),
			PrimaryKeyDescriptorV1::single("id"),
			LocalityPolicyV1::ColumnProjection(ColumnLocalityDescriptorV1 {
				columns: vec![cluster_column("id", 64)],
				projection: ProjectionFunctionV1::ZOrder,
				spread_factor: TABLE_SPREAD_DEFAULT_FACTOR,
				prefix_bits: TABLE_SPREAD_DEFAULT_PREFIX_BITS,
			}),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.unwrap_err();
		assert!(
			overflow
				.to_string()
				.contains("WPBC_REJECT_CLUSTERING_WIDTH_OVERFLOW")
		);

		let over_natural_width = TableSchemaDescriptorV1::new(
			TableObjectId::new([7; 16]),
			1,
			"analytics",
			"events",
			&table_schema(DataType::UInt8, false),
			PrimaryKeyDescriptorV1::single("id"),
			LocalityPolicyV1::ColumnProjection(ColumnLocalityDescriptorV1 {
				columns: vec![cluster_column("id", 16)],
				projection: ProjectionFunctionV1::ZOrder,
				spread_factor: TABLE_SPREAD_DEFAULT_FACTOR,
				prefix_bits: TABLE_SPREAD_DEFAULT_PREFIX_BITS,
			}),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.unwrap_err();
		assert!(
			over_natural_width
				.to_string()
				.contains("WPBC_REJECT_CLUSTERING_WIDTH_INVALID")
		);

		for data_type in [
			DataType::Float64,
			DataType::Decimal128(10, 2),
			DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
		] {
			let unsupported = TableSchemaDescriptorV1::new(
				TableObjectId::new([7; 16]),
				1,
				"analytics",
				"events",
				&schema_with_clustering_candidate(data_type),
				PrimaryKeyDescriptorV1::single("id"),
				LocalityPolicyV1::ColumnProjection(ColumnLocalityDescriptorV1 {
					columns: vec![cluster_column("candidate", 8)],
					projection: ProjectionFunctionV1::ZOrder,
					spread_factor: TABLE_SPREAD_DEFAULT_FACTOR,
					prefix_bits: TABLE_SPREAD_DEFAULT_PREFIX_BITS,
				}),
				SchemaEvolutionPolicyV1::AdditiveOnly,
			)
			.unwrap_err();
			assert!(
				unsupported
					.to_string()
					.contains("WPBC_REJECT_CLUSTERING_TYPE_UNSUPPORTED")
			);
		}
	}

	#[test]
	fn column_projection_ranges_reuse_table_spread_shape() {
		let tenant = TenantId::new(9);
		let descriptor = clustered_descriptor(
			ProjectionFunctionV1::ZOrder,
			vec![
				cluster_column("event_hour", 24),
				cluster_column("customer_id", 20),
			],
		);
		let projection = match &descriptor.locality_policy {
			LocalityPolicyV1::ColumnProjection(projection) => projection,
			_ => unreachable!(),
		};
		let ranges =
			column_projection_prefix_ranges(tenant, descriptor.table_object_id, projection)
				.unwrap();
		assert!(!ranges.is_empty());
		assert!(ranges.len() <= usize::from(TABLE_SPREAD_DEFAULT_FACTOR));
	}

	#[test]
	fn string_prefix_clustering_uses_lexicographic_bytes() {
		let projection = ColumnLocalityDescriptorV1 {
			columns: vec![cluster_string_column("region", 1)],
			projection: ProjectionFunctionV1::ZOrder,
			spread_factor: TABLE_SPREAD_DEFAULT_FACTOR,
			prefix_bits: TABLE_SPREAD_DEFAULT_PREFIX_BITS,
		};
		let batch = clustered_batch();
		let regions = (0..batch.num_rows())
			.map(|row| normalize_clustering_row(&batch, &projection, row).unwrap()[0])
			.collect::<Vec<_>>();

		assert_eq!(
			regions,
			vec![u64::from(b'u'), u64::from(b'e'), u64::from(b'a')]
		);
		assert!(regions[2] < regions[1]);
		assert!(regions[1] < regions[0]);
		assert_eq!(z_order_encode(&[regions[1]], &[8]), u64::from(b'e'));
	}

	#[test]
	fn column_projection_predicate_ranges_prune_z_suffix() {
		let tenant = TenantId::new(9);
		let descriptor = clustered_descriptor(
			ProjectionFunctionV1::ZOrder,
			vec![cluster_column("event_hour", 4)],
		);
		let projection = match &descriptor.locality_policy {
			LocalityPolicyV1::ColumnProjection(projection) => projection,
			_ => unreachable!(),
		};
		let mut bytecode = Vec::new();
		push_col(&mut bytecode, 1);
		push_int(&mut bytecode, 0xa000_0000);
		bytecode.push(OP_EQ);
		let program = WpbcProgram::compile(&bytecode).unwrap();
		let ranges = match column_projection_predicate_ranges(
			tenant,
			descriptor.table_object_id,
			&clustered_schema(),
			projection,
			&program,
			COLUMN_PROJECTION_RANGE_ARC_BUDGET,
		)
		.unwrap()
		{
			ColumnProjectionRangePruning::Pruned(ranges) => ranges,
			other => panic!("expected pruned ranges, got {other:?}"),
		};
		let entropy_bits = 64 - u32::from(projection.prefix_bits) - 4;
		let entropy_mask = low_mask(entropy_bits);

		assert!(!ranges.is_empty());
		assert!(ranges.len() <= usize::from(TABLE_SPREAD_DEFAULT_FACTOR));
		for (start, end) in ranges {
			assert_eq!((start.get() >> entropy_bits) & 0xf, 0xa);
			assert_eq!((end.get() >> entropy_bits) & 0xf, 0xa);
			assert_eq!(start.get() & entropy_mask, 0);
			assert_eq!(end.get() & entropy_mask, entropy_mask);
		}
	}

	#[test]
	fn column_projection_predicate_ranges_prune_in_list_values() {
		let tenant = TenantId::new(9);
		let descriptor = clustered_descriptor(
			ProjectionFunctionV1::ZOrder,
			vec![cluster_string_column("region", 1)],
		);
		let projection = match &descriptor.locality_policy {
			LocalityPolicyV1::ColumnProjection(projection) => projection,
			_ => unreachable!(),
		};
		let mut bytecode = Vec::new();
		push_col(&mut bytecode, 3);
		push_str(&mut bytecode, "eu");
		push_str(&mut bytecode, "us");
		in_list(&mut bytecode, 2);
		let program = WpbcProgram::compile(&bytecode).unwrap();
		let ranges = match column_projection_predicate_ranges(
			tenant,
			descriptor.table_object_id,
			&clustered_schema(),
			projection,
			&program,
			COLUMN_PROJECTION_RANGE_ARC_BUDGET,
		)
		.unwrap()
		{
			ColumnProjectionRangePruning::Pruned(ranges) => ranges,
			other => panic!("expected pruned ranges, got {other:?}"),
		};
		let entropy_bits = 64 - u32::from(projection.prefix_bits) - 8;
		let suffixes = ranges
			.iter()
			.map(|(start, end)| {
				assert_eq!(start.get() & low_mask(entropy_bits), 0);
				assert_eq!(end.get() & low_mask(entropy_bits), low_mask(entropy_bits));
				(start.get() >> entropy_bits) & 0xff
			})
			.collect::<BTreeSet<_>>();

		assert_eq!(suffixes, BTreeSet::from([u64::from(b'e'), u64::from(b'u')]));
	}

	#[test]
	fn column_projection_predicate_ranges_return_empty_for_unsatisfiable_bounds() {
		let descriptor = clustered_descriptor(
			ProjectionFunctionV1::ZOrder,
			vec![cluster_column("event_hour", 4)],
		);
		let projection = match &descriptor.locality_policy {
			LocalityPolicyV1::ColumnProjection(projection) => projection,
			_ => unreachable!(),
		};
		let mut bytecode = Vec::new();
		push_col(&mut bytecode, 1);
		push_int(&mut bytecode, 0x5000_0000);
		bytecode.push(OP_EQ);
		push_col(&mut bytecode, 1);
		push_int(&mut bytecode, 0xa000_0000);
		bytecode.push(OP_GT);
		bytecode.push(OP_AND);
		let program = WpbcProgram::compile(&bytecode).unwrap();

		assert_eq!(
			column_projection_predicate_ranges(
				TenantId::new(9),
				descriptor.table_object_id,
				&clustered_schema(),
				projection,
				&program,
				COLUMN_PROJECTION_RANGE_ARC_BUDGET,
			)
			.unwrap(),
			ColumnProjectionRangePruning::Pruned(Vec::new())
		);
	}

	#[test]
	fn column_projection_predicate_ranges_fall_back_for_disjunctions() {
		let descriptor = clustered_descriptor(
			ProjectionFunctionV1::ZOrder,
			vec![cluster_column("event_hour", 4)],
		);
		let projection = match &descriptor.locality_policy {
			LocalityPolicyV1::ColumnProjection(projection) => projection,
			_ => unreachable!(),
		};
		let mut bytecode = Vec::new();
		push_col(&mut bytecode, 1);
		push_int(&mut bytecode, 0x2000_0000);
		bytecode.push(OP_GTE);
		push_col(&mut bytecode, 1);
		push_int(&mut bytecode, 0xe000_0000);
		bytecode.push(OP_LTE);
		bytecode.push(OP_OR);
		let program = WpbcProgram::compile(&bytecode).unwrap();

		assert_eq!(
			column_projection_predicate_ranges(
				TenantId::new(9),
				descriptor.table_object_id,
				&clustered_schema(),
				projection,
				&program,
				COLUMN_PROJECTION_RANGE_ARC_BUDGET,
			)
			.unwrap(),
			ColumnProjectionRangePruning::Fallback
		);
	}

	#[test]
	fn column_projection_predicate_ranges_fall_back_when_arc_budget_overflows() {
		let descriptor = clustered_descriptor(
			ProjectionFunctionV1::ZOrder,
			vec![cluster_column("event_hour", 4)],
		);
		let projection = match &descriptor.locality_policy {
			LocalityPolicyV1::ColumnProjection(projection) => projection,
			_ => unreachable!(),
		};
		let mut bytecode = Vec::new();
		push_col(&mut bytecode, 1);
		push_int(&mut bytecode, 0xa000_0000);
		bytecode.push(OP_GTE);
		push_col(&mut bytecode, 1);
		push_int(&mut bytecode, 0xb000_0000);
		bytecode.push(OP_LTE);
		bytecode.push(OP_AND);
		let program = WpbcProgram::compile(&bytecode).unwrap();

		assert_eq!(
			column_projection_predicate_ranges(
				TenantId::new(9),
				descriptor.table_object_id,
				&clustered_schema(),
				projection,
				&program,
				0,
			)
			.unwrap(),
			ColumnProjectionRangePruning::Overflow
		);
	}

	#[test]
	fn column_projection_predicate_ranges_overflow_realistic_sparse_z_slice() {
		let descriptor = clustered_descriptor(
			ProjectionFunctionV1::ZOrder,
			vec![
				cluster_column("event_hour", 16),
				cluster_column("customer_id", 16),
			],
		);
		let projection = match &descriptor.locality_policy {
			LocalityPolicyV1::ColumnProjection(projection) => projection,
			_ => unreachable!(),
		};
		let mut bytecode = Vec::new();
		push_col(&mut bytecode, 1);
		push_int(&mut bytecode, 0x8000_0000);
		bytecode.push(OP_EQ);
		let program = WpbcProgram::compile(&bytecode).unwrap();

		assert_eq!(
			column_projection_predicate_ranges(
				TenantId::new(9),
				descriptor.table_object_id,
				&clustered_schema(),
				projection,
				&program,
				COLUMN_PROJECTION_RANGE_ARC_BUDGET,
			)
			.unwrap(),
			ColumnProjectionRangePruning::Overflow
		);
	}

	#[test]
	fn z_order_encoder_uses_msb_first_uneven_widths() {
		let coords = [0b101_u64, 0b11_u64];
		let widths = [3_u8, 2_u8];
		assert_eq!(z_order_encode(&coords, &widths), 0b11011);
		assert_eq!(z_order_encode(&[0b1010], &[4]), 0b1010);
	}

	#[test]
	fn z_order_bmi2_matches_portable_when_available() {
		let cases: &[(&[u64], &[u8])] = &[
			(&[0b101, 0b11], &[3, 2]),
			(&[0xabc, 0x123, 0x3f], &[12, 12, 6]),
			(&[0xf, 0x00ff, 0x1555, 0x2a], &[4, 8, 13, 6]),
			(&[0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc], &[8, 8, 8, 8, 8, 8]),
		];
		for (coords, widths) in cases {
			assert_eq!(
				z_order_encode(coords, widths),
				z_order_encode_portable(coords, widths)
			);
			#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
			if std::arch::is_x86_feature_detected!("bmi2") {
				assert_eq!(
					unsafe { z_order_encode_bmi2_unchecked(coords, widths) },
					z_order_encode_portable(coords, widths)
				);
			}
		}
	}

	#[test]
	fn signed_clustering_normalization_preserves_order() {
		assert_eq!(normalize_signed_int(i8::MIN as i64, 8, 8), 0x00);
		assert_eq!(normalize_signed_int(-1, 8, 8), 0x7f);
		assert_eq!(normalize_signed_int(0, 8, 8), 0x80);
		assert_eq!(normalize_signed_int(i8::MAX as i64, 8, 8), 0xff);
		assert_eq!(normalize_signed_int(-1, 8, 4), 0x7);
		assert_eq!(normalize_signed_int(0, 8, 12), 0x800);
		assert!(normalize_signed_int(-2, 16, 16) < normalize_signed_int(-1, 16, 16));
		assert!(normalize_signed_int(-1, 16, 16) < normalize_signed_int(0, 16, 16));
	}

	#[test]
	fn catalog_resolves_by_id_and_name() {
		let tenant = TenantId::new(9);
		let descriptor = descriptor_for(&table_schema(DataType::Int64, false));
		let table_object_id = descriptor.table_object_id;
		let generation = descriptor.schema_generation;
		let mut catalog = TableSchemaCatalogV1::default();
		catalog.upsert(tenant, descriptor.clone()).unwrap();
		assert_eq!(
			catalog.get(tenant, table_object_id, generation),
			Some(&descriptor)
		);
		assert_eq!(
			catalog.latest_by_name(tenant, "analytics", "events"),
			Some(&descriptor)
		);
		assert_eq!(
			catalog.latest_by_id(tenant, table_object_id),
			Some(&descriptor)
		);
		catalog.upsert(tenant, descriptor.clone()).unwrap();
		assert_eq!(catalog.len(), 1);
	}

	#[test]
	fn catalog_rejects_table_object_id_reuse_under_different_name() {
		let tenant = TenantId::new(9);
		let descriptor = descriptor_for_generation(&table_schema(DataType::Int64, false), 1);
		let conflicting = TableSchemaDescriptorV1::new(
			descriptor.table_object_id,
			2,
			"analytics",
			"events_copy",
			&table_schema(DataType::Int64, false),
			PrimaryKeyDescriptorV1::single("id"),
			LocalityPolicyV1::default_table_spread(),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.unwrap();
		let mut catalog = TableSchemaCatalogV1::default();
		catalog.upsert(tenant, descriptor.clone()).unwrap();
		let err = catalog.upsert(tenant, conflicting).unwrap_err();
		assert!(err.to_string().contains("table_object_id"));
		assert_eq!(
			catalog.latest_by_id(tenant, descriptor.table_object_id),
			Some(&descriptor)
		);
	}

	#[test]
	fn catalog_rejects_generation_regression() {
		let tenant = TenantId::new(9);
		let newest = descriptor_for_generation(&table_schema(DataType::Int64, false), 2);
		let stale = descriptor_for_generation(&table_schema(DataType::Int64, false), 1);
		let mut catalog = TableSchemaCatalogV1::default();
		catalog.upsert(tenant, newest.clone()).unwrap();
		let err = catalog.upsert(tenant, stale).unwrap_err();
		assert!(err.to_string().contains("regress schema_generation"));
		assert_eq!(
			catalog.latest_by_name(tenant, "analytics", "events"),
			Some(&newest)
		);
	}

	#[test]
	fn row_count_wire_encoding_uses_zero_for_max_batch() {
		assert_eq!(decode_table_write_row_count(0), TABLE_WRITE_MAX_ROWS);
		assert_eq!(
			encode_table_write_row_count(TABLE_WRITE_MAX_ROWS).unwrap(),
			0
		);
		assert_eq!(encode_table_write_row_count(3).unwrap(), 3);
		assert!(encode_table_write_row_count(0).is_err());
		assert!(encode_table_write_row_count(TABLE_WRITE_MAX_ROWS + 1).is_err());
	}

	#[test]
	fn table_write_validation_derives_pk_identity_and_locality() {
		let descriptor = descriptor_for(&table_schema(DataType::Int64, false));
		let batch = table_batch(vec![Some(1), Some(2), Some(3)]);
		let meta = table_meta(&descriptor, batch.num_rows());
		let admission = validate_table_write_request_batch(&meta, &batch, &descriptor).unwrap();

		assert_eq!(admission.canonical_primary_keys.len(), 3);
		assert_eq!(
			admission.canonical_primary_keys[0],
			1_i64.to_le_bytes().to_vec()
		);
		assert_eq!(
			admission.key_identities[0],
			table_row_identity(
				meta.tenant_id,
				meta.table_object_id,
				&admission.canonical_primary_keys[0]
			)
		);
		let ranges = table_spread_prefix_ranges(
			meta.tenant_id,
			meta.table_object_id,
			match descriptor.locality_policy {
				LocalityPolicyV1::TableSpreadProjection(spread) => spread,
				_ => unreachable!(),
			},
		)
		.unwrap();
		assert!(admission.localities.iter().all(|locality| {
			ranges
				.iter()
				.any(|(start, end)| start.get() <= locality.get() && locality.get() <= end.get())
		}));
	}

	#[test]
	fn table_write_validation_rejects_duplicate_pk() {
		let descriptor = descriptor_for(&table_schema(DataType::Int64, false));
		let batch = table_batch(vec![Some(1), Some(1)]);
		let meta = table_meta(&descriptor, batch.num_rows());
		let err = validate_table_write_request_batch(&meta, &batch, &descriptor).unwrap_err();
		assert_eq!(err.code, WpbcRejectCode::WPBC_REJECT_DUPLICATE_PK_IN_BATCH);
	}

	#[test]
	fn table_write_validation_rejects_null_pk() {
		let descriptor = descriptor_for(&table_schema(DataType::Int64, false));
		let batch = table_batch(vec![Some(1), None]);
		let meta = table_meta(&descriptor, batch.num_rows());
		let err = validate_table_write_request_batch(&meta, &batch, &descriptor).unwrap_err();
		assert_eq!(err.code, WpbcRejectCode::WPBC_REJECT_PK_NULL);
	}

	#[test]
	fn table_write_validation_accepts_column_projection_zorder() {
		let descriptor = clustered_descriptor(
			ProjectionFunctionV1::ZOrder,
			vec![
				cluster_string_column("region", 1),
				cluster_column("event_hour", 20),
				cluster_column("customer_id", 20),
			],
		);
		let batch = clustered_batch();
		let meta = table_meta(&descriptor, batch.num_rows());
		let admission = validate_table_write_request_batch(&meta, &batch, &descriptor).unwrap();
		assert_eq!(admission.localities.len(), 3);
		let projection = match &descriptor.locality_policy {
			LocalityPolicyV1::ColumnProjection(projection) => projection,
			_ => unreachable!(),
		};
		let ranges =
			column_projection_prefix_ranges(meta.tenant_id, meta.table_object_id, projection)
				.unwrap();
		assert!(admission.localities.iter().all(|locality| {
			ranges
				.iter()
				.any(|(start, end)| start.get() <= locality.get() && locality.get() <= end.get())
		}));
		assert_ne!(admission.localities[0], admission.localities[1]);
	}

	#[test]
	fn clustering_row_normalization_rejects_null_value() {
		let projection = ColumnLocalityDescriptorV1 {
			columns: vec![cluster_column("event_hour", 4)],
			projection: ProjectionFunctionV1::ZOrder,
			spread_factor: TABLE_SPREAD_DEFAULT_FACTOR,
			prefix_bits: TABLE_SPREAD_DEFAULT_PREFIX_BITS,
		};
		let batch = RecordBatch::try_new(
			Arc::new(Schema::new(vec![
				Field::new("id", DataType::Int64, false),
				Field::new("event_hour", DataType::UInt32, true),
				Field::new("customer_id", DataType::UInt32, false),
				Field::new("region", DataType::LargeUtf8, false),
			])),
			vec![
				Arc::new(Int64Array::from(vec![1, 2])) as Arc<dyn Array>,
				Arc::new(UInt32Array::from(vec![Some(10_u32), None])) as Arc<dyn Array>,
				Arc::new(UInt32Array::from(vec![100_u32, 101])) as Arc<dyn Array>,
				Arc::new(LargeStringArray::from(vec!["us", "eu"])) as Arc<dyn Array>,
			],
		)
		.unwrap();
		let err = normalize_clustering_row(&batch, &projection, 1).unwrap_err();
		assert_eq!(err.code, WpbcRejectCode::WPBC_REJECT_CLUSTERING_VALUE_NULL);
	}

	#[test]
	fn table_write_validation_accepts_column_projection_random_hash() {
		let descriptor = clustered_descriptor(
			ProjectionFunctionV1::RandomHash,
			vec![
				cluster_column("event_hour", 24),
				cluster_column("customer_id", 20),
			],
		);
		let batch = clustered_batch();
		let meta = table_meta(&descriptor, batch.num_rows());
		let admission = validate_table_write_request_batch(&meta, &batch, &descriptor).unwrap();
		assert_eq!(admission.localities.len(), 3);
	}
}
