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

pub const TABLE_SCHEMA_DESCRIPTOR_VERSION: u16 = 1;
pub const TABLE_SPREAD_DEFAULT_FACTOR: u16 = 4;
pub const TABLE_SPREAD_MAX_FACTOR: u16 = 256;
pub const TABLE_SPREAD_DEFAULT_PREFIX_BITS: u8 = 16;
pub const TABLE_WRITE_MAX_ROWS: usize = 65_536;
pub const TABLE_FIELD_DEFAULT_METADATA_KEY: &str = "wendelon.default.v1";

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
		write!(f, "{:?}: {}", self.code, self.message)
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
	for pk in &canonical_primary_keys {
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
			LocalityPolicyV1::ColumnProjection(_) => {
				return Err(TableWriteValidationError::new(
					WpbcRejectCode::WPBC_REJECT_LOCALITY_UNSUPPORTED,
					"ColumnProjection locality is reserved for ADR 038",
				));
			}
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
	pub columns: Vec<String>,
	pub projection: ProjectionFunctionV1,
	pub spread_factor: u16,
	pub prefix_bits: u8,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionFunctionV1 {
	RandomHash,
	ZOrder,
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
			if projection.columns.is_empty() {
				return Err(Error::new(
					ErrorKind::InvalidData,
					"column locality projection requires at least one column",
				));
			}
			for column in &projection.columns {
				validate_descriptor_name("locality column", column)?;
				schema.field_with_name(column).map_err(|_| {
					Error::new(
						ErrorKind::InvalidData,
						format!("locality column {column:?} is not present in Arrow schema"),
					)
				})?;
			}
			Ok(())
		}
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
	use arrow::array::{Int64Array, LargeStringArray};
	use arrow::datatypes::{Field, Schema};
	use arrow::record_batch::RecordBatch;
	use std::collections::HashMap;
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
	fn table_write_validation_rejects_column_projection_in_v1() {
		let schema = table_schema(DataType::Int64, false);
		let descriptor = TableSchemaDescriptorV1::new(
			TableObjectId::new([7; 16]),
			1,
			"analytics",
			"events",
			&schema,
			PrimaryKeyDescriptorV1::single("id"),
			LocalityPolicyV1::ColumnProjection(ColumnLocalityDescriptorV1 {
				columns: vec!["name".to_owned()],
				projection: ProjectionFunctionV1::RandomHash,
				spread_factor: TABLE_SPREAD_DEFAULT_FACTOR,
				prefix_bits: TABLE_SPREAD_DEFAULT_PREFIX_BITS,
			}),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.unwrap();
		let batch = table_batch(vec![Some(1), Some(2)]);
		let meta = table_meta(&descriptor, batch.num_rows());
		let err = validate_table_write_request_batch(&meta, &batch, &descriptor).unwrap_err();
		assert_eq!(err.code, WpbcRejectCode::WPBC_REJECT_LOCALITY_UNSUPPORTED);
	}
}
