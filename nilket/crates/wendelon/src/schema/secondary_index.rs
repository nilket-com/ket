use crate::schema::table::{
	PrimaryKeyDescriptor, RowIdentityPolicy, STORAGE_ROW_HLC_COLUMN, STORAGE_ROW_KEY_ID_COLUMN,
	STORAGE_ROW_ROUTING_LOCALITY_COLUMN, STORAGE_ROW_TABLE_OBJECT_ID_COLUMN,
	STORAGE_ROW_TOMBSTONE_COLUMN, TableObjectId, TableSchemaCatalog, TableSchemaDescriptor,
};
use crate::schema::{LogicalSchemaFingerprint, NodeId, SlabId, TenantId};
use arrow::datatypes::{DataType, TimeUnit};
use bytemuck::{Pod, Zeroable};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::io::{Error, ErrorKind};

pub const INDEX_REJECT_LOGICAL_FINGERPRINT_MISMATCH: &str =
	"INDEX_REJECT_LOGICAL_FINGERPRINT_MISMATCH";
pub const INDEX_REJECT_UNSUPPORTED_COLUMN_TYPE: &str = "INDEX_REJECT_UNSUPPORTED_COLUMN_TYPE";
pub const INDEX_REJECT_PK_COLUMN: &str = "INDEX_REJECT_PK_COLUMN";
pub const INDEX_REJECT_SYNTHETIC_COLUMN: &str = "INDEX_REJECT_SYNTHETIC_COLUMN";
pub const SCHEMA_UPDATE_REJECT_INDEX_ALTER_UNSUPPORTED: &str =
	"SCHEMA_UPDATE_REJECT_INDEX_ALTER_UNSUPPORTED";
pub const SCHEMA_UPDATE_REJECT_INDEX_OBJECT_ID_COLLISION: &str =
	"SCHEMA_UPDATE_REJECT_INDEX_OBJECT_ID_COLLISION";
pub const SCHEMA_UPDATE_REJECT_INDEX_NAME_COLLISION: &str =
	"SCHEMA_UPDATE_REJECT_INDEX_NAME_COLLISION";
pub const SCHEMA_UPDATE_REJECT_INDEX_COLUMN_COLLISION: &str =
	"SCHEMA_UPDATE_REJECT_INDEX_COLUMN_COLLISION";

#[derive(
	Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Pod, Zeroable, Serialize, Deserialize,
)]
#[repr(transparent)]
#[serde(transparent)]
pub struct IndexObjectId([u8; 16]);

impl IndexObjectId {
	pub const ZERO: Self = Self([0; 16]);

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

impl fmt::Debug for IndexObjectId {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "IndexObjectId(0x")?;
		for byte in self.as_bytes() {
			write!(f, "{byte:02x}")?;
		}
		write!(f, ")")
	}
}

impl From<[u8; 16]> for IndexObjectId {
	fn from(value: [u8; 16]) -> Self {
		Self::new(value)
	}
}

impl From<IndexObjectId> for [u8; 16] {
	fn from(value: IndexObjectId) -> Self {
		value.0
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IndexArrowTypeTag {
	Boolean,
	Int8,
	Int16,
	Int32,
	Int64,
	UInt8,
	UInt16,
	UInt32,
	UInt64,
	Date32,
	Date64,
	TimestampSecond,
	TimestampMillisecond,
	TimestampMicrosecond,
	TimestampNanosecond,
	Utf8,
	LargeUtf8,
	Binary,
	LargeBinary,
}

impl IndexArrowTypeTag {
	pub fn from_data_type(data_type: &DataType) -> Option<Self> {
		match data_type {
			DataType::Boolean => Some(Self::Boolean),
			DataType::Int8 => Some(Self::Int8),
			DataType::Int16 => Some(Self::Int16),
			DataType::Int32 => Some(Self::Int32),
			DataType::Int64 => Some(Self::Int64),
			DataType::UInt8 => Some(Self::UInt8),
			DataType::UInt16 => Some(Self::UInt16),
			DataType::UInt32 => Some(Self::UInt32),
			DataType::UInt64 => Some(Self::UInt64),
			DataType::Date32 => Some(Self::Date32),
			DataType::Date64 => Some(Self::Date64),
			DataType::Timestamp(TimeUnit::Second, _) => Some(Self::TimestampSecond),
			DataType::Timestamp(TimeUnit::Millisecond, _) => Some(Self::TimestampMillisecond),
			DataType::Timestamp(TimeUnit::Microsecond, _) => Some(Self::TimestampMicrosecond),
			DataType::Timestamp(TimeUnit::Nanosecond, _) => Some(Self::TimestampNanosecond),
			DataType::Utf8 => Some(Self::Utf8),
			DataType::LargeUtf8 => Some(Self::LargeUtf8),
			DataType::Binary => Some(Self::Binary),
			DataType::LargeBinary => Some(Self::LargeBinary),
			_ => None,
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IndexKeyEncoding {
	OrderedScalar,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecondaryIndexKeyDescriptor {
	pub column_name: String,
	pub column_ordinal: u32,
	pub arrow_type_tag: IndexArrowTypeTag,
	pub encoding: IndexKeyEncoding,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecondaryIndexDescriptor {
	pub index_object_id: IndexObjectId,
	pub table_object_id: TableObjectId,
	pub namespace: String,
	pub index_name: String,
	pub table_schema_generation: u64,
	pub logical_schema_fingerprint: LogicalSchemaFingerprint,
	pub key: SecondaryIndexKeyDescriptor,
}

impl SecondaryIndexDescriptor {
	/// Build the supported single-column descriptor from the authoritative table
	/// contract. Callers name the table column; they do not reconstruct ordinals,
	/// Arrow tags, generations, or fingerprints by hand.
	pub fn for_table_column(
		index_object_id: IndexObjectId,
		index_name: impl Into<String>,
		table: &TableSchemaDescriptor,
		column_name: &str,
	) -> std::io::Result<Self> {
		let schema = table.arrow_schema()?;
		let (column_ordinal, field) = schema
			.fields()
			.iter()
			.enumerate()
			.find(|(_, field)| field.name() == column_name)
			.ok_or_else(|| {
				Error::new(
					ErrorKind::InvalidInput,
					format!("secondary index column does not exist: {column_name}"),
				)
			})?;
		let arrow_type_tag =
			IndexArrowTypeTag::from_data_type(field.data_type()).ok_or_else(|| {
				Error::new(
					ErrorKind::InvalidInput,
					format!(
						"{INDEX_REJECT_UNSUPPORTED_COLUMN_TYPE}: column {column_name} has type {}",
						field.data_type()
					),
				)
			})?;
		let descriptor = Self {
			index_object_id,
			table_object_id: table.table_object_id,
			namespace: table.namespace.clone(),
			index_name: index_name.into(),
			table_schema_generation: table.schema_generation,
			logical_schema_fingerprint: table.logical_schema_fingerprint,
			key: SecondaryIndexKeyDescriptor {
				column_name: column_name.to_owned(),
				column_ordinal: u32::try_from(column_ordinal).map_err(|_| {
					Error::new(
						ErrorKind::InvalidInput,
						"secondary index column ordinal exceeds u32",
					)
				})?,
				arrow_type_tag,
				encoding: IndexKeyEncoding::OrderedScalar,
			},
		};
		descriptor.validate_against_table(table)?;
		Ok(descriptor)
	}

	pub fn validate_against_table(&self, table: &TableSchemaDescriptor) -> std::io::Result<()> {
		table.validate()?;
		if self.table_object_id != table.table_object_id {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"{INDEX_REJECT_LOGICAL_FINGERPRINT_MISMATCH}: index table_object_id does not match table descriptor",
				),
			));
		}
		if self.namespace != table.namespace {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"secondary index namespace must match the table namespace",
			));
		}
		if self.table_schema_generation != table.schema_generation {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"secondary index table_schema_generation does not match the current table descriptor",
			));
		}
		if self.logical_schema_fingerprint != table.logical_schema_fingerprint {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"{INDEX_REJECT_LOGICAL_FINGERPRINT_MISMATCH}: index logical fingerprint does not match the current table descriptor",
				),
			));
		}
		if self.index_name.is_empty() || self.index_name.len() > 128 {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"secondary index name length is out of bounds",
			));
		}
		if self.key.encoding != IndexKeyEncoding::OrderedScalar {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!("{INDEX_REJECT_UNSUPPORTED_COLUMN_TYPE}: unsupported index key encoding"),
			));
		}
		if is_storage_synthetic_column(self.key.column_name.as_str()) {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"{INDEX_REJECT_SYNTHETIC_COLUMN}: secondary indexes cannot target storage synthetic columns",
				),
			));
		}
		if is_primary_key_column(&table.row_identity_policy, self.key.column_name.as_str()) {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"{INDEX_REJECT_PK_COLUMN}: secondary indexes cannot target the declared primary key column",
				),
			));
		}
		let schema = table.arrow_schema()?;
		let field = schema
			.fields()
			.get(self.key.column_ordinal as usize)
			.ok_or_else(|| {
				Error::new(
					ErrorKind::InvalidData,
					"secondary index column_ordinal is outside the table schema",
				)
			})?;
		if field.name() != &self.key.column_name {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"secondary index column_name does not match the field at column_ordinal",
			));
		}
		let Some(actual_tag) = IndexArrowTypeTag::from_data_type(field.data_type()) else {
			return Err(Error::new(
				ErrorKind::InvalidData,
				format!(
					"{INDEX_REJECT_UNSUPPORTED_COLUMN_TYPE}: unsupported indexed Arrow type {:?}",
					field.data_type()
				),
			));
		};
		if actual_tag != self.key.arrow_type_tag {
			return Err(Error::new(
				ErrorKind::InvalidData,
				"secondary index arrow_type_tag does not match the table field type",
			));
		}
		Ok(())
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SecondaryIndexState {
	Building,
	Ready,
	Dropping,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecondaryIndexCatalogRow {
	pub descriptor: SecondaryIndexDescriptor,
	pub state: SecondaryIndexState,
	/// Durable per-replica lifecycle receipts. The state gives the receipts their
	/// unambiguous meaning: initial-backfill completion in Building/Ready, then
	/// drop-GC completion after `mark_dropping` clears the set. Reusing the
	/// existing field preserves the postcard row shape for pre-ADR-081 snapshots.
	#[serde(default)]
	pub drop_gc_completed_by: BTreeSet<u32>,
}

impl SecondaryIndexCatalogRow {
	pub fn has_backfill_completion(&self, replica_id: NodeId) -> bool {
		self.state != SecondaryIndexState::Dropping
			&& self.drop_gc_completed_by.contains(&replica_id.id)
	}

	pub fn backfill_completed_by(&self) -> BTreeSet<u32> {
		if self.state == SecondaryIndexState::Dropping {
			BTreeSet::new()
		} else {
			self.drop_gc_completed_by.clone()
		}
	}
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecondaryIndexCatalog {
	#[serde(default)]
	by_id: BTreeMap<SecondaryIndexCatalogKey, SecondaryIndexCatalogRow>,
	#[serde(default)]
	by_name: BTreeMap<SecondaryIndexNameKey, IndexObjectId>,
	#[serde(default)]
	by_table: BTreeMap<SecondaryIndexTableKey, BTreeSet<IndexObjectId>>,
}

impl SecondaryIndexCatalog {
	pub fn create(
		&mut self,
		tenant_id: TenantId,
		descriptor: SecondaryIndexDescriptor,
		tables: &TableSchemaCatalog,
	) -> std::io::Result<()> {
		let table = tables
			.latest_by_id(tenant_id, descriptor.table_object_id)
			.ok_or_else(|| {
				Error::new(
					ErrorKind::NotFound,
					"secondary index create references an unknown table_object_id",
				)
			})?;
		descriptor.validate_against_table(table)?;
		let key = SecondaryIndexCatalogKey {
			tenant_id,
			index_object_id: descriptor.index_object_id,
		};
		if let Some(existing) = self.by_id.get(&key) {
			if existing.descriptor == descriptor && existing.state == SecondaryIndexState::Building
			{
				return Ok(());
			}
			return Err(Error::new(
				ErrorKind::AlreadyExists,
				format!(
					"{SCHEMA_UPDATE_REJECT_INDEX_OBJECT_ID_COLLISION}: secondary index_object_id already exists with a different catalog row"
				),
			));
		}
		let name_key = SecondaryIndexNameKey {
			tenant_id,
			namespace: descriptor.namespace.clone(),
			index_name: descriptor.index_name.clone(),
		};
		if self.by_name.contains_key(&name_key) {
			return Err(Error::new(
				ErrorKind::AlreadyExists,
				format!(
					"{SCHEMA_UPDATE_REJECT_INDEX_NAME_COLLISION}: secondary index name is already bound in this namespace"
				),
			));
		}
		for existing_id in self
			.by_table
			.get(&SecondaryIndexTableKey {
				tenant_id,
				table_object_id: descriptor.table_object_id,
			})
			.into_iter()
			.flatten()
		{
			if let Some(existing) = self.by_id.get(&SecondaryIndexCatalogKey {
				tenant_id,
				index_object_id: *existing_id,
			}) {
				if existing.descriptor.key.column_ordinal == descriptor.key.column_ordinal
					&& existing.state != SecondaryIndexState::Dropping
				{
					return Err(Error::new(
						ErrorKind::AlreadyExists,
						format!(
							"{SCHEMA_UPDATE_REJECT_INDEX_COLUMN_COLLISION}: secondary index already exists for this table column"
						),
					));
				}
			}
		}
		self.by_name.insert(name_key, descriptor.index_object_id);
		self.by_table
			.entry(SecondaryIndexTableKey {
				tenant_id,
				table_object_id: descriptor.table_object_id,
			})
			.or_default()
			.insert(descriptor.index_object_id);
		self.by_id.insert(
			key,
			SecondaryIndexCatalogRow {
				descriptor,
				state: SecondaryIndexState::Building,
				drop_gc_completed_by: BTreeSet::new(),
			},
		);
		Ok(())
	}

	pub fn mark_backfill_complete(
		&mut self,
		tenant_id: TenantId,
		index_object_id: IndexObjectId,
		replica_id: NodeId,
		current_voters: &BTreeSet<u32>,
	) -> std::io::Result<()> {
		let Some(row) = self.by_id.get_mut(&SecondaryIndexCatalogKey {
			tenant_id,
			index_object_id,
		}) else {
			return Err(Error::new(
				ErrorKind::NotFound,
				"secondary index backfill-complete references an unknown index",
			));
		};
		match row.state {
			SecondaryIndexState::Building | SecondaryIndexState::Ready => {
				row.drop_gc_completed_by.insert(replica_id.id);
				if row.state == SecondaryIndexState::Building
					&& current_voters
						.iter()
						.all(|voter| row.drop_gc_completed_by.contains(voter))
				{
					row.state = SecondaryIndexState::Ready;
				}
				Ok(())
			}
			SecondaryIndexState::Dropping => Err(Error::new(
				ErrorKind::InvalidData,
				"secondary index backfill-complete cannot advance a Dropping index",
			)),
		}
	}

	pub fn has_backfill_completion(
		&self,
		tenant_id: TenantId,
		index_object_id: IndexObjectId,
		replica_id: NodeId,
	) -> bool {
		self.get(tenant_id, index_object_id)
			.is_some_and(|row| row.has_backfill_completion(replica_id))
	}

	/// Re-evaluate Building rows after a Raft voter-set change. Removing an
	/// incomplete voter drops its outstanding requirement immediately; adding a
	/// voter never demotes Ready and its planner remains scan-only until it earns
	/// its own receipt.
	pub fn reconcile_backfill_readiness(
		&mut self,
		current_voters: &BTreeSet<u32>,
	) -> Vec<(TenantId, TableObjectId, IndexObjectId)> {
		if current_voters.is_empty() {
			return Vec::new();
		}
		let mut promoted = Vec::new();
		for (key, row) in &mut self.by_id {
			if row.state == SecondaryIndexState::Building
				&& current_voters
					.iter()
					.all(|voter| row.drop_gc_completed_by.contains(voter))
			{
				row.state = SecondaryIndexState::Ready;
				promoted.push((
					key.tenant_id,
					row.descriptor.table_object_id,
					row.descriptor.index_object_id,
				));
			}
		}
		promoted
	}

	pub fn for_tenant(&self, tenant_id: TenantId) -> Vec<&SecondaryIndexCatalogRow> {
		self.by_id
			.iter()
			.filter(|(key, _)| key.tenant_id == tenant_id)
			.map(|(_, row)| row)
			.collect()
	}

	pub fn rows(&self) -> Vec<(TenantId, &SecondaryIndexCatalogRow)> {
		self.by_id
			.iter()
			.map(|(key, row)| (key.tenant_id, row))
			.collect()
	}

	pub fn mark_dropping(
		&mut self,
		tenant_id: TenantId,
		index_object_id: IndexObjectId,
	) -> std::io::Result<()> {
		let Some(row) = self.by_id.get_mut(&SecondaryIndexCatalogKey {
			tenant_id,
			index_object_id,
		}) else {
			return Ok(());
		};
		match row.state {
			SecondaryIndexState::Building | SecondaryIndexState::Ready => {
				row.state = SecondaryIndexState::Dropping;
				row.drop_gc_completed_by.clear();
			}
			SecondaryIndexState::Dropping => {}
		}
		Ok(())
	}

	pub fn mark_drop_gc_complete(
		&mut self,
		tenant_id: TenantId,
		index_object_id: IndexObjectId,
		replica_id: NodeId,
		current_voters: &BTreeSet<u32>,
	) -> std::io::Result<()> {
		let key = SecondaryIndexCatalogKey {
			tenant_id,
			index_object_id,
		};
		let remove = {
			let Some(row) = self.by_id.get_mut(&key) else {
				return Ok(());
			};
			if row.state != SecondaryIndexState::Dropping {
				return Err(Error::new(
					ErrorKind::InvalidData,
					"secondary index drop-GC complete requires Dropping state",
				));
			}
			row.drop_gc_completed_by.insert(replica_id.id);
			current_voters
				.iter()
				.all(|voter| row.drop_gc_completed_by.contains(voter))
		};
		if remove {
			self.remove_by_key(tenant_id, index_object_id);
		}
		Ok(())
	}

	pub fn get(
		&self,
		tenant_id: TenantId,
		index_object_id: IndexObjectId,
	) -> Option<&SecondaryIndexCatalogRow> {
		self.by_id.get(&SecondaryIndexCatalogKey {
			tenant_id,
			index_object_id,
		})
	}

	pub fn by_name(
		&self,
		tenant_id: TenantId,
		namespace: &str,
		index_name: &str,
	) -> Option<&SecondaryIndexCatalogRow> {
		let index_object_id = self.by_name.get(&SecondaryIndexNameKey {
			tenant_id,
			namespace: namespace.to_owned(),
			index_name: index_name.to_owned(),
		})?;
		self.get(tenant_id, *index_object_id)
	}

	pub fn for_table(
		&self,
		tenant_id: TenantId,
		table_object_id: TableObjectId,
	) -> Vec<&SecondaryIndexCatalogRow> {
		self.by_table
			.get(&SecondaryIndexTableKey {
				tenant_id,
				table_object_id,
			})
			.into_iter()
			.flatten()
			.filter_map(|index_object_id| self.get(tenant_id, *index_object_id))
			.collect()
	}

	fn remove_by_key(&mut self, tenant_id: TenantId, index_object_id: IndexObjectId) {
		let key = SecondaryIndexCatalogKey {
			tenant_id,
			index_object_id,
		};
		let Some(row) = self.by_id.remove(&key) else {
			return;
		};
		self.by_name.remove(&SecondaryIndexNameKey {
			tenant_id,
			namespace: row.descriptor.namespace,
			index_name: row.descriptor.index_name,
		});
		let table_key = SecondaryIndexTableKey {
			tenant_id,
			table_object_id: row.descriptor.table_object_id,
		};
		let should_remove = if let Some(ids) = self.by_table.get_mut(&table_key) {
			ids.remove(&index_object_id);
			ids.is_empty()
		} else {
			false
		};
		if should_remove {
			self.by_table.remove(&table_key);
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecondaryIndexDropRequest {
	pub index_object_id: IndexObjectId,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecondaryIndexLifecycleComplete {
	pub index_object_id: IndexObjectId,
	pub replica_id: NodeId,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecondaryIndexReplicaStatus {
	pub replica_id: NodeId,
	pub initial_backfill_complete: bool,
	pub coverage_gap_slabs: Vec<SlabId>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecondaryIndexStatus {
	pub descriptor: SecondaryIndexDescriptor,
	pub state: SecondaryIndexState,
	pub backfill_completed_by: BTreeSet<u32>,
	pub replicas: Vec<SecondaryIndexReplicaStatus>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecondaryIndexStatusRequest {
	pub tenant_id: TenantId,
}

pub fn encode_secondary_index_descriptor(
	descriptor: &SecondaryIndexDescriptor,
) -> std::io::Result<Vec<u8>> {
	postcard::to_allocvec(descriptor).map_err(|err| {
		Error::new(
			ErrorKind::InvalidData,
			format!("failed to encode secondary index descriptor: {err}"),
		)
	})
}

pub fn decode_secondary_index_descriptor(
	bytes: &[u8],
) -> std::io::Result<SecondaryIndexDescriptor> {
	postcard::from_bytes(bytes).map_err(|err| {
		Error::new(
			ErrorKind::InvalidData,
			format!("failed to decode secondary index descriptor: {err}"),
		)
	})
}

pub fn encode_secondary_index_drop_request(
	request: &SecondaryIndexDropRequest,
) -> std::io::Result<Vec<u8>> {
	postcard::to_allocvec(request).map_err(|err| {
		Error::new(
			ErrorKind::InvalidData,
			format!("failed to encode secondary index drop request: {err}"),
		)
	})
}

pub fn decode_secondary_index_drop_request(
	bytes: &[u8],
) -> std::io::Result<SecondaryIndexDropRequest> {
	postcard::from_bytes(bytes).map_err(|err| {
		Error::new(
			ErrorKind::InvalidData,
			format!("failed to decode secondary index drop request: {err}"),
		)
	})
}

pub fn encode_secondary_index_lifecycle_complete(
	complete: &SecondaryIndexLifecycleComplete,
) -> std::io::Result<Vec<u8>> {
	postcard::to_allocvec(complete).map_err(|err| {
		Error::new(
			ErrorKind::InvalidData,
			format!("failed to encode secondary index lifecycle-complete payload: {err}"),
		)
	})
}

pub fn decode_secondary_index_lifecycle_complete(
	bytes: &[u8],
) -> std::io::Result<SecondaryIndexLifecycleComplete> {
	postcard::from_bytes(bytes).map_err(|err| {
		Error::new(
			ErrorKind::InvalidData,
			format!("failed to decode secondary index lifecycle-complete payload: {err}"),
		)
	})
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
struct SecondaryIndexCatalogKey {
	tenant_id: TenantId,
	index_object_id: IndexObjectId,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
struct SecondaryIndexNameKey {
	tenant_id: TenantId,
	namespace: String,
	index_name: String,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
struct SecondaryIndexTableKey {
	tenant_id: TenantId,
	table_object_id: TableObjectId,
}

fn is_primary_key_column(policy: &RowIdentityPolicy, column_name: &str) -> bool {
	matches!(
		policy.primary_key(),
		Some(PrimaryKeyDescriptor { columns, .. })
			if columns.first().is_some_and(|column| column == column_name)
	)
}

pub fn is_storage_synthetic_column(column_name: &str) -> bool {
	matches!(
		column_name,
		STORAGE_ROW_HLC_COLUMN
			| STORAGE_ROW_KEY_ID_COLUMN
			| STORAGE_ROW_ROUTING_LOCALITY_COLUMN
			| STORAGE_ROW_TABLE_OBJECT_ID_COLUMN
			| STORAGE_ROW_TOMBSTONE_COLUMN
	) || column_name.starts_with("__row_")
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::schema::{
		LocalityPolicy, PrimaryKeyDescriptor, SchemaEvolutionPolicy, TableObjectId,
		TableSchemaDescriptor,
	};
	use arrow::datatypes::{DataType, Field, Schema};

	fn table() -> TableSchemaDescriptor {
		TableSchemaDescriptor::new(
			TableObjectId::new([0x52; 16]),
			7,
			"analytics",
			"events",
			&Schema::new(vec![
				Field::new("id", DataType::Int64, false),
				Field::new("customer_id", DataType::UInt64, true),
				Field::new("payload", DataType::Float64, true),
			]),
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
			LocalityPolicy::default_table_spread(),
			SchemaEvolutionPolicy::AdditiveOnly,
		)
		.unwrap()
	}

	fn descriptor(
		column_name: &str,
		column_ordinal: u32,
		tag: IndexArrowTypeTag,
	) -> SecondaryIndexDescriptor {
		let table = table();
		SecondaryIndexDescriptor {
			index_object_id: IndexObjectId::new([0x99; 16]),
			table_object_id: table.table_object_id,
			namespace: table.namespace.clone(),
			index_name: "events_customer_id_idx".to_owned(),
			table_schema_generation: table.schema_generation,
			logical_schema_fingerprint: table.logical_schema_fingerprint,
			key: SecondaryIndexKeyDescriptor {
				column_name: column_name.to_owned(),
				column_ordinal,
				arrow_type_tag: tag,
				encoding: IndexKeyEncoding::OrderedScalar,
			},
		}
	}

	#[test]
	fn descriptor_accepts_supported_non_pk_column() {
		let table = table();
		let descriptor = descriptor("customer_id", 1, IndexArrowTypeTag::UInt64);
		descriptor.validate_against_table(&table).unwrap();
	}

	#[test]
	fn supported_constructor_derives_descriptor_coordinates_from_the_table() {
		let table = table();
		let descriptor = SecondaryIndexDescriptor::for_table_column(
			IndexObjectId::new([0x91; 16]),
			"events_customer_idx",
			&table,
			"customer_id",
		)
		.unwrap();
		assert_eq!(descriptor.table_object_id, table.table_object_id);
		assert_eq!(descriptor.table_schema_generation, table.schema_generation);
		assert_eq!(
			descriptor.logical_schema_fingerprint,
			table.logical_schema_fingerprint
		);
		assert_eq!(descriptor.key.column_ordinal, 1);
		assert_eq!(descriptor.key.arrow_type_tag, IndexArrowTypeTag::UInt64);
	}

	#[test]
	fn descriptor_rejects_pk_synthetic_and_unsupported_type() {
		let table = table();
		let pk = descriptor("id", 0, IndexArrowTypeTag::Int64)
			.validate_against_table(&table)
			.unwrap_err();
		assert!(pk.to_string().contains(INDEX_REJECT_PK_COLUMN));

		let synthetic = descriptor(STORAGE_ROW_KEY_ID_COLUMN, 1, IndexArrowTypeTag::UInt64)
			.validate_against_table(&table)
			.unwrap_err();
		assert!(
			synthetic
				.to_string()
				.contains(INDEX_REJECT_SYNTHETIC_COLUMN)
		);

		let unsupported = descriptor("payload", 2, IndexArrowTypeTag::UInt64)
			.validate_against_table(&table)
			.unwrap_err();
		assert!(
			unsupported
				.to_string()
				.contains(INDEX_REJECT_UNSUPPORTED_COLUMN_TYPE)
		);
	}

	#[test]
	fn catalog_lifecycle_is_linear_and_drop_waits_for_all_voters() {
		let tenant = TenantId::new(7);
		let table = table();
		let mut tables = TableSchemaCatalog::default();
		tables.upsert(tenant, table.clone()).unwrap();
		let mut catalog = SecondaryIndexCatalog::default();
		let descriptor = descriptor("customer_id", 1, IndexArrowTypeTag::UInt64);
		let index_object_id = descriptor.index_object_id;
		catalog.create(tenant, descriptor, &tables).unwrap();
		assert_eq!(
			catalog.get(tenant, index_object_id).unwrap().state,
			SecondaryIndexState::Building
		);
		catalog
			.mark_backfill_complete(
				tenant,
				index_object_id,
				NodeId {
					id: 1,
					_reserved: 0,
				},
				&BTreeSet::from([1, 2]),
			)
			.unwrap();
		assert_eq!(
			catalog.get(tenant, index_object_id).unwrap().state,
			SecondaryIndexState::Building
		);
		catalog
			.mark_backfill_complete(
				tenant,
				index_object_id,
				NodeId {
					id: 2,
					_reserved: 0,
				},
				&BTreeSet::from([1, 2]),
			)
			.unwrap();
		assert_eq!(
			catalog.get(tenant, index_object_id).unwrap().state,
			SecondaryIndexState::Ready
		);
		catalog.mark_dropping(tenant, index_object_id).unwrap();
		let voters = BTreeSet::from([1, 2]);
		catalog
			.mark_drop_gc_complete(
				tenant,
				index_object_id,
				NodeId {
					id: 1,
					_reserved: 0,
				},
				&voters,
			)
			.unwrap();
		assert!(catalog.get(tenant, index_object_id).is_some());
		catalog
			.mark_drop_gc_complete(
				tenant,
				index_object_id,
				NodeId {
					id: 2,
					_reserved: 0,
				},
				&voters,
			)
			.unwrap();
		assert!(catalog.get(tenant, index_object_id).is_none());
	}

	#[test]
	fn backfill_receipts_are_durable_and_membership_rules_are_explicit() {
		let tenant = TenantId::new(7);
		let table = table();
		let table_object_id = table.table_object_id;
		let mut tables = TableSchemaCatalog::default();
		tables.upsert(tenant, table).unwrap();
		let mut catalog = SecondaryIndexCatalog::default();
		let descriptor = descriptor("customer_id", 1, IndexArrowTypeTag::UInt64);
		let index_object_id = descriptor.index_object_id;
		catalog.create(tenant, descriptor, &tables).unwrap();
		catalog
			.mark_backfill_complete(
				tenant,
				index_object_id,
				NodeId {
					id: 1,
					_reserved: 0,
				},
				&BTreeSet::from([1, 2]),
			)
			.unwrap();
		assert_eq!(
			catalog.get(tenant, index_object_id).unwrap().state,
			SecondaryIndexState::Building
		);

		let encoded = postcard::to_allocvec(&catalog).unwrap();
		let mut recovered: SecondaryIndexCatalog = postcard::from_bytes(&encoded).unwrap();
		assert!(recovered.has_backfill_completion(
			tenant,
			index_object_id,
			NodeId {
				id: 1,
				_reserved: 0
			}
		));
		// Removing the incomplete voter drops its outstanding requirement without
		// requiring a replica to re-submit a completion it already recorded.
		assert_eq!(
			recovered.reconcile_backfill_readiness(&BTreeSet::from([1])),
			vec![(tenant, table_object_id, index_object_id)]
		);
		assert_eq!(
			recovered.get(tenant, index_object_id).unwrap().state,
			SecondaryIndexState::Ready
		);
		// Adding a voter does not demote the durable catalog state; planner
		// eligibility on that voter remains controlled by its own receipt.
		assert!(!recovered.has_backfill_completion(
			tenant,
			index_object_id,
			NodeId {
				id: 2,
				_reserved: 0
			}
		));
		assert_eq!(
			recovered.get(tenant, index_object_id).unwrap().state,
			SecondaryIndexState::Ready
		);
	}

	#[test]
	fn legacy_catalog_row_keeps_drop_receipts_and_defaults_backfill_receipts() {
		#[derive(Serialize)]
		struct LegacySecondaryIndexCatalogRow {
			descriptor: SecondaryIndexDescriptor,
			state: SecondaryIndexState,
			drop_gc_completed_by: BTreeSet<u32>,
		}

		let legacy = LegacySecondaryIndexCatalogRow {
			descriptor: descriptor("customer_id", 1, IndexArrowTypeTag::UInt64),
			state: SecondaryIndexState::Dropping,
			drop_gc_completed_by: BTreeSet::from([1, 3]),
		};
		let encoded = postcard::to_allocvec(&legacy).unwrap();
		let decoded: SecondaryIndexCatalogRow = postcard::from_bytes(&encoded).unwrap();
		assert_eq!(decoded.drop_gc_completed_by, BTreeSet::from([1, 3]));
		assert!(!decoded.has_backfill_completion(NodeId {
			id: 1,
			_reserved: 0,
		}));
	}

	#[test]
	fn catalog_uniqueness_rejects_use_schema_update_taxonomy() {
		let tenant = TenantId::new(7);
		let table = table();
		let mut tables = TableSchemaCatalog::default();
		tables.upsert(tenant, table).unwrap();
		let descriptor = descriptor("customer_id", 1, IndexArrowTypeTag::UInt64);
		let mut catalog = SecondaryIndexCatalog::default();
		catalog.create(tenant, descriptor.clone(), &tables).unwrap();

		let mut object_collision = descriptor.clone();
		object_collision.index_name = "events_customer_id_idx_v2".to_owned();
		let err = catalog
			.create(tenant, object_collision, &tables)
			.unwrap_err();
		assert!(
			err.to_string()
				.contains(SCHEMA_UPDATE_REJECT_INDEX_OBJECT_ID_COLLISION)
		);

		let mut name_collision = descriptor.clone();
		name_collision.index_object_id = IndexObjectId::new([0x98; 16]);
		let err = catalog.create(tenant, name_collision, &tables).unwrap_err();
		assert!(
			err.to_string()
				.contains(SCHEMA_UPDATE_REJECT_INDEX_NAME_COLLISION)
		);

		let mut column_collision = descriptor;
		column_collision.index_object_id = IndexObjectId::new([0x97; 16]);
		column_collision.index_name = "events_customer_id_idx_v3".to_owned();
		let err = catalog
			.create(tenant, column_collision, &tables)
			.unwrap_err();
		assert!(
			err.to_string()
				.contains(SCHEMA_UPDATE_REJECT_INDEX_COLUMN_COLLISION)
		);
	}
	fn pd22_changed_table() -> TableSchemaDescriptor {
		let original = table();
		let schema = original.arrow_schema().unwrap();
		let mut fields = schema
			.fields()
			.iter()
			.map(|field| field.as_ref().clone())
			.collect::<Vec<_>>();
		fields.push(Field::new("added", DataType::Int64, true));
		TableSchemaDescriptor::new(
			original.table_object_id,
			original.schema_generation + 1,
			"analytics",
			"events",
			&Schema::new(fields),
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
			LocalityPolicy::default_table_spread(),
			SchemaEvolutionPolicy::AdditiveOnly,
		)
		.unwrap()
	}

	#[test]
	fn pd22_live_catalog_refuses_changed_fingerprint_with_the_same_index_id() {
		let tenant = TenantId::new(7);
		let mut tables = TableSchemaCatalog::default();
		tables.upsert(tenant, table()).unwrap();
		let old = descriptor("customer_id", 1, IndexArrowTypeTag::UInt64);
		let mut catalog = SecondaryIndexCatalog::default();
		catalog.create(tenant, old.clone(), &tables).unwrap();
		let changed = pd22_changed_table();
		tables.upsert(tenant, changed.clone()).unwrap();
		let new = SecondaryIndexDescriptor::for_table_column(
			old.index_object_id,
			old.index_name.clone(),
			&changed,
			"customer_id",
		)
		.unwrap();
		assert_ne!(
			old.logical_schema_fingerprint,
			new.logical_schema_fingerprint
		);
		let error = catalog.create(tenant, new, &tables).unwrap_err();
		assert_eq!(error.kind(), ErrorKind::AlreadyExists);
		assert!(
			error
				.to_string()
				.contains(SCHEMA_UPDATE_REJECT_INDEX_OBJECT_ID_COLLISION)
		);
		assert_eq!(
			catalog.get(tenant, old.index_object_id).unwrap().descriptor,
			old
		);
	}

	#[test]
	fn pd22_catalog_can_reuse_an_index_id_after_drop_gc_for_a_new_fingerprint() {
		let tenant = TenantId::new(7);
		let mut tables = TableSchemaCatalog::default();
		tables.upsert(tenant, table()).unwrap();
		let old = descriptor("customer_id", 1, IndexArrowTypeTag::UInt64);
		let mut catalog = SecondaryIndexCatalog::default();
		catalog.create(tenant, old.clone(), &tables).unwrap();
		catalog.mark_dropping(tenant, old.index_object_id).unwrap();
		catalog
			.mark_drop_gc_complete(
				tenant,
				old.index_object_id,
				NodeId {
					id: 1,
					_reserved: 0,
				},
				&BTreeSet::from([1]),
			)
			.unwrap();
		assert!(catalog.get(tenant, old.index_object_id).is_none());
		let changed = pd22_changed_table();
		tables.upsert(tenant, changed.clone()).unwrap();
		let new = SecondaryIndexDescriptor::for_table_column(
			old.index_object_id,
			old.index_name.clone(),
			&changed,
			"customer_id",
		)
		.unwrap();
		assert_ne!(
			old.logical_schema_fingerprint,
			new.logical_schema_fingerprint
		);
		catalog.create(tenant, new.clone(), &tables).unwrap();
		assert_eq!(
			catalog.get(tenant, old.index_object_id).unwrap().descriptor,
			new
		);
	}
}
