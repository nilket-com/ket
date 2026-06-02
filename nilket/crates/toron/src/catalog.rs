use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use arrow::datatypes::{DataType, Schema, SchemaRef};
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;
use uuid::Uuid;

use crate::distributed::budget::{
	ByteCount, QueryBudget, TenantBudgetPolicy, TenantDescriptorSyncAccountant,
};
use crate::distributed::task::RowCount;
use crate::error::{CatalogError, StorageError};
use crate::ir::{
	ColumnOrdinal, CreateTableRowIdentity, LogicalTypeRegistry, LogicalTypeResolver,
	LogicalTypeSpec, TableId, decode_column_logical_type_with_resolver,
};
use crate::storage::TenantId;
use wendelon::integration::compute::SchemaIntentId;
use wendelon::schema::{
	LocalityPolicy, MultiRowLayoutDescriptor, PhysicalDescriptorFingerprint,
	PrimaryKeyDescriptorV1, RecordLayoutPolicy, RowIdentityPolicy, SchemaEvolutionPolicyV1,
	TableObjectId, TableSchemaDescriptor, TenantId as WendelonTenantId,
};
use wendelon_client::{
	Client, ClientError, DropOperationId, LogicalTypeDefinition, NdvFact as StorageNdvFact,
	SchemaGeneration, SchemaUpdateReceipt, TableOptimizerFacts as StorageTableOptimizerFacts,
	TableSchemaUpdateRequest,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableRef {
	pub schema: Option<String>,
	pub table: String,
}

impl TableRef {
	pub fn new(table: impl Into<String>) -> Self {
		Self {
			schema: None,
			table: table.into(),
		}
	}

	pub fn with_schema(schema: impl Into<String>, table: impl Into<String>) -> Self {
		Self {
			schema: Some(schema.into()),
			table: table.into(),
		}
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableRowIdentity {
	AppendOnly,
	PrimaryKey {
		column: String,
		ordinal: ColumnOrdinal,
	},
	Unsupported,
}

impl TableRowIdentity {
	fn matches_create_identity(&self, row_identity: &CreateTableRowIdentity) -> bool {
		match (self, row_identity) {
			(Self::AppendOnly, CreateTableRowIdentity::AppendOnly) => true,
			(
				Self::PrimaryKey { column, .. },
				CreateTableRowIdentity::PrimaryKey {
					column: requested_column,
				},
			) => column == requested_column,
			_ => false,
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DistinctCountFact {
	Exact(u64),
	Approx { estimate: u64, rel_err_ppm: u32 },
}

impl DistinctCountFact {
	pub const fn point_estimate(self) -> u64 {
		match self {
			Self::Exact(value) => value,
			Self::Approx { estimate, .. } => estimate,
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DescriptorColumnNdvFact {
	pub column_ordinal: ColumnOrdinal,
	pub distinct_count: DistinctCountFact,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RangeFact {
	pub arrow_type_tag: u8,
	pub null_count: u64,
	pub non_null_count: u64,
	pub min: Option<Vec<u8>>,
	pub max: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DescriptorColumnRangeFact {
	pub column_ordinal: ColumnOrdinal,
	pub range: RangeFact,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TableOptimizerFacts {
	pub byte_upper_bound: Option<ByteCount>,
	pub row_upper_bound: Option<RowCount>,
	pub column_ndv: BTreeMap<ColumnOrdinal, DistinctCountFact>,
	pub column_range: BTreeMap<ColumnOrdinal, RangeFact>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DescriptorOptimizerFacts {
	pub schema_generation: u64,
	pub physical_descriptor_fingerprint: PhysicalDescriptorFingerprint,
	pub manifest_epoch: u64,
	pub materialized_byte_upper_bound: Option<ByteCount>,
	pub row_upper_bound: Option<RowCount>,
	#[serde(default)]
	pub column_ndv: Vec<DescriptorColumnNdvFact>,
	#[serde(default)]
	pub column_range: Vec<DescriptorColumnRangeFact>,
}

impl DescriptorOptimizerFacts {
	pub fn for_descriptor(
		self,
		descriptor: &wendelon_client::TableSchemaDescriptor,
	) -> TableOptimizerFacts {
		if self.schema_generation != descriptor.schema_generation
			|| self.physical_descriptor_fingerprint != descriptor.physical_descriptor_fingerprint
		{
			return TableOptimizerFacts::default();
		}

		let byte_upper_bound = match (
			self.materialized_byte_upper_bound,
			self.row_upper_bound.map(RowCount::get),
		) {
			(None, Some(0)) => Some(ByteCount::ZERO),
			(byte_upper_bound, _) => byte_upper_bound,
		};
		TableOptimizerFacts {
			byte_upper_bound,
			row_upper_bound: self.row_upper_bound,
			column_ndv: descriptor_column_ndv(self.column_ndv, descriptor),
			column_range: descriptor_column_range(self.column_range, descriptor),
		}
	}
}

fn descriptor_optimizer_facts_from_storage(
	facts: StorageTableOptimizerFacts,
	descriptor: &wendelon_client::TableSchemaDescriptor,
) -> Option<DescriptorOptimizerFacts> {
	if facts.table_object_id != descriptor.table_object_id {
		return None;
	}
	if facts.schema_generation != descriptor.schema_generation
		|| facts.physical_descriptor_fingerprint != descriptor.physical_descriptor_fingerprint
	{
		return None;
	}
	let column_ndv = facts
		.column_ndv
		.into_iter()
		.map(|fact| DescriptorColumnNdvFact {
			column_ordinal: ColumnOrdinal(u32::from(fact.column_ordinal)),
			distinct_count: match fact.distinct {
				StorageNdvFact::Exact { count } => DistinctCountFact::Exact(count),
				StorageNdvFact::Approx {
					estimate,
					rel_err_ppm,
				} => DistinctCountFact::Approx {
					estimate,
					rel_err_ppm,
				},
			},
		})
		.collect();
	let column_range = facts
		.column_range
		.into_iter()
		.map(|fact| DescriptorColumnRangeFact {
			column_ordinal: ColumnOrdinal(u32::from(fact.column_ordinal)),
			range: RangeFact {
				arrow_type_tag: fact.arrow_type_tag,
				null_count: fact.null_count,
				non_null_count: fact.non_null_count,
				min: fact.min,
				max: fact.max,
			},
		})
		.collect();
	Some(DescriptorOptimizerFacts {
		schema_generation: facts.schema_generation,
		physical_descriptor_fingerprint: facts.physical_descriptor_fingerprint,
		manifest_epoch: facts.producer_epoch,
		materialized_byte_upper_bound: None,
		row_upper_bound: facts.row_upper_bound.map(RowCount::new),
		column_ndv,
		column_range,
	})
}

fn descriptor_column_ndv(
	column_ndv: Vec<DescriptorColumnNdvFact>,
	descriptor: &wendelon_client::TableSchemaDescriptor,
) -> BTreeMap<ColumnOrdinal, DistinctCountFact> {
	let column_count = descriptor
		.arrow_schema()
		.map(|schema| schema.fields().len())
		.unwrap_or(0);
	// Wendelon emits at most one fact per ordinal; BTreeMap keeps duplicate input deterministic.
	column_ndv
		.into_iter()
		.filter(|fact| (fact.column_ordinal.0 as usize) < column_count)
		.map(|fact| (fact.column_ordinal, fact.distinct_count))
		.collect()
}

fn descriptor_column_range(
	column_range: Vec<DescriptorColumnRangeFact>,
	descriptor: &wendelon_client::TableSchemaDescriptor,
) -> BTreeMap<ColumnOrdinal, RangeFact> {
	let column_count = descriptor
		.arrow_schema()
		.map(|schema| schema.fields().len())
		.unwrap_or(0);
	column_range
		.into_iter()
		.filter(|fact| (fact.column_ordinal.0 as usize) < column_count)
		.map(|fact| (fact.column_ordinal, fact.range))
		.collect()
}

#[derive(Clone)]
pub struct TableBinding {
	pub legacy_table_id: TableId,
	pub table: TableObjectId,
	pub schema_generation: u64,
	pub fingerprint: PhysicalDescriptorFingerprint,
	pub schema: SchemaRef,
	pub row_identity: TableRowIdentity,
	pub pbc_eligible_columns: HashSet<ColumnOrdinal>,
	pub primary_key_columns: HashSet<ColumnOrdinal>,
	pub optimizer_facts: TableOptimizerFacts,
}

impl TableBinding {
	pub fn from_descriptor(
		legacy_table_id: TableId,
		descriptor: &wendelon_client::TableSchemaDescriptor,
	) -> Result<Self, CatalogError> {
		Self::from_descriptor_with_optimizer_facts(legacy_table_id, descriptor, None)
	}

	pub fn from_descriptor_with_optimizer_facts(
		legacy_table_id: TableId,
		descriptor: &wendelon_client::TableSchemaDescriptor,
		optimizer_facts: Option<DescriptorOptimizerFacts>,
	) -> Result<Self, CatalogError> {
		Self::from_descriptor_with_logical_types_and_optimizer_facts(
			legacy_table_id,
			descriptor,
			LogicalTypeResolver::seed_only(),
			optimizer_facts,
		)
	}

	pub fn from_descriptor_with_logical_types(
		legacy_table_id: TableId,
		descriptor: &wendelon_client::TableSchemaDescriptor,
		logical_types: LogicalTypeResolver<'_>,
	) -> Result<Self, CatalogError> {
		Self::from_descriptor_with_logical_types_and_optimizer_facts(
			legacy_table_id,
			descriptor,
			logical_types,
			None,
		)
	}

	pub fn from_descriptor_with_logical_types_and_optimizer_facts(
		legacy_table_id: TableId,
		descriptor: &wendelon_client::TableSchemaDescriptor,
		logical_types: LogicalTypeResolver<'_>,
		optimizer_facts: Option<DescriptorOptimizerFacts>,
	) -> Result<Self, CatalogError> {
		let schema = Arc::new(descriptor.arrow_schema().map_err(|error| {
			CatalogError::FingerprintResolution {
				message: error.to_string(),
			}
		})?);
		validate_logical_column_metadata(schema.as_ref(), logical_types)?;

		let row_identity = table_row_identity(&schema, descriptor)?;
		let primary_key_columns = primary_key_columns(&schema, descriptor)?;

		let mut binding = Self::new_with_row_identity(
			legacy_table_id,
			descriptor.table_object_id,
			descriptor.schema_generation,
			descriptor.physical_descriptor_fingerprint,
			schema,
			row_identity,
			primary_key_columns,
		);
		binding.optimizer_facts = optimizer_facts
			.map(|facts| facts.for_descriptor(descriptor))
			.unwrap_or_default();
		Ok(binding)
	}

	pub fn new(
		legacy_table_id: TableId,
		table: TableObjectId,
		fingerprint: PhysicalDescriptorFingerprint,
		schema: SchemaRef,
	) -> Self {
		Self::new_with_schema_generation(legacy_table_id, table, 0, fingerprint, schema)
	}

	pub fn new_with_schema_generation(
		legacy_table_id: TableId,
		table: TableObjectId,
		schema_generation: u64,
		fingerprint: PhysicalDescriptorFingerprint,
		schema: SchemaRef,
	) -> Self {
		Self::new_with_primary_key_columns(
			legacy_table_id,
			table,
			schema_generation,
			fingerprint,
			schema,
			HashSet::new(),
		)
	}

	pub fn new_with_primary_key_columns(
		legacy_table_id: TableId,
		table: TableObjectId,
		schema_generation: u64,
		fingerprint: PhysicalDescriptorFingerprint,
		schema: SchemaRef,
		primary_key_columns: HashSet<ColumnOrdinal>,
	) -> Self {
		let row_identity = if primary_key_columns.is_empty() {
			TableRowIdentity::AppendOnly
		} else {
			TableRowIdentity::Unsupported
		};
		Self::new_with_row_identity(
			legacy_table_id,
			table,
			schema_generation,
			fingerprint,
			schema,
			row_identity,
			primary_key_columns,
		)
	}

	pub fn new_with_row_identity(
		legacy_table_id: TableId,
		table: TableObjectId,
		schema_generation: u64,
		fingerprint: PhysicalDescriptorFingerprint,
		schema: SchemaRef,
		row_identity: TableRowIdentity,
		primary_key_columns: HashSet<ColumnOrdinal>,
	) -> Self {
		let pbc_eligible_columns = pbc_eligible_columns(&schema);
		Self {
			legacy_table_id,
			table,
			schema_generation,
			fingerprint,
			schema,
			row_identity,
			pbc_eligible_columns,
			primary_key_columns,
			optimizer_facts: TableOptimizerFacts::default(),
		}
	}

	pub fn column_is_pbc_eligible(&self, ordinal: ColumnOrdinal) -> bool {
		self.pbc_eligible_columns.contains(&ordinal)
	}
}

#[derive(Debug, Clone)]
pub struct InvalidatedDescriptor {
	pub table_id: TableId,
	pub table_object_id: TableObjectId,
	pub fingerprint: PhysicalDescriptorFingerprint,
	pub refs: Vec<TableRef>,
}

#[derive(Clone)]
pub struct CreateTableOutcome {
	pub binding: TableBinding,
	pub created: bool,
}

#[async_trait::async_trait]
pub trait Catalog: Send + Sync {
	fn scoped(self: Arc<Self>, tenant_id: TenantId) -> Arc<dyn Catalog>;

	fn descriptor_refresh_metrics(&self) -> Arc<DescriptorRefreshMetrics>;

	async fn resolve_table(&self, table: &TableRef) -> Result<TableBinding, CatalogError>;

	async fn resolve_table_local(&self, table: &TableRef) -> Result<TableBinding, CatalogError> {
		self.resolve_table(table).await
	}

	async fn resolve_table_id(&self, table: TableId) -> Result<TableBinding, CatalogError>;

	async fn resolve_durable_logical_type_by_name(
		&self,
		name: &str,
	) -> Result<LogicalTypeSpec, CatalogError>;

	fn logical_type_registry(&self) -> LogicalTypeRegistry {
		LogicalTypeRegistry::default()
	}

	async fn refreshed_logical_type_registry(&self) -> Result<LogicalTypeRegistry, CatalogError> {
		Ok(self.logical_type_registry())
	}

	async fn refreshed_table_schema_descriptors(
		&self,
	) -> Result<Vec<wendelon_client::TableSchemaDescriptor>, CatalogError> {
		Ok(Vec::new())
	}

	async fn invalidate_table_descriptor(
		&self,
		table: TableId,
	) -> Result<Option<InvalidatedDescriptor>, CatalogError>;

	async fn create_table_with_outcome(
		&self,
		name: &TableRef,
		schema: SchemaRef,
		row_identity: CreateTableRowIdentity,
		if_not_exists: bool,
	) -> Result<CreateTableOutcome, CatalogError>;

	async fn create_table(
		&self,
		name: &TableRef,
		schema: SchemaRef,
		if_not_exists: bool,
	) -> Result<TableBinding, CatalogError> {
		self.create_table_with_outcome(
			name,
			schema,
			CreateTableRowIdentity::AppendOnly,
			if_not_exists,
		)
		.await
		.map(|outcome| outcome.binding)
	}

	async fn drop_table(
		&self,
		name: &TableRef,
		if_exists: bool,
	) -> Result<Option<TableBinding>, CatalogError>;
}

#[async_trait::async_trait]
trait SchemaUpdateClient: Send + Sync {
	async fn submit_table_schema_update(
		&self,
		request: TableSchemaUpdateRequest,
	) -> Result<SchemaUpdateReceipt, ClientError>;
}

#[async_trait::async_trait]
impl SchemaUpdateClient for Client {
	async fn submit_table_schema_update(
		&self,
		request: TableSchemaUpdateRequest,
	) -> Result<SchemaUpdateReceipt, ClientError> {
		Client::submit_table_schema_update(self, request).await
	}
}

#[async_trait::async_trait]
trait DescriptorSyncClient: Send + Sync {
	async fn list_logical_type_definitions(
		&self,
		tenant_id: WendelonTenantId,
	) -> Result<Vec<LogicalTypeDefinition>, ClientError>;

	async fn get_table_schema_descriptor(
		&self,
		tenant_id: WendelonTenantId,
		namespace: &str,
		table_name: &str,
	) -> Result<Option<wendelon_client::TableSchemaDescriptor>, ClientError>;

	async fn get_table_optimizer_facts(
		&self,
		tenant_id: WendelonTenantId,
		namespace: &str,
		table_name: &str,
		descriptor: &wendelon_client::TableSchemaDescriptor,
	) -> Result<Option<wendelon_client::TableOptimizerFacts>, ClientError>;

	async fn list_table_schema_descriptors(
		&self,
		tenant_id: WendelonTenantId,
	) -> Result<Vec<wendelon_client::TableSchemaDescriptor>, ClientError>;
}

#[async_trait::async_trait]
impl DescriptorSyncClient for Client {
	async fn list_logical_type_definitions(
		&self,
		tenant_id: WendelonTenantId,
	) -> Result<Vec<LogicalTypeDefinition>, ClientError> {
		Client::list_logical_type_definitions(self, tenant_id).await
	}

	async fn get_table_schema_descriptor(
		&self,
		tenant_id: WendelonTenantId,
		namespace: &str,
		table_name: &str,
	) -> Result<Option<wendelon_client::TableSchemaDescriptor>, ClientError> {
		Client::get_table_schema_descriptor(self, tenant_id, namespace, table_name).await
	}

	async fn get_table_optimizer_facts(
		&self,
		tenant_id: WendelonTenantId,
		namespace: &str,
		table_name: &str,
		descriptor: &wendelon_client::TableSchemaDescriptor,
	) -> Result<Option<wendelon_client::TableOptimizerFacts>, ClientError> {
		Client::get_table_optimizer_facts(self, tenant_id, namespace, table_name, descriptor).await
	}

	async fn list_table_schema_descriptors(
		&self,
		tenant_id: WendelonTenantId,
	) -> Result<Vec<wendelon_client::TableSchemaDescriptor>, ClientError> {
		Client::list_table_schema_descriptors(self, tenant_id).await
	}
}

#[derive(Default)]
pub struct DescriptorSyncMetrics {
	success: AtomicU64,
	not_found: AtomicU64,
	throttled: AtomicU64,
	error: AtomicU64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DescriptorSyncMetricSnapshot {
	pub success: u64,
	pub not_found: u64,
	pub throttled: u64,
	pub error: u64,
}

impl DescriptorSyncMetrics {
	pub fn snapshot(&self) -> DescriptorSyncMetricSnapshot {
		DescriptorSyncMetricSnapshot {
			success: self.success.load(Ordering::Relaxed),
			not_found: self.not_found.load(Ordering::Relaxed),
			throttled: self.throttled.load(Ordering::Relaxed),
			error: self.error.load(Ordering::Relaxed),
		}
	}

	fn record_success(&self) {
		self.success.fetch_add(1, Ordering::Relaxed);
	}

	fn record_not_found(&self) {
		self.not_found.fetch_add(1, Ordering::Relaxed);
	}

	fn record_throttled(&self) {
		self.throttled.fetch_add(1, Ordering::Relaxed);
	}

	fn record_error(&self) {
		self.error.fetch_add(1, Ordering::Relaxed);
	}
}

#[derive(Default)]
pub struct DescriptorRefreshMetrics {
	invalidated: AtomicU64,
	missing: AtomicU64,
	replacement: AtomicU64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DescriptorRefreshMetricSnapshot {
	pub invalidated: u64,
	pub missing: u64,
	pub replacement: u64,
}

impl DescriptorRefreshMetrics {
	pub fn snapshot(&self) -> DescriptorRefreshMetricSnapshot {
		DescriptorRefreshMetricSnapshot {
			invalidated: self.invalidated.load(Ordering::Relaxed),
			missing: self.missing.load(Ordering::Relaxed),
			replacement: self.replacement.load(Ordering::Relaxed),
		}
	}

	pub fn record_invalidated(&self) {
		self.invalidated.fetch_add(1, Ordering::Relaxed);
	}

	pub fn record_missing(&self) {
		self.missing.fetch_add(1, Ordering::Relaxed);
	}

	pub fn record_replacement(&self) {
		self.replacement.fetch_add(1, Ordering::Relaxed);
	}
}

pub struct MockCatalog {
	tenant_id: TenantId,
	inner: Arc<RwLock<MockCatalogInner>>,
	descriptor_refresh_metrics: Arc<DescriptorRefreshMetrics>,
}

struct MockCatalogInner {
	by_ref: HashMap<(TenantId, TableRef), TableId>,
	by_id: HashMap<(TenantId, TableId), TableBinding>,
	descriptors: HashMap<(TenantId, TableId), wendelon_client::TableSchemaDescriptor>,
	logical_types: HashMap<TenantId, LogicalTypeRegistry>,
	next_table_id: u64,
}

impl Default for MockCatalog {
	fn default() -> Self {
		Self {
			tenant_id: TenantId(1),
			inner: Arc::new(RwLock::new(MockCatalogInner::default())),
			descriptor_refresh_metrics: Arc::new(DescriptorRefreshMetrics::default()),
		}
	}
}

impl Default for MockCatalogInner {
	fn default() -> Self {
		Self {
			by_ref: HashMap::new(),
			by_id: HashMap::new(),
			descriptors: HashMap::new(),
			logical_types: HashMap::new(),
			next_table_id: 1,
		}
	}
}

impl MockCatalog {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn register_descriptor(
		&mut self,
		legacy_table_id: TableId,
		descriptor: wendelon_client::TableSchemaDescriptor,
	) -> Result<(), CatalogError> {
		self.register_descriptor_for_tenant(self.tenant_id, legacy_table_id, descriptor)
	}

	pub fn register_descriptor_for_tenant(
		&mut self,
		tenant_id: TenantId,
		legacy_table_id: TableId,
		descriptor: wendelon_client::TableSchemaDescriptor,
	) -> Result<(), CatalogError> {
		let table_ref = TableRef::with_schema(&descriptor.namespace, &descriptor.table_name);
		let unqualified_ref = TableRef::new(&descriptor.table_name);
		let mut inner = self.inner.write().expect("MockCatalog lock poisoned");
		let resolver = inner
			.logical_types
			.get(&tenant_id)
			.map(LogicalTypeResolver::with_registry)
			.unwrap_or_else(LogicalTypeResolver::seed_only);
		let binding = TableBinding::from_descriptor_with_logical_types(
			legacy_table_id,
			&descriptor,
			resolver,
		)?;

		inner.by_ref.insert((tenant_id, table_ref), legacy_table_id);
		inner
			.by_ref
			.insert((tenant_id, unqualified_ref), legacy_table_id);
		inner.by_id.insert((tenant_id, legacy_table_id), binding);
		inner
			.descriptors
			.insert((tenant_id, legacy_table_id), descriptor);
		bump_next_table_id(&mut inner, legacy_table_id);
		Ok(())
	}

	pub fn register_table(
		&mut self,
		table_ref: TableRef,
		binding: TableBinding,
	) -> Result<(), CatalogError> {
		self.register_table_for_tenant(self.tenant_id, table_ref, binding)
	}

	pub fn register_table_for_tenant(
		&mut self,
		tenant_id: TenantId,
		table_ref: TableRef,
		binding: TableBinding,
	) -> Result<(), CatalogError> {
		let legacy_table_id = binding.legacy_table_id;
		let mut inner = self.inner.write().expect("MockCatalog lock poisoned");

		inner.by_ref.insert((tenant_id, table_ref), legacy_table_id);
		inner.by_id.insert((tenant_id, legacy_table_id), binding);
		bump_next_table_id(&mut inner, legacy_table_id);
		Ok(())
	}

	pub fn seed_optimizer_facts(
		&mut self,
		table_ref: &TableRef,
		facts: TableOptimizerFacts,
	) -> Result<(), CatalogError> {
		let mut inner = self.inner.write().expect("MockCatalog lock poisoned");
		let table_id = inner
			.by_ref
			.get(&(self.tenant_id, table_ref.clone()))
			.copied()
			.ok_or_else(|| CatalogError::TableNotFound {
				table: table_name(table_ref),
			})?;
		let binding = inner
			.by_id
			.get_mut(&(self.tenant_id, table_id))
			.ok_or_else(|| CatalogError::TableNotFound {
				table: table_name(table_ref),
			})?;
		binding.optimizer_facts = facts;
		Ok(())
	}

	pub fn seed_optimizer_facts_for_table_id(
		&mut self,
		table_id: TableId,
		facts: TableOptimizerFacts,
	) -> Result<(), CatalogError> {
		let mut inner = self.inner.write().expect("MockCatalog lock poisoned");
		let binding = inner
			.by_id
			.get_mut(&(self.tenant_id, table_id))
			.ok_or_else(|| CatalogError::TableNotFound {
				table: format!("{table_id:?}"),
			})?;
		binding.optimizer_facts = facts;
		Ok(())
	}

	pub fn register_logical_type_for_tenant(
		&self,
		tenant_id: TenantId,
		definition: wendelon_client::LogicalTypeDefinition,
	) -> Result<(), CatalogError> {
		let mut inner = self.inner.write().expect("MockCatalog lock poisoned");
		inner
			.logical_types
			.entry(tenant_id)
			.or_default()
			.register_definition(definition)
			.map_err(|error| CatalogError::AdminFailed {
				message: error.to_string(),
			})?;
		Ok(())
	}
}

#[async_trait::async_trait]
impl Catalog for MockCatalog {
	fn scoped(self: Arc<Self>, tenant_id: TenantId) -> Arc<dyn Catalog> {
		Arc::new(Self {
			tenant_id,
			inner: Arc::clone(&self.inner),
			descriptor_refresh_metrics: Arc::clone(&self.descriptor_refresh_metrics),
		})
	}

	fn descriptor_refresh_metrics(&self) -> Arc<DescriptorRefreshMetrics> {
		Arc::clone(&self.descriptor_refresh_metrics)
	}

	async fn resolve_table(&self, table: &TableRef) -> Result<TableBinding, CatalogError> {
		let inner = self.inner.read().expect("MockCatalog lock poisoned");
		let table_id = inner
			.by_ref
			.get(&(self.tenant_id, table.clone()))
			.copied()
			.ok_or_else(|| CatalogError::TableNotFound {
				table: table_name(table),
			})?;
		inner
			.by_id
			.get(&(self.tenant_id, table_id))
			.cloned()
			.ok_or_else(|| CatalogError::TableNotFound {
				table: table_name(table),
			})
	}

	async fn resolve_table_id(&self, table: TableId) -> Result<TableBinding, CatalogError> {
		let inner = self.inner.read().expect("MockCatalog lock poisoned");
		inner
			.by_id
			.get(&(self.tenant_id, table))
			.cloned()
			.ok_or_else(|| CatalogError::TableNotFound {
				table: format!("{table:?}"),
			})
	}

	async fn resolve_durable_logical_type_by_name(
		&self,
		name: &str,
	) -> Result<LogicalTypeSpec, CatalogError> {
		let inner = self.inner.read().expect("MockCatalog lock poisoned");
		let Some(registry) = inner.logical_types.get(&self.tenant_id) else {
			return Err(CatalogError::LogicalTypeNotFound {
				name: name.to_string(),
			});
		};
		LogicalTypeResolver::durable_only(registry)
			.spec_by_name(name)
			.ok_or_else(|| CatalogError::LogicalTypeNotFound {
				name: name.to_string(),
			})
	}

	fn logical_type_registry(&self) -> LogicalTypeRegistry {
		let inner = self.inner.read().expect("MockCatalog lock poisoned");
		inner
			.logical_types
			.get(&self.tenant_id)
			.cloned()
			.unwrap_or_default()
	}

	async fn refreshed_table_schema_descriptors(
		&self,
	) -> Result<Vec<wendelon_client::TableSchemaDescriptor>, CatalogError> {
		let inner = self.inner.read().expect("MockCatalog lock poisoned");
		Ok(sorted_descriptors_for_tenant(
			inner
				.descriptors
				.iter()
				.filter_map(|((tenant_id, _), descriptor)| {
					(*tenant_id == self.tenant_id).then(|| descriptor.clone())
				}),
		))
	}

	async fn invalidate_table_descriptor(
		&self,
		table: TableId,
	) -> Result<Option<InvalidatedDescriptor>, CatalogError> {
		let mut inner = self.inner.write().expect("MockCatalog lock poisoned");
		let Some(binding) = inner.by_id.remove(&(self.tenant_id, table)) else {
			self.descriptor_refresh_metrics.record_missing();
			return Ok(None);
		};
		inner.descriptors.remove(&(self.tenant_id, table));
		let mut refs = Vec::new();
		inner.by_ref.retain(|(tenant_id, table_ref), table_id| {
			let remove = *tenant_id == self.tenant_id && *table_id == table;
			if remove {
				refs.push(table_ref.clone());
			}
			!remove
		});
		self.descriptor_refresh_metrics.record_invalidated();
		Ok(Some(InvalidatedDescriptor {
			table_id: table,
			table_object_id: binding.table,
			fingerprint: binding.fingerprint,
			refs,
		}))
	}

	async fn create_table_with_outcome(
		&self,
		name: &TableRef,
		schema: SchemaRef,
		row_identity: CreateTableRowIdentity,
		if_not_exists: bool,
	) -> Result<CreateTableOutcome, CatalogError> {
		let mut inner = self.inner.write().expect("MockCatalog lock poisoned");

		if let Some(existing_id) = inner.by_ref.get(&(self.tenant_id, name.clone())).copied() {
			let existing = inner
				.by_id
				.get(&(self.tenant_id, existing_id))
				.expect("MockCatalog by_ref/by_id consistency invariant")
				.clone();
			if if_not_exists {
				if existing.schema.as_ref() == schema.as_ref()
					&& existing.row_identity.matches_create_identity(&row_identity)
				{
					return Ok(CreateTableOutcome {
						binding: existing,
						created: false,
					});
				}
				return Err(CatalogError::SchemaMismatch {
					table: table_name(name),
				});
			}
			return Err(CatalogError::TableAlreadyExists {
				table: table_name(name),
			});
		}

		while inner
			.by_id
			.keys()
			.any(|(_, table_id)| *table_id == TableId(inner.next_table_id))
		{
			inner.next_table_id = inner
				.next_table_id
				.checked_add(1)
				.expect("TableId overflow");
		}
		let legacy_table_id = TableId(inner.next_table_id);
		inner.next_table_id = inner
			.next_table_id
			.checked_add(1)
			.expect("TableId overflow");

		let (table_object_id, fingerprint) =
			mock_table_identity(name, schema.as_ref(), &row_identity, legacy_table_id)?;
		let descriptor =
			create_table_descriptor(name, schema.as_ref(), &row_identity, table_object_id)?;
		let logical_types = inner
			.logical_types
			.get(&self.tenant_id)
			.cloned()
			.unwrap_or_default();
		let mut binding = TableBinding::from_descriptor_with_logical_types(
			legacy_table_id,
			&descriptor,
			LogicalTypeResolver::with_registry(&logical_types),
		)?;
		binding.fingerprint = fingerprint;

		inner
			.by_ref
			.insert((self.tenant_id, name.clone()), legacy_table_id);
		if name.schema.is_some() {
			inner
				.by_ref
				.entry((self.tenant_id, TableRef::new(name.table.clone())))
				.or_insert(legacy_table_id);
		}
		inner
			.by_id
			.insert((self.tenant_id, legacy_table_id), binding.clone());
		inner
			.descriptors
			.insert((self.tenant_id, legacy_table_id), descriptor);

		Ok(CreateTableOutcome {
			binding,
			created: true,
		})
	}

	async fn drop_table(
		&self,
		name: &TableRef,
		if_exists: bool,
	) -> Result<Option<TableBinding>, CatalogError> {
		let mut inner = self.inner.write().expect("MockCatalog lock poisoned");

		let Some(table_id) = inner.by_ref.get(&(self.tenant_id, name.clone())).copied() else {
			if if_exists {
				return Ok(None);
			}
			return Err(CatalogError::TableNotFound {
				table: table_name(name),
			});
		};

		let binding = inner
			.by_id
			.remove(&(self.tenant_id, table_id))
			.ok_or_else(|| CatalogError::TableNotFound {
				table: table_name(name),
			})?;
		inner
			.by_ref
			.retain(|(tenant_id, _), id| *tenant_id != self.tenant_id || *id != table_id);
		inner.descriptors.remove(&(self.tenant_id, table_id));
		Ok(Some(binding))
	}
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct DescriptorSyncKey {
	tenant_id: TenantId,
	namespace: String,
	table_name: String,
}

impl DescriptorSyncKey {
	fn from_table(tenant_id: TenantId, table: &TableRef) -> Self {
		Self {
			tenant_id,
			namespace: table
				.schema
				.clone()
				.unwrap_or_else(|| "default".to_string()),
			table_name: table.table.clone(),
		}
	}

	fn table_ref(&self) -> TableRef {
		TableRef::with_schema(self.namespace.clone(), self.table_name.clone())
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum DescriptorSyncOutcome {
	Synced,
	NotFound,
	Throttled,
	Failed(String),
}

struct DescriptorSyncFlight {
	outcome: Mutex<Option<DescriptorSyncOutcome>>,
	notify: Notify,
}

impl DescriptorSyncFlight {
	fn new() -> Self {
		Self {
			outcome: Mutex::new(None),
			notify: Notify::new(),
		}
	}

	async fn wait(&self) -> DescriptorSyncOutcome {
		loop {
			let notified = self.notify.notified();
			tokio::pin!(notified);
			if let Some(outcome) = self
				.outcome
				.lock()
				.expect("descriptor sync flight poisoned")
				.clone()
			{
				return outcome;
			}
			notified.as_mut().await;
		}
	}
}

struct DescriptorSyncFlights {
	accountant: TenantDescriptorSyncAccountant,
	inflight: Mutex<HashMap<DescriptorSyncKey, Arc<DescriptorSyncFlight>>>,
}

impl DescriptorSyncFlights {
	fn new(accountant: TenantDescriptorSyncAccountant) -> Self {
		Self {
			accountant,
			inflight: Mutex::new(HashMap::new()),
		}
	}

	fn join_or_own(self: &Arc<Self>, key: DescriptorSyncKey) -> DescriptorSyncParticipant {
		let mut inflight = self
			.inflight
			.lock()
			.expect("descriptor sync flights poisoned");
		if let Some(flight) = inflight.get(&key) {
			return DescriptorSyncParticipant::Waiter(Arc::clone(flight));
		}

		let flight = Arc::new(DescriptorSyncFlight::new());
		inflight.insert(key.clone(), Arc::clone(&flight));
		DescriptorSyncParticipant::Owner(DescriptorSyncOwner {
			flights: Arc::clone(self),
			key,
			flight,
			completed: false,
		})
	}

	fn finish(
		&self,
		key: &DescriptorSyncKey,
		flight: &Arc<DescriptorSyncFlight>,
		outcome: DescriptorSyncOutcome,
	) {
		*flight
			.outcome
			.lock()
			.expect("descriptor sync flight poisoned") = Some(outcome);
		let mut inflight = self
			.inflight
			.lock()
			.expect("descriptor sync flights poisoned");
		if inflight
			.get(key)
			.is_some_and(|current| Arc::ptr_eq(current, flight))
		{
			inflight.remove(key);
		}
		flight.notify.notify_waiters();
	}
}

enum DescriptorSyncParticipant {
	Owner(DescriptorSyncOwner),
	Waiter(Arc<DescriptorSyncFlight>),
}

struct DescriptorSyncOwner {
	flights: Arc<DescriptorSyncFlights>,
	key: DescriptorSyncKey,
	flight: Arc<DescriptorSyncFlight>,
	completed: bool,
}

impl DescriptorSyncOwner {
	fn finish(mut self, outcome: DescriptorSyncOutcome) {
		self.flights.finish(&self.key, &self.flight, outcome);
		self.completed = true;
	}
}

impl Drop for DescriptorSyncOwner {
	fn drop(&mut self) {
		if !self.completed {
			self.flights.finish(
				&self.key,
				&self.flight,
				DescriptorSyncOutcome::Failed(
					"descriptor sync failed: owner cancelled".to_string(),
				),
			);
		}
	}
}

pub struct WendelonCatalog {
	client: Option<Arc<dyn SchemaUpdateClient>>,
	descriptor_sync_client: Option<Arc<dyn DescriptorSyncClient>>,
	descriptor_sync_flights: Option<Arc<DescriptorSyncFlights>>,
	descriptor_sync_metrics: Arc<DescriptorSyncMetrics>,
	descriptor_refresh_metrics: Arc<DescriptorRefreshMetrics>,
	tenant_id: TenantId,
	registry: Arc<WendelonDescriptorRegistry>,
}

impl Default for WendelonCatalog {
	fn default() -> Self {
		Self {
			client: None,
			descriptor_sync_client: None,
			descriptor_sync_flights: None,
			descriptor_sync_metrics: Arc::new(DescriptorSyncMetrics::default()),
			descriptor_refresh_metrics: Arc::new(DescriptorRefreshMetrics::default()),
			tenant_id: TenantId(1),
			registry: Arc::new(WendelonDescriptorRegistry::default()),
		}
	}
}

#[derive(Default)]
pub struct WendelonDescriptorRegistry {
	inner: RwLock<WendelonDescriptorState>,
}

struct WendelonDescriptorState {
	by_ref: HashMap<(TenantId, TableRef), TableId>,
	by_id: HashMap<(TenantId, TableId), TableBinding>,
	descriptors: HashMap<(TenantId, TableId), wendelon_client::TableSchemaDescriptor>,
	logical_types: HashMap<TenantId, LogicalTypeRegistry>,
	finalized_drops: HashMap<(TenantId, TableId), DropOperationId>,
	next_table_id: u64,
}

impl Default for WendelonDescriptorState {
	fn default() -> Self {
		Self {
			by_ref: HashMap::new(),
			by_id: HashMap::new(),
			descriptors: HashMap::new(),
			logical_types: HashMap::new(),
			finalized_drops: HashMap::new(),
			next_table_id: 1,
		}
	}
}

impl WendelonDescriptorRegistry {
	pub fn from_descriptors(
		descriptors: impl IntoIterator<Item = (TableId, wendelon_client::TableSchemaDescriptor)>,
	) -> Result<Self, CatalogError> {
		Self::from_descriptors_for_tenant(TenantId(1), descriptors)
	}

	pub fn from_descriptors_for_tenant(
		tenant_id: TenantId,
		descriptors: impl IntoIterator<Item = (TableId, wendelon_client::TableSchemaDescriptor)>,
	) -> Result<Self, CatalogError> {
		Self::from_descriptors_for_tenant_with_facts(
			tenant_id,
			descriptors
				.into_iter()
				.map(|(table_id, descriptor)| (table_id, descriptor, None)),
		)
	}

	pub fn from_descriptors_for_tenant_with_facts(
		tenant_id: TenantId,
		descriptors: impl IntoIterator<
			Item = (
				TableId,
				wendelon_client::TableSchemaDescriptor,
				Option<DescriptorOptimizerFacts>,
			),
		>,
	) -> Result<Self, CatalogError> {
		let registry = Self::default();
		for (table_id, descriptor, optimizer_facts) in descriptors {
			registry.register_descriptor_for_tenant_with_facts(
				tenant_id,
				table_id,
				descriptor,
				optimizer_facts,
			)?;
		}
		Ok(registry)
	}

	pub fn from_tenant_descriptors(
		descriptors: impl IntoIterator<
			Item = (TenantId, TableId, wendelon_client::TableSchemaDescriptor),
		>,
	) -> Result<Self, CatalogError> {
		Self::from_tenant_descriptors_with_facts(
			descriptors
				.into_iter()
				.map(|(tenant_id, table_id, descriptor)| (tenant_id, table_id, descriptor, None)),
		)
	}

	pub fn from_tenant_descriptors_with_facts(
		descriptors: impl IntoIterator<
			Item = (
				TenantId,
				TableId,
				wendelon_client::TableSchemaDescriptor,
				Option<DescriptorOptimizerFacts>,
			),
		>,
	) -> Result<Self, CatalogError> {
		let registry = Self::default();
		for (tenant_id, table_id, descriptor, optimizer_facts) in descriptors {
			if tenant_id.0 == 0 {
				return Err(CatalogError::AdminFailed {
					message: "descriptor bootstrap tenant_id 0 is reserved".to_string(),
				});
			}
			if table_id.0 == 0 {
				return Err(CatalogError::AdminFailed {
					message: "descriptor bootstrap table_id 0 is reserved".to_string(),
				});
			}
			let table_ref = TableRef::with_schema(&descriptor.namespace, &descriptor.table_name);
			let unqualified_ref = TableRef::new(&descriptor.table_name);
			let mut state = registry
				.inner
				.write()
				.expect("WendelonDescriptorRegistry lock poisoned");
			if state.by_id.contains_key(&(tenant_id, table_id)) {
				return Err(CatalogError::AdminFailed {
					message: format!(
						"duplicate descriptor bootstrap table_id {} for tenant {}",
						table_id.0, tenant_id.0
					),
				});
			}
			if state.by_ref.contains_key(&(tenant_id, table_ref.clone())) {
				return Err(CatalogError::AdminFailed {
					message: format!(
						"duplicate descriptor bootstrap table {} for tenant {}",
						table_name(&table_ref),
						tenant_id.0
					),
				});
			}
			let resolver = state
				.logical_types
				.get(&tenant_id)
				.map(LogicalTypeResolver::with_registry)
				.unwrap_or_else(LogicalTypeResolver::seed_only);
			let binding = TableBinding::from_descriptor_with_logical_types_and_optimizer_facts(
				table_id,
				&descriptor,
				resolver,
				optimizer_facts,
			)?;
			insert_wendelon_descriptor_locked(
				&mut state,
				tenant_id,
				table_ref,
				Some(unqualified_ref),
				table_id,
				binding,
				descriptor,
			)?;
		}
		Ok(registry)
	}

	pub fn register_descriptor_for_tenant(
		&self,
		tenant_id: TenantId,
		table_id: TableId,
		descriptor: wendelon_client::TableSchemaDescriptor,
	) -> Result<(), CatalogError> {
		self.register_descriptor_for_tenant_with_facts(tenant_id, table_id, descriptor, None)
	}

	pub fn register_descriptor_for_tenant_with_facts(
		&self,
		tenant_id: TenantId,
		table_id: TableId,
		descriptor: wendelon_client::TableSchemaDescriptor,
		optimizer_facts: Option<DescriptorOptimizerFacts>,
	) -> Result<(), CatalogError> {
		let table_ref = TableRef::with_schema(&descriptor.namespace, &descriptor.table_name);
		let unqualified_ref = TableRef::new(&descriptor.table_name);
		let mut state = self
			.inner
			.write()
			.expect("WendelonDescriptorRegistry lock poisoned");
		let resolver = state
			.logical_types
			.get(&tenant_id)
			.map(LogicalTypeResolver::with_registry)
			.unwrap_or_else(LogicalTypeResolver::seed_only);
		let binding = TableBinding::from_descriptor_with_logical_types_and_optimizer_facts(
			table_id,
			&descriptor,
			resolver,
			optimizer_facts,
		)?;
		insert_wendelon_descriptor_locked(
			&mut state,
			tenant_id,
			table_ref,
			Some(unqualified_ref),
			table_id,
			binding,
			descriptor,
		)
	}

	pub fn sync_descriptor_for_tenant(
		&self,
		tenant_id: TenantId,
		requested: &TableRef,
		descriptor: wendelon_client::TableSchemaDescriptor,
	) -> Result<TableBinding, CatalogError> {
		self.sync_descriptor_for_tenant_with_facts(tenant_id, requested, descriptor, None)
	}

	pub fn sync_descriptor_for_tenant_with_facts(
		&self,
		tenant_id: TenantId,
		requested: &TableRef,
		descriptor: wendelon_client::TableSchemaDescriptor,
		optimizer_facts: Option<DescriptorOptimizerFacts>,
	) -> Result<TableBinding, CatalogError> {
		let table_ref = canonical_table_ref(requested);
		let namespace = table_ref
			.schema
			.as_deref()
			.expect("canonical table ref has schema");
		if descriptor.namespace != namespace || descriptor.table_name != table_ref.table {
			return Err(CatalogError::AdminFailed {
				message: format!(
					"descriptor sync failed: requested {}, got {}.{}",
					table_name(&table_ref),
					descriptor.namespace,
					descriptor.table_name
				),
			});
		}

		let unqualified_ref = TableRef::new(&descriptor.table_name);
		let mut state = self
			.inner
			.write()
			.expect("WendelonDescriptorRegistry lock poisoned");
		let logical_types = state
			.logical_types
			.get(&tenant_id)
			.cloned()
			.unwrap_or_default();
		let resolver = LogicalTypeResolver::with_registry(&logical_types);

		if let Some(existing_table_id) = state.by_ref.get(&(tenant_id, table_ref.clone())).copied()
		{
			let existing = state
				.by_id
				.get(&(tenant_id, existing_table_id))
				.cloned()
				.ok_or_else(|| CatalogError::TableNotFound {
					table: table_name(&table_ref),
				})?;
			let candidate = TableBinding::from_descriptor_with_logical_types_and_optimizer_facts(
				existing_table_id,
				&descriptor,
				resolver,
				optimizer_facts,
			)?;
			if same_descriptor_identity(&existing, &candidate) {
				if existing.optimizer_facts != candidate.optimizer_facts {
					state
						.by_id
						.insert((tenant_id, existing_table_id), candidate.clone());
					state
						.descriptors
						.insert((tenant_id, existing_table_id), descriptor);
					state
						.by_ref
						.entry((tenant_id, unqualified_ref))
						.or_insert(existing_table_id);
					return Ok(candidate);
				}
				return Ok(existing);
			}
			return Err(CatalogError::AdminFailed {
				message: format!(
					"descriptor sync failed: table {} already exists with a different descriptor identity",
					table_name(&table_ref)
				),
			});
		}

		let table_id = allocate_wendelon_table_id_locked(&mut state);
		let binding = TableBinding::from_descriptor_with_logical_types_and_optimizer_facts(
			table_id,
			&descriptor,
			resolver,
			optimizer_facts,
		)?;
		insert_wendelon_descriptor_locked(
			&mut state,
			tenant_id,
			table_ref,
			Some(unqualified_ref),
			table_id,
			binding.clone(),
			descriptor,
		)?;
		Ok(binding)
	}

	pub fn sync_latest_descriptor_for_tenant(
		&self,
		tenant_id: TenantId,
		descriptor: wendelon_client::TableSchemaDescriptor,
	) -> Result<TableBinding, CatalogError> {
		self.sync_latest_descriptor_for_tenant_with_facts(tenant_id, descriptor, None)
	}

	pub fn sync_latest_descriptor_for_tenant_with_facts(
		&self,
		tenant_id: TenantId,
		descriptor: wendelon_client::TableSchemaDescriptor,
		optimizer_facts: Option<DescriptorOptimizerFacts>,
	) -> Result<TableBinding, CatalogError> {
		let table_ref = TableRef::with_schema(&descriptor.namespace, &descriptor.table_name);
		let unqualified_ref = TableRef::new(&descriptor.table_name);
		let mut state = self
			.inner
			.write()
			.expect("WendelonDescriptorRegistry lock poisoned");
		let logical_types = state
			.logical_types
			.get(&tenant_id)
			.cloned()
			.unwrap_or_default();
		let resolver = LogicalTypeResolver::with_registry(&logical_types);

		if let Some(existing_table_id) = state.by_ref.get(&(tenant_id, table_ref.clone())).copied()
		{
			let existing = state
				.by_id
				.get(&(tenant_id, existing_table_id))
				.cloned()
				.ok_or_else(|| CatalogError::TableNotFound {
					table: table_name(&table_ref),
				})?;
			let binding = TableBinding::from_descriptor_with_logical_types_and_optimizer_facts(
				existing_table_id,
				&descriptor,
				resolver,
				optimizer_facts,
			)?;
			if existing.table != binding.table {
				return Err(CatalogError::AdminFailed {
					message: format!(
						"descriptor list sync failed: table {} already exists with a different table_object_id",
						table_name(&table_ref)
					),
				});
			}
			state
				.by_id
				.insert((tenant_id, existing_table_id), binding.clone());
			state
				.descriptors
				.insert((tenant_id, existing_table_id), descriptor);
			state
				.by_ref
				.entry((tenant_id, unqualified_ref))
				.or_insert(existing_table_id);
			return Ok(binding);
		}

		let table_id = allocate_wendelon_table_id_locked(&mut state);
		let binding = TableBinding::from_descriptor_with_logical_types_and_optimizer_facts(
			table_id,
			&descriptor,
			resolver,
			optimizer_facts,
		)?;
		insert_wendelon_descriptor_locked(
			&mut state,
			tenant_id,
			table_ref,
			Some(unqualified_ref),
			table_id,
			binding.clone(),
			descriptor,
		)?;
		Ok(binding)
	}

	pub fn register_logical_type_for_tenant(
		&self,
		tenant_id: TenantId,
		definition: LogicalTypeDefinition,
	) -> Result<(), CatalogError> {
		self.register_logical_types_for_tenant(tenant_id, [definition])
	}

	pub fn register_logical_types_for_tenant(
		&self,
		tenant_id: TenantId,
		definitions: impl IntoIterator<Item = LogicalTypeDefinition>,
	) -> Result<(), CatalogError> {
		let mut state = self
			.inner
			.write()
			.expect("WendelonDescriptorRegistry lock poisoned");
		let mut registry = state
			.logical_types
			.get(&tenant_id)
			.cloned()
			.unwrap_or_default();
		for definition in definitions {
			registry.register_definition(definition).map_err(|error| {
				CatalogError::AdminFailed {
					message: error.to_string(),
				}
			})?;
		}
		state.logical_types.insert(tenant_id, registry);
		Ok(())
	}

	pub fn logical_type_registry(&self, tenant_id: TenantId) -> LogicalTypeRegistry {
		let state = self
			.inner
			.read()
			.expect("WendelonDescriptorRegistry lock poisoned");
		state
			.logical_types
			.get(&tenant_id)
			.cloned()
			.unwrap_or_default()
	}

	pub fn resolve_logical_type_by_name(
		&self,
		tenant_id: TenantId,
		name: &str,
	) -> Option<crate::ir::LogicalTypeSpec> {
		let state = self
			.inner
			.read()
			.expect("WendelonDescriptorRegistry lock poisoned");
		let registry = state.logical_types.get(&tenant_id)?;
		LogicalTypeResolver::with_registry(registry).spec_by_name(name)
	}

	pub fn resolve_durable_logical_type_by_name(
		&self,
		tenant_id: TenantId,
		name: &str,
	) -> Option<LogicalTypeSpec> {
		let state = self
			.inner
			.read()
			.expect("WendelonDescriptorRegistry lock poisoned");
		let registry = state.logical_types.get(&tenant_id)?;
		LogicalTypeResolver::durable_only(registry).spec_by_name(name)
	}

	pub fn resolve_table(
		&self,
		tenant_id: TenantId,
		table: &TableRef,
	) -> Result<TableBinding, CatalogError> {
		let state = self
			.inner
			.read()
			.expect("WendelonDescriptorRegistry lock poisoned");
		let table_id = state
			.by_ref
			.get(&(tenant_id, table.clone()))
			.ok_or_else(|| CatalogError::TableNotFound {
				table: table_name(table),
			})?;
		state
			.by_id
			.get(&(tenant_id, *table_id))
			.cloned()
			.ok_or_else(|| CatalogError::TableNotFound {
				table: table_name(table),
			})
	}

	pub fn resolve_table_id(
		&self,
		tenant_id: TenantId,
		table: TableId,
	) -> Result<TableBinding, CatalogError> {
		let state = self
			.inner
			.read()
			.expect("WendelonDescriptorRegistry lock poisoned");
		state
			.by_id
			.get(&(tenant_id, table))
			.cloned()
			.ok_or_else(|| CatalogError::TableNotFound {
				table: format!("{table:?}"),
			})
	}

	pub fn descriptor(
		&self,
		tenant_id: TenantId,
		table: TableId,
	) -> Result<wendelon_client::TableSchemaDescriptor, StorageError> {
		let state = self
			.inner
			.read()
			.expect("WendelonDescriptorRegistry lock poisoned");
		state
			.descriptors
			.get(&(tenant_id, table))
			.cloned()
			.ok_or(StorageError::TableNotFound { table })
	}

	pub fn descriptor_snapshot_for_tenant(
		&self,
		tenant_id: TenantId,
	) -> Vec<wendelon_client::TableSchemaDescriptor> {
		let state = self
			.inner
			.read()
			.expect("WendelonDescriptorRegistry lock poisoned");
		sorted_descriptors_for_tenant(state.descriptors.iter().filter_map(
			|((entry_tenant, _), descriptor)| {
				(*entry_tenant == tenant_id).then(|| descriptor.clone())
			},
		))
	}

	fn try_resolve_table(&self, tenant_id: TenantId, table: &TableRef) -> Option<TableBinding> {
		let state = self
			.inner
			.read()
			.expect("WendelonDescriptorRegistry lock poisoned");
		let table_id = state.by_ref.get(&(tenant_id, table.clone()))?;
		state.by_id.get(&(tenant_id, *table_id)).cloned()
	}

	fn allocate_table_id(&self) -> TableId {
		let mut state = self
			.inner
			.write()
			.expect("WendelonDescriptorRegistry lock poisoned");
		while state
			.by_id
			.keys()
			.any(|(_, table_id)| *table_id == TableId(state.next_table_id))
		{
			state.next_table_id = state
				.next_table_id
				.checked_add(1)
				.expect("TableId overflow");
		}
		let table_id = TableId(state.next_table_id);
		state.next_table_id = state
			.next_table_id
			.checked_add(1)
			.expect("TableId overflow");
		table_id
	}

	fn insert_descriptor(
		&self,
		tenant_id: TenantId,
		name: TableRef,
		binding: TableBinding,
		descriptor: wendelon_client::TableSchemaDescriptor,
	) -> Result<(), CatalogError> {
		let mut state = self
			.inner
			.write()
			.expect("WendelonDescriptorRegistry lock poisoned");
		if state.by_ref.contains_key(&(tenant_id, name.clone())) {
			return Err(CatalogError::TableAlreadyExists {
				table: table_name(&name),
			});
		}
		let unqualified_ref = name
			.schema
			.is_some()
			.then(|| TableRef::new(name.table.clone()));
		insert_wendelon_descriptor_locked(
			&mut state,
			tenant_id,
			name,
			unqualified_ref,
			binding.legacy_table_id,
			binding,
			descriptor,
		)
	}

	pub fn mark_finalized_drop(
		&self,
		tenant_id: TenantId,
		table: TableId,
		operation_id: DropOperationId,
	) -> Result<(), CatalogError> {
		let mut state = self
			.inner
			.write()
			.expect("WendelonDescriptorRegistry lock poisoned");
		if !state.by_id.contains_key(&(tenant_id, table)) {
			return Err(CatalogError::TableNotFound {
				table: format!("{table:?}"),
			});
		}
		state
			.finalized_drops
			.insert((tenant_id, table), operation_id);
		Ok(())
	}

	pub fn consume_finalized_drop(
		&self,
		tenant_id: TenantId,
		table: TableId,
		operation_id: DropOperationId,
	) -> Result<(), CatalogError> {
		let mut state = self
			.inner
			.write()
			.expect("WendelonDescriptorRegistry lock poisoned");
		match state.finalized_drops.get(&(tenant_id, table)).copied() {
			Some(existing) if existing == operation_id => {
				state.finalized_drops.remove(&(tenant_id, table));
				Ok(())
			}
			Some(existing) => Err(CatalogError::AdminFailed {
				message: format!(
					"finalized DROP marker for {table:?} has operation_id {existing:?}, expected {operation_id:?}",
				),
			}),
			None => Err(CatalogError::AdminFailed {
				message: format!(
					"missing finalized DROP marker for {table:?}; Wendelon DROP must complete before local descriptor removal",
				),
			}),
		}
	}

	pub fn remove_table(&self, table: TableId) -> Result<TableBinding, CatalogError> {
		self.remove_table_for_tenant(TenantId(1), table)
	}

	pub fn remove_table_for_tenant(
		&self,
		tenant_id: TenantId,
		table: TableId,
	) -> Result<TableBinding, CatalogError> {
		let mut state = self
			.inner
			.write()
			.expect("WendelonDescriptorRegistry lock poisoned");
		let binding =
			state
				.by_id
				.remove(&(tenant_id, table))
				.ok_or_else(|| CatalogError::TableNotFound {
					table: format!("{table:?}"),
				})?;
		state
			.by_ref
			.retain(|(entry_tenant, _), table_id| *entry_tenant != tenant_id || *table_id != table);
		state.descriptors.remove(&(tenant_id, table));
		state.finalized_drops.remove(&(tenant_id, table));
		Ok(binding)
	}

	pub fn invalidate_descriptor_for_tenant(
		&self,
		tenant_id: TenantId,
		table: TableId,
	) -> Option<InvalidatedDescriptor> {
		let mut state = self
			.inner
			.write()
			.expect("WendelonDescriptorRegistry lock poisoned");
		let binding = state.by_id.remove(&(tenant_id, table))?;
		state.descriptors.remove(&(tenant_id, table));
		state.finalized_drops.remove(&(tenant_id, table));
		let mut refs = Vec::new();
		state.by_ref.retain(|(entry_tenant, table_ref), table_id| {
			let remove = *entry_tenant == tenant_id && *table_id == table;
			if remove {
				refs.push(table_ref.clone());
			}
			!remove
		});
		Some(InvalidatedDescriptor {
			table_id: table,
			table_object_id: binding.table,
			fingerprint: binding.fingerprint,
			refs,
		})
	}
}

fn insert_wendelon_descriptor_locked(
	state: &mut WendelonDescriptorState,
	tenant_id: TenantId,
	table_ref: TableRef,
	unqualified_ref: Option<TableRef>,
	table_id: TableId,
	binding: TableBinding,
	descriptor: wendelon_client::TableSchemaDescriptor,
) -> Result<(), CatalogError> {
	if state.by_ref.contains_key(&(tenant_id, table_ref.clone()))
		|| state.by_id.contains_key(&(tenant_id, table_id))
	{
		return Err(CatalogError::TableAlreadyExists {
			table: table_name(&table_ref),
		});
	}

	state.by_ref.insert((tenant_id, table_ref), table_id);
	if let Some(unqualified_ref) = unqualified_ref {
		state
			.by_ref
			.entry((tenant_id, unqualified_ref))
			.or_insert(table_id);
	}
	state.by_id.insert((tenant_id, table_id), binding);
	state.descriptors.insert((tenant_id, table_id), descriptor);
	bump_next_wendelon_table_id(state, table_id);
	Ok(())
}

fn allocate_wendelon_table_id_locked(state: &mut WendelonDescriptorState) -> TableId {
	while state
		.by_id
		.keys()
		.any(|(_, table_id)| *table_id == TableId(state.next_table_id))
	{
		state.next_table_id = state
			.next_table_id
			.checked_add(1)
			.expect("TableId overflow");
	}
	let table_id = TableId(state.next_table_id);
	state.next_table_id = state
		.next_table_id
		.checked_add(1)
		.expect("TableId overflow");
	table_id
}

fn same_descriptor_identity(existing: &TableBinding, candidate: &TableBinding) -> bool {
	existing.table == candidate.table
		&& existing.schema_generation == candidate.schema_generation
		&& existing.fingerprint == candidate.fingerprint
		&& existing.schema.as_ref() == candidate.schema.as_ref()
}

fn default_descriptor_sync_flights() -> Arc<DescriptorSyncFlights> {
	Arc::new(DescriptorSyncFlights::new(
		TenantDescriptorSyncAccountant::new(TenantBudgetPolicy::from_query_budget(
			QueryBudget::default(),
		)),
	))
}

impl WendelonCatalog {
	pub fn from_descriptors(
		descriptors: impl IntoIterator<Item = (TableId, wendelon_client::TableSchemaDescriptor)>,
	) -> Result<Self, CatalogError> {
		Ok(Self {
			registry: Arc::new(WendelonDescriptorRegistry::from_descriptors(descriptors)?),
			..Self::default()
		})
	}

	pub fn from_descriptors_with_optimizer_facts(
		descriptors: impl IntoIterator<
			Item = (
				TableId,
				wendelon_client::TableSchemaDescriptor,
				Option<DescriptorOptimizerFacts>,
			),
		>,
	) -> Result<Self, CatalogError> {
		Ok(Self {
			registry: Arc::new(
				WendelonDescriptorRegistry::from_descriptors_for_tenant_with_facts(
					TenantId(1),
					descriptors,
				)?,
			),
			..Self::default()
		})
	}

	pub fn with_client(
		client: Arc<Client>,
		tenant_id: impl Into<TenantId>,
		descriptors: impl IntoIterator<Item = (TableId, wendelon_client::TableSchemaDescriptor)>,
	) -> Result<Self, CatalogError> {
		let tenant_id = tenant_id.into();
		let schema_update_client: Arc<dyn SchemaUpdateClient> = client.clone();
		let descriptor_sync_client: Arc<dyn DescriptorSyncClient> = client.clone();
		Ok(Self {
			client: Some(schema_update_client),
			descriptor_sync_client: Some(descriptor_sync_client),
			descriptor_sync_flights: Some(default_descriptor_sync_flights()),
			descriptor_sync_metrics: Arc::new(DescriptorSyncMetrics::default()),
			descriptor_refresh_metrics: Arc::new(DescriptorRefreshMetrics::default()),
			tenant_id,
			registry: Arc::new(WendelonDescriptorRegistry::from_descriptors_for_tenant(
				tenant_id,
				descriptors,
			)?),
		})
	}

	pub fn with_client_and_registry(
		client: Arc<Client>,
		tenant_id: TenantId,
		registry: Arc<WendelonDescriptorRegistry>,
	) -> Self {
		Self::with_client_and_registry_and_descriptor_sync_accountant(
			client,
			tenant_id,
			registry,
			TenantDescriptorSyncAccountant::new(TenantBudgetPolicy::from_query_budget(
				QueryBudget::default(),
			)),
		)
	}

	pub fn with_client_and_registry_and_descriptor_sync_accountant(
		client: Arc<Client>,
		tenant_id: TenantId,
		registry: Arc<WendelonDescriptorRegistry>,
		descriptor_sync_accountant: TenantDescriptorSyncAccountant,
	) -> Self {
		let descriptor_sync_client: Arc<dyn DescriptorSyncClient> = client.clone();
		let schema_update_client: Arc<dyn SchemaUpdateClient> = client;
		Self {
			client: Some(schema_update_client),
			descriptor_sync_client: Some(descriptor_sync_client),
			descriptor_sync_flights: Some(Arc::new(DescriptorSyncFlights::new(
				descriptor_sync_accountant,
			))),
			descriptor_sync_metrics: Arc::new(DescriptorSyncMetrics::default()),
			descriptor_refresh_metrics: Arc::new(DescriptorRefreshMetrics::default()),
			tenant_id,
			registry,
		}
	}

	pub fn descriptor_registry(&self) -> Arc<WendelonDescriptorRegistry> {
		self.registry.clone()
	}

	pub fn descriptor_sync_metrics(&self) -> Arc<DescriptorSyncMetrics> {
		Arc::clone(&self.descriptor_sync_metrics)
	}

	pub fn descriptor_refresh_metrics(&self) -> Arc<DescriptorRefreshMetrics> {
		Arc::clone(&self.descriptor_refresh_metrics)
	}

	async fn resolve_table_after_local_miss(
		&self,
		table: &TableRef,
	) -> Result<TableBinding, CatalogError> {
		let Some(client) = self.descriptor_sync_client.as_ref() else {
			return Err(CatalogError::TableNotFound {
				table: table_name(table),
			});
		};
		let Some(flights) = self.descriptor_sync_flights.as_ref() else {
			return Err(CatalogError::TableNotFound {
				table: table_name(table),
			});
		};

		let key = DescriptorSyncKey::from_table(self.tenant_id, table);
		let table_ref = key.table_ref();
		match Arc::clone(flights).join_or_own(key.clone()) {
			DescriptorSyncParticipant::Owner(owner) => {
				self.resolve_table_sync_owner(client, owner, key).await
			}
			DescriptorSyncParticipant::Waiter(flight) => match flight.wait().await {
				DescriptorSyncOutcome::Synced => {
					self.registry.resolve_table(self.tenant_id, &table_ref)
				}
				DescriptorSyncOutcome::NotFound => Err(CatalogError::TableNotFound {
					table: table_name(&table_ref),
				}),
				DescriptorSyncOutcome::Throttled => Err(descriptor_sync_resource_exhausted()),
				DescriptorSyncOutcome::Failed(message) => {
					Err(CatalogError::AdminFailed { message })
				}
			},
		}
	}

	async fn resolve_table_sync_owner(
		&self,
		client: &Arc<dyn DescriptorSyncClient>,
		owner: DescriptorSyncOwner,
		key: DescriptorSyncKey,
	) -> Result<TableBinding, CatalogError> {
		let table_ref = key.table_ref();
		let reservation = match owner.flights.accountant.try_reserve(self.tenant_id) {
			Ok(reservation) => reservation,
			Err(_) => {
				self.descriptor_sync_metrics.record_throttled();
				owner.finish(DescriptorSyncOutcome::Throttled);
				return Err(descriptor_sync_resource_exhausted());
			}
		};

		let wendelon_tenant = wendelon_tenant_id(self.tenant_id);
		let logical_types_result = client.list_logical_type_definitions(wendelon_tenant).await;
		let descriptor_result = match logical_types_result {
			Ok(definitions) => match self
				.registry
				.register_logical_types_for_tenant(self.tenant_id, definitions)
			{
				Ok(()) => client
					.get_table_schema_descriptor(wendelon_tenant, &key.namespace, &key.table_name)
					.await
					.map_err(|error| error.to_string()),
				Err(error) => Err(error.to_string()),
			},
			Err(error) => Err(error.to_string()),
		};

		let result = match descriptor_result {
			Ok(Some(descriptor)) => {
				let optimizer_facts = client
					.get_table_optimizer_facts(
						wendelon_tenant,
						&key.namespace,
						&key.table_name,
						&descriptor,
					)
					.await
					.ok()
					.flatten()
					.and_then(|facts| descriptor_optimizer_facts_from_storage(facts, &descriptor));
				self.registry
					.sync_descriptor_for_tenant_with_facts(
						self.tenant_id,
						&table_ref,
						descriptor,
						optimizer_facts,
					)
					.map_err(|error| descriptor_sync_admin_failed(&table_ref, error))
			}
			Ok(None) => Err(CatalogError::TableNotFound {
				table: table_name(&table_ref),
			}),
			Err(error) => Err(descriptor_sync_admin_failed(&table_ref, error)),
		};
		drop(reservation);

		match result {
			Ok(binding) => {
				self.descriptor_sync_metrics.record_success();
				owner.finish(DescriptorSyncOutcome::Synced);
				Ok(binding)
			}
			Err(CatalogError::TableNotFound { table }) => {
				self.descriptor_sync_metrics.record_not_found();
				owner.finish(DescriptorSyncOutcome::NotFound);
				Err(CatalogError::TableNotFound { table })
			}
			Err(error) => {
				self.descriptor_sync_metrics.record_error();
				let message = match &error {
					CatalogError::AdminFailed { message } => message.clone(),
					other => {
						format!(
							"descriptor sync failed for {}: {other}",
							table_name(&table_ref)
						)
					}
				};
				owner.finish(DescriptorSyncOutcome::Failed(message));
				Err(error)
			}
		}
	}
}

#[async_trait::async_trait]
impl Catalog for WendelonCatalog {
	fn scoped(self: Arc<Self>, tenant_id: TenantId) -> Arc<dyn Catalog> {
		Arc::new(Self {
			client: self.client.clone(),
			descriptor_sync_client: self.descriptor_sync_client.clone(),
			descriptor_sync_flights: self.descriptor_sync_flights.clone(),
			descriptor_sync_metrics: Arc::clone(&self.descriptor_sync_metrics),
			descriptor_refresh_metrics: Arc::clone(&self.descriptor_refresh_metrics),
			tenant_id,
			registry: Arc::clone(&self.registry),
		})
	}

	fn descriptor_refresh_metrics(&self) -> Arc<DescriptorRefreshMetrics> {
		Arc::clone(&self.descriptor_refresh_metrics)
	}

	async fn resolve_table(&self, table: &TableRef) -> Result<TableBinding, CatalogError> {
		match self.registry.resolve_table(self.tenant_id, table) {
			Ok(binding) => Ok(binding),
			Err(CatalogError::TableNotFound { .. }) => {
				self.resolve_table_after_local_miss(table).await
			}
			Err(error) => Err(error),
		}
	}

	async fn resolve_table_local(&self, table: &TableRef) -> Result<TableBinding, CatalogError> {
		self.registry.resolve_table(self.tenant_id, table)
	}

	async fn resolve_table_id(&self, table: TableId) -> Result<TableBinding, CatalogError> {
		self.registry.resolve_table_id(self.tenant_id, table)
	}

	async fn resolve_durable_logical_type_by_name(
		&self,
		name: &str,
	) -> Result<LogicalTypeSpec, CatalogError> {
		if let Some(spec) = self
			.registry
			.resolve_durable_logical_type_by_name(self.tenant_id, name)
		{
			return Ok(spec);
		}

		let Some(client) = self.descriptor_sync_client.as_ref() else {
			return Err(CatalogError::LogicalTypeNotFound {
				name: name.to_string(),
			});
		};
		let definitions = client
			.list_logical_type_definitions(wendelon_tenant_id(self.tenant_id))
			.await
			.map_err(|error| CatalogError::AdminFailed {
				message: error.to_string(),
			})?;
		self.registry
			.register_logical_types_for_tenant(self.tenant_id, definitions)?;
		self.registry
			.resolve_durable_logical_type_by_name(self.tenant_id, name)
			.ok_or_else(|| CatalogError::LogicalTypeNotFound {
				name: name.to_string(),
			})
	}

	fn logical_type_registry(&self) -> LogicalTypeRegistry {
		self.registry.logical_type_registry(self.tenant_id)
	}

	async fn refreshed_logical_type_registry(&self) -> Result<LogicalTypeRegistry, CatalogError> {
		let Some(client) = self.descriptor_sync_client.as_ref() else {
			return Ok(self.registry.logical_type_registry(self.tenant_id));
		};
		let definitions = client
			.list_logical_type_definitions(wendelon_tenant_id(self.tenant_id))
			.await
			.map_err(|error| CatalogError::AdminFailed {
				message: error.to_string(),
			})?;
		self.registry
			.register_logical_types_for_tenant(self.tenant_id, definitions)?;
		Ok(self.registry.logical_type_registry(self.tenant_id))
	}

	async fn refreshed_table_schema_descriptors(
		&self,
	) -> Result<Vec<wendelon_client::TableSchemaDescriptor>, CatalogError> {
		let Some(client) = self.descriptor_sync_client.as_ref() else {
			return Ok(self.registry.descriptor_snapshot_for_tenant(self.tenant_id));
		};
		self.refreshed_logical_type_registry().await?;
		let mut descriptors = client
			.list_table_schema_descriptors(wendelon_tenant_id(self.tenant_id))
			.await
			.map_err(|error| CatalogError::AdminFailed {
				message: error.to_string(),
			})?;
		descriptors = sorted_descriptors_for_tenant(descriptors);
		for descriptor in descriptors.iter().cloned() {
			self.registry
				.sync_latest_descriptor_for_tenant(self.tenant_id, descriptor)?;
		}
		Ok(descriptors)
	}

	async fn invalidate_table_descriptor(
		&self,
		table: TableId,
	) -> Result<Option<InvalidatedDescriptor>, CatalogError> {
		let invalidated = self
			.registry
			.invalidate_descriptor_for_tenant(self.tenant_id, table);
		if invalidated.is_some() {
			self.descriptor_refresh_metrics.record_invalidated();
		} else {
			self.descriptor_refresh_metrics.record_missing();
		}
		Ok(invalidated)
	}

	async fn create_table_with_outcome(
		&self,
		name: &TableRef,
		schema: SchemaRef,
		row_identity: CreateTableRowIdentity,
		if_not_exists: bool,
	) -> Result<CreateTableOutcome, CatalogError> {
		if let Some(existing) = self.registry.try_resolve_table(self.tenant_id, name) {
			if if_not_exists {
				if existing.schema.as_ref() == schema.as_ref()
					&& existing.row_identity.matches_create_identity(&row_identity)
				{
					return Ok(CreateTableOutcome {
						binding: existing,
						created: false,
					});
				}
				return Err(CatalogError::SchemaMismatch {
					table: table_name(name),
				});
			}
			return Err(CatalogError::TableAlreadyExists {
				table: table_name(name),
			});
		}

		let client = self
			.client
			.as_ref()
			.ok_or_else(|| CatalogError::AdminFailed {
				message: "CREATE TABLE requires a configured Wendelon catalog admin client"
					.to_string(),
			})?;
		let legacy_table_id = self.registry.allocate_table_id();
		let table_object_id = TableObjectId::new(*Uuid::new_v4().as_bytes());
		let row_identity_policy =
			create_table_row_identity_policy(name, schema.as_ref(), &row_identity)?;
		let descriptor = TableSchemaDescriptor::new_with_layout(
			table_object_id,
			1,
			name.schema.as_deref().unwrap_or("default"),
			&name.table,
			schema.as_ref(),
			row_identity_policy,
			LocalityPolicy::default_table_spread(),
			RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::default()),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.map_err(|error| CatalogError::FingerprintResolution {
			message: error.to_string(),
		})?;
		let intent_id = SchemaIntentId::new(*Uuid::new_v4().as_bytes());
		let request = TableSchemaUpdateRequest::create(
			wendelon_tenant_id(self.tenant_id),
			descriptor.clone(),
			intent_id,
		);

		if let Err(error) = client.submit_table_schema_update(request).await {
			let schema_update_rejected = matches!(error, ClientError::SchemaUpdateRejected { .. });
			let original_error = map_wendelon_admin_error(name, error);
			if if_not_exists
				&& schema_update_rejected
				&& let Ok(existing) = self.resolve_table_after_local_miss(name).await
			{
				if existing.schema.as_ref() == schema.as_ref()
					&& existing.row_identity.matches_create_identity(&row_identity)
				{
					return Ok(CreateTableOutcome {
						binding: existing,
						created: false,
					});
				}
				return Err(CatalogError::SchemaMismatch {
					table: table_name(name),
				});
			}
			return Err(original_error);
		}

		let logical_types = self.registry.logical_type_registry(self.tenant_id);
		let binding = TableBinding::from_descriptor_with_logical_types(
			legacy_table_id,
			&descriptor,
			LogicalTypeResolver::with_registry(&logical_types),
		)?;
		self.registry.insert_descriptor(
			self.tenant_id,
			name.clone(),
			binding.clone(),
			descriptor,
		)?;
		Ok(CreateTableOutcome {
			binding,
			created: true,
		})
	}

	async fn drop_table(
		&self,
		name: &TableRef,
		if_exists: bool,
	) -> Result<Option<TableBinding>, CatalogError> {
		let binding = match self.registry.try_resolve_table(self.tenant_id, name) {
			Some(binding) => binding,
			None if if_exists => return Ok(None),
			None => {
				return Err(CatalogError::TableNotFound {
					table: table_name(name),
				});
			}
		};
		let operation_id = DropOperationId {
			tenant_id: wendelon_tenant_id(self.tenant_id),
			table_object_id: binding.table,
			schema_generation: SchemaGeneration::new(binding.schema_generation),
		};
		self.registry.consume_finalized_drop(
			self.tenant_id,
			binding.legacy_table_id,
			operation_id,
		)?;
		self.registry
			.remove_table_for_tenant(self.tenant_id, binding.legacy_table_id)?;
		Ok(Some(binding))
	}
}

impl From<WendelonTenantId> for TenantId {
	fn from(value: WendelonTenantId) -> Self {
		Self(value.get())
	}
}

fn wendelon_tenant_id(tenant_id: TenantId) -> WendelonTenantId {
	WendelonTenantId::new(tenant_id.0)
}

fn sorted_descriptors_for_tenant(
	descriptors: impl IntoIterator<Item = wendelon_client::TableSchemaDescriptor>,
) -> Vec<wendelon_client::TableSchemaDescriptor> {
	let mut descriptors = descriptors.into_iter().collect::<Vec<_>>();
	descriptors.sort_by(|left, right| {
		left.namespace
			.cmp(&right.namespace)
			.then_with(|| left.table_name.cmp(&right.table_name))
			.then_with(|| left.schema_generation.cmp(&right.schema_generation))
			.then_with(|| left.table_object_id.cmp(&right.table_object_id))
	});
	descriptors
}

fn table_name(table: &TableRef) -> String {
	match &table.schema {
		Some(schema) => format!("{schema}.{}", table.table),
		None => table.table.clone(),
	}
}

fn canonical_table_ref(table: &TableRef) -> TableRef {
	TableRef::with_schema(
		table.schema.as_deref().unwrap_or("default"),
		table.table.clone(),
	)
}

fn descriptor_sync_resource_exhausted() -> CatalogError {
	CatalogError::ResourceExhausted {
		message: "tenant descriptor sync limit exhausted".to_string(),
	}
}

fn descriptor_sync_admin_failed(table: &TableRef, error: impl std::fmt::Display) -> CatalogError {
	CatalogError::AdminFailed {
		message: format!("descriptor sync failed for {}: {error}", table_name(table)),
	}
}

fn bump_next_table_id(inner: &mut MockCatalogInner, table_id: TableId) {
	inner.next_table_id = inner
		.next_table_id
		.max(table_id.0.checked_add(1).expect("TableId overflow"));
}

fn bump_next_wendelon_table_id(state: &mut WendelonDescriptorState, table_id: TableId) {
	state.next_table_id = state
		.next_table_id
		.max(table_id.0.checked_add(1).expect("TableId overflow"));
}

fn map_wendelon_admin_error(table: &TableRef, error: ClientError) -> CatalogError {
	match error {
		ClientError::Admin(
			wendelon_client::WendelonAdminError::TableMissing { .. }
			| wendelon_client::WendelonAdminError::TableAlreadyDropped { .. },
		) => CatalogError::TableNotFound {
			table: table_name(table),
		},
		ClientError::Admin(wendelon_client::WendelonAdminError::GenerationMismatch { .. }) => {
			CatalogError::AdminFailed {
				message: format!(
					"table {} schema generation changed during admin operation",
					table_name(table)
				),
			}
		}
		ClientError::Admin(wendelon_client::WendelonAdminError::TableAlreadyDropping {
			existing_operation_id,
			..
		}) => CatalogError::AdminFailed {
			message: format!(
				"table {} drop already in progress under operation {existing_operation_id:?}",
				table_name(table),
			),
		},
		ClientError::Admin(wendelon_client::WendelonAdminError::DependentIndexesPresent {
			indexes,
		}) => CatalogError::AdminFailed {
			message: format!(
				"table {} has {} active dependent indexes; drop them first",
				table_name(table),
				indexes.len(),
			),
		},
		ClientError::Admin(wendelon_client::WendelonAdminError::Transport { message }) => {
			CatalogError::AdminFailed { message }
		}
		other => CatalogError::AdminFailed {
			message: other.to_string(),
		},
	}
}

fn mock_table_identity(
	name: &TableRef,
	schema: &Schema,
	row_identity: &CreateTableRowIdentity,
	legacy_table_id: TableId,
) -> Result<(TableObjectId, PhysicalDescriptorFingerprint), CatalogError> {
	let mut object_id = [0_u8; 16];
	object_id[8..].copy_from_slice(&legacy_table_id.0.to_be_bytes());
	let table_object_id = TableObjectId::new(object_id);

	let descriptor = create_table_descriptor(name, schema, row_identity, table_object_id)?;

	Ok((table_object_id, descriptor.physical_descriptor_fingerprint))
}

fn create_table_descriptor(
	name: &TableRef,
	schema: &Schema,
	row_identity: &CreateTableRowIdentity,
	table_object_id: TableObjectId,
) -> Result<TableSchemaDescriptor, CatalogError> {
	let row_identity_policy = create_table_row_identity_policy(name, schema, row_identity)?;
	TableSchemaDescriptor::new_with_layout(
		table_object_id,
		1,
		name.schema.as_deref().unwrap_or("default"),
		&name.table,
		schema,
		row_identity_policy,
		LocalityPolicy::default_table_spread(),
		RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::default()),
		SchemaEvolutionPolicyV1::AdditiveOnly,
	)
	.map_err(|error| CatalogError::FingerprintResolution {
		message: error.to_string(),
	})
}

fn create_table_row_identity_policy(
	table: &TableRef,
	schema: &Schema,
	row_identity: &CreateTableRowIdentity,
) -> Result<RowIdentityPolicy, CatalogError> {
	match row_identity {
		CreateTableRowIdentity::AppendOnly => Ok(RowIdentityPolicy::AppendOnly),
		CreateTableRowIdentity::PrimaryKey { column } => {
			schema
				.index_of(column)
				.map_err(|_| CatalogError::SchemaMismatch {
					table: table_name(table),
				})?;
			Ok(RowIdentityPolicy::PrimaryKey(
				PrimaryKeyDescriptorV1::single(column.as_str()),
			))
		}
	}
}

fn table_row_identity(
	schema: &Schema,
	descriptor: &wendelon_client::TableSchemaDescriptor,
) -> Result<TableRowIdentity, CatalogError> {
	match &descriptor.row_identity_policy {
		RowIdentityPolicy::AppendOnly => Ok(TableRowIdentity::AppendOnly),
		RowIdentityPolicy::PrimaryKey(primary_key) => {
			let [column] = primary_key.columns.as_slice() else {
				return Ok(TableRowIdentity::Unsupported);
			};
			let ordinal =
				schema
					.index_of(column)
					.map_err(|_| CatalogError::FingerprintResolution {
						message: format!(
							"primary key column {column:?} missing from descriptor schema"
						),
					})?;
			Ok(TableRowIdentity::PrimaryKey {
				column: column.clone(),
				ordinal: ColumnOrdinal(u32::try_from(ordinal).expect("column ordinal overflow")),
			})
		}
		RowIdentityPolicy::AppendOnlyContentHash => Ok(TableRowIdentity::Unsupported),
	}
}

fn pbc_eligible_columns(schema: &Schema) -> HashSet<ColumnOrdinal> {
	schema
		.fields()
		.iter()
		.enumerate()
		.filter_map(|(ordinal, field)| {
			is_pbc_eligible_type(field.data_type()).then_some(ColumnOrdinal(
				u32::try_from(ordinal).expect("column ordinal overflow"),
			))
		})
		.collect()
}

fn validate_logical_column_metadata(
	schema: &Schema,
	logical_types: LogicalTypeResolver<'_>,
) -> Result<(), CatalogError> {
	for field in schema.fields() {
		decode_column_logical_type_with_resolver(field, logical_types).map_err(|error| {
			CatalogError::FingerprintResolution {
				message: error.to_string(),
			}
		})?;
	}
	Ok(())
}

fn primary_key_columns(
	schema: &Schema,
	descriptor: &wendelon_client::TableSchemaDescriptor,
) -> Result<HashSet<ColumnOrdinal>, CatalogError> {
	descriptor
		.primary_key_descriptor()
		.map(|primary_key| {
			primary_key
				.columns
				.iter()
				.map(|column| {
					let ordinal = schema.index_of(column).map_err(|_| {
						CatalogError::FingerprintResolution {
							message: format!(
								"primary key column {column:?} missing from descriptor schema"
							),
						}
					})?;
					Ok(ColumnOrdinal(
						u32::try_from(ordinal).expect("column ordinal overflow"),
					))
				})
				.collect()
		})
		.unwrap_or_else(|| Ok(HashSet::new()))
}

fn is_pbc_eligible_type(data_type: &DataType) -> bool {
	matches!(
		data_type,
		DataType::Boolean
			| DataType::Int32
			| DataType::Int64
			| DataType::Float32
			| DataType::Float64
			| DataType::Utf8
			| DataType::LargeUtf8
	)
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::binder::{PlannerConfig, bind};
	use crate::distributed::budget::{ByteCount, TenantBudget};
	use crate::ir::{
		DURABLE_LOGICAL_TYPE_MIN_ID, LOGICAL_TYPE_ID_METADATA_KEY, field_with_logical_type_metadata,
	};
	use crate::sql_planner::SqlPlanner;
	use arrow::datatypes::{DataType, Field, Schema};
	use std::collections::VecDeque;
	use std::num::NonZeroU32;
	use std::sync::atomic::Ordering;
	use std::time::Duration;
	use tokio::time::{sleep, timeout};
	use wendelon::schema::{
		LocalityPolicy, PrimaryKeyDescriptorV1, RowIdentityPolicy, SchemaEvolutionPolicyV1,
		TableObjectId, TableSchemaDescriptor,
	};
	use wendelon_client::LogicalTypePhysical;

	struct FakeDescriptorSyncClient {
		responses: tokio::sync::Mutex<VecDeque<Result<Option<TableSchemaDescriptor>, ClientError>>>,
		list_responses: Mutex<VecDeque<Result<Vec<TableSchemaDescriptor>, ClientError>>>,
		facts_responses: Mutex<VecDeque<Result<Option<StorageTableOptimizerFacts>, ClientError>>>,
		requests: tokio::sync::Mutex<Vec<(WendelonTenantId, String, String)>>,
		logical_type_definitions: Vec<LogicalTypeDefinition>,
		calls: AtomicU64,
		active: AtomicU64,
		max_active: AtomicU64,
		started: Notify,
		release: Notify,
		hold_until_released: bool,
	}

	impl FakeDescriptorSyncClient {
		fn new(
			responses: impl IntoIterator<Item = Result<Option<TableSchemaDescriptor>, ClientError>>,
		) -> Self {
			Self {
				responses: tokio::sync::Mutex::new(responses.into_iter().collect()),
				list_responses: Mutex::new(VecDeque::new()),
				facts_responses: Mutex::new(VecDeque::new()),
				requests: tokio::sync::Mutex::new(Vec::new()),
				logical_type_definitions: Vec::new(),
				calls: AtomicU64::new(0),
				active: AtomicU64::new(0),
				max_active: AtomicU64::new(0),
				started: Notify::new(),
				release: Notify::new(),
				hold_until_released: false,
			}
		}

		fn with_logical_types(
			mut self,
			definitions: impl IntoIterator<Item = LogicalTypeDefinition>,
		) -> Self {
			self.logical_type_definitions = definitions.into_iter().collect();
			self
		}

		fn with_optimizer_facts(
			self,
			responses: impl IntoIterator<Item = Result<Option<StorageTableOptimizerFacts>, ClientError>>,
		) -> Self {
			*self
				.facts_responses
				.lock()
				.expect("fake facts response lock poisoned") = responses.into_iter().collect();
			self
		}

		fn held(
			responses: impl IntoIterator<Item = Result<Option<TableSchemaDescriptor>, ClientError>>,
		) -> Self {
			Self {
				hold_until_released: true,
				..Self::new(responses)
			}
		}

		fn call_count(&self) -> u64 {
			self.calls.load(Ordering::SeqCst)
		}

		fn max_active(&self) -> u64 {
			self.max_active.load(Ordering::SeqCst)
		}

		async fn requests(&self) -> Vec<(WendelonTenantId, String, String)> {
			self.requests.lock().await.clone()
		}

		async fn wait_until_started(&self) {
			self.wait_until_calls(1).await;
		}

		async fn wait_until_calls(&self, expected: u64) {
			while self.call_count() < expected {
				self.started.notified().await;
			}
		}

		fn release(&self) {
			self.release.notify_waiters();
		}
	}

	#[async_trait::async_trait]
	impl DescriptorSyncClient for FakeDescriptorSyncClient {
		async fn list_logical_type_definitions(
			&self,
			_tenant_id: WendelonTenantId,
		) -> Result<Vec<LogicalTypeDefinition>, ClientError> {
			Ok(self.logical_type_definitions.clone())
		}

		async fn get_table_schema_descriptor(
			&self,
			tenant_id: WendelonTenantId,
			namespace: &str,
			table_name: &str,
		) -> Result<Option<TableSchemaDescriptor>, ClientError> {
			self.calls.fetch_add(1, Ordering::SeqCst);
			let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
			let mut max_active = self.max_active.load(Ordering::SeqCst);
			while active > max_active
				&& self
					.max_active
					.compare_exchange(max_active, active, Ordering::SeqCst, Ordering::SeqCst)
					.is_err()
			{
				max_active = self.max_active.load(Ordering::SeqCst);
			}
			self.requests.lock().await.push((
				tenant_id,
				namespace.to_string(),
				table_name.to_string(),
			));
			self.started.notify_waiters();
			if self.hold_until_released {
				self.release.notified().await;
			}
			self.active.fetch_sub(1, Ordering::SeqCst);
			self.responses
				.lock()
				.await
				.pop_front()
				.expect("fake descriptor response")
		}

		async fn get_table_optimizer_facts(
			&self,
			_tenant_id: WendelonTenantId,
			_namespace: &str,
			_table_name: &str,
			_descriptor: &TableSchemaDescriptor,
		) -> Result<Option<StorageTableOptimizerFacts>, ClientError> {
			self.facts_responses
				.lock()
				.expect("fake facts response lock poisoned")
				.pop_front()
				.unwrap_or(Ok(None))
		}

		async fn list_table_schema_descriptors(
			&self,
			_tenant_id: WendelonTenantId,
		) -> Result<Vec<TableSchemaDescriptor>, ClientError> {
			self.list_responses
				.lock()
				.expect("fake descriptor list response lock poisoned")
				.pop_front()
				.expect("fake descriptor list response")
		}
	}

	struct FakeSchemaUpdateClient {
		responses: tokio::sync::Mutex<VecDeque<Result<SchemaUpdateReceipt, ClientError>>>,
		calls: AtomicU64,
	}

	impl FakeSchemaUpdateClient {
		fn new(
			responses: impl IntoIterator<Item = Result<SchemaUpdateReceipt, ClientError>>,
		) -> Self {
			Self {
				responses: tokio::sync::Mutex::new(responses.into_iter().collect()),
				calls: AtomicU64::new(0),
			}
		}

		fn call_count(&self) -> u64 {
			self.calls.load(Ordering::SeqCst)
		}
	}

	#[async_trait::async_trait]
	impl SchemaUpdateClient for FakeSchemaUpdateClient {
		async fn submit_table_schema_update(
			&self,
			_request: TableSchemaUpdateRequest,
		) -> Result<SchemaUpdateReceipt, ClientError> {
			self.calls.fetch_add(1, Ordering::SeqCst);
			self.responses
				.lock()
				.await
				.pop_front()
				.expect("fake schema update response")
		}
	}

	fn catalog_with_fake_client(
		tenant_id: TenantId,
		registry: Arc<WendelonDescriptorRegistry>,
		client: Arc<FakeDescriptorSyncClient>,
		accountant: TenantDescriptorSyncAccountant,
	) -> WendelonCatalog {
		WendelonCatalog {
			client: None,
			descriptor_sync_client: Some(client),
			descriptor_sync_flights: Some(Arc::new(DescriptorSyncFlights::new(accountant))),
			descriptor_sync_metrics: Arc::new(DescriptorSyncMetrics::default()),
			descriptor_refresh_metrics: Arc::new(DescriptorRefreshMetrics::default()),
			tenant_id,
			registry,
		}
	}

	fn descriptor_sync_accountant(limit: u32) -> TenantDescriptorSyncAccountant {
		TenantDescriptorSyncAccountant::new(TenantBudgetPolicy::new(
			TenantBudget::with_query_and_descriptor_sync_limits(
				ByteCount::new(8),
				ByteCount::new(16),
				NonZeroU32::new(2).expect("nonzero"),
				NonZeroU32::new(limit).expect("nonzero"),
			),
			HashMap::new(),
		))
	}

	fn expect_catalog_error(
		result: Result<TableBinding, CatalogError>,
		message: &str,
	) -> CatalogError {
		match result {
			Ok(_) => panic!("{message}"),
			Err(error) => error,
		}
	}

	#[tokio::test]
	async fn mock_catalog_resolves_descriptor_identity_by_name_and_id() {
		let descriptor = descriptor();
		let mut catalog = MockCatalog::new();
		catalog
			.register_descriptor(TableId(7), descriptor.clone())
			.expect("register descriptor");

		let by_id = catalog
			.resolve_table_id(TableId(7))
			.await
			.expect("resolve id");
		let by_unqualified = catalog
			.resolve_table(&TableRef::new("orders"))
			.await
			.expect("resolve unqualified");
		let by_qualified = catalog
			.resolve_table(&TableRef::with_schema("bench", "orders"))
			.await
			.expect("resolve qualified");

		assert_eq!(by_id.table, descriptor.table_object_id);
		assert_eq!(
			by_unqualified.fingerprint,
			descriptor.physical_descriptor_fingerprint
		);
		assert_eq!(
			by_qualified.fingerprint,
			descriptor.physical_descriptor_fingerprint
		);
		assert!(by_id.primary_key_columns.contains(&ColumnOrdinal(0)));
	}

	#[tokio::test]
	async fn wendelon_catalog_from_descriptors_preserves_physical_fingerprint() {
		let descriptor = descriptor();
		let catalog =
			WendelonCatalog::from_descriptors([(TableId(9), descriptor.clone())]).expect("catalog");

		let resolved = catalog
			.resolve_table(&TableRef::with_schema("bench", "orders"))
			.await
			.expect("resolve table");

		assert_eq!(resolved.legacy_table_id, TableId(9));
		assert_eq!(resolved.table, descriptor.table_object_id);
		assert_eq!(
			resolved.fingerprint,
			descriptor.physical_descriptor_fingerprint
		);
		assert!(resolved.column_is_pbc_eligible(ColumnOrdinal(1)));
	}

	#[tokio::test]
	async fn descriptor_sidecar_facts_populate_table_optimizer_facts() {
		let descriptor = descriptor();
		let binding = TableBinding::from_descriptor_with_optimizer_facts(
			TableId(9),
			&descriptor,
			Some(descriptor_optimizer_facts(
				&descriptor,
				Some(4_096),
				Some(1_000),
			)),
		)
		.expect("binding with optimizer facts");

		assert_eq!(
			binding.optimizer_facts,
			TableOptimizerFacts {
				byte_upper_bound: Some(ByteCount::new(4_096)),
				row_upper_bound: Some(RowCount::new(1_000)),
				column_ndv: Default::default(),
				column_range: Default::default(),
			}
		);
	}

	#[tokio::test]
	async fn legacy_descriptor_paths_still_default_unknown() {
		let descriptor = descriptor();
		let binding =
			TableBinding::from_descriptor(TableId(9), &descriptor).expect("legacy binding");
		let registry = WendelonDescriptorRegistry::from_descriptors_for_tenant(
			TenantId(7),
			[(TableId(9), descriptor.clone())],
		)
		.expect("legacy registry");

		assert_eq!(binding.optimizer_facts, TableOptimizerFacts::default());
		assert_eq!(
			registry
				.resolve_table(TenantId(7), &TableRef::with_schema("bench", "orders"))
				.expect("resolve legacy registry")
				.optimizer_facts,
			TableOptimizerFacts::default()
		);
	}

	#[tokio::test]
	async fn mismatched_facts_default_to_unknown() {
		let descriptor = descriptor();
		let stale_descriptor = descriptor_with_identity(0xCB, descriptor.schema_generation);
		for stale_facts in [
			DescriptorOptimizerFacts {
				physical_descriptor_fingerprint: stale_descriptor.physical_descriptor_fingerprint,
				..descriptor_optimizer_facts(&descriptor, Some(1_024), Some(10))
			},
			DescriptorOptimizerFacts {
				schema_generation: descriptor.schema_generation + 1,
				..descriptor_optimizer_facts(&descriptor, Some(1_024), Some(10))
			},
		] {
			let binding = TableBinding::from_descriptor_with_optimizer_facts(
				TableId(9),
				&descriptor,
				Some(stale_facts),
			)
			.expect("binding with stale facts");

			assert_eq!(binding.optimizer_facts, TableOptimizerFacts::default());
		}
	}

	#[tokio::test]
	async fn known_empty_sidecar_normalizes_to_zero_bytes() {
		let descriptor = descriptor();
		let binding = TableBinding::from_descriptor_with_optimizer_facts(
			TableId(9),
			&descriptor,
			Some(descriptor_optimizer_facts(&descriptor, None, Some(0))),
		)
		.expect("binding with empty facts");

		assert_eq!(
			binding.optimizer_facts,
			TableOptimizerFacts {
				byte_upper_bound: Some(ByteCount::ZERO),
				row_upper_bound: Some(RowCount::new(0)),
				column_ndv: Default::default(),
				column_range: Default::default(),
			}
		);
	}

	#[tokio::test]
	async fn resolve_table_bind_miss_fetches_descriptor_and_returns_binding() {
		let descriptor = descriptor();
		let client = Arc::new(FakeDescriptorSyncClient::new([Ok(Some(
			descriptor.clone(),
		))]));
		let catalog = catalog_with_fake_client(
			TenantId(7),
			Arc::new(WendelonDescriptorRegistry::default()),
			Arc::clone(&client),
			descriptor_sync_accountant(1),
		);

		let resolved = catalog
			.resolve_table(&TableRef::with_schema("bench", "orders"))
			.await
			.expect("resolve synced descriptor");

		assert_eq!(client.call_count(), 1);
		assert_eq!(resolved.table, descriptor.table_object_id);
		assert_eq!(
			resolved.fingerprint,
			descriptor.physical_descriptor_fingerprint
		);
		assert_eq!(catalog.descriptor_sync_metrics().snapshot().success, 1);
	}

	#[tokio::test]
	async fn resolve_table_bind_miss_maps_storage_optimizer_facts() {
		let descriptor = descriptor();
		let facts = StorageTableOptimizerFacts {
			table_object_id: descriptor.table_object_id,
			schema_generation: descriptor.schema_generation,
			physical_descriptor_fingerprint: descriptor.physical_descriptor_fingerprint,
			producer_epoch: 17,
			row_upper_bound: None,
			column_ndv: vec![wendelon_client::ColumnNdvFact {
				column_ordinal: 1,
				distinct: StorageNdvFact::Approx {
					estimate: 100,
					rel_err_ppm: 26_000,
				},
			}],
			column_range: Vec::new(),
		};
		let client = Arc::new(
			FakeDescriptorSyncClient::new([Ok(Some(descriptor.clone()))])
				.with_optimizer_facts([Ok(Some(facts))]),
		);
		let catalog = catalog_with_fake_client(
			TenantId(7),
			Arc::new(WendelonDescriptorRegistry::default()),
			Arc::clone(&client),
			descriptor_sync_accountant(1),
		);

		let resolved = catalog
			.resolve_table(&TableRef::with_schema("bench", "orders"))
			.await
			.expect("resolve synced descriptor");

		assert_eq!(
			resolved.optimizer_facts.column_ndv.get(&ColumnOrdinal(1)),
			Some(&DistinctCountFact::Approx {
				estimate: 100,
				rel_err_ppm: 26_000,
			})
		);
		assert_eq!(resolved.optimizer_facts.row_upper_bound, None);
		assert_eq!(resolved.optimizer_facts.byte_upper_bound, None);
	}

	#[tokio::test]
	async fn resolve_table_bind_miss_maps_storage_range_optimizer_facts() {
		let descriptor = descriptor();
		let facts = StorageTableOptimizerFacts {
			table_object_id: descriptor.table_object_id,
			schema_generation: descriptor.schema_generation,
			physical_descriptor_fingerprint: descriptor.physical_descriptor_fingerprint,
			producer_epoch: 17,
			row_upper_bound: None,
			column_ndv: Vec::new(),
			column_range: vec![wendelon_client::ColumnRangeFact {
				column_ordinal: 1,
				arrow_type_tag: wendelon_client::ARROW_STATS_TAG_INT32,
				null_count: 2,
				non_null_count: 10,
				min: Some(wendelon_client::encode_signed_optimizer_fact_scalar(0, 4).unwrap()),
				max: Some(wendelon_client::encode_signed_optimizer_fact_scalar(99, 4).unwrap()),
			}],
		};
		let client = Arc::new(
			FakeDescriptorSyncClient::new([Ok(Some(descriptor.clone()))])
				.with_optimizer_facts([Ok(Some(facts))]),
		);
		let catalog = catalog_with_fake_client(
			TenantId(7),
			Arc::new(WendelonDescriptorRegistry::default()),
			Arc::clone(&client),
			descriptor_sync_accountant(1),
		);

		let resolved = catalog
			.resolve_table(&TableRef::with_schema("bench", "orders"))
			.await
			.expect("resolve synced descriptor");

		let range = resolved
			.optimizer_facts
			.column_range
			.get(&ColumnOrdinal(1))
			.expect("amount range fact");
		assert_eq!(range.arrow_type_tag, wendelon_client::ARROW_STATS_TAG_INT32);
		assert_eq!(range.null_count, 2);
		assert_eq!(range.non_null_count, 10);
		assert_eq!(resolved.optimizer_facts.row_upper_bound, None);
		assert_eq!(resolved.optimizer_facts.byte_upper_bound, None);
	}

	#[test]
	fn produced_ndv_never_writes_hard_bound() {
		let descriptor = descriptor();
		let facts = descriptor_optimizer_facts_from_storage(
			StorageTableOptimizerFacts {
				table_object_id: descriptor.table_object_id,
				schema_generation: descriptor.schema_generation,
				physical_descriptor_fingerprint: descriptor.physical_descriptor_fingerprint,
				producer_epoch: 17,
				row_upper_bound: None,
				column_ndv: vec![wendelon_client::ColumnNdvFact {
					column_ordinal: 1,
					distinct: StorageNdvFact::Exact { count: 4 },
				}],
				column_range: Vec::new(),
			},
			&descriptor,
		)
		.expect("storage facts map");

		assert_eq!(facts.materialized_byte_upper_bound, None);
		assert_eq!(facts.row_upper_bound, None);
		let table_facts = facts.for_descriptor(&descriptor);
		assert_eq!(table_facts.byte_upper_bound, None);
		assert_eq!(table_facts.row_upper_bound, None);
		assert_eq!(
			table_facts.column_ndv.get(&ColumnOrdinal(1)),
			Some(&DistinctCountFact::Exact(4))
		);
	}

	#[test]
	fn storage_optimizer_facts_map_hard_row_bound() {
		let descriptor = descriptor();
		let facts = descriptor_optimizer_facts_from_storage(
			StorageTableOptimizerFacts {
				table_object_id: descriptor.table_object_id,
				schema_generation: descriptor.schema_generation,
				physical_descriptor_fingerprint: descriptor.physical_descriptor_fingerprint,
				producer_epoch: 17,
				row_upper_bound: Some(12),
				column_ndv: Vec::new(),
				column_range: Vec::new(),
			},
			&descriptor,
		)
		.expect("storage facts map");

		assert_eq!(facts.materialized_byte_upper_bound, None);
		assert_eq!(facts.row_upper_bound, Some(RowCount::new(12)));
		let table_facts = facts.for_descriptor(&descriptor);
		assert_eq!(table_facts.byte_upper_bound, None);
		assert_eq!(table_facts.row_upper_bound, Some(RowCount::new(12)));
	}

	#[test]
	fn storage_optimizer_facts_known_empty_normalizes_to_zero_bytes() {
		let descriptor = descriptor();
		let facts = descriptor_optimizer_facts_from_storage(
			StorageTableOptimizerFacts {
				table_object_id: descriptor.table_object_id,
				schema_generation: descriptor.schema_generation,
				physical_descriptor_fingerprint: descriptor.physical_descriptor_fingerprint,
				producer_epoch: 17,
				row_upper_bound: Some(0),
				column_ndv: Vec::new(),
				column_range: Vec::new(),
			},
			&descriptor,
		)
		.expect("storage facts map");

		assert_eq!(facts.materialized_byte_upper_bound, None);
		assert_eq!(facts.row_upper_bound, Some(RowCount::new(0)));
		let table_facts = facts.for_descriptor(&descriptor);
		assert_eq!(table_facts.byte_upper_bound, Some(ByteCount::ZERO));
		assert_eq!(table_facts.row_upper_bound, Some(RowCount::new(0)));
	}

	#[test]
	fn descriptor_identity_mismatch_drops_fact() {
		let descriptor = descriptor();
		let stale_descriptor = descriptor_with_identity(0xCB, descriptor.schema_generation);
		let facts = StorageTableOptimizerFacts {
			table_object_id: descriptor.table_object_id,
			schema_generation: descriptor.schema_generation + 1,
			physical_descriptor_fingerprint: descriptor.physical_descriptor_fingerprint,
			producer_epoch: 17,
			row_upper_bound: Some(12),
			column_ndv: vec![wendelon_client::ColumnNdvFact {
				column_ordinal: 1,
				distinct: StorageNdvFact::Exact { count: 4 },
			}],
			column_range: Vec::new(),
		};
		assert_eq!(
			descriptor_optimizer_facts_from_storage(facts, &descriptor),
			None
		);

		let facts = StorageTableOptimizerFacts {
			table_object_id: descriptor.table_object_id,
			schema_generation: descriptor.schema_generation,
			physical_descriptor_fingerprint: stale_descriptor.physical_descriptor_fingerprint,
			producer_epoch: 17,
			row_upper_bound: Some(12),
			column_ndv: vec![wendelon_client::ColumnNdvFact {
				column_ordinal: 1,
				distinct: StorageNdvFact::Exact { count: 4 },
			}],
			column_range: Vec::new(),
		};
		assert_eq!(
			descriptor_optimizer_facts_from_storage(facts, &descriptor),
			None
		);
	}

	#[tokio::test]
	async fn facts_fetch_failure_syncs_descriptor_without_facts() {
		let descriptor = descriptor();
		let client = Arc::new(
			FakeDescriptorSyncClient::new([Ok(Some(descriptor.clone()))]).with_optimizer_facts([
				Err(ClientError::Io(std::io::Error::new(
					std::io::ErrorKind::ConnectionReset,
					"facts unavailable",
				))),
			]),
		);
		let catalog = catalog_with_fake_client(
			TenantId(7),
			Arc::new(WendelonDescriptorRegistry::default()),
			Arc::clone(&client),
			descriptor_sync_accountant(1),
		);

		let resolved = catalog
			.resolve_table(&TableRef::with_schema("bench", "orders"))
			.await
			.expect("resolve synced descriptor");

		assert_eq!(resolved.table, descriptor.table_object_id);
		assert!(resolved.optimizer_facts.column_ndv.is_empty());
	}

	#[tokio::test]
	async fn resolve_table_bind_miss_syncs_logical_types_before_descriptor_decode() {
		let definition = LogicalTypeDefinition {
			logical_type_id: DURABLE_LOGICAL_TYPE_MIN_ID,
			name: "PersonId".to_string(),
			physical: LogicalTypePhysical::Int64,
			refinement: None,
		};
		let descriptor = logical_descriptor(definition.logical_type_id);
		let client = Arc::new(
			FakeDescriptorSyncClient::new([Ok(Some(descriptor.clone()))])
				.with_logical_types([definition.clone()]),
		);
		let registry = Arc::new(WendelonDescriptorRegistry::default());
		let catalog = catalog_with_fake_client(
			TenantId(7),
			Arc::clone(&registry),
			Arc::clone(&client),
			descriptor_sync_accountant(1),
		);

		let resolved = catalog
			.resolve_table(&TableRef::with_schema("bench", "orders"))
			.await
			.expect("resolve synced descriptor with dynamic logical id");

		assert_eq!(client.call_count(), 1);
		assert_eq!(
			resolved.schema.field(0).metadata()[LOGICAL_TYPE_ID_METADATA_KEY],
			DURABLE_LOGICAL_TYPE_MIN_ID.to_string()
		);
		assert_eq!(
			registry
				.logical_type_registry(TenantId(7))
				.spec_by_name("PersonId")
				.expect("synced logical type")
				.id
				.0,
			definition.logical_type_id
		);
	}

	#[tokio::test]
	async fn durable_logical_type_name_lookup_refreshes_list_before_not_found() {
		let definition = LogicalTypeDefinition {
			logical_type_id: DURABLE_LOGICAL_TYPE_MIN_ID,
			name: "PersonId".to_string(),
			physical: LogicalTypePhysical::Int64,
			refinement: None,
		};
		let client =
			Arc::new(FakeDescriptorSyncClient::new([]).with_logical_types([definition.clone()]));
		let registry = Arc::new(WendelonDescriptorRegistry::default());
		let catalog = catalog_with_fake_client(
			TenantId(7),
			Arc::clone(&registry),
			Arc::clone(&client),
			descriptor_sync_accountant(1),
		);

		let spec = catalog
			.resolve_durable_logical_type_by_name("PersonId")
			.await
			.expect("refresh logical type list before unknown");

		assert_eq!(spec.id.0, definition.logical_type_id);
		assert_eq!(spec.name, "PersonId");
		assert_eq!(spec.physical, DataType::Int64);
		assert!(
			registry
				.logical_type_registry(TenantId(7))
				.spec_by_name("PersonId")
				.is_some()
		);
	}

	#[tokio::test]
	async fn resolve_table_local_hit_does_not_call_wendelon() {
		let descriptor = descriptor();
		let registry = Arc::new(
			WendelonDescriptorRegistry::from_descriptors_for_tenant(
				TenantId(7),
				[(TableId(9), descriptor.clone())],
			)
			.expect("registry"),
		);
		let client = Arc::new(FakeDescriptorSyncClient::new([]));
		let catalog = catalog_with_fake_client(
			TenantId(7),
			registry,
			Arc::clone(&client),
			descriptor_sync_accountant(1),
		);

		let resolved = catalog
			.resolve_table(&TableRef::with_schema("bench", "orders"))
			.await
			.expect("local hit");

		assert_eq!(client.call_count(), 0);
		assert_eq!(resolved.table, descriptor.table_object_id);
		assert_eq!(catalog.descriptor_sync_metrics().snapshot().success, 0);
	}

	#[tokio::test]
	async fn resolve_table_unqualified_miss_fetches_default_namespace() {
		let descriptor = default_descriptor_with_identity(0xD0, 1);
		let client = Arc::new(FakeDescriptorSyncClient::new([Ok(Some(descriptor))]));
		let catalog = catalog_with_fake_client(
			TenantId(7),
			Arc::new(WendelonDescriptorRegistry::default()),
			Arc::clone(&client),
			descriptor_sync_accountant(1),
		);

		catalog
			.resolve_table(&TableRef::new("orders"))
			.await
			.expect("resolve default");

		assert_eq!(
			client.requests().await,
			vec![(
				WendelonTenantId::new(7),
				"default".to_string(),
				"orders".to_string()
			)]
		);
	}

	#[tokio::test]
	async fn resolve_table_qualified_miss_fetches_exact_namespace() {
		let descriptor = descriptor();
		let client = Arc::new(FakeDescriptorSyncClient::new([Ok(Some(descriptor))]));
		let catalog = catalog_with_fake_client(
			TenantId(7),
			Arc::new(WendelonDescriptorRegistry::default()),
			Arc::clone(&client),
			descriptor_sync_accountant(1),
		);

		catalog
			.resolve_table(&TableRef::with_schema("bench", "orders"))
			.await
			.expect("resolve exact");

		assert_eq!(
			client.requests().await,
			vec![(
				WendelonTenantId::new(7),
				"bench".to_string(),
				"orders".to_string()
			)]
		);
	}

	#[tokio::test]
	async fn resolve_table_not_found_remains_table_not_found() {
		let client = Arc::new(FakeDescriptorSyncClient::new([Ok(None)]));
		let catalog = catalog_with_fake_client(
			TenantId(7),
			Arc::new(WendelonDescriptorRegistry::default()),
			Arc::clone(&client),
			descriptor_sync_accountant(1),
		);

		let error = expect_catalog_error(
			catalog
				.resolve_table(&TableRef::with_schema("bench", "missing"))
				.await,
			"missing",
		);

		assert!(matches!(error, CatalogError::TableNotFound { .. }));
		assert_eq!(catalog.descriptor_sync_metrics().snapshot().not_found, 1);
	}

	#[tokio::test]
	async fn resolve_table_fetch_error_is_admin_failed() {
		let client = Arc::new(FakeDescriptorSyncClient::new([Err(ClientError::Io(
			std::io::Error::new(std::io::ErrorKind::BrokenPipe, "offline"),
		))]));
		let catalog = catalog_with_fake_client(
			TenantId(7),
			Arc::new(WendelonDescriptorRegistry::default()),
			Arc::clone(&client),
			descriptor_sync_accountant(1),
		);

		let error = expect_catalog_error(
			catalog
				.resolve_table(&TableRef::with_schema("bench", "orders"))
				.await,
			"transport error",
		);

		assert!(matches!(error, CatalogError::AdminFailed { .. }));
		assert_eq!(catalog.descriptor_sync_metrics().snapshot().error, 1);
	}

	#[tokio::test]
	async fn resolve_table_auth_error_is_not_table_not_found() {
		let client = Arc::new(FakeDescriptorSyncClient::new([Err(
			ClientError::AuthorizationFailed,
		)]));
		let catalog = catalog_with_fake_client(
			TenantId(7),
			Arc::new(WendelonDescriptorRegistry::default()),
			Arc::clone(&client),
			descriptor_sync_accountant(1),
		);

		let error = expect_catalog_error(
			catalog
				.resolve_table(&TableRef::with_schema("bench", "orders"))
				.await,
			"auth error",
		);

		assert!(matches!(error, CatalogError::AdminFailed { .. }));
	}

	#[tokio::test]
	async fn resolve_table_throttled_is_resource_exhausted() {
		let accountant = descriptor_sync_accountant(1);
		let _held = accountant.try_reserve(TenantId(7)).expect("hold slot");
		let client = Arc::new(FakeDescriptorSyncClient::new([]));
		let catalog = catalog_with_fake_client(
			TenantId(7),
			Arc::new(WendelonDescriptorRegistry::default()),
			Arc::clone(&client),
			accountant,
		);

		let error = expect_catalog_error(
			catalog
				.resolve_table(&TableRef::with_schema("bench", "orders"))
				.await,
			"throttled",
		);

		assert!(matches!(error, CatalogError::ResourceExhausted { .. }));
		assert_eq!(client.call_count(), 0);
		assert_eq!(catalog.descriptor_sync_metrics().snapshot().throttled, 1);
	}

	#[tokio::test]
	async fn concurrent_same_table_bind_misses_issue_one_descriptor_get() {
		let descriptor = default_descriptor_with_identity(0xD1, 1);
		let client = Arc::new(FakeDescriptorSyncClient::held([Ok(Some(descriptor))]));
		let catalog = Arc::new(catalog_with_fake_client(
			TenantId(7),
			Arc::new(WendelonDescriptorRegistry::default()),
			Arc::clone(&client),
			descriptor_sync_accountant(1),
		));

		let owner_catalog = Arc::clone(&catalog);
		let owner =
			tokio::spawn(
				async move { owner_catalog.resolve_table(&TableRef::new("orders")).await },
			);
		client.wait_until_started().await;

		let waiter_catalog = Arc::clone(&catalog);
		let waiter = tokio::spawn(async move {
			waiter_catalog
				.resolve_table(&TableRef::with_schema("default", "orders"))
				.await
		});
		sleep(Duration::from_millis(10)).await;
		client.release();

		let owner = owner.await.expect("owner task").expect("owner result");
		let waiter = waiter.await.expect("waiter task").expect("waiter result");

		assert_eq!(client.call_count(), 1);
		assert_eq!(client.max_active(), 1);
		assert_eq!(owner.legacy_table_id, waiter.legacy_table_id);
		assert_eq!(catalog.descriptor_sync_metrics().snapshot().success, 1);
	}

	#[tokio::test]
	async fn concurrent_same_table_bind_misses_share_one_sync_reservation() {
		let descriptor = default_descriptor_with_identity(0xD2, 1);
		let client = Arc::new(FakeDescriptorSyncClient::held([Ok(Some(descriptor))]));
		let accountant = descriptor_sync_accountant(1);
		let catalog = Arc::new(catalog_with_fake_client(
			TenantId(7),
			Arc::new(WendelonDescriptorRegistry::default()),
			Arc::clone(&client),
			accountant.clone(),
		));

		let owner_catalog = Arc::clone(&catalog);
		let owner =
			tokio::spawn(
				async move { owner_catalog.resolve_table(&TableRef::new("orders")).await },
			);
		client.wait_until_started().await;

		let waiter_catalog = Arc::clone(&catalog);
		let waiter = tokio::spawn(async move {
			waiter_catalog
				.resolve_table(&TableRef::with_schema("default", "orders"))
				.await
		});
		sleep(Duration::from_millis(10)).await;

		assert_eq!(accountant.active(TenantId(7)), 1);
		client.release();
		owner.await.expect("owner task").expect("owner result");
		waiter.await.expect("waiter task").expect("waiter result");
		assert_eq!(accountant.active(TenantId(7)), 0);
	}

	#[tokio::test]
	async fn concurrent_same_table_bind_misses_converge_to_one_local_table_id() {
		let descriptor = default_descriptor_with_identity(0xD3, 1);
		let client = Arc::new(FakeDescriptorSyncClient::held([Ok(Some(descriptor))]));
		let catalog = Arc::new(catalog_with_fake_client(
			TenantId(7),
			Arc::new(WendelonDescriptorRegistry::default()),
			Arc::clone(&client),
			descriptor_sync_accountant(1),
		));

		let owner_catalog = Arc::clone(&catalog);
		let owner =
			tokio::spawn(
				async move { owner_catalog.resolve_table(&TableRef::new("orders")).await },
			);
		client.wait_until_started().await;
		let waiter_catalog = Arc::clone(&catalog);
		let waiter = tokio::spawn(async move {
			waiter_catalog
				.resolve_table(&TableRef::with_schema("default", "orders"))
				.await
		});
		sleep(Duration::from_millis(10)).await;
		client.release();

		let owner = owner.await.expect("owner task").expect("owner result");
		let waiter = waiter.await.expect("waiter task").expect("waiter result");

		assert_eq!(owner.legacy_table_id, waiter.legacy_table_id);
		assert_eq!(owner.legacy_table_id, TableId(1));
	}

	#[tokio::test]
	async fn qualified_default_and_unqualified_bind_misses_share_singleflight() {
		let descriptor = default_descriptor_with_identity(0xD4, 1);
		let client = Arc::new(FakeDescriptorSyncClient::held([Ok(Some(descriptor))]));
		let catalog = Arc::new(catalog_with_fake_client(
			TenantId(7),
			Arc::new(WendelonDescriptorRegistry::default()),
			Arc::clone(&client),
			descriptor_sync_accountant(1),
		));

		let unqualified = Arc::clone(&catalog);
		let unqualified =
			tokio::spawn(async move { unqualified.resolve_table(&TableRef::new("orders")).await });
		client.wait_until_started().await;
		let qualified = Arc::clone(&catalog);
		let qualified = tokio::spawn(async move {
			qualified
				.resolve_table(&TableRef::with_schema("default", "orders"))
				.await
		});
		sleep(Duration::from_millis(10)).await;
		client.release();

		unqualified
			.await
			.expect("unqualified task")
			.expect("unqualified");
		qualified.await.expect("qualified task").expect("qualified");
		assert_eq!(client.call_count(), 1);
	}

	#[tokio::test]
	async fn tenant_descriptor_sync_accountant_releases_on_cancelled_flight() {
		let descriptor = default_descriptor_with_identity(0xD5, 1);
		let client = Arc::new(FakeDescriptorSyncClient::held([Ok(Some(descriptor))]));
		let accountant = descriptor_sync_accountant(1);
		let catalog = Arc::new(catalog_with_fake_client(
			TenantId(7),
			Arc::new(WendelonDescriptorRegistry::default()),
			Arc::clone(&client),
			accountant.clone(),
		));

		let first_catalog = Arc::clone(&catalog);
		let first =
			tokio::spawn(
				async move { first_catalog.resolve_table(&TableRef::new("orders")).await },
			);
		client.wait_until_started().await;
		assert_eq!(accountant.active(TenantId(7)), 1);
		first.abort();
		let _ = first.await;
		assert_eq!(accountant.active(TenantId(7)), 0);

		let second_catalog = Arc::clone(&catalog);
		let second =
			tokio::spawn(
				async move { second_catalog.resolve_table(&TableRef::new("orders")).await },
			);
		client.wait_until_calls(2).await;
		client.release();
		second.await.expect("second task").expect("second resolve");
	}

	#[tokio::test]
	async fn descriptor_sync_waiter_observes_cancelled_owner() {
		let descriptor = default_descriptor_with_identity(0xD6, 1);
		let client = Arc::new(FakeDescriptorSyncClient::held([Ok(Some(descriptor))]));
		let catalog = Arc::new(catalog_with_fake_client(
			TenantId(7),
			Arc::new(WendelonDescriptorRegistry::default()),
			Arc::clone(&client),
			descriptor_sync_accountant(1),
		));

		let owner_catalog = Arc::clone(&catalog);
		let owner =
			tokio::spawn(
				async move { owner_catalog.resolve_table(&TableRef::new("orders")).await },
			);
		client.wait_until_started().await;

		let waiter_catalog = Arc::clone(&catalog);
		let waiter = tokio::spawn(async move {
			waiter_catalog
				.resolve_table(&TableRef::with_schema("default", "orders"))
				.await
		});
		sleep(Duration::from_millis(10)).await;
		owner.abort();
		let _ = owner.await;

		let result = timeout(Duration::from_secs(1), waiter)
			.await
			.expect("waiter completed")
			.expect("waiter task");
		let Err(error) = result else {
			panic!("waiter should observe cancelled owner");
		};
		match error {
			CatalogError::AdminFailed { message } => {
				assert!(message.contains("owner cancelled"));
			}
			other => panic!("unexpected waiter error: {other}"),
		}
	}

	#[tokio::test]
	async fn storage_scan_uses_synced_descriptor_after_bind_miss() {
		let descriptor = default_descriptor_with_identity(0xD7, 1);
		let registry = Arc::new(WendelonDescriptorRegistry::default());
		let client = Arc::new(FakeDescriptorSyncClient::new([Ok(Some(
			descriptor.clone(),
		))]));
		let catalog = catalog_with_fake_client(
			TenantId(7),
			Arc::clone(&registry),
			Arc::clone(&client),
			descriptor_sync_accountant(1),
		);

		let binding = catalog
			.resolve_table(&TableRef::new("orders"))
			.await
			.expect("resolve synced");
		let stored = registry
			.descriptor(TenantId(7), binding.legacy_table_id)
			.expect("storage-visible descriptor");

		assert_eq!(stored.table_object_id, descriptor.table_object_id);
		assert_eq!(
			stored.physical_descriptor_fingerprint,
			descriptor.physical_descriptor_fingerprint
		);
	}

	#[tokio::test]
	async fn sql_select_binds_wendelon_table_missing_from_startup_bootstrap() {
		let descriptor = default_descriptor_with_identity(0xD8, 1);
		let client = Arc::new(FakeDescriptorSyncClient::new([Ok(Some(
			descriptor.clone(),
		))]));
		let catalog = catalog_with_fake_client(
			TenantId(7),
			Arc::new(WendelonDescriptorRegistry::default()),
			Arc::clone(&client),
			descriptor_sync_accountant(1),
		);
		let plan = SqlPlanner::new()
			.plan("SELECT id, amount FROM orders")
			.expect("plan select");

		bind(&plan, &catalog, &PlannerConfig::default())
			.await
			.expect("bind select");

		assert_eq!(client.call_count(), 1);
	}

	#[tokio::test]
	async fn sql_insert_binds_wendelon_table_missing_from_startup_bootstrap() {
		let descriptor = default_descriptor_with_identity(0xD9, 1);
		let client = Arc::new(FakeDescriptorSyncClient::new([Ok(Some(descriptor))]));
		let catalog = catalog_with_fake_client(
			TenantId(7),
			Arc::new(WendelonDescriptorRegistry::default()),
			Arc::clone(&client),
			descriptor_sync_accountant(1),
		);
		let plan = SqlPlanner::new()
			.plan("INSERT INTO orders (id, amount, region) VALUES (1, 20, 'west')")
			.expect("plan insert");

		bind(&plan, &catalog, &PlannerConfig::default())
			.await
			.expect("bind insert");

		assert_eq!(client.call_count(), 1);
	}

	#[tokio::test]
	async fn create_if_not_exists_local_miss_does_not_descriptor_get() {
		let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
		let client = Arc::new(FakeDescriptorSyncClient::new([]));
		let catalog = catalog_with_fake_client(
			TenantId(7),
			Arc::new(WendelonDescriptorRegistry::default()),
			Arc::clone(&client),
			descriptor_sync_accountant(1),
		);

		let error = expect_catalog_error(
			catalog
				.create_table(&TableRef::new("orders"), schema, true)
				.await,
			"admin client missing",
		);

		assert!(matches!(error, CatalogError::AdminFailed { .. }));
		assert_eq!(client.call_count(), 0);
	}

	#[tokio::test]
	async fn wendelon_catalog_if_not_exists_recovers_compatible_remote_create_race() {
		let descriptor = default_descriptor_with_identity(0xDA, 1);
		let schema = Arc::new(descriptor.arrow_schema().expect("descriptor schema"));
		let admin = Arc::new(FakeSchemaUpdateClient::new([Err(
			ClientError::SchemaUpdateRejected {
				reason_code: 3,
				message: "table schema create cannot replace an existing table".to_string(),
			},
		)]));
		let descriptor_sync = Arc::new(FakeDescriptorSyncClient::new([Ok(Some(
			descriptor.clone(),
		))]));
		let catalog = WendelonCatalog {
			client: Some(admin.clone()),
			descriptor_sync_client: Some(descriptor_sync.clone()),
			descriptor_sync_flights: Some(Arc::new(DescriptorSyncFlights::new(
				descriptor_sync_accountant(1),
			))),
			descriptor_sync_metrics: Arc::new(DescriptorSyncMetrics::default()),
			descriptor_refresh_metrics: Arc::new(DescriptorRefreshMetrics::default()),
			tenant_id: TenantId(7),
			registry: Arc::new(WendelonDescriptorRegistry::default()),
		};

		let outcome = catalog
			.create_table_with_outcome(
				&TableRef::new("orders"),
				schema.clone(),
				CreateTableRowIdentity::PrimaryKey {
					column: "id".to_string(),
				},
				true,
			)
			.await
			.expect("compatible remote create race should resolve existing descriptor");

		assert!(!outcome.created);
		assert_eq!(outcome.binding.schema.as_ref(), schema.as_ref());
		assert_eq!(outcome.binding.table, descriptor.table_object_id);
		assert_eq!(admin.call_count(), 1);
		assert_eq!(descriptor_sync.call_count(), 1);
	}

	#[tokio::test]
	async fn registry_sync_descriptor_for_tenant_rejects_name_mismatch() {
		let registry = WendelonDescriptorRegistry::default();

		let error = expect_catalog_error(
			registry.sync_descriptor_for_tenant(
				TenantId(7),
				&TableRef::with_schema("bench", "invoices"),
				descriptor(),
			),
			"name mismatch",
		);

		assert!(matches!(error, CatalogError::AdminFailed { .. }));
	}

	#[tokio::test]
	async fn registry_sync_descriptor_for_tenant_allocates_fresh_local_table_id() {
		let descriptor = descriptor();
		let registry = WendelonDescriptorRegistry::from_descriptors_for_tenant(
			TenantId(7),
			[(
				TableId(7),
				named_descriptor_with_identity(0xA1, 1, "bench", "customers"),
			)],
		)
		.expect("registry");

		let resolved = registry
			.sync_descriptor_for_tenant(
				TenantId(7),
				&TableRef::with_schema("bench", "orders"),
				descriptor.clone(),
			)
			.expect("sync descriptor");

		assert_ne!(resolved.legacy_table_id, TableId(7));
		assert_eq!(resolved.legacy_table_id, TableId(8));
		assert_eq!(resolved.table, descriptor.table_object_id);
	}

	#[tokio::test]
	async fn registry_sync_descriptor_for_tenant_preserves_physical_fingerprint() {
		let descriptor = descriptor();
		let registry = WendelonDescriptorRegistry::default();

		let resolved = registry
			.sync_descriptor_for_tenant(
				TenantId(7),
				&TableRef::with_schema("bench", "orders"),
				descriptor.clone(),
			)
			.expect("sync descriptor");

		assert_eq!(resolved.table, descriptor.table_object_id);
		assert_eq!(
			resolved.fingerprint,
			descriptor.physical_descriptor_fingerprint
		);
	}

	#[tokio::test]
	async fn registry_sync_descriptor_for_tenant_returns_existing_same_identity() {
		let descriptor = descriptor();
		let registry = WendelonDescriptorRegistry::from_descriptors_for_tenant(
			TenantId(7),
			[(TableId(9), descriptor.clone())],
		)
		.expect("registry");

		let resolved = registry
			.sync_descriptor_for_tenant(
				TenantId(7),
				&TableRef::with_schema("bench", "orders"),
				descriptor,
			)
			.expect("same identity");

		assert_eq!(resolved.legacy_table_id, TableId(9));
	}

	#[tokio::test]
	async fn facts_refresh_updates_binding_without_descriptor_identity_change() {
		let descriptor = descriptor();
		let registry = WendelonDescriptorRegistry::from_descriptors_for_tenant_with_facts(
			TenantId(7),
			[(
				TableId(9),
				descriptor.clone(),
				Some(descriptor_optimizer_facts(
					&descriptor,
					Some(8_192),
					Some(1_000),
				)),
			)],
		)
		.expect("registry");

		let refreshed = registry
			.sync_descriptor_for_tenant_with_facts(
				TenantId(7),
				&TableRef::with_schema("bench", "orders"),
				descriptor.clone(),
				Some(descriptor_optimizer_facts(
					&descriptor,
					Some(1_024),
					Some(1_000),
				)),
			)
			.expect("refresh facts");

		assert_eq!(refreshed.legacy_table_id, TableId(9));
		assert_eq!(
			refreshed.optimizer_facts.byte_upper_bound,
			Some(ByteCount::new(1_024))
		);
		assert_eq!(
			registry
				.resolve_table(TenantId(7), &TableRef::with_schema("bench", "orders"))
				.expect("resolve refreshed facts")
				.optimizer_facts
				.byte_upper_bound,
			Some(ByteCount::new(1_024))
		);
	}

	#[tokio::test]
	async fn registry_sync_descriptor_for_tenant_rejects_existing_conflict() {
		let registry = WendelonDescriptorRegistry::from_descriptors_for_tenant(
			TenantId(7),
			[(TableId(9), descriptor_with_identity(0xCA, 3))],
		)
		.expect("registry");

		let error = expect_catalog_error(
			registry.sync_descriptor_for_tenant(
				TenantId(7),
				&TableRef::with_schema("bench", "orders"),
				descriptor_with_identity(0xCB, 3),
			),
			"conflict",
		);

		assert!(matches!(error, CatalogError::AdminFailed { .. }));
	}

	#[tokio::test]
	async fn registry_invalidate_descriptor_for_tenant_removes_all_aliases_and_payload() {
		let descriptor = descriptor();
		let registry = WendelonDescriptorRegistry::from_descriptors_for_tenant(
			TenantId(7),
			[(TableId(9), descriptor.clone())],
		)
		.expect("registry");

		let invalidated = registry
			.invalidate_descriptor_for_tenant(TenantId(7), TableId(9))
			.expect("invalidated descriptor");

		assert_eq!(invalidated.table_id, TableId(9));
		assert_eq!(invalidated.table_object_id, descriptor.table_object_id);
		assert_eq!(
			invalidated.fingerprint,
			descriptor.physical_descriptor_fingerprint
		);
		assert!(invalidated.refs.contains(&TableRef::new("orders")));
		assert!(
			invalidated
				.refs
				.contains(&TableRef::with_schema("bench", "orders"))
		);
		assert!(
			registry
				.resolve_table(TenantId(7), &TableRef::new("orders"))
				.is_err()
		);
		assert!(registry.resolve_table_id(TenantId(7), TableId(9)).is_err());
		assert!(registry.descriptor(TenantId(7), TableId(9)).is_err());
		assert!(
			registry
				.invalidate_descriptor_for_tenant(TenantId(7), TableId(9))
				.is_none()
		);
	}

	#[tokio::test]
	async fn registry_invalidate_descriptor_for_tenant_is_tenant_scoped() {
		let tenant_7_descriptor = descriptor_with_identity(0xA7, 7);
		let tenant_8_descriptor = descriptor_with_identity(0xA8, 8);
		let registry = WendelonDescriptorRegistry::default();
		registry
			.register_descriptor_for_tenant(TenantId(7), TableId(1), tenant_7_descriptor)
			.expect("tenant 7 descriptor");
		registry
			.register_descriptor_for_tenant(TenantId(8), TableId(1), tenant_8_descriptor.clone())
			.expect("tenant 8 descriptor");

		let invalidated = registry.invalidate_descriptor_for_tenant(TenantId(7), TableId(1));

		assert!(invalidated.is_some());
		assert!(
			registry
				.resolve_table(TenantId(7), &TableRef::new("orders"))
				.is_err()
		);
		assert_eq!(
			registry
				.resolve_table(TenantId(8), &TableRef::new("orders"))
				.expect("tenant 8 still present")
				.table,
			tenant_8_descriptor.table_object_id
		);
	}

	#[tokio::test]
	async fn descriptor_sync_is_tenant_scoped_across_same_table_name() {
		let tenant_7_descriptor = default_descriptor_with_identity(0xE7, 1);
		let tenant_8_descriptor = default_descriptor_with_identity(0xE8, 1);
		let registry = Arc::new(WendelonDescriptorRegistry::default());
		let tenant_7_client = Arc::new(FakeDescriptorSyncClient::new([Ok(Some(
			tenant_7_descriptor.clone(),
		))]));
		let tenant_8_client = Arc::new(FakeDescriptorSyncClient::new([Ok(Some(
			tenant_8_descriptor.clone(),
		))]));
		let tenant_7 = catalog_with_fake_client(
			TenantId(7),
			Arc::clone(&registry),
			Arc::clone(&tenant_7_client),
			descriptor_sync_accountant(1),
		);
		let tenant_8 = catalog_with_fake_client(
			TenantId(8),
			registry,
			Arc::clone(&tenant_8_client),
			descriptor_sync_accountant(1),
		);

		let resolved_7 = tenant_7
			.resolve_table(&TableRef::new("orders"))
			.await
			.expect("tenant 7");
		let resolved_8 = tenant_8
			.resolve_table(&TableRef::new("orders"))
			.await
			.expect("tenant 8");

		assert_eq!(resolved_7.table, tenant_7_descriptor.table_object_id);
		assert_eq!(resolved_8.table, tenant_8_descriptor.table_object_id);
		assert_ne!(resolved_7.table, resolved_8.table);
	}

	#[tokio::test]
	async fn wendelon_catalog_scoped_views_resolve_same_table_name_per_tenant() {
		let tenant_7_descriptor = descriptor_with_identity(0xA7, 7);
		let tenant_8_descriptor = descriptor_with_identity(0xA8, 8);
		let registry = Arc::new(WendelonDescriptorRegistry::default());
		registry
			.register_descriptor_for_tenant(TenantId(7), TableId(1), tenant_7_descriptor.clone())
			.expect("tenant 7 descriptor");
		registry
			.register_descriptor_for_tenant(TenantId(8), TableId(1), tenant_8_descriptor.clone())
			.expect("tenant 8 descriptor");
		let catalog = Arc::new(WendelonCatalog {
			client: None,
			descriptor_sync_client: None,
			descriptor_sync_flights: None,
			descriptor_sync_metrics: Arc::new(DescriptorSyncMetrics::default()),
			descriptor_refresh_metrics: Arc::new(DescriptorRefreshMetrics::default()),
			tenant_id: TenantId(1),
			registry,
		});

		let tenant_7 = Arc::clone(&catalog).scoped(TenantId(7));
		let tenant_8 = Arc::clone(&catalog).scoped(TenantId(8));
		let table = TableRef::with_schema("bench", "orders");
		let resolved_7 = tenant_7.resolve_table(&table).await.expect("tenant 7");
		let resolved_8 = tenant_8.resolve_table(&table).await.expect("tenant 8");

		assert_eq!(resolved_7.table, tenant_7_descriptor.table_object_id);
		assert_eq!(resolved_8.table, tenant_8_descriptor.table_object_id);
		assert_ne!(resolved_7.table, resolved_8.table);
	}

	#[tokio::test]
	async fn wendelon_registry_descriptor_lookup_uses_request_tenant() {
		let tenant_7_descriptor = descriptor_with_identity(0xB7, 7);
		let tenant_8_descriptor = descriptor_with_identity(0xB8, 8);
		let registry = WendelonDescriptorRegistry::default();
		registry
			.register_descriptor_for_tenant(TenantId(7), TableId(1), tenant_7_descriptor.clone())
			.expect("tenant 7 descriptor");
		registry
			.register_descriptor_for_tenant(TenantId(8), TableId(1), tenant_8_descriptor.clone())
			.expect("tenant 8 descriptor");

		assert_eq!(
			registry
				.descriptor(TenantId(7), TableId(1))
				.expect("tenant 7 descriptor")
				.table_object_id,
			tenant_7_descriptor.table_object_id
		);
		assert_eq!(
			registry
				.descriptor(TenantId(8), TableId(1))
				.expect("tenant 8 descriptor")
				.table_object_id,
			tenant_8_descriptor.table_object_id
		);
		assert!(registry.descriptor(TenantId(9), TableId(1)).is_err());
	}

	fn descriptor() -> TableSchemaDescriptor {
		descriptor_with_identity(0xCA, 3)
	}

	fn logical_descriptor(logical_type_id: u32) -> TableSchemaDescriptor {
		let schema = Schema::new(vec![
			field_with_logical_type_metadata(
				"id",
				DataType::Int64,
				false,
				crate::ir::LogicalTypeId(logical_type_id),
			),
			Field::new("amount", DataType::Int32, true),
		]);

		TableSchemaDescriptor::new(
			TableObjectId::new([0xCC; 16]),
			3,
			"bench",
			"orders",
			&schema,
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("id")),
			LocalityPolicy::default_table_spread(),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.expect("descriptor")
	}

	fn default_descriptor_with_identity(id_byte: u8, generation: u64) -> TableSchemaDescriptor {
		named_descriptor_with_identity(id_byte, generation, "default", "orders")
	}

	fn descriptor_with_identity(id_byte: u8, generation: u64) -> TableSchemaDescriptor {
		named_descriptor_with_identity(id_byte, generation, "bench", "orders")
	}

	fn descriptor_optimizer_facts(
		descriptor: &TableSchemaDescriptor,
		materialized_bytes: Option<u64>,
		rows: Option<u64>,
	) -> DescriptorOptimizerFacts {
		DescriptorOptimizerFacts {
			schema_generation: descriptor.schema_generation,
			physical_descriptor_fingerprint: descriptor.physical_descriptor_fingerprint,
			manifest_epoch: 11,
			materialized_byte_upper_bound: materialized_bytes.map(ByteCount::new),
			row_upper_bound: rows.map(RowCount::new),
			column_ndv: Vec::new(),
			column_range: Vec::new(),
		}
	}

	fn named_descriptor_with_identity(
		id_byte: u8,
		generation: u64,
		namespace: &str,
		table_name: &str,
	) -> TableSchemaDescriptor {
		let schema = Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("amount", DataType::Int32, true),
			Field::new("region", DataType::Utf8, true),
		]);

		TableSchemaDescriptor::new(
			TableObjectId::new([id_byte; 16]),
			generation,
			namespace,
			table_name,
			&schema,
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("id")),
			LocalityPolicy::default_table_spread(),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.expect("descriptor")
	}
}
