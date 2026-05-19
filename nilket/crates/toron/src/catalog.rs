use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::RwLock;

use arrow::datatypes::{DataType, Schema, SchemaRef};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::{CatalogError, StorageError};
use crate::ir::{ColumnOrdinal, TableId};
use wendelon::integration::compute::SchemaIntentId;
use wendelon::schema::{
	LocalityPolicy, MultiRowLayoutDescriptor, PhysicalDescriptorFingerprint, RecordLayoutPolicy,
	RowIdentityPolicy, SchemaEvolutionPolicyV1, TableObjectId, TableSchemaDescriptor, TenantId,
};
use wendelon_client::{
	Client, ClientError, DropOperationId, SchemaGeneration, TableSchemaUpdateRequest,
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

#[derive(Clone)]
pub struct TableBinding {
	pub legacy_table_id: TableId,
	pub table: TableObjectId,
	pub schema_generation: u64,
	pub fingerprint: PhysicalDescriptorFingerprint,
	pub schema: SchemaRef,
	pub pbc_eligible_columns: HashSet<ColumnOrdinal>,
	pub primary_key_columns: HashSet<ColumnOrdinal>,
}

impl TableBinding {
	pub fn from_descriptor(
		legacy_table_id: TableId,
		descriptor: &wendelon_client::TableSchemaDescriptor,
	) -> Result<Self, CatalogError> {
		let schema = Arc::new(descriptor.arrow_schema().map_err(|error| {
			CatalogError::FingerprintResolution {
				message: error.to_string(),
			}
		})?);

		let primary_key_columns = primary_key_columns(&schema, descriptor)?;

		Ok(Self::new_with_primary_key_columns(
			legacy_table_id,
			descriptor.table_object_id,
			descriptor.schema_generation,
			descriptor.physical_descriptor_fingerprint,
			schema,
			primary_key_columns,
		))
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
		let pbc_eligible_columns = pbc_eligible_columns(&schema);
		Self {
			legacy_table_id,
			table,
			schema_generation,
			fingerprint,
			schema,
			pbc_eligible_columns,
			primary_key_columns,
		}
	}

	pub fn column_is_pbc_eligible(&self, ordinal: ColumnOrdinal) -> bool {
		self.pbc_eligible_columns.contains(&ordinal)
	}
}

#[async_trait::async_trait]
pub trait Catalog: Send + Sync {
	async fn resolve_table(&self, table: &TableRef) -> Result<TableBinding, CatalogError>;

	async fn resolve_table_id(&self, table: TableId) -> Result<TableBinding, CatalogError>;

	async fn create_table(
		&self,
		name: &TableRef,
		schema: SchemaRef,
		if_not_exists: bool,
	) -> Result<TableBinding, CatalogError>;

	async fn drop_table(
		&self,
		name: &TableRef,
		if_exists: bool,
	) -> Result<Option<TableBinding>, CatalogError>;
}

#[derive(Default)]
pub struct MockCatalog {
	inner: RwLock<MockCatalogInner>,
}

struct MockCatalogInner {
	by_ref: HashMap<TableRef, TableId>,
	by_id: HashMap<TableId, TableBinding>,
	next_table_id: u64,
}

impl Default for MockCatalogInner {
	fn default() -> Self {
		Self {
			by_ref: HashMap::new(),
			by_id: HashMap::new(),
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
		let table_ref = TableRef::with_schema(&descriptor.namespace, &descriptor.table_name);
		let unqualified_ref = TableRef::new(&descriptor.table_name);
		let binding = TableBinding::from_descriptor(legacy_table_id, &descriptor)?;
		let inner = self.inner.get_mut().expect("MockCatalog lock poisoned");

		inner.by_ref.insert(table_ref, legacy_table_id);
		inner.by_ref.insert(unqualified_ref, legacy_table_id);
		inner.by_id.insert(legacy_table_id, binding);
		bump_next_table_id(inner, legacy_table_id);
		Ok(())
	}

	pub fn register_table(
		&mut self,
		table_ref: TableRef,
		binding: TableBinding,
	) -> Result<(), CatalogError> {
		let legacy_table_id = binding.legacy_table_id;
		let inner = self.inner.get_mut().expect("MockCatalog lock poisoned");

		inner.by_ref.insert(table_ref, legacy_table_id);
		inner.by_id.insert(legacy_table_id, binding);
		bump_next_table_id(inner, legacy_table_id);
		Ok(())
	}
}

#[async_trait::async_trait]
impl Catalog for MockCatalog {
	async fn resolve_table(&self, table: &TableRef) -> Result<TableBinding, CatalogError> {
		let inner = self.inner.read().expect("MockCatalog lock poisoned");
		let table_id =
			inner
				.by_ref
				.get(table)
				.copied()
				.ok_or_else(|| CatalogError::TableNotFound {
					table: table_name(table),
				})?;
		inner
			.by_id
			.get(&table_id)
			.cloned()
			.ok_or_else(|| CatalogError::TableNotFound {
				table: table_name(table),
			})
	}

	async fn resolve_table_id(&self, table: TableId) -> Result<TableBinding, CatalogError> {
		let inner = self.inner.read().expect("MockCatalog lock poisoned");
		inner
			.by_id
			.get(&table)
			.cloned()
			.ok_or_else(|| CatalogError::TableNotFound {
				table: format!("{table:?}"),
			})
	}

	async fn create_table(
		&self,
		name: &TableRef,
		schema: SchemaRef,
		if_not_exists: bool,
	) -> Result<TableBinding, CatalogError> {
		let mut inner = self.inner.write().expect("MockCatalog lock poisoned");

		if let Some(existing_id) = inner.by_ref.get(name).copied() {
			let existing = inner
				.by_id
				.get(&existing_id)
				.expect("MockCatalog by_ref/by_id consistency invariant")
				.clone();
			if if_not_exists {
				if existing.schema.as_ref() == schema.as_ref() {
					return Ok(existing);
				}
				return Err(CatalogError::SchemaMismatch {
					table: table_name(name),
				});
			}
			return Err(CatalogError::TableAlreadyExists {
				table: table_name(name),
			});
		}

		while inner.by_id.contains_key(&TableId(inner.next_table_id)) {
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
			mock_table_identity(name, schema.as_ref(), legacy_table_id)?;
		let binding = TableBinding::new(
			legacy_table_id,
			table_object_id,
			fingerprint,
			schema.clone(),
		);

		inner.by_ref.insert(name.clone(), legacy_table_id);
		if name.schema.is_some() {
			inner
				.by_ref
				.entry(TableRef::new(name.table.clone()))
				.or_insert(legacy_table_id);
		}
		inner.by_id.insert(legacy_table_id, binding.clone());

		Ok(binding)
	}

	async fn drop_table(
		&self,
		name: &TableRef,
		if_exists: bool,
	) -> Result<Option<TableBinding>, CatalogError> {
		let mut inner = self.inner.write().expect("MockCatalog lock poisoned");

		let Some(table_id) = inner.by_ref.get(name).copied() else {
			if if_exists {
				return Ok(None);
			}
			return Err(CatalogError::TableNotFound {
				table: table_name(name),
			});
		};

		let binding = inner
			.by_id
			.remove(&table_id)
			.ok_or_else(|| CatalogError::TableNotFound {
				table: table_name(name),
			})?;
		inner.by_ref.retain(|_, id| *id != table_id);
		Ok(Some(binding))
	}
}

pub struct WendelonCatalog {
	client: Option<Arc<Client>>,
	tenant_id: TenantId,
	registry: Arc<WendelonDescriptorRegistry>,
}

impl Default for WendelonCatalog {
	fn default() -> Self {
		Self {
			client: None,
			tenant_id: TenantId::new(1),
			registry: Arc::new(WendelonDescriptorRegistry::default()),
		}
	}
}

#[derive(Default)]
pub struct WendelonDescriptorRegistry {
	inner: RwLock<WendelonDescriptorState>,
}

struct WendelonDescriptorState {
	by_ref: HashMap<TableRef, TableId>,
	by_id: HashMap<TableId, TableBinding>,
	descriptors: HashMap<TableId, wendelon_client::TableSchemaDescriptor>,
	finalized_drops: HashMap<TableId, DropOperationId>,
	next_table_id: u64,
}

impl Default for WendelonDescriptorState {
	fn default() -> Self {
		Self {
			by_ref: HashMap::new(),
			by_id: HashMap::new(),
			descriptors: HashMap::new(),
			finalized_drops: HashMap::new(),
			next_table_id: 1,
		}
	}
}

impl WendelonDescriptorRegistry {
	pub fn from_descriptors(
		descriptors: impl IntoIterator<Item = (TableId, wendelon_client::TableSchemaDescriptor)>,
	) -> Result<Self, CatalogError> {
		let registry = Self::default();
		for (table_id, descriptor) in descriptors {
			let table_ref = TableRef::with_schema(&descriptor.namespace, &descriptor.table_name);
			let unqualified_ref = TableRef::new(&descriptor.table_name);
			let binding = TableBinding::from_descriptor(table_id, &descriptor)?;
			let mut state = registry
				.inner
				.write()
				.expect("WendelonDescriptorRegistry lock poisoned");
			insert_wendelon_descriptor_locked(
				&mut state,
				table_ref,
				Some(unqualified_ref),
				table_id,
				binding,
				descriptor,
			)?;
		}
		Ok(registry)
	}

	pub fn resolve_table(&self, table: &TableRef) -> Result<TableBinding, CatalogError> {
		let state = self
			.inner
			.read()
			.expect("WendelonDescriptorRegistry lock poisoned");
		let table_id = state
			.by_ref
			.get(table)
			.ok_or_else(|| CatalogError::TableNotFound {
				table: table_name(table),
			})?;
		state
			.by_id
			.get(table_id)
			.cloned()
			.ok_or_else(|| CatalogError::TableNotFound {
				table: table_name(table),
			})
	}

	pub fn resolve_table_id(&self, table: TableId) -> Result<TableBinding, CatalogError> {
		let state = self
			.inner
			.read()
			.expect("WendelonDescriptorRegistry lock poisoned");
		state
			.by_id
			.get(&table)
			.cloned()
			.ok_or_else(|| CatalogError::TableNotFound {
				table: format!("{table:?}"),
			})
	}

	pub fn descriptor(
		&self,
		table: TableId,
	) -> Result<wendelon_client::TableSchemaDescriptor, StorageError> {
		let state = self
			.inner
			.read()
			.expect("WendelonDescriptorRegistry lock poisoned");
		state
			.descriptors
			.get(&table)
			.cloned()
			.ok_or(StorageError::TableNotFound { table })
	}

	fn try_resolve_table(&self, table: &TableRef) -> Option<TableBinding> {
		let state = self
			.inner
			.read()
			.expect("WendelonDescriptorRegistry lock poisoned");
		let table_id = state.by_ref.get(table)?;
		state.by_id.get(table_id).cloned()
	}

	fn allocate_table_id(&self) -> TableId {
		let mut state = self
			.inner
			.write()
			.expect("WendelonDescriptorRegistry lock poisoned");
		while state.by_id.contains_key(&TableId(state.next_table_id)) {
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
		name: TableRef,
		binding: TableBinding,
		descriptor: wendelon_client::TableSchemaDescriptor,
	) -> Result<(), CatalogError> {
		let mut state = self
			.inner
			.write()
			.expect("WendelonDescriptorRegistry lock poisoned");
		if state.by_ref.contains_key(&name) {
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
			name,
			unqualified_ref,
			binding.legacy_table_id,
			binding,
			descriptor,
		)
	}

	pub fn mark_finalized_drop(
		&self,
		table: TableId,
		operation_id: DropOperationId,
	) -> Result<(), CatalogError> {
		let mut state = self
			.inner
			.write()
			.expect("WendelonDescriptorRegistry lock poisoned");
		if !state.by_id.contains_key(&table) {
			return Err(CatalogError::TableNotFound {
				table: format!("{table:?}"),
			});
		}
		state.finalized_drops.insert(table, operation_id);
		Ok(())
	}

	pub fn consume_finalized_drop(
		&self,
		table: TableId,
		operation_id: DropOperationId,
	) -> Result<(), CatalogError> {
		let mut state = self
			.inner
			.write()
			.expect("WendelonDescriptorRegistry lock poisoned");
		match state.finalized_drops.get(&table).copied() {
			Some(existing) if existing == operation_id => {
				state.finalized_drops.remove(&table);
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
		let mut state = self
			.inner
			.write()
			.expect("WendelonDescriptorRegistry lock poisoned");
		let binding = state
			.by_id
			.remove(&table)
			.ok_or_else(|| CatalogError::TableNotFound {
				table: format!("{table:?}"),
			})?;
		state.by_ref.retain(|_, table_id| *table_id != table);
		state.descriptors.remove(&table);
		state.finalized_drops.remove(&table);
		Ok(binding)
	}
}

fn insert_wendelon_descriptor_locked(
	state: &mut WendelonDescriptorState,
	table_ref: TableRef,
	unqualified_ref: Option<TableRef>,
	table_id: TableId,
	binding: TableBinding,
	descriptor: wendelon_client::TableSchemaDescriptor,
) -> Result<(), CatalogError> {
	if state.by_ref.contains_key(&table_ref) || state.by_id.contains_key(&table_id) {
		return Err(CatalogError::TableAlreadyExists {
			table: table_name(&table_ref),
		});
	}

	state.by_ref.insert(table_ref, table_id);
	if let Some(unqualified_ref) = unqualified_ref {
		state.by_ref.entry(unqualified_ref).or_insert(table_id);
	}
	state.by_id.insert(table_id, binding);
	state.descriptors.insert(table_id, descriptor);
	bump_next_wendelon_table_id(state, table_id);
	Ok(())
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

	pub fn with_client(
		client: Arc<Client>,
		tenant_id: TenantId,
		descriptors: impl IntoIterator<Item = (TableId, wendelon_client::TableSchemaDescriptor)>,
	) -> Result<Self, CatalogError> {
		Ok(Self {
			client: Some(client),
			tenant_id,
			registry: Arc::new(WendelonDescriptorRegistry::from_descriptors(descriptors)?),
		})
	}

	pub fn with_client_and_registry(
		client: Arc<Client>,
		tenant_id: TenantId,
		registry: Arc<WendelonDescriptorRegistry>,
	) -> Self {
		Self {
			client: Some(client),
			tenant_id,
			registry,
		}
	}

	pub fn descriptor_registry(&self) -> Arc<WendelonDescriptorRegistry> {
		self.registry.clone()
	}
}

#[async_trait::async_trait]
impl Catalog for WendelonCatalog {
	async fn resolve_table(&self, table: &TableRef) -> Result<TableBinding, CatalogError> {
		self.registry.resolve_table(table)
	}

	async fn resolve_table_id(&self, table: TableId) -> Result<TableBinding, CatalogError> {
		self.registry.resolve_table_id(table)
	}

	async fn create_table(
		&self,
		name: &TableRef,
		schema: SchemaRef,
		if_not_exists: bool,
	) -> Result<TableBinding, CatalogError> {
		if let Some(existing) = self.registry.try_resolve_table(name) {
			if if_not_exists {
				if existing.schema.as_ref() == schema.as_ref() {
					return Ok(existing);
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
		let descriptor = TableSchemaDescriptor::new_with_layout(
			table_object_id,
			1,
			name.schema.as_deref().unwrap_or("default"),
			&name.table,
			schema.as_ref(),
			RowIdentityPolicy::AppendOnly,
			LocalityPolicy::default_table_spread(),
			RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::default()),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.map_err(|error| CatalogError::FingerprintResolution {
			message: error.to_string(),
		})?;
		let intent_id = SchemaIntentId::new(*Uuid::new_v4().as_bytes());
		let request =
			TableSchemaUpdateRequest::create(self.tenant_id, descriptor.clone(), intent_id);

		client
			.submit_table_schema_update(request)
			.await
			.map_err(|error| map_wendelon_admin_error(name, error))?;

		let binding = TableBinding::from_descriptor(legacy_table_id, &descriptor)?;
		self.registry
			.insert_descriptor(name.clone(), binding.clone(), descriptor)?;
		Ok(binding)
	}

	async fn drop_table(
		&self,
		name: &TableRef,
		if_exists: bool,
	) -> Result<Option<TableBinding>, CatalogError> {
		let binding = match self.registry.try_resolve_table(name) {
			Some(binding) => binding,
			None if if_exists => return Ok(None),
			None => {
				return Err(CatalogError::TableNotFound {
					table: table_name(name),
				});
			}
		};
		let operation_id = DropOperationId {
			tenant_id: self.tenant_id,
			table_object_id: binding.table,
			schema_generation: SchemaGeneration::new(binding.schema_generation),
		};
		self.registry
			.consume_finalized_drop(binding.legacy_table_id, operation_id)?;
		self.registry.remove_table(binding.legacy_table_id)?;
		Ok(Some(binding))
	}
}

fn table_name(table: &TableRef) -> String {
	match &table.schema {
		Some(schema) => format!("{schema}.{}", table.table),
		None => table.table.clone(),
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
	legacy_table_id: TableId,
) -> Result<(TableObjectId, PhysicalDescriptorFingerprint), CatalogError> {
	let mut object_id = [0_u8; 16];
	object_id[8..].copy_from_slice(&legacy_table_id.0.to_be_bytes());
	let table_object_id = TableObjectId::new(object_id);

	let descriptor = TableSchemaDescriptor::new_with_layout(
		table_object_id,
		1,
		name.schema.as_deref().unwrap_or("default"),
		&name.table,
		schema,
		RowIdentityPolicy::AppendOnly,
		LocalityPolicy::default_table_spread(),
		RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::default()),
		SchemaEvolutionPolicyV1::AdditiveOnly,
	)
	.map_err(|error| CatalogError::FingerprintResolution {
		message: error.to_string(),
	})?;

	Ok((table_object_id, descriptor.physical_descriptor_fingerprint))
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
	use arrow::datatypes::{Field, Schema};
	use wendelon::schema::{
		LocalityPolicy, PrimaryKeyDescriptorV1, RowIdentityPolicy, SchemaEvolutionPolicyV1,
		TableObjectId, TableSchemaDescriptor,
	};

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

	fn descriptor() -> TableSchemaDescriptor {
		let schema = Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("amount", DataType::Int32, true),
			Field::new("region", DataType::Utf8, true),
		]);

		TableSchemaDescriptor::new(
			TableObjectId::new([0xCA; 16]),
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
}
