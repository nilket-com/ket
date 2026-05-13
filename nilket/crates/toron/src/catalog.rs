use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::datatypes::{DataType, Schema, SchemaRef};
use serde::{Deserialize, Serialize};

use crate::error::CatalogError;
use crate::ir::{ColumnOrdinal, TableId};
use wendelon::schema::{PhysicalDescriptorFingerprint, TableObjectId};

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
	pub fingerprint: PhysicalDescriptorFingerprint,
	pub schema: SchemaRef,
	pub pbc_eligible_columns: HashSet<ColumnOrdinal>,
	pub primary_key_columns: HashSet<ColumnOrdinal>,
}

impl TableBinding {
	pub fn from_descriptor(
		legacy_table_id: TableId,
		descriptor: &wendelon_client::TableSchemaDescriptorV1,
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
		Self::new_with_primary_key_columns(
			legacy_table_id,
			table,
			fingerprint,
			schema,
			HashSet::new(),
		)
	}

	pub fn new_with_primary_key_columns(
		legacy_table_id: TableId,
		table: TableObjectId,
		fingerprint: PhysicalDescriptorFingerprint,
		schema: SchemaRef,
		primary_key_columns: HashSet<ColumnOrdinal>,
	) -> Self {
		let pbc_eligible_columns = pbc_eligible_columns(&schema);
		Self {
			legacy_table_id,
			table,
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
}

#[derive(Default)]
pub struct MockCatalog {
	by_ref: HashMap<TableRef, TableId>,
	by_id: HashMap<TableId, TableBinding>,
}

impl MockCatalog {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn register_descriptor(
		&mut self,
		legacy_table_id: TableId,
		descriptor: wendelon_client::TableSchemaDescriptorV1,
	) -> Result<(), CatalogError> {
		let table_ref = TableRef::with_schema(&descriptor.namespace, &descriptor.table_name);
		let unqualified_ref = TableRef::new(&descriptor.table_name);
		let binding = TableBinding::from_descriptor(legacy_table_id, &descriptor)?;

		self.by_ref.insert(table_ref, legacy_table_id);
		self.by_ref.insert(unqualified_ref, legacy_table_id);
		self.by_id.insert(legacy_table_id, binding);
		Ok(())
	}

	pub fn register_table(
		&mut self,
		table_ref: TableRef,
		binding: TableBinding,
	) -> Result<(), CatalogError> {
		let legacy_table_id = binding.legacy_table_id;
		self.by_ref.insert(table_ref, legacy_table_id);
		self.by_id.insert(legacy_table_id, binding);
		Ok(())
	}
}

#[async_trait::async_trait]
impl Catalog for MockCatalog {
	async fn resolve_table(&self, table: &TableRef) -> Result<TableBinding, CatalogError> {
		let table_id = self
			.by_ref
			.get(table)
			.ok_or_else(|| CatalogError::TableNotFound {
				table: table_name(table),
			})?;
		self.resolve_table_id(*table_id).await
	}

	async fn resolve_table_id(&self, table: TableId) -> Result<TableBinding, CatalogError> {
		self.by_id
			.get(&table)
			.cloned()
			.ok_or_else(|| CatalogError::TableNotFound {
				table: format!("{table:?}"),
			})
	}
}

#[derive(Default)]
pub struct WendelonCatalog {
	by_ref: HashMap<TableRef, TableId>,
	by_id: HashMap<TableId, TableBinding>,
}

impl WendelonCatalog {
	pub fn from_descriptors(
		descriptors: impl IntoIterator<Item = (TableId, wendelon_client::TableSchemaDescriptorV1)>,
	) -> Result<Self, CatalogError> {
		let mut catalog = Self::default();
		for (table_id, descriptor) in descriptors {
			let table_ref = TableRef::with_schema(&descriptor.namespace, &descriptor.table_name);
			let unqualified_ref = TableRef::new(&descriptor.table_name);
			let binding = TableBinding::from_descriptor(table_id, &descriptor)?;

			catalog.by_ref.insert(table_ref, table_id);
			catalog.by_ref.insert(unqualified_ref, table_id);
			catalog.by_id.insert(table_id, binding);
		}
		Ok(catalog)
	}
}

#[async_trait::async_trait]
impl Catalog for WendelonCatalog {
	async fn resolve_table(&self, table: &TableRef) -> Result<TableBinding, CatalogError> {
		let table_id = self
			.by_ref
			.get(table)
			.ok_or_else(|| CatalogError::TableNotFound {
				table: table_name(table),
			})?;
		self.resolve_table_id(*table_id).await
	}

	async fn resolve_table_id(&self, table: TableId) -> Result<TableBinding, CatalogError> {
		self.by_id
			.get(&table)
			.cloned()
			.ok_or_else(|| CatalogError::TableNotFound {
				table: format!("{table:?}"),
			})
	}
}

fn table_name(table: &TableRef) -> String {
	match &table.schema {
		Some(schema) => format!("{schema}.{}", table.table),
		None => table.table.clone(),
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

fn primary_key_columns(
	schema: &Schema,
	descriptor: &wendelon_client::TableSchemaDescriptorV1,
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
		LocalityPolicyV1, PrimaryKeyDescriptorV1, RowIdentityPolicy, SchemaEvolutionPolicyV1,
		TableObjectId, TableSchemaDescriptorV1,
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

	fn descriptor() -> TableSchemaDescriptorV1 {
		let schema = Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("amount", DataType::Int32, true),
			Field::new("region", DataType::Utf8, true),
		]);

		TableSchemaDescriptorV1::new(
			TableObjectId::new([0xCA; 16]),
			3,
			"bench",
			"orders",
			&schema,
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("id")),
			LocalityPolicyV1::default_table_spread(),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.expect("descriptor")
	}
}
