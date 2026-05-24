#![allow(unused_imports)]

use super::super::support::*;
use super::super::{
	core_invariants::*, distributed_execution::*, network_tls::*, observability_audit::*,
	observability_telemetry::*, predicate_pushdown::*, ring_placement::*, sql_planner_surface::*,
};
use super::{
	ddl_admin::*, descriptor_invalidation::*, dml_delete_update::*, returning::*, row_identity::*,
};

#[test]
pub(crate) fn adr029_catalog_create_table_returns_binding() {
	let catalog = std::fs::read_to_string(manifest_dir().join("src/catalog.rs"))
		.expect("read catalog source");
	let error =
		std::fs::read_to_string(manifest_dir().join("src/error.rs")).expect("read error source");

	assert!(catalog.contains("async fn create_table("));
	assert!(catalog.contains(") -> Result<TableBinding, CatalogError>;"));
	assert!(error.contains("TableAlreadyExists"));
	assert!(error.contains("SchemaMismatch"));
	assert!(error.contains("AdminUnavailable"));
	assert!(error.contains("AdminFailed"));
	assert!(error.contains("FingerprintResolution"));
}

#[test]
pub(crate) fn adr029_mock_catalog_is_interior_mutable() {
	let catalog = std::fs::read_to_string(manifest_dir().join("src/catalog.rs"))
		.expect("read catalog source");

	assert!(catalog.contains("inner: Arc<RwLock<MockCatalogInner>>"));
	assert!(catalog.contains("next_table_id: u64"));
	assert!(catalog.contains("self.inner.write()"));
	assert!(catalog.contains("bump_next_table_id(&mut inner, legacy_table_id)"));
}

#[test]
pub(crate) fn adr029_mock_catalog_identity_uses_descriptor_fingerprint() {
	let catalog = std::fs::read_to_string(manifest_dir().join("src/catalog.rs"))
		.expect("read catalog source");
	let body = brace_blocks_after_marker(&catalog, "fn mock_table_identity")
		.into_iter()
		.next()
		.expect("mock_table_identity body")
		.1;

	assert!(body.contains("TableObjectId::new"));
	assert!(body.contains("create_table_descriptor"));
	assert!(catalog.contains("TableSchemaDescriptor::new_with_layout"));
	assert!(body.contains("physical_descriptor_fingerprint"));
	assert!(!catalog.contains("PhysicalDescriptorFingerprint::from_bytes"));
}

#[test]
pub(crate) fn adr029_mock_catalog_reference_insertion_does_not_overwrite() {
	let catalog = std::fs::read_to_string(manifest_dir().join("src/catalog.rs"))
		.expect("read catalog source");
	let impl_block = catalog
		.split("impl Catalog for MockCatalog")
		.nth(1)
		.and_then(|rest| rest.split("impl Catalog for WendelonCatalog").next())
		.expect("MockCatalog Catalog impl");
	let create_body = brace_blocks_after_marker(impl_block, "async fn create_table")
		.into_iter()
		.next()
		.expect("MockCatalog::create_table body")
		.1;

	assert!(create_body.contains("insert((self.tenant_id, name.clone()), legacy_table_id)"));
	assert!(create_body.contains(".entry((self.tenant_id, TableRef::new(name.table.clone())))"));
	assert!(create_body.contains(".or_insert(legacy_table_id)"));
	assert_eq!(
		create_body
			.matches(".insert((self.tenant_id, name.clone()), legacy_table_id)")
			.count(),
		1
	);
}

#[test]
pub(crate) fn adr029_wendelon_catalog_create_table_has_admin_boundary() {
	let catalog = std::fs::read_to_string(manifest_dir().join("src/catalog.rs"))
		.expect("read catalog source");
	let impl_block = catalog
		.split("impl Catalog for WendelonCatalog")
		.nth(1)
		.expect("WendelonCatalog Catalog impl");
	let body = brace_blocks_after_marker(impl_block, "async fn create_table")
		.into_iter()
		.next()
		.expect("WendelonCatalog::create_table body")
		.1;

	assert!(body.contains("TableSchemaUpdateRequest::create"));
	assert!(body.contains("submit_table_schema_update"));
	assert!(!body.contains("CatalogError::AdminUnavailable"));
}

#[test]
pub(crate) fn adr030_catalog_has_no_insert_method() {
	let catalog = std::fs::read_to_string(manifest_dir().join("src/catalog.rs"))
		.expect("read catalog source");
	let trait_body = brace_blocks_after_marker(&catalog, "pub trait Catalog")
		.into_iter()
		.next()
		.expect("Catalog trait body")
		.1;

	assert!(!trait_body.contains("insert"));
	assert!(!trait_body.contains("write_"));
}

#[test]
pub(crate) fn adr030_coordinator_insert_uses_wendelon_client_wrapper() {
	let manifest_dir = manifest_dir();
	let storage =
		std::fs::read_to_string(manifest_dir.join("src/storage.rs")).expect("read storage source");
	let write_impl =
		brace_blocks_after_marker(&storage, "impl StorageWrite for WendelonStorageWrite")
			.into_iter()
			.next()
			.expect("WendelonStorageWrite impl")
			.1;

	assert!(write_impl.contains("self\n\t\t\t.client\n\t\t\t.table_write("));
	assert!(write_impl.contains("wendelon_client::WriteOptions"));
	assert!(!storage.contains("TableWriteRequest {"));
	for (path, line, text) in source_lines(&manifest_dir.join("src")) {
		assert!(
			!text.contains("TableWriteRequest {"),
			"ADR 030 D8: Toron must not construct low-level TableWriteRequest at {}:{}: {}",
			path.display(),
			line,
			text
		);
	}
}

#[test]
pub(crate) fn adr030_wendelon_storage_write_mirrors_scan_error_mapping() {
	let storage = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	let write_impl =
		brace_blocks_after_marker(&storage, "impl StorageWrite for WendelonStorageWrite")
			.into_iter()
			.next()
			.expect("WendelonStorageWrite impl")
			.1;

	assert!(write_impl.contains("descriptor(request.tenant_id, request.table_id)?"));
	assert!(write_impl.contains("StorageError::DescriptorEvolved"));
	assert!(write_impl.contains("StorageError::Internal"));
	assert!(storage.contains("StorageError::TableNotFound"));
}

#[test]
pub(crate) fn adr034_catalog_drop_table_returns_optional_binding() {
	let catalog = std::fs::read_to_string(manifest_dir().join("src/catalog.rs"))
		.expect("read catalog source");
	let trait_block = brace_blocks_after_marker(&catalog, "pub trait Catalog")
		.into_iter()
		.next()
		.expect("Catalog trait")
		.1;

	assert!(trait_block.contains("async fn drop_table("));
	assert!(trait_block.contains(") -> Result<Option<TableBinding>, CatalogError>;"));
}

#[test]
pub(crate) fn adr034_mock_catalog_drop_table_cleans_aliases() {
	let catalog = std::fs::read_to_string(manifest_dir().join("src/catalog.rs"))
		.expect("read catalog source");
	let impl_block = catalog
		.split("impl Catalog for MockCatalog")
		.nth(1)
		.and_then(|rest| rest.split("impl Catalog for WendelonCatalog").next())
		.expect("MockCatalog Catalog impl");
	let drop_body = brace_blocks_after_marker(impl_block, "async fn drop_table")
		.into_iter()
		.next()
		.expect("MockCatalog::drop_table body")
		.1;

	assert!(drop_body.contains(".remove(&(self.tenant_id, table_id))"));
	assert!(drop_body.contains(".by_ref"));
	assert!(drop_body.contains(".retain(|(tenant_id, _), id|"));
	assert!(drop_body.contains("*id != table_id"));
	assert!(!drop_body.contains("by_ref.insert"));
	assert!(!drop_body.contains("or_insert"));
}

#[test]
pub(crate) fn adr039_wendelon_catalog_drop_table_consumes_finalized_marker() {
	let catalog = std::fs::read_to_string(manifest_dir().join("src/catalog.rs"))
		.expect("read catalog source");
	let impl_block = catalog
		.split("impl Catalog for WendelonCatalog")
		.nth(1)
		.expect("WendelonCatalog Catalog impl");
	let drop_body = brace_blocks_after_marker(impl_block, "async fn drop_table")
		.into_iter()
		.next()
		.expect("WendelonCatalog::drop_table body")
		.1;

	assert!(!drop_body.contains("CatalogError::AdminUnavailable"));
	assert!(drop_body.contains("DropOperationId"));
	assert!(drop_body.contains("consume_finalized_drop"));
	assert!(drop_body.contains("remove_table"));
	assert!(!drop_body.contains("drop_table_schema"));
	assert!(drop_body.contains("if_exists"));
	assert!(drop_body.contains("Ok(None)"));
	assert!(drop_body.contains("CatalogError::TableNotFound"));
}

#[test]
pub(crate) fn adr034_catalog_trait_grows_by_exactly_one_method() {
	let catalog = std::fs::read_to_string(manifest_dir().join("src/catalog.rs"))
		.expect("read catalog source");
	let trait_block = catalog
		.split("pub trait Catalog")
		.nth(1)
		.and_then(|rest| rest.split("#[derive(Default)]").next())
		.expect("Catalog trait");

	assert!(trait_block.contains("async fn drop_table"));
	for forbidden in [
		"async fn truncate_table",
		"async fn replace_table",
		"async fn drop_schema",
		"async fn alter_table",
		"async fn rename_table",
		"async fn drop_table_outcome",
	] {
		assert!(
			!trait_block.contains(forbidden),
			"ADR 034: Catalog must not grow beyond drop_table (found {forbidden})"
		);
	}
}

#[test]
pub(crate) fn adr037_ctas_reads_source_before_catalog_create() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("coordinator");
	let shared = brace_blocks_after_marker(&coordinator, "async fn execute_write_plan_with_source")
		.into_iter()
		.next()
		.expect("shared mutation driver body")
		.1;
	let ctas_arm = shared
		.split("ExecutionPlan::CreateTableAsSelect")
		.nth(1)
		.and_then(|rest| rest.split("ExecutionPlan::Delete").next())
		.expect("CTAS arm");
	let read = ctas_arm
		.find("source_executor.execute_read_plan(source)")
		.expect("source read");
	let create = ctas_arm
		.find("create_table_with_outcome")
		.expect("catalog create");

	assert!(read < create);
}

#[test]
pub(crate) fn adr035_in_memory_identity_layout_is_prefix_suffix() {
	let storage = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	let body = storage
		.split("fn in_memory_row_identity")
		.nth(1)
		.and_then(|rest| rest.split("fn take_record_batch_rows").next())
		.expect("in_memory_row_identity body");

	assert!(body.contains("bytes[0..16].copy_from_slice(table_object_id.as_bytes())"));
	assert!(body.contains("bytes[24..32].copy_from_slice(&ordinal.to_be_bytes())"));
}

#[test]
pub(crate) fn adr035_in_memory_storage_tracks_row_ids_and_tombstones() {
	let storage = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	let data = storage
		.split("struct InMemoryTableData")
		.nth(1)
		.and_then(|rest| rest.split("pub struct InMemoryTableStorage").next())
		.expect("InMemoryTableData block");

	for field in ["row_key_ids", "deleted_row_key_ids", "next_row_key_ordinal"] {
		assert!(data.contains(field), "missing ADR 035 field {field}");
	}
}

#[test]
pub(crate) fn adr035_wendelon_adapter_calls_table_delete() {
	let storage = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	let adapter = storage
		.split("impl StorageDeleteRows for WendelonStorageDeleteRows")
		.nth(1)
		.and_then(|rest| rest.split("fn append_projection").next())
		.expect("WendelonStorageDeleteRows adapter block");

	assert!(adapter.contains(".table_delete("));
	assert!(!adapter.contains(".table_delete_rows("));
}

#[test]
pub(crate) fn adr038_storage_admin_trait_has_one_method() {
	let storage = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	let trait_body = storage
		.split("pub trait StorageAdmin")
		.nth(1)
		.and_then(|rest| rest.split("pub struct InMemoryStorageScan").next())
		.expect("StorageAdmin trait block");

	assert_eq!(trait_body.matches("async fn").count(), 2);
	assert!(trait_body.contains("async fn drop_table_storage("));
	assert!(trait_body.contains("async fn create_logical_type("));
}

#[test]
pub(crate) fn adr038_in_memory_storage_admin_removes_table_entry() {
	let storage = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	let body = brace_blocks_after_marker(&storage, "impl StorageAdmin for InMemoryTableStorage")
		.into_iter()
		.next()
		.expect("InMemoryTableStorage StorageAdmin impl")
		.1;

	assert!(body.contains("tables.remove(&request.table_id)"));
}

#[test]
pub(crate) fn adr038_drop_table_storage_first_catalog_second() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("coordinator");
	let helper = brace_blocks_after_marker(
		&coordinator,
		"async fn drop_existing_table_with_storage_admin",
	)
	.into_iter()
	.next()
	.expect("drop_existing_table_with_storage_admin helper body")
	.1;

	assert!(
		helper.find(".drop_table_storage").unwrap()
			< helper.find("catalog.drop_table(name, false)").unwrap()
	);
}

#[test]
pub(crate) fn adr038_wendelon_create_table_admin_unavailable_removed() {
	let catalog = std::fs::read_to_string(manifest_dir().join("src/catalog.rs"))
		.expect("read catalog source");
	let create_body = wendelon_catalog_method_body(&catalog, "async fn create_table");

	assert!(!create_body.contains("CatalogError::AdminUnavailable"));
	assert!(!create_body.contains("CREATE TABLE is not wired"));
}

#[test]
pub(crate) fn adr038_wendelon_create_uses_table_schema_update_request_create() {
	let catalog = std::fs::read_to_string(manifest_dir().join("src/catalog.rs"))
		.expect("read catalog source");
	let create_body = wendelon_catalog_method_body(&catalog, "async fn create_table");

	assert!(create_body.contains("TableSchemaUpdateRequest::create"));
	assert!(create_body.contains("submit_table_schema_update"));
}

#[test]
pub(crate) fn adr038_wendelon_create_uses_uuid_object_id_not_fingerprint() {
	let catalog = std::fs::read_to_string(manifest_dir().join("src/catalog.rs"))
		.expect("read catalog source");
	let create_body = wendelon_catalog_method_body(&catalog, "async fn create_table");

	assert!(create_body.contains("Uuid::new_v4"));
	for forbidden in ["blake3", "xxh3", "Sha2", "Sha3"] {
		assert!(!create_body.contains(forbidden), "unexpected {forbidden}");
	}
}

#[test]
pub(crate) fn adr038_drop_route_uses_storage_admin_sink_when_configured() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("coordinator");

	assert!(coordinator.contains("admin: Arc<dyn StorageAdmin>"));
	assert!(coordinator.contains("self.admin.as_ref()"));
	assert!(coordinator.contains(".drop_table_storage"));
}

#[test]
pub(crate) fn adr039_wendelon_drop_admin_unavailable_lifted() {
	let catalog = std::fs::read_to_string(manifest_dir().join("src/catalog.rs"))
		.expect("read catalog source");
	let drop_body = wendelon_catalog_method_body(&catalog, "async fn drop_table");

	assert!(!drop_body.contains("CatalogError::AdminUnavailable"));
	assert!(drop_body.contains("consume_finalized_drop"));
	assert!(drop_body.contains("remove_table"));
}

#[test]
pub(crate) fn adr039_wendelon_admin_error_mapping_is_structured() {
	let catalog = std::fs::read_to_string(manifest_dir().join("src/catalog.rs"))
		.expect("read catalog source");
	let storage = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	let catalog_body = brace_blocks_after_marker(&catalog, "fn map_wendelon_admin_error")
		.into_iter()
		.next()
		.expect("catalog admin error mapper")
		.1;
	let storage_body =
		brace_blocks_after_marker(&storage, "fn map_wendelon_admin_error_to_storage")
			.into_iter()
			.next()
			.expect("storage admin error mapper")
			.1;

	for body in [catalog_body, storage_body] {
		assert!(body.contains("ClientError::Admin"));
		assert!(body.contains("WendelonAdminError::"));
		assert!(!body.contains(".contains("));
	}
}

#[test]
pub(crate) fn adr039_toron_does_not_import_wendelon_internal_stage_types() {
	assert_toron_sources_do_not_contain(&[
		"TableDropPrepareRequest",
		"TableReclaimCompletionRequest",
		"TableDropFinalizeRequest",
		"DropStage",
	]);
}

#[test]
pub(crate) fn adr039_toron_does_not_orchestrate_wendelon_drop_stages() {
	let storage = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	let body = brace_blocks_after_marker(&storage, "impl StorageAdmin for WendelonStorageAdmin")
		.into_iter()
		.next()
		.expect("WendelonStorageAdmin StorageAdmin impl")
		.1;

	assert!(body.contains(".drop_table_schema("));
	assert!(body.contains("TableSchemaDropRequest::current"));
	assert!(!body.contains("TableSchemaDropRequest::V1"));
	for forbidden in [
		"submit_table_drop_prepare",
		"submit_table_reclaim_completion",
		"submit_table_drop_finalize",
		"submit_table_schema_drop",
		"TableDropPrepareRequest",
		"TableReclaimCompletionRequest",
		"TableDropFinalizeRequest",
	] {
		assert!(!body.contains(forbidden), "unexpected {forbidden}");
	}
}

#[test]
pub(crate) fn adr039_no_catalog_drop_table_with_proof_method() {
	assert_toron_sources_do_not_contain(&["drop_table_with_proof"]);
}

#[test]
pub(crate) fn adr061_catalog_create_exposes_created_outcome() {
	let catalog =
		std::fs::read_to_string(manifest_dir().join("src/catalog.rs")).expect("read catalog");
	let outcome = brace_blocks_after_marker(&catalog, "pub struct CreateTableOutcome")
		.into_iter()
		.next()
		.expect("CreateTableOutcome")
		.1;

	assert!(outcome.contains("pub binding: TableBinding,"));
	assert!(outcome.contains("pub created: bool,"));
	assert!(catalog.contains("async fn create_table_with_outcome("));
	assert!(catalog.contains(".map(|outcome| outcome.binding)"));
	assert!(catalog.contains("created: false"));
	assert!(catalog.contains("created: true"));
}

#[test]
pub(crate) fn adr061_no_wendelon_wire_worker_or_sql_protocol_change() {
	let wire = std::fs::read_to_string(manifest_dir().join("src/wire.rs")).expect("read wire");
	let sql_wire =
		std::fs::read_to_string(manifest_dir().join("src/sql_wire.rs")).expect("read SQL wire");
	let server =
		std::fs::read_to_string(manifest_dir().join("src/server/mod.rs")).expect("read server");
	let wendelon_sources = source_files(&wendelon_manifest_dir().join("src"))
		.into_iter()
		.map(|path| std::fs::read_to_string(path).expect("read Wendelon source"))
		.collect::<Vec<_>>()
		.join("\n");

	for source in [&wire, &sql_wire, &server, &wendelon_sources] {
		assert!(!source.contains("CreateTableOutcome"));
		assert!(!source.contains("create_table_with_outcome"));
		assert!(!source.contains("CtasIfNotExists"));
	}
}

#[test]
pub(crate) fn adr062_catalog_create_request_carries_row_identity() {
	let catalog =
		std::fs::read_to_string(manifest_dir().join("src/catalog.rs")).expect("read catalog");
	let trait_body = brace_blocks_after_marker(&catalog, "pub trait Catalog")
		.into_iter()
		.next()
		.expect("Catalog trait")
		.1;

	assert!(trait_body.contains("row_identity: CreateTableRowIdentity"));
	assert!(catalog.contains("create_table_row_identity_policy"));
}

#[test]
pub(crate) fn adr062_wendelon_catalog_maps_primary_key_descriptor() {
	let catalog =
		std::fs::read_to_string(manifest_dir().join("src/catalog.rs")).expect("read catalog");

	assert!(catalog.contains("CreateTableRowIdentity::PrimaryKey"));
	assert!(catalog.contains("RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single"));
	assert!(catalog.contains("RowIdentityPolicy::AppendOnly"));
}

#[test]
pub(crate) fn adr063_logical_update_has_no_wendelon_identity_types() {
	let logical = std::fs::read_to_string(manifest_dir().join("src/ir.rs")).expect("read IR");
	let update = brace_blocks_after_marker(&logical, "Update {")
		.into_iter()
		.next()
		.expect("logical update")
		.1;

	for forbidden in [
		"TableRowIdentity",
		"RowIdentityPolicy",
		"RowIdentityProjection",
		"TableObjectId",
		"PhysicalDescriptorFingerprint",
	] {
		assert!(
			!update.contains(forbidden),
			"logical UPDATE leaked {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr063_no_wendelon_wire_storage_or_descriptor_change() {
	let wendelon_source = source_files(&wendelon_manifest_dir().join("src"))
		.into_iter()
		.map(|path| std::fs::read_to_string(path).expect("read Wendelon source"))
		.collect::<Vec<_>>()
		.join("\n");

	for forbidden in [
		"LogicalPlan::Update",
		"ExecutionPlan::Update",
		"Statement::Update",
		"UpdateNotFragmentable",
		"StorageUpdate",
		"TableUpdateRequest",
		"TableUpdateResponse",
	] {
		assert!(
			!wendelon_source.contains(forbidden),
			"ADR 063 leaked UPDATE surface into Wendelon: {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr038_strict_admin_sink_default_is_unsupported_storage_admin() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("coordinator");

	assert!(coordinator.contains("UnsupportedStorageAdmin"));
	assert!(coordinator.contains("Arc::new(UnsupportedStorageAdmin)"));
	assert!(coordinator.contains("pub fn with_storage_admin("));
	assert!(coordinator.contains("pub fn with_writer_deleter_and_admin("));
}

#[test]
pub(crate) fn adr038_in_memory_admin_validates_object_id_and_fingerprint() {
	let storage = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	let body = brace_blocks_after_marker(&storage, "impl StorageAdmin for InMemoryTableStorage")
		.into_iter()
		.next()
		.expect("InMemoryTableStorage StorageAdmin impl")
		.1;

	assert!(body.contains("data.table_object_id != request.table_object_id"));
	assert!(
		body.contains("data.physical_descriptor_fingerprint")
			&& body.contains("request.physical_descriptor_fingerprint")
	);
}

#[test]
pub(crate) fn adr038_wendelon_create_row_identity_policy_is_create_request_driven_after_adr062() {
	let catalog = std::fs::read_to_string(manifest_dir().join("src/catalog.rs"))
		.expect("read catalog source");

	assert!(catalog.contains("CreateTableRowIdentity::AppendOnly"));
	assert!(catalog.contains("CreateTableRowIdentity::PrimaryKey"));
	assert!(catalog.contains("RowIdentityPolicy::AppendOnly"));
	assert!(catalog.contains("RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single"));
	assert!(!catalog.contains("RowIdentityPolicy::ByOrdinal"));
}

#[test]
pub(crate) fn adr038_no_separate_catalog_cache_after_admin() {
	let catalog = std::fs::read_to_string(manifest_dir().join("src/catalog.rs"))
		.expect("read catalog source");

	for forbidden in ["refresh_catalog", "evict_descriptor"] {
		assert!(
			!catalog.contains(forbidden),
			"unexpected helper {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr038_catalog_drop_table_only_called_from_routing_helper() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("coordinator");
	let helper = brace_blocks_after_marker(
		&coordinator,
		"async fn drop_existing_table_with_storage_admin",
	)
	.into_iter()
	.next()
	.expect("drop_existing_table_with_storage_admin helper")
	.1;

	assert_eq!(coordinator.matches(".drop_table(").count(), 1);
	assert!(helper.contains("catalog.drop_table(name, false).await"));
}

#[test]
pub(crate) fn adr035_wendelon_projection_change_is_privileged() {
	let columnar = std::fs::read_to_string(wendelon_manifest_dir().join("src/storage/columnar.rs"))
		.expect("read Wendelon columnar source");
	let insert_required = columnar
		.split("fn insert_required_column")
		.nth(1)
		.and_then(|rest| rest.split("fn remap_column_fetch_requirement").next())
		.expect("insert_required_column body");
	let core = std::fs::read_to_string(wendelon_manifest_dir().join("src/client/core.rs"))
		.expect("read Wendelon client core source");
	let table = std::fs::read_to_string(wendelon_manifest_dir().join("src/schema/table.rs"))
		.expect("read Wendelon table schema source");
	let client =
		std::fs::read_to_string(wendelon_manifest_dir().join("crates/wendelon-client/src/lib.rs"))
			.expect("read Wendelon client wrapper source");

	assert!(columnar.contains("cannot appear in PBC PROJECT output"));
	assert!(insert_required.contains("RequirementColumn::Storage"));
	assert!(insert_required.contains("requirement.storage_columns.insert(storage_column)"));
	assert!(core.contains("pub row_identity: RowIdentityProjection,"));
	assert!(table.contains("pub row_identity_projection: u8,"));
	assert!(client.contains("pub row_identity: RowIdentityProjection,"));
	assert!(client.contains("row_identity_projection: options.row_identity.as_u8()"));
	assert!(client.contains("row_identity: options.row_identity"));
}
