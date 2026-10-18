#![allow(unused_imports)]

use super::support::*;
use super::{
	core_invariants::*, distributed_execution::*, network_tls::*, observability_audit::*,
	observability_telemetry::*, predicate_pushdown::*, sql_planner_surface::*,
	storage_wendelon_boundary::*,
};

#[test]
pub(crate) fn adr040_nilket_is_leaf_workspace_package() {
	let nilket_cargo_path = workspace_root_dir().join("crates/nilket/Cargo.toml");
	let nilket_cargo = std::fs::read_to_string(&nilket_cargo_path).expect("read nilket Cargo.toml");

	assert!(nilket_cargo.contains("name = \"nilket\""));
	assert!(nilket_cargo.contains("toron = { path = \"../toron\" }"));
	assert!(nilket_cargo.contains("wendelon = { path = \"../wendelon\" }"));

	let mut offenders = Vec::new();
	for cargo_path in workspace_cargo_tomls() {
		if cargo_path == nilket_cargo_path {
			continue;
		}
		let source = std::fs::read_to_string(&cargo_path).expect("read Cargo.toml");
		if source.lines().any(|line| {
			let trimmed = line.trim();
			trimmed.starts_with("nilket ") || trimmed.starts_with("nilket=")
		}) {
			offenders.push((cargo_path, 1, "depends on nilket".to_string()));
		}
	}

	assert!(
		offenders.is_empty(),
		"workspace crate depends on nilket:\n{}",
		format_offenders(&workspace_root_dir(), &offenders)
	);
}

#[test]
pub(crate) fn adr040_nilket_root_has_no_role_runtime_dependencies() {
	let cargo = std::fs::read_to_string(workspace_root_dir().join("crates/nilket/Cargo.toml"))
		.expect("read nilket Cargo.toml");

	for forbidden in ["clap", "tokio", "tracing"] {
		assert!(
			!cargo
				.lines()
				.any(|line| line.trim_start().starts_with(forbidden)),
			"nilket root has forbidden direct dependency {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr040_nilket_delegates_to_two_roles() {
	let main = std::fs::read_to_string(workspace_root_dir().join("crates/nilket/src/main.rs"))
		.expect("read nilket main");

	assert!(main.contains("Some(\"wendelon\") => wendelon::cli::run("));
	assert!(main.contains("Some(\"toron\") => toron::cli::run("));
}

#[test]
pub(crate) fn adr532_adds_only_the_local_profile_to_adr040_root_commands() {
	let main = std::fs::read_to_string(workspace_root_dir().join("crates/nilket/src/main.rs"))
		.expect("read nilket main");

	for forbidden in [
		"Some(\"query\")",
		"Some(\"sql\")",
		"Some(\"inspect\")",
		"Some(\"diag\")",
		"Some(\"standalone\")",
	] {
		assert!(
			!main.contains(forbidden),
			"nilket root dispatches {forbidden}"
		);
	}
	assert!(main.contains("Some(\"local\")"));
	assert!(main.contains("None => run_local("));
	assert!(
		!main.contains("Some(\"toron\") => run_local"),
		"TORON-532 keeps the compute-role noun distinct from the local profile"
	);
	let positive_control = "Some(\"toron\") => run_local(options)";
	assert!(
		positive_control.contains("Some(\"toron\") => run_local"),
		"positive control: the TORON-532 role/profile assay must fire"
	);
}

#[test]
pub(crate) fn adr040_wendelon_cli_module_exists() {
	let cli = std::fs::read_to_string(wendelon_manifest_dir().join("src/cli.rs"))
		.expect("read Wendelon CLI source");

	assert!(cli.contains("pub fn run<I>(program: &str, args: I) -> ExitCode"));
}

#[test]
pub(crate) fn adr040_toron_cli_module_exists() {
	let cli =
		std::fs::read_to_string(manifest_dir().join("src/cli.rs")).expect("read Toron CLI source");

	assert!(cli.contains("pub fn run<I>(program: &str, args: I) -> ExitCode"));
}

#[test]
pub(crate) fn adr040_wendelon_cli_does_not_import_toron_runtime() {
	let cli = std::fs::read_to_string(wendelon_manifest_dir().join("src/cli.rs"))
		.expect("read Wendelon CLI source");

	assert!(!cli.contains("use toron::"));
}

#[test]
pub(crate) fn adr040_toron_cli_does_not_import_wendelon_runtime() {
	let cli =
		std::fs::read_to_string(manifest_dir().join("src/cli.rs")).expect("read Toron CLI source");

	for forbidden in [
		"use wendelon::",
		"WendelonNode",
		"SlabManager",
		"TableDropPrepareRequest",
		"TableReclaimCompletionRequest",
		"TableDropFinalizeRequest",
	] {
		assert!(!cli.contains(forbidden), "Toron CLI contains {forbidden}");
	}
}

#[test]
pub(crate) fn adr040_wendelon_binary_delegates_to_cli_runner() {
	let main = wendelon_manifest_dir().join("src/main.rs");
	if !main.exists() {
		return;
	}
	let source = std::fs::read_to_string(main).expect("read Wendelon main");

	assert!(source.contains("wendelon::cli::run"));
	assert!(!source.contains("tokio::main"));
	assert!(!source.contains("WendelonNode::bootstrap"));
}

#[test]
pub(crate) fn adr041_schema_create_or_replace_no_longer_rejected() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	let body = brace_blocks_after_marker(&planner, "fn lower_create_table(")
		.into_iter()
		.next()
		.expect("lower_create_table body")
		.1;

	assert!(body.contains("or_replace: *or_replace"));
	assert!(!body.contains("unsupported CREATE TABLE clause: OR REPLACE"));
}

#[test]
pub(crate) fn adr041_ctas_or_replace_no_longer_rejected() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	let body = brace_blocks_after_marker(&planner, "fn lower_create_table_as_select")
		.into_iter()
		.next()
		.expect("lower_create_table_as_select body")
		.1;

	assert!(body.contains("or_replace: *or_replace"));
	assert!(!body.contains("unsupported CREATE TABLE clause: OR REPLACE"));
}

#[test]
pub(crate) fn adr041_ctas_if_not_exists_is_supported_after_adr061() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");
	let body = brace_blocks_after_marker(&planner, "fn lower_create_table_as_select")
		.into_iter()
		.next()
		.expect("lower_create_table_as_select body")
		.1;

	assert!(body.contains("if_not_exists: *if_not_exists"));
	assert!(!body.contains("CTAS does not support IF NOT EXISTS in v1"));
}

#[test]
pub(crate) fn adr041_or_replace_if_not_exists_is_explicitly_rejected() {
	let planner = std::fs::read_to_string(manifest_dir().join("src/sql_planner.rs"))
		.expect("read SQL planner source");

	assert!(planner.contains("CREATE TABLE cannot combine OR REPLACE and IF NOT EXISTS"));
	assert_eq!(
		planner
			.matches("CREATE TABLE cannot combine OR REPLACE and IF NOT EXISTS")
			.count(),
		2
	);
}

#[test]
pub(crate) fn adr041_no_statement_sequence_ir() {
	let logical =
		std::fs::read_to_string(manifest_dir().join("src/ir.rs")).expect("read logical IR source");
	let execution = std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs"))
		.expect("read execution IR source");

	assert!(!logical.contains("LogicalPlan::Sequence"));
	assert!(!logical.contains("Sequence {"));
	assert!(!execution.contains("ExecutionPlan::Sequence"));
	assert!(!execution.contains("Sequence {"));
}

#[test]
pub(crate) fn adr041_no_wendelon_replace_request() {
	let mut offenders = source_lines(&manifest_dir().join("src"));
	offenders.extend(source_lines(&wendelon_manifest_dir().join("src")));
	offenders.retain(|(_, _, line)| {
		[
			"TableSchemaReplaceRequest",
			"submit_table_replace",
			"drop_table_with_replacement",
		]
		.iter()
		.any(|forbidden| line.contains(forbidden))
	});

	assert!(
		offenders.is_empty(),
		"replacement protocol symbol found:\n{}",
		format_offenders(&workspace_root_dir(), &offenders)
	);
}

#[test]
pub(crate) fn adr041_only_explicit_drop_reaches_the_storage_admin_drop() {
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
	let catalog = brace_blocks_after_marker(&coordinator, "async fn execute_catalog_plan")
		.into_iter()
		.next()
		.expect("execute_catalog_plan helper")
		.1;
	let create_arm = catalog
		.split("ExecutionPlan::CreateTable")
		.nth(1)
		.and_then(|rest| rest.split("ExecutionPlan::DropTable").next())
		.expect("CreateTable arm");
	let drop_arm = catalog
		.split("ExecutionPlan::DropTable")
		.nth(1)
		.and_then(|rest| rest.split("other =>").next())
		.expect("DropTable arm");

	assert_eq!(coordinator.matches("DropTableStorageRequest {").count(), 2);
	assert!(helper.contains("DropTableStorageRequest {"));
	// No replacement path calls the name-resolving helper. ADR 548 funnels both
	// post-CAS retirement and lost-CAS cleanup through one object-identified drop
	// helper, only after the ledger proves which object the saga owns.
	assert!(!create_arm.contains("drop_existing_table_with_storage_admin"));
	assert!(create_arm.contains("refuse_destructive_or_replace"));
	assert!(drop_arm.contains("drop_existing_table_with_storage_admin"));
	assert!(!create_arm.contains("DropTableStorageRequest {"));
}

#[test]
pub(crate) fn adr041_ctas_replace_refuses_before_materializing_source() {
	let coordinator =
		std::fs::read_to_string(manifest_dir().join("src/coordinator.rs")).expect("coordinator");
	let shared = brace_blocks_after_marker(&coordinator, "async fn execute_write_plan_with_source")
		.into_iter()
		.next()
		.expect("shared write driver")
		.1;
	let ctas_arm = shared
		.split("ExecutionPlan::CreateTableAsSelect")
		.nth(1)
		.and_then(|rest| rest.split("ExecutionPlan::Delete").next())
		.expect("CTAS arm");
	let source = ctas_arm
		.find("source_executor.execute_read_plan(source)")
		.expect("source materialization");
	let validate = ctas_arm
		.find("validate_refined_logical_columns(\"CTAS\"")
		.expect("value validation");
	let provision = ctas_arm.find(".provision_ctas(").expect("saga provision");
	let dispatch = ctas_arm
		.find("CtasSagaMutation::MarkRowDispatched")
		.expect("durable row dispatch boundary");
	let finish =
		brace_blocks_after_marker(&coordinator, "async fn finish_ctas_after_rows_committed")
			.into_iter()
			.next()
			.expect("staged publication helper")
			.1;
	// Source/materialization/validation leave no provisional object on failure.
	assert!(source < validate);
	assert!(validate < provision);
	assert!(provision < dispatch);
	assert!(finish.contains("CtasSagaMutation::Publish"));
	assert!(finish.contains("CtasSagaMutation::MarkRetiringOld"));
	assert!(!ctas_arm.contains("drop_existing_table_with_storage_admin"));
}

#[test]
pub(crate) fn adr041_toron_cli_reservations_preserved() {
	let cli =
		std::fs::read_to_string(manifest_dir().join("src/cli.rs")).expect("read Toron CLI source");

	assert!(cli.contains("[verb, rest @ ..] if verb == \"sql\" && !rest.is_empty()"));
	assert!(cli.contains("run_sql(rest)"));
	assert!(cli.contains("[verb, rest @ ..] if verb == \"inspect\" => run_inspect(rest)"));
	assert!(!cli.contains("is reserved; production execution is not wired in ADR 040"));
	assert!(!cli.contains("reserved(\"inspect\")"));
}

#[test]
pub(crate) fn adr042_toron_start_is_not_reserved() {
	let cli =
		std::fs::read_to_string(manifest_dir().join("src/cli.rs")).expect("read Toron CLI source");

	assert!(cli.contains("crate::daemon::run_start(PathBuf::from(config_path))"));
	assert!(!cli.contains("[verb] if verb == \"start\" => reserved(\"start\")"));
}

#[test]
pub(crate) fn adr042_toron_inspect_remains_reserved_after_sql_lift() {
	let cli =
		std::fs::read_to_string(manifest_dir().join("src/cli.rs")).expect("read Toron CLI source");

	assert!(cli.contains("[verb, rest @ ..] if verb == \"sql\" && !rest.is_empty()"));
	assert!(cli.contains("run_sql(rest)"));
	assert!(cli.contains("[verb, rest @ ..] if verb == \"inspect\" => run_inspect(rest)"));
	assert!(!cli.contains("reserved(\"inspect\")"));
}

#[test]
pub(crate) fn adr042_nilket_root_stays_runtime_free() {
	let nilket_manifest =
		std::fs::read_to_string(workspace_root_dir().join("crates/nilket/Cargo.toml"))
			.expect("read nilket manifest");
	let nilket_src =
		std::fs::read_to_string(workspace_root_dir().join("crates/nilket/src/main.rs"))
			.expect("read nilket main");

	for forbidden in ["tokio", "clap", "tracing"] {
		assert!(
			!nilket_manifest.contains(forbidden),
			"nilket manifest gained direct {forbidden} dependency"
		);
		assert!(
			!nilket_src.contains(forbidden),
			"nilket root source gained direct {forbidden} runtime surface"
		);
	}
}

#[test]
pub(crate) fn adr042_toron_cli_owns_runtime_not_nilket() {
	let daemon = std::fs::read_to_string(manifest_dir().join("src/daemon.rs"))
		.expect("read Toron daemon source");
	let cli =
		std::fs::read_to_string(manifest_dir().join("src/cli.rs")).expect("read Toron CLI source");
	let nilket_src =
		std::fs::read_to_string(workspace_root_dir().join("crates/nilket/src/main.rs"))
			.expect("read nilket main");

	assert!(daemon.contains("tokio::runtime::Builder::new_multi_thread()"));
	assert!(cli.contains("crate::daemon::run_start"));
	assert!(!nilket_src.contains("tokio::runtime"));
}

#[test]
pub(crate) fn adr042_toron_config_surface_exists() {
	let lib = std::fs::read_to_string(manifest_dir().join("src/lib.rs")).expect("read lib");
	let config =
		std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read config");

	assert!(lib.contains("pub mod config;"));
	assert!(config.contains("pub struct ToronBootstrapConfig"));
	assert!(config.contains("pub fn load_toron_bootstrap_config"));
	assert!(config.contains("pub fn render_example_config"));
	assert!(config.contains("pub fn write_example_config"));
}

#[test]
pub(crate) fn adr042_descriptor_bootstrap_is_product_type_with_table_ids() {
	let config =
		std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read config");

	assert!(config.contains("pub struct DescriptorBootstrap"));
	assert!(config.contains("pub tables: Vec<DescriptorBootstrapEntry>"));
	assert!(config.contains("pub struct DescriptorBootstrapEntry"));
	assert!(config.contains("pub tenant_id: u64"));
	assert!(config.contains("pub table_id: u64"));
	assert!(config.contains("pub descriptor: wendelon_client::TableSchemaDescriptor"));
	assert!(config.contains("Vec<(TenantId, TableId, wendelon_client::TableSchemaDescriptor)>"));
	assert!(config.contains("pub type DescriptorBootstrapRecord"));
	assert!(config.contains("optimizer_facts: Option<DescriptorOptimizerFacts>"));
}

#[test]
pub(crate) fn adr042_descriptor_bootstrap_has_no_v1_type_suffix() {
	let config =
		std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read config");

	assert!(!config.contains("ToronDescriptorBootstrapV1"));
	assert!(!config.contains("ToronDescriptorBootstrapTableV1"));
	assert!(!config.contains("DescriptorBootstrapV1"));
	assert!(!config.contains("DescriptorBootstrapEntryV1"));
}

#[test]
pub(crate) fn adr042_daemon_uses_single_descriptor_registry() {
	let daemon = std::fs::read_to_string(manifest_dir().join("src/daemon.rs"))
		.expect("read Toron daemon source");
	let startup = brace_blocks_after_marker(&daemon, "async fn start_with_client")
		.into_iter()
		.next()
		.expect("ToronDaemon::start_with_client")
		.1;

	assert_eq!(
		startup
			.matches("WendelonDescriptorRegistry::from_tenant_descriptors")
			.count(),
		1
	);
	assert!(startup.contains("let registry = Arc::new("));
	assert!(startup.contains("WendelonCatalog::with_client_and_registry"));
	assert!(startup.contains("WendelonStorageScan::with_descriptor_registry"));
	assert!(startup.contains("WendelonStorageWrite::with_descriptor_registry"));
	assert!(startup.contains("WendelonStorageDeleteRows::with_descriptor_registry"));
	assert!(startup.contains("WendelonStorageAdmin::new"));
	assert!(startup.matches("Arc::clone(&registry)").count() >= 5);
}

#[test]
pub(crate) fn adr042_descriptor_bootstrap_is_startup_only() {
	let daemon = std::fs::read_to_string(manifest_dir().join("src/daemon.rs"))
		.expect("read Toron daemon source");
	let startup = brace_blocks_after_marker(&daemon, "async fn start_with_client")
		.into_iter()
		.next()
		.expect("ToronDaemon::start_with_client")
		.1;

	assert!(startup.contains("load_descriptor_bootstrap_with_optimizer_facts(path)?"));
	for forbidden in [
		"list_descriptors",
		"refresh_descriptor",
		"watch_descriptor",
		"refresh_topology",
	] {
		assert!(
			!daemon.contains(forbidden),
			"daemon contains descriptor discovery/refresh token {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr042_unauthenticated_task_listener_is_loopback_only() {
	let config =
		std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read config");
	let daemon = std::fs::read_to_string(manifest_dir().join("src/daemon.rs"))
		.expect("read Toron daemon source");

	assert!(config.contains("NON_LOOPBACK_BIND_DIAGNOSTIC"));
	assert!(config.contains(".parse::<SocketAddr>()"));
	assert!(config.contains("task_listener_bind.ip().is_loopback()"));
	assert!(daemon.contains("TcpListener::bind(config.worker.task_listener_bind)"));
}

#[test]
pub(crate) fn adr043_toron_sql_is_not_reserved() {
	let cli =
		std::fs::read_to_string(manifest_dir().join("src/cli.rs")).expect("read Toron CLI source");

	assert!(cli.contains("run_sql(rest)"));
	assert!(cli.contains("toron sql requires <statement...>"));
	assert!(!cli.contains("reserved(\"sql\")"));
}

#[test]
pub(crate) fn adr043_toron_inspect_remains_reserved() {
	let cli =
		std::fs::read_to_string(manifest_dir().join("src/cli.rs")).expect("read Toron CLI source");

	assert!(cli.contains("[verb, rest @ ..] if verb == \"inspect\" => run_inspect(rest)"));
	assert!(!cli.contains("reserved(\"inspect\")"));
}

#[test]
pub(crate) fn adr043_nilket_root_sql_stays_rejected() {
	let nilket_src =
		std::fs::read_to_string(workspace_root_dir().join("crates/nilket/src/main.rs"))
			.expect("read nilket main");

	assert!(!nilket_src.contains("Some(\"sql\")"));
	assert!(nilket_src.contains("Some(\"toron\") => toron::cli::run"));
}

#[test]
pub(crate) fn adr043_nilket_root_stays_runtime_free() {
	let nilket_manifest =
		std::fs::read_to_string(workspace_root_dir().join("crates/nilket/Cargo.toml"))
			.expect("read nilket manifest");

	for forbidden in ["tokio", "quinn", "rustls", "arrow-cast"] {
		assert!(
			!nilket_manifest.contains(forbidden),
			"nilket manifest gained direct runtime dependency {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr043_toron_cli_sql_is_client_only() {
	let cli =
		std::fs::read_to_string(manifest_dir().join("src/cli.rs")).expect("read Toron CLI source");
	for forbidden in [
		"DistributedQueryCoordinator",
		"QueryCoordinator",
		"WendelonCatalog",
		"WendelonStorageScan",
		"WendelonStorageWrite",
		"WendelonStorageDeleteRows",
		"WendelonStorageAdmin",
		"WendelonDescriptorRegistry",
	] {
		assert!(
			!cli.contains(forbidden),
			"Toron CLI SQL client must not construct {forbidden}"
		);
	}
	assert!(cli.contains("toron_client::ToronClient::loopback"));
	assert!(!cli.contains("ToronDaemon"));
}

#[test]
pub(crate) fn adr043_sql_listener_config_surface_exists() {
	let config =
		std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read config");

	for required in [
		"pub struct SqlConfig",
		"pub enum SqlTransport",
		"pub struct CoordinatorConfig",
		"pub struct StaticWorkerConfig",
		"pub sql: Option<SqlConfig>",
		"pub coordinator: CoordinatorConfig",
		"transport = \"quic\"",
		"listener_bind = \"127.0.0.1:7742\"",
	] {
		assert!(
			config.contains(required),
			"missing ADR 043 config token {required}"
		);
	}
}

#[test]
pub(crate) fn adr043_sql_listener_is_loopback_only() {
	let config =
		std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read config");

	assert!(config.contains("NON_LOOPBACK_SQL_BIND_DIAGNOSTIC"));
	assert!(config.contains("listener_bind.ip().is_loopback()"));
	assert!(config.contains("!listener_bind.ip().is_loopback() && !tls_present"));
	assert!(config.contains("sql.statement_timeout_seconds must be greater than 0"));
}

#[test]
pub(crate) fn adr043_task_transport_is_quic_by_default_and_loopback_only() {
	let daemon = std::fs::read_to_string(manifest_dir().join("src/daemon.rs"))
		.expect("read Toron daemon source");
	let transport = std::fs::read_to_string(manifest_dir().join("src/server/transport.rs"))
		.expect("read Toron transport source");

	assert!(daemon.contains("QuicEndpoint::bind_loopback(worker_bind_endpoint)"));
	assert!(daemon.contains("QuicTransport::new()?"));
	assert!(transport.contains("pub struct QuicTransport"));
	assert!(transport.contains("QuicEndpoint::client_loopback()?"));
	assert!(transport.contains("connect_worker"));
}

#[test]
pub(crate) fn adr043_sql_wire_uses_length_prefixed_postcard() {
	let wire = std::fs::read_to_string(manifest_dir().join("../toron-client/src/protocol.rs"))
		.expect("read SQL wire");

	assert!(wire.contains("postcard::to_allocvec(frame)"));
	assert!(wire.contains("postcard::from_bytes(&payload)"));
	assert!(wire.contains("to_le_bytes()"));
	assert!(wire.contains("u32::from_le_bytes"));
}

#[test]
pub(crate) fn adr043_sql_wire_uses_arrow_ipc_not_row_strings() {
	let wire = std::fs::read_to_string(manifest_dir().join("../toron-client/src/protocol.rs"))
		.expect("read SQL wire");

	assert!(wire.contains("pub arrow_ipc_bytes: ByteBuf"));
	assert!(!wire.contains("Vec<String>"));
	assert!(!wire.contains("rows: Vec"));
}

#[test]
pub(crate) fn adr043_sql_wire_types_have_no_v1_suffix() {
	assert_toron_sources_do_not_contain(&[
		"SqlFrameV1",
		"SqlMessageV1",
		"SqlErrorCodeV1",
		"ExecuteRequestV1",
	]);
}

#[test]
pub(crate) fn adr043_new_sql_protocol_names_are_brand_clean() {
	assert_toron_sources_do_not_contain(&[
		"ToronSql",
		"ToronQuic",
		"TORON_QUIC",
		"toron-loopback",
		"ToronCoordinatorConfig",
		"ToronStaticWorker",
		"ToronWendelonTlsConfig",
		"RawToronSql",
		"RawToronCoordinator",
		"RawToronStatic",
	]);
}

#[test]
pub(crate) fn adr043_no_grpc_protobuf_or_arrow_flight() {
	assert_toron_sources_do_not_contain_identifiers(&[
		"tonic",
		"prost",
		"arrow_flight",
		"arrow-flight",
	]);
}

#[test]
pub(crate) fn adr043_no_remote_sql_auth_policy() {
	for path in [
		"src/cli.rs",
		"src/daemon.rs",
		"../toron-client/src/protocol.rs",
		"src/quic.rs",
	] {
		let source = std::fs::read_to_string(manifest_dir().join(path)).expect("read source");
		for forbidden in [
			"TORON_SQL_TOKEN",
			"Authorization",
			"Bearer",
			"client_tenant",
			"tenant_claim",
			"remote_sql_auth",
		] {
			assert!(
				!source.contains(forbidden),
				"{path} contains remote SQL auth policy token {forbidden}"
			);
		}
	}
}

#[test]
pub(crate) fn adr043_daemon_uses_single_descriptor_registry_for_sql() {
	let daemon = std::fs::read_to_string(manifest_dir().join("src/daemon.rs"))
		.expect("read Toron daemon source");
	let startup = brace_blocks_after_marker(&daemon, "async fn start_with_client")
		.into_iter()
		.next()
		.expect("ToronDaemon::start_with_client")
		.1;

	assert_eq!(
		startup
			.matches("WendelonDescriptorRegistry::from_tenant_descriptors")
			.count(),
		1
	);
	assert!(startup.matches("Arc::clone(&registry)").count() >= 5);
	assert!(startup.contains("DistributedQueryCoordinator"));
}

#[test]
pub(crate) fn adr043_sql_uses_distributed_coordinator_not_local_bypass() {
	let daemon = std::fs::read_to_string(manifest_dir().join("src/daemon.rs"))
		.expect("read Toron daemon source");

	assert!(daemon.contains("DistributedQueryCoordinator"));
	assert!(daemon.contains("sql_execution_context(tenant_id, frame.query_id, session_id)"));
	assert!(daemon.contains("execute_query_with_context(&plan, context)"));
	assert!(!daemon.contains("QueryCoordinator::new"));
	assert!(!daemon.contains("Executor::new("));
}

#[test]
pub(crate) fn adr043_static_worker_registry_config_is_coordinator_scoped() {
	let config =
		std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read config");
	let daemon = std::fs::read_to_string(manifest_dir().join("src/daemon.rs"))
		.expect("read Toron daemon source");

	assert!(config.contains("pub struct CoordinatorConfig"));
	assert!(config.contains("static_workers: Vec<RawStaticWorkerConfig>"));
	assert!(daemon.contains("StaticWorkerRegistry::new(registry_workers)"));
}

#[test]
pub(crate) fn adr043_no_wendelon_descriptor_refresh() {
	let daemon = std::fs::read_to_string(manifest_dir().join("src/daemon.rs"))
		.expect("read Toron daemon source");
	for forbidden in [
		"list_descriptors",
		"refresh_descriptor",
		"watch_descriptor",
		"refresh_topology",
	] {
		assert!(
			!daemon.contains(forbidden),
			"daemon contains descriptor refresh token {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr043_wendelon_client_has_real_provider_mode_for_sql() {
	let daemon = std::fs::read_to_string(manifest_dir().join("src/daemon.rs"))
		.expect("read Toron daemon source");

	assert!(daemon.contains("Client::connect_quic(client_config).await"));
	assert!(!daemon.contains("providers: HashMap::new()"));
}

#[test]
pub(crate) fn adr043_no_fixture_storage_in_production_sql() {
	let daemon = std::fs::read_to_string(manifest_dir().join("src/daemon.rs"))
		.expect("read Toron daemon source");

	assert!(!daemon.contains("InMemoryTableStorage"));
	assert!(!daemon.contains("FixtureStorageScan"));
	assert!(!daemon.contains("toron_fixture_worker"));
}

#[test]
pub(crate) fn adr043_sql_ready_does_not_mutate_adr042_ready_line() {
	let daemon = std::fs::read_to_string(manifest_dir().join("src/daemon.rs"))
		.expect("read Toron daemon source");

	assert!(daemon.contains("println!(\"READY {}\", daemon.handle().worker_endpoint);"));
	assert!(daemon.contains("println!(\"SQL_READY {sql_endpoint}\");"));
}

#[test]
pub(crate) fn adr043_sql_wire_has_one_protocol_owner_after_toron_532() {
	let wire = std::fs::read_to_string(manifest_dir().join("../toron-client/src/protocol.rs"))
		.expect("read SQL wire");

	assert!(wire.contains("postcard::to_allocvec(frame)"));
	assert!(!wire.contains("toron::"));
	assert!(!wire.contains("wendelon::"));
}

#[test]
pub(crate) fn adr042_no_public_query_service() {
	let manifest_dir = manifest_dir();
	let offenders = source_lines(&manifest_dir.join("src"))
		.into_iter()
		.filter(|(_, _, line)| {
			[
				"ToronQueryService",
				"ExecuteSqlRequest",
				"SqlListener",
				"QueryListener",
				"ArrowResultService",
			]
			.iter()
			.any(|symbol| line.contains(symbol))
		})
		.collect::<Vec<_>>();

	assert!(
		offenders.is_empty(),
		"Toron source contains ADR 042 public query service symbol(s):\n{}",
		format_offenders(&manifest_dir, &offenders)
	);
}

#[test]
pub(crate) fn adr042_observability_tracing_section_is_extended_not_replaced() {
	let config =
		std::fs::read_to_string(manifest_dir().join("src/config.rs")).expect("read config");
	let daemon = std::fs::read_to_string(manifest_dir().join("src/daemon.rs"))
		.expect("read Toron daemon source");

	assert!(config.contains("[observability]"));
	assert!(config.contains("level = \"info\""));
	assert!(config.contains("format = \"pretty\""));
	assert!(config.contains("pub level: String"));
	assert!(config.contains("pub format: TracingFormat"));
	assert!(daemon.contains("install_tracing"));
	assert!(daemon.contains("tracing_subscriber::fmt()"));
	assert!(daemon.contains("observability::spawn"));
}

#[test]
pub(crate) fn adr042_no_fixture_storage_in_production_daemon() {
	let manifest_dir = manifest_dir();
	let daemon = std::fs::read_to_string(manifest_dir.join("src/daemon.rs"))
		.expect("read Toron daemon source");
	let config = std::fs::read_to_string(manifest_dir.join("src/config.rs"))
		.expect("read Toron config source");
	let production = format!("{daemon}\n{config}");

	for forbidden in [
		"toron_fixture_worker",
		"FixtureStorageScan",
		"vals",
		"orders",
		"customers",
	] {
		assert!(
			!production.contains(forbidden),
			"production daemon/config contains fixture token {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr042_no_wendelon_node_in_toron_start() {
	let manifest_dir = manifest_dir();
	let daemon = std::fs::read_to_string(manifest_dir.join("src/daemon.rs"))
		.expect("read Toron daemon source");
	let config = std::fs::read_to_string(manifest_dir.join("src/config.rs"))
		.expect("read Toron config source");
	let production = format!("{daemon}\n{config}");

	for forbidden in ["WendelonNode", "raft_storage", "SlabManager", "data_server"] {
		assert!(
			!production.contains(forbidden),
			"Toron start production source contains Wendelon server token {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr042_fixture_worker_source_remains_isolated() {
	let fixture = std::fs::read_to_string(manifest_dir().join("src/bin/toron_fixture_worker.rs"))
		.expect("read fixture worker source");

	for forbidden in [
		"crate::daemon",
		"toron::daemon",
		"crate::config",
		"toron::config",
	] {
		assert!(
			!fixture.contains(forbidden),
			"fixture worker imports production daemon/config token {forbidden}"
		);
	}
}

#[test]
pub(crate) fn adr041_replace_refuses_before_any_catalog_mutation() {
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
	let create = brace_blocks_after_marker(&coordinator, "async fn execute_catalog_plan")
		.into_iter()
		.next()
		.expect("execute_catalog_plan helper")
		.1;
	let write = brace_blocks_after_marker(&coordinator, "async fn execute_write_plan_with_source")
		.into_iter()
		.next()
		.expect("shared write driver")
		.1;

	assert!(helper.contains(") -> Result<bool, QueryError>"));
	assert!(!helper.contains("Result<Option<TableBinding>"));
	let create_arm = create
		.split("ExecutionPlan::CreateTable")
		.nth(1)
		.and_then(|rest| rest.split("ExecutionPlan::DropTable").next())
		.expect("CreateTable arm");
	let refuse = create_arm
		.find("refuse_destructive_or_replace")
		.expect("plain CREATE destructive-replace refusal");
	let fresh_create = create_arm.find(".create_table").expect("fresh create");
	assert!(refuse < fresh_create);
	assert!(!create_arm.contains("drop_existing_table_with_storage_admin"));

	let ctas = write
		.split("ExecutionPlan::CreateTableAsSelect")
		.nth(1)
		.and_then(|rest| rest.split("ExecutionPlan::Delete").next())
		.expect("CTAS arm");
	let finish =
		brace_blocks_after_marker(&coordinator, "async fn finish_ctas_after_rows_committed")
			.into_iter()
			.next()
			.expect("staged publication helper")
			.1;
	assert!(finish.contains("CtasSagaMutation::Publish"));
	assert!(finish.contains("CtasSagaMutation::MarkRetiringOld"));
	assert!(!ctas.contains("refuse_destructive_or_replace"));
	assert!(!ctas.contains("drop_existing_table_with_storage_admin"));
}
