#![allow(dead_code, unused_imports)]

pub(crate) use std::path::{Path, PathBuf};
pub(crate) use std::sync::{Arc, Mutex};

pub(crate) use arrow::array::{Int32Array, Int64Array};
pub(crate) use arrow::datatypes::{DataType, Field, Schema};
pub(crate) use arrow::record_batch::RecordBatch;
pub(crate) use toron::binder::{PlannerConfig, bind};
pub(crate) use toron::catalog::MockCatalog;
pub(crate) use toron::distributed::identity::WorkerId;
pub(crate) use toron::error::{BindingError, StorageError};
pub(crate) use toron::exec::Executor;
pub(crate) use toron::ir::{CreateTableRowIdentity, Expr, LogicalPlan, ScalarValue, TableId};
pub(crate) use toron::network::tls::{
	DEFAULT_CLUSTER_DOMAIN, ListenerRole, PeerKind, admit_san, canonical_client_dns_name,
	canonical_daemon_dns_name, canonical_monitor_dns_name,
};
pub(crate) use toron::storage::{ScanRequest, ScanStream, StorageScan, TenantId};
pub(crate) use wendelon::schema::{
	LocalityPolicy, PrimaryKeyDescriptor, RowIdentityPolicy, SchemaEvolutionPolicy, TableObjectId,
	TableSchemaDescriptor,
};

pub(crate) fn enum_variant_count(enum_body: &str) -> usize {
	enum_body
		.lines()
		.filter(|line| {
			let trimmed = line.trim_start();
			trimmed
				.chars()
				.next()
				.is_some_and(|first| first.is_ascii_uppercase())
				&& (trimmed.ends_with(',') || trimmed.ends_with('{'))
		})
		.count()
}

pub(crate) fn derive_items_before(source: &str, marker: &str) -> Vec<String> {
	let before = source
		.split(marker)
		.next()
		.unwrap_or_else(|| panic!("missing marker {marker}"));
	let derives = before.rsplit("#[derive").next().unwrap_or("");
	derives
		.split(')')
		.next()
		.and_then(|line| line.split('(').nth(1))
		.unwrap_or("")
		.split(',')
		.map(str::trim)
		.filter(|item| !item.is_empty())
		.map(ToOwned::to_owned)
		.collect()
}

pub(crate) struct RecordingStorageScan {
	pub(crate) batch: RecordBatch,
	pub(crate) observations: Arc<Mutex<Vec<bool>>>,
}

#[async_trait::async_trait]
impl StorageScan for RecordingStorageScan {
	async fn scan(&self, request: ScanRequest) -> Result<ScanStream, StorageError> {
		self.observations
			.lock()
			.unwrap()
			.push(request.pbc_predicate.is_some());
		Ok(ScanStream::from_batches(
			self.batch.schema(),
			request.session_id,
			request.query_id,
			toron::storage::ScanOrigin::BaseRelation,
			vec![self.batch.clone()],
		))
	}
}

pub(crate) fn wendelon_catalog_method_body(source: &str, method: &str) -> String {
	let impl_block = source
		.split("impl Catalog for WendelonCatalog")
		.nth(1)
		.expect("WendelonCatalog Catalog impl");
	brace_blocks_after_marker(impl_block, method)
		.into_iter()
		.next()
		.unwrap_or_else(|| panic!("{method} body"))
		.1
}

pub(crate) fn source_lines(root: &Path) -> Vec<(PathBuf, usize, String)> {
	let files = source_files(root);
	files
		.into_iter()
		.flat_map(|file| {
			std::fs::read_to_string(&file)
				.expect("read source")
				.lines()
				.enumerate()
				.map(|(idx, line)| (file.clone(), idx + 1, line.to_string()))
				.collect::<Vec<_>>()
		})
		.collect()
}

pub(crate) fn toron_source_lines_for_adr039_negative_guards() -> Vec<(PathBuf, usize, String)> {
	let manifest_dir = manifest_dir();
	let architecture_test = manifest_dir.join("tests/architecture_test.rs");
	let architecture_dir = manifest_dir.join("tests/architecture");
	let mut lines = source_lines(&manifest_dir.join("src"));
	lines.extend(
		source_lines(&manifest_dir.join("tests"))
			.into_iter()
			.filter(|(path, _, _)| {
				path != &architecture_test && !path.starts_with(&architecture_dir)
			}),
	);
	lines
}

pub(crate) fn assert_toron_sources_do_not_contain(forbidden: &[&str]) {
	let manifest_dir = manifest_dir();
	let offenders = toron_source_lines_for_adr039_negative_guards()
		.into_iter()
		.filter(|(_, _, line)| forbidden.iter().any(|symbol| line.contains(symbol)))
		.collect::<Vec<_>>();

	assert!(
		offenders.is_empty(),
		"Toron source contains forbidden symbol(s):\n{}",
		format_offenders(&manifest_dir, &offenders)
	);
}

pub(crate) fn assert_bounded_resource_tenant_labels_only(observability: &str) {
	for metric in [
		"tenant_memory_bytes_used",
		"tenant_memory_bytes_limit",
		"tenant_concurrency_active",
		"tenant_concurrency_limit",
		"tenant_admission_rejections_total",
	] {
		assert!(
			observability.contains(metric),
			"resource metric {metric} missing from observability source"
		);
	}
	for general_metric in [
		"listener_up",
		"listener_accepts_total",
		"listener_rejects_total",
		"metrics_scrapes_total",
		"daemon_ready",
		"daemon_uptime_seconds",
		"descriptors_loaded",
		"static_workers_configured",
	] {
		let offenders = observability
			.lines()
			.filter(|line| line.contains(general_metric) && line.contains("tenant_id"))
			.collect::<Vec<_>>();
		assert!(
			offenders.is_empty(),
			"general metric {general_metric} gained a tenant_id label: {offenders:?}"
		);
	}
	assert!(observability.contains("tenant_id=\\\"{label}\\\""));
	assert!(observability.contains("\"other\""));
	assert!(observability.contains("configured_tenant_ids"));
}

pub(crate) fn source_files(root: &Path) -> Vec<PathBuf> {
	let mut files = Vec::new();
	collect_rs_files(root, &mut files);
	files.sort();
	files
}

pub(crate) fn binder_root_source() -> String {
	std::fs::read_to_string(manifest_dir().join("src/binder.rs")).expect("read binder root source")
}

pub(crate) fn binder_source_files() -> Vec<PathBuf> {
	let src = manifest_dir().join("src");
	let module_root = src.join("binder");
	let mut production = Vec::new();
	let mut tests = Vec::new();

	if module_root.exists() {
		for path in source_files(&module_root) {
			let is_test_file = path
				.strip_prefix(&module_root)
				.ok()
				.and_then(|relative| relative.components().next())
				.is_some_and(|component| component.as_os_str() == std::ffi::OsStr::new("tests"));
			if is_test_file || path.file_name() == Some(std::ffi::OsStr::new("tests.rs")) {
				tests.push(path);
			} else {
				production.push(path);
			}
		}
	}

	production.sort();
	tests.sort();

	let mut files = vec![src.join("binder.rs")];
	files.extend(production);
	files.extend(tests);
	files
}

pub(crate) fn binder_source() -> String {
	binder_source_files()
		.into_iter()
		.map(|path| std::fs::read_to_string(path).expect("read binder source"))
		.collect::<Vec<_>>()
		.join("\n")
}

pub(crate) fn exec_root_source() -> String {
	std::fs::read_to_string(manifest_dir().join("src/exec.rs")).expect("read exec root source")
}

pub(crate) fn exec_join_root_source() -> String {
	std::fs::read_to_string(manifest_dir().join("src/exec/join.rs"))
		.expect("read exec join root source")
}

pub(crate) fn exec_source_files() -> Vec<PathBuf> {
	let src = manifest_dir().join("src");
	let module_root = src.join("exec");
	let mut files = vec![src.join("exec.rs")];
	if module_root.exists() {
		files.extend(source_files(&module_root));
	}
	files
}

pub(crate) fn exec_source() -> String {
	exec_source_files()
		.into_iter()
		.map(|path| std::fs::read_to_string(path).expect("read exec source"))
		.collect::<Vec<_>>()
		.join("\n")
}

pub(crate) fn wendelon_storage_manager_source() -> String {
	let src = wendelon_manifest_dir().join("src/storage");
	let module_root = src.join("manager");
	let mut files = vec![src.join("manager.rs")];
	if module_root.exists() {
		files.extend(source_files(&module_root));
	}
	files
		.into_iter()
		.map(|path| std::fs::read_to_string(path).expect("read Wendelon storage manager source"))
		.collect::<Vec<_>>()
		.join("\n")
}

pub(crate) fn architecture_corpus_source() -> String {
	let architecture_dir = manifest_dir().join("tests/architecture");
	source_files(&architecture_dir)
		.into_iter()
		.map(|path| std::fs::read_to_string(path).expect("read architecture source"))
		.collect::<Vec<_>>()
		.join("\n")
}

pub(crate) fn collect_rs_files(dir: &Path, out: &mut Vec<PathBuf>) {
	for entry in std::fs::read_dir(dir).expect("read source dir") {
		let entry = entry.expect("read dir entry");
		let path = entry.path();
		if path.is_dir() {
			collect_rs_files(&path, out);
		} else if path.extension().and_then(|ext| ext.to_str()) == Some("rs") {
			out.push(path);
		}
	}
}

pub(crate) fn brace_blocks_after_marker(source: &str, marker: &str) -> Vec<(usize, String)> {
	let mut blocks = Vec::new();
	let mut search_start = 0usize;
	while let Some(offset) = source[search_start..].find(marker) {
		let start = search_start + offset;
		let line = source[..start].matches('\n').count() + 1;
		let mut depth = 0usize;
		let mut saw_open = false;
		let mut end = None;
		for (relative_idx, ch) in source[start..].char_indices() {
			match ch {
				'{' => {
					saw_open = true;
					depth += 1;
				}
				'}' if saw_open => {
					depth = depth.saturating_sub(1);
					if depth == 0 {
						end = Some(start + relative_idx + ch.len_utf8());
						break;
					}
				}
				_ => {}
			}
		}
		if let Some(end) = end {
			blocks.push((line, source[start..end].to_string()));
			search_start = end;
		} else {
			blocks.push((line, source[start..].to_string()));
			break;
		}
	}
	blocks
}

pub(crate) fn format_offenders(
	manifest_dir: &Path,
	offenders: &[(PathBuf, usize, String)],
) -> String {
	offenders
		.iter()
		.map(|(file, line, source)| {
			format!(
				"{}:{line}: {}",
				file.strip_prefix(manifest_dir).unwrap_or(file).display(),
				source.trim()
			)
		})
		.collect::<Vec<_>>()
		.join("\n")
}

pub(crate) fn manifest_dir() -> PathBuf {
	PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

pub(crate) fn wendelon_manifest_dir() -> PathBuf {
	manifest_dir()
		.parent()
		.expect("crates directory")
		.join("wendelon")
}

pub(crate) fn workspace_root_dir() -> PathBuf {
	manifest_dir()
		.parent()
		.expect("crates directory")
		.parent()
		.expect("workspace root")
		.to_path_buf()
}

pub(crate) fn tls_pki_source_text() -> String {
	let src_dir = workspace_root_dir().join("crates/tls-pki/src");
	source_files(&src_dir)
		.into_iter()
		.map(|path| std::fs::read_to_string(path).expect("read tls-pki source"))
		.collect::<Vec<_>>()
		.join("\n")
}

pub(crate) fn observability_http_source_text() -> String {
	let src_dir = workspace_root_dir().join("crates/observability/src/http");
	source_files(&src_dir)
		.into_iter()
		.map(|path| std::fs::read_to_string(path).expect("read observability::http source"))
		.collect::<Vec<_>>()
		.join("\n")
}

pub(crate) fn observability_audit_source_text() -> String {
	let src_dir = workspace_root_dir().join("crates/observability/src/audit");
	source_files(&src_dir)
		.into_iter()
		.map(|path| std::fs::read_to_string(path).expect("read observability::audit source"))
		.collect::<Vec<_>>()
		.join("\n")
}

pub(crate) fn observability_telemetry_source_text() -> String {
	let src_dir = workspace_root_dir().join("crates/observability/src/telemetry");
	source_files(&src_dir)
		.into_iter()
		.map(|path| std::fs::read_to_string(path).expect("read observability::telemetry source"))
		.collect::<Vec<_>>()
		.join("\n")
}

pub(crate) fn observability_manifest() -> String {
	std::fs::read_to_string(workspace_root_dir().join("crates/observability/Cargo.toml"))
		.expect("read observability manifest")
}

pub(crate) fn workspace_cargo_tomls() -> Vec<PathBuf> {
	let workspace_root = workspace_root_dir();
	let mut manifests = Vec::new();
	for relative_root in ["crates", "crates/wendelon/crates"] {
		let root = workspace_root.join(relative_root);
		for entry in std::fs::read_dir(root).expect("read workspace crate dir") {
			let path = entry.expect("read dir entry").path();
			let cargo_toml = path.join("Cargo.toml");
			if cargo_toml.exists() {
				manifests.push(cargo_toml);
			}
		}
	}
	manifests
}

pub(crate) fn descriptor() -> TableSchemaDescriptor {
	let schema = Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("amount", DataType::Int32, true),
		Field::new("ts", DataType::Int64, true),
	]);
	TableSchemaDescriptor::new(
		TableObjectId::new([0xA2; 16]),
		1,
		"architecture",
		"sales",
		&schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
		LocalityPolicy::default_table_spread(),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.expect("descriptor")
}

pub(crate) fn batch() -> RecordBatch {
	let schema = Arc::new(Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("amount", DataType::Int32, true),
		Field::new("ts", DataType::Int64, true),
	]));
	RecordBatch::try_new(
		schema,
		vec![
			Arc::new(Int64Array::from(vec![1, 2, 3])),
			Arc::new(Int32Array::from(vec![Some(10), Some(150), Some(200)])),
			Arc::new(Int64Array::from(vec![1_000, 2_000, 3_000])),
		],
	)
	.expect("batch")
}
