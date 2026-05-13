use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use arrow::array::{Int32Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use toron::binder::{PlannerConfig, bind};
use toron::catalog::MockCatalog;
use toron::error::{BindingError, StorageError};
use toron::exec::Executor;
use toron::ir::{Expr, LogicalPlan, ScalarValue, TableId};
use toron::storage::{ScanRequest, ScanStream, StorageScan};
use wendelon::schema::{
	LocalityPolicyV1, PrimaryKeyDescriptorV1, RowIdentityPolicy, SchemaEvolutionPolicyV1,
	TableObjectId, TableSchemaDescriptorV1,
};

#[test]
fn no_eval_time_string_lookup() {
	let manifest_dir = manifest_dir();
	let offenders = source_lines(&manifest_dir.join("src"))
		.into_iter()
		.filter(|(_, _, line)| line.contains("column_by_name"))
		.collect::<Vec<_>>();

	assert!(
		offenders.is_empty(),
		"RecordBatch::column_by_name found outside binding code:\n{}",
		format_offenders(&manifest_dir, &offenders)
	);
}

#[tokio::test]
async fn pbc_engaged_on_eligible_filter() {
	let batch = batch();
	let observations = Arc::new(Mutex::new(Vec::new()));
	let storage = Arc::new(RecordingStorageScan {
		batch,
		observations: observations.clone(),
	});
	let executor = Executor::new(storage);
	let mut catalog = MockCatalog::new();
	catalog
		.register_descriptor(TableId(1), descriptor())
		.expect("register descriptor");
	let logical = LogicalPlan::Filter {
		input: Box::new(LogicalPlan::Scan {
			table: TableId(1),
			projection: None,
		}),
		predicate: Expr::Gt(
			Box::new(Expr::Column("amount".to_string())),
			Box::new(Expr::Literal(ScalarValue::Int32(Some(100)))),
		),
	};
	let bound = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect("bind");

	executor
		.execute_bound(&bound, None)
		.await
		.expect("execute bound");

	let observations = observations.lock().unwrap();
	assert_eq!(observations.len(), 1);
	assert!(
		observations[0],
		"eligible filter did not send pbc_predicate"
	);
}

#[test]
fn no_public_result_string_boundaries() {
	let manifest_dir = manifest_dir();
	let result_marker = ["Result", "<"].concat();
	let string_marker = ["String", ">"].concat();
	let offenders = source_lines(&manifest_dir.join("src"))
		.into_iter()
		.filter(|(_, _, line)| {
			line.contains("pub ") && line.contains(&result_marker) && line.contains(&string_marker)
		})
		.collect::<Vec<_>>();

	assert!(
		offenders.is_empty(),
		"public Result<_, String> boundaries found:\n{}",
		format_offenders(&manifest_dir, &offenders)
	);
}

#[test]
fn execution_expr_has_no_string_column_variant() {
	let source = std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs"))
		.expect("read execution IR source");

	assert!(!source.contains("Column(String)"));
	assert!(source.contains("Column(ResolvedColumn)"));
}

#[tokio::test]
async fn experimental_kernels_are_gated() {
	let mut catalog = MockCatalog::new();
	catalog
		.register_descriptor(TableId(1), descriptor())
		.expect("register descriptor");
	let logical = LogicalPlan::Project {
		input: Box::new(LogicalPlan::Scan {
			table: TableId(1),
			projection: None,
		}),
		exprs: vec![Expr::TimeBucket(
			std::time::Duration::from_secs(60),
			Box::new(Expr::Column("ts".to_string())),
		)],
	};

	let err = bind(&logical, &catalog, &PlannerConfig::default())
		.await
		.expect_err("default planner must reject experimental kernels");
	assert!(matches!(err, BindingError::ExperimentalKernel { .. }));
}

#[test]
fn scan_request_requires_descriptor_pin() {
	let source = std::fs::read_to_string(manifest_dir().join("src/storage.rs"))
		.expect("read storage source");
	let scan_request = source
		.split("pub struct ScanRequest")
		.nth(1)
		.and_then(|rest| rest.split("pub struct ScanStream").next())
		.expect("ScanRequest block");

	assert!(
		scan_request
			.contains("pub physical_descriptor_fingerprint: PhysicalDescriptorFingerprint,")
	);
	assert!(!scan_request.contains("physical_descriptor_fingerprint: Option<"));
}

#[test]
fn scan_stream_collection_localized() {
	let manifest_dir = manifest_dir();
	let try_collect_marker = ["try", "_", "collect"].concat();
	let offenders = source_lines(&manifest_dir.join("src"))
		.into_iter()
		.filter(|(path, _, line)| {
			let relative = path.strip_prefix(&manifest_dir).unwrap_or(path);
			if relative == Path::new("src/coordinator.rs") || relative == Path::new("src/error.rs")
			{
				return false;
			}
			line.contains("ScanStream::collect")
				|| line.contains(".batches.collect()")
				|| line.contains(&try_collect_marker)
		})
		.collect::<Vec<_>>();

	assert!(
		offenders.is_empty(),
		"ScanStream collection outside coordinator boundary:\n{}",
		format_offenders(&manifest_dir, &offenders)
	);
}

struct RecordingStorageScan {
	batch: RecordBatch,
	observations: Arc<Mutex<Vec<bool>>>,
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
			vec![self.batch.clone()],
		))
	}
}

fn source_lines(root: &Path) -> Vec<(PathBuf, usize, String)> {
	let mut files = Vec::new();
	collect_rs_files(root, &mut files);
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

fn collect_rs_files(dir: &Path, out: &mut Vec<PathBuf>) {
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

fn format_offenders(manifest_dir: &Path, offenders: &[(PathBuf, usize, String)]) -> String {
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

fn manifest_dir() -> PathBuf {
	PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn descriptor() -> TableSchemaDescriptorV1 {
	let schema = Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("amount", DataType::Int32, true),
		Field::new("ts", DataType::Int64, true),
	]);
	TableSchemaDescriptorV1::new(
		TableObjectId::new([0xA2; 16]),
		1,
		"architecture",
		"sales",
		&schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("id")),
		LocalityPolicyV1::default_table_spread(),
		SchemaEvolutionPolicyV1::AdditiveOnly,
	)
	.expect("descriptor")
}

fn batch() -> RecordBatch {
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
