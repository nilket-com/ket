pub mod distributed;

use crate::distributed::fragment::FragmentError;
use crate::ir::TableId;
use crate::rune_engine::RuneSandboxViolation;
pub use distributed::{
	EstablishmentFailure, PartitionMissingReason, RuntimeError, TaskFailureKind,
};

use thiserror::Error;
use wendelon::schema::PbcRejectCode;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum BudgetExhaustedDetail {
	#[default]
	None,
	Message(String),
}

impl BudgetExhaustedDetail {
	pub fn message(message: impl Into<String>) -> Self {
		Self::Message(message.into())
	}
}

impl std::fmt::Display for BudgetExhaustedDetail {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::None => Ok(()),
			Self::Message(message) => write!(f, ", detail {message}"),
		}
	}
}

#[derive(Debug, Error)]
pub enum StorageError {
	#[error("table not found: {table:?}")]
	TableNotFound { table: TableId },
	#[error("table was already dropped by a different durable operation: {table:?}")]
	TableAlreadyDropped { table: TableId },
	#[error("descriptor evolved for {table_id:?} during query planning/execution")]
	DescriptorEvolved { table_id: TableId },
	#[error("storage operation cancelled")]
	Cancelled,
	#[error("read bound unavailable")]
	ReadBoundUnavailable {
		#[source]
		source: wendelon_client::ClientError,
	},
	#[error(
		"table write rejected: {} (0x{:04X})",
		.reject_code.name(),
		.reject_code.0
	)]
	TableWriteRejected {
		reject_code: PbcRejectCode,
		#[source]
		source: wendelon_client::ClientError,
	},
	#[error("invalid PBC program: {message}")]
	InvalidPbcProgram { message: String },
	#[error("storage permission denied")]
	PermissionDenied {
		#[source]
		source: wendelon_client::ClientError,
	},
	#[error("storage unavailable")]
	Unavailable {
		#[source]
		source: wendelon_client::ClientError,
	},
	#[error("wendelon query failed: {}({})", .status.as_str(), .status.0)]
	QueryFailed {
		status: wendelon::schema::ReadStatus,
		#[source]
		source: wendelon_client::ClientError,
	},
	#[error("storage table reference integrity failure: {failure:?}")]
	TableReferenceIntegrity {
		failure: wendelon_client::TableReferenceIntegrityFailure,
		#[source]
		source: wendelon_client::ClientError,
	},
	#[error("storage client error")]
	Client {
		#[source]
		source: wendelon_client::ClientError,
	},
	#[error("storage arrow error: {0}")]
	Arrow(#[from] arrow::error::ArrowError),
	#[error("internal storage error: {message}")]
	Internal { message: String },
}

impl From<wendelon_client::ClientError> for StorageError {
	fn from(source: wendelon_client::ClientError) -> Self {
		use wendelon_client::ClientError;

		match source {
			source @ ClientError::AuthorizationFailed => Self::PermissionDenied { source },
			ClientError::Io(error)
				if error.kind() == std::io::ErrorKind::Interrupted
					&& (error.to_string().contains("QUERY_CANCELLED")
						|| error.to_string().contains("TABLE_QUERY_CANCELLED")) =>
			{
				Self::Cancelled
			}
			ClientError::TableDescriptorEvolved { .. } => Self::Internal {
				message: "descriptor evolved without table context".to_string(),
			},
			ClientError::TableReferenceIntegrityFailed { failure } => {
				Self::TableReferenceIntegrity {
					failure: failure.clone(),
					source: ClientError::TableReferenceIntegrityFailed { failure },
				}
			}
			ClientError::Io(error)
				if wendelon_client::table_reference_integrity_failure_from_io(&error).is_some() =>
			{
				let failure = wendelon_client::table_reference_integrity_failure_from_io(&error)
					.expect("integrity failure checked above");
				Self::TableReferenceIntegrity {
					failure,
					source: ClientError::Io(error),
				}
			}
			source @ ClientError::QueryFailed { .. } => {
				let ClientError::QueryFailed { status } = source else {
					unreachable!("bound by the arm pattern above");
				};
				Self::QueryFailed {
					status,
					source: ClientError::QueryFailed { status },
				}
			}
			source @ ClientError::QuotaExceeded { .. }
			| source @ ClientError::TenantSuspended { .. }
			| source @ ClientError::TenantUnavailable { .. }
			| source @ ClientError::Io(_) => Self::Unavailable { source },
			source @ ClientError::Admin(wendelon_client::WendelonAdminError::Transport {
				..
			}) => Self::Unavailable { source },
			source @ ClientError::ReadFailed { .. } => Self::ReadBoundUnavailable { source },
			ClientError::TableWriteRejected { reject_code } => Self::TableWriteRejected {
				reject_code,
				source: ClientError::TableWriteRejected { reject_code },
			},
			source @ ClientError::WriteFailed { .. }
			| source @ ClientError::MutationStopped { .. }
			| source @ ClientError::MutationAnswerInconsistent { .. }
			| source @ ClientError::SchemaUpdateRejected { .. }
			| source @ ClientError::Admin(_) => Self::Client { source },
		}
	}
}

#[derive(Debug, Error)]
pub enum BindingError {
	#[error(transparent)]
	Catalog(#[from] CatalogError),
	#[error("unknown column: {column}")]
	UnknownColumn { column: String },
	#[error("ambiguous column: {column}")]
	AmbiguousColumn { column: String },
	#[error("ambiguous column reference: {name}")]
	AmbiguousColumnRef { name: String, found_in: Vec<String> },
	#[error("unknown table reference: {qualifier}")]
	UnknownTableRef { qualifier: String },
	#[error("ambiguous table reference: {qualifier}")]
	AmbiguousTableRef {
		qualifier: String,
		found_in: Vec<String>,
	},
	#[error("unknown column {column} in table reference {qualifier}")]
	UnknownColumnInRelation {
		qualifier: String,
		column: String,
		available: Vec<String>,
	},
	#[error("join predicate is not an equality predicate")]
	JoinPredicateNotEqui,
	#[error("join key type mismatch: left {left_type}, right {right_type}")]
	JoinKeyTypeMismatch {
		left_type: String,
		right_type: String,
	},
	#[error("unsupported join key type: {type_name}")]
	JoinKeyTypeUnsupported { type_name: String },
	#[error("computed join keys are not supported")]
	JoinComputedKeyUnsupported,
	#[error("join exchange partition count mismatch: left {left}, right {right}")]
	JoinPartitionCountMismatch { left: u32, right: u32 },
	#[error("join exchange keys do not match join keys")]
	JoinKeyExchangeMismatch,
	#[error("broadcast join build side must be the left published input")]
	BroadcastBuildSidePlacement,
	#[error("broadcast cap exceeded at bind for {dimension}: estimated {estimated}, cap {cap}")]
	BroadcastCapExceededAtBind {
		dimension: String,
		estimated: u64,
		cap: u64,
	},
	#[error("type mismatch: {message}")]
	TypeMismatch { message: String },
	#[error("nominal type mismatch: {left}({physical}) vs {right}({physical})")]
	NominalTypeMismatch {
		left: String,
		right: String,
		physical: String,
	},
	#[error("experimental kernel is not enabled: {kernel}")]
	ExperimentalKernel { kernel: &'static str },
	#[error("unresolved table: {table}")]
	UnresolvedTable { table: String },
	#[error("unsupported expression: {message}")]
	UnsupportedExpression { message: String },
}

#[derive(Debug, Error)]
pub enum CatalogError {
	#[error("table not found: {table}")]
	TableNotFound { table: String },
	#[error("schema not found: {schema}")]
	SchemaNotFound { schema: String },
	#[error("table already exists: {table}")]
	TableAlreadyExists { table: String },
	#[error("logical type not found: {name}")]
	LogicalTypeNotFound { name: String },
	#[error("existing table schema does not match CREATE TABLE IF NOT EXISTS: {table}")]
	SchemaMismatch { table: String },
	#[error("catalog admin operation unavailable: {message}")]
	AdminUnavailable { message: String },
	#[error("catalog admin operation failed: {message}")]
	AdminFailed { message: String },
	#[error("catalog resource exhausted: {message}")]
	ResourceExhausted { message: String },
	#[error("fingerprint resolution failed: {message}")]
	FingerprintResolution { message: String },
	#[error(transparent)]
	Storage(#[from] StorageError),
}

#[derive(Debug, Error)]
pub enum FrontendError {
	#[error("SQL parse failed: {message}")]
	SqlParse { message: String },
	#[error("Rune compile failed: {message}")]
	RuneCompile {
		message: String,
		span: Option<RuneDiagnosticSpan>,
	},
	#[error("Rune runtime failed: {message}")]
	RuneRuntime {
		message: String,
		span: Option<RuneDiagnosticSpan>,
	},
	#[error("Rune sandbox violation: {message}")]
	RuneSandbox {
		kind: RuneSandboxViolation,
		message: String,
	},
	#[error("unsupported frontend construct: {message}")]
	Unsupported { message: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuneDiagnosticSpan {
	pub start: u32,
	pub end: u32,
}

#[derive(Debug, Error)]
pub enum ExecutionError {
	#[error(transparent)]
	Storage(#[from] StorageError),
	#[error("compute error: {0}")]
	Compute(#[from] arrow::error::ArrowError),
	#[error(transparent)]
	Binding(#[from] BindingError),
	#[error("not implemented: {feature}")]
	NotImplemented { feature: &'static str },
	#[error("column not found: {name}")]
	ColumnNotFound { name: String },
	#[error("invalid expression: {message}")]
	InvalidExpression { message: String },
	#[error("kernel error: {message}")]
	Kernel { message: String },
	#[error(
		"budget exhausted: {dimension:?}, producer {producer}, requested {requested:?}, available {available:?}{detail}"
	)]
	BudgetExhausted {
		dimension: crate::distributed::budget::BudgetDimension,
		producer: &'static str,
		requested: crate::distributed::budget::ByteCount,
		available: crate::distributed::budget::ByteCount,
		detail: BudgetExhaustedDetail,
	},
	#[error(
		"tenant concurrency exhausted: tenant {tenant_id:?}, requested {requested}, available {available}"
	)]
	ConcurrencyExhausted {
		tenant_id: crate::storage::TenantId,
		requested: u32,
		available: u32,
	},
}

#[derive(Debug, Error)]
pub enum WireError {
	#[error("wire decode failed: {0}")]
	Decode(#[from] postcard::Error),
	#[error("wire frame is too large to address: {payload_len} bytes")]
	FrameTooLarge { payload_len: usize },
}

#[derive(Debug, Error)]
pub enum SpillError {
	#[error("spill random generation failed during {operation}: {message}")]
	Random {
		operation: &'static str,
		message: String,
	},
	#[error("spill encryption failed during {operation}: {message}")]
	Crypto {
		operation: &'static str,
		message: String,
	},
	#[error("spill id not found: {id}")]
	MissingSpill { id: u64 },
	#[error("spill file is empty: {id}")]
	EmptySpill { id: u64 },
	#[error("spill IO error: {0}")]
	Io(#[from] std::io::Error),
	#[error("spill arrow error: {0}")]
	Arrow(#[from] arrow::error::ArrowError),
}

#[derive(Debug, Error)]
pub enum QueryError {
	#[error(transparent)]
	Fragment(#[from] FragmentError),
	#[error(transparent)]
	Execution(#[from] ExecutionError),
	#[error(transparent)]
	Storage(#[from] StorageError),
	#[error(transparent)]
	Binding(#[from] BindingError),
	#[error(transparent)]
	Catalog(#[from] CatalogError),
	#[error(transparent)]
	Frontend(#[from] FrontendError),
	#[error(transparent)]
	Wire(#[from] WireError),
	#[error(transparent)]
	Spill(#[from] SpillError),
	#[error(transparent)]
	Runtime(#[from] RuntimeError),
	#[error("query IO error: {0}")]
	Io(#[from] std::io::Error),
	#[error("query arrow error: {0}")]
	Arrow(#[from] arrow::error::ArrowError),
	#[error("remote worker error: {message}")]
	Remote { message: String },
	#[error("unexpected message: expected {expected}")]
	UnexpectedMessage { expected: &'static str },
	#[error("schema changed repeatedly during query admission/execution")]
	SchemaInstability,
	#[error("internal query error: {message}")]
	Internal { message: String },
}

#[cfg(test)]
mod tests {
	use super::*;
	use arrow::datatypes::{DataType, Field, Schema};
	use std::path::{Path, PathBuf};
	use wendelon::schema::{
		LocalityPolicy, PbcRejectCode, PrimaryKeyDescriptor, ReadStatus, RowIdentityPolicy,
		SchemaEvolutionPolicy, TableObjectId, TableSchemaDescriptor, TenantId,
	};
	use wendelon_client::ClientError;

	#[test]
	fn binding_error_experimental_kernel_is_constructible() {
		// `LinearRegress` is the gate's sole remaining member after ADR 549
		// graduated `TIME_BUCKET`; naming a graduated kernel here would document a
		// refusal that no longer happens.
		let err = BindingError::ExperimentalKernel {
			kernel: "LinearRegress",
		};

		assert!(err.to_string().contains("LinearRegress"));
	}

	#[test]
	fn wendelon_client_errors_preserve_origin() {
		let err = StorageError::from(ClientError::AuthorizationFailed);
		assert!(matches!(
			err,
			StorageError::PermissionDenied {
				source: ClientError::AuthorizationFailed
			}
		));

		let err = StorageError::from(ClientError::TenantUnavailable {
			tenant_id: TenantId::new(7),
		});
		assert!(matches!(
			err,
			StorageError::Unavailable {
				source: ClientError::TenantUnavailable { .. }
			}
		));

		let err = StorageError::from(ClientError::ReadFailed {
			status: ReadStatus::RETENTION_EXPIRED,
		});
		match err {
			StorageError::ReadBoundUnavailable {
				source: ClientError::ReadFailed { status },
			} => assert_eq!(status, ReadStatus::RETENTION_EXPIRED),
			other => panic!("unexpected storage error mapping: {other:?}"),
		}

		let err = StorageError::from(ClientError::TableWriteRejected {
			reject_code: PbcRejectCode::PBC_REJECT_SCHEMA_STALE,
		});
		assert_eq!(
			err.to_string(),
			"table write rejected: PBC_REJECT_SCHEMA_STALE (0x3701)"
		);
		match err {
			StorageError::TableWriteRejected {
				reject_code: stored_code,
				source: ClientError::TableWriteRejected { reject_code },
			} => {
				assert_eq!(stored_code, PbcRejectCode::PBC_REJECT_SCHEMA_STALE);
				assert_eq!(reject_code, PbcRejectCode::PBC_REJECT_SCHEMA_STALE);
			}
			other => panic!("unexpected storage error mapping: {other:?}"),
		}

		let err = StorageError::from(ClientError::TableDescriptorEvolved {
			expected: descriptor_fingerprint(),
			observed: Some(descriptor_fingerprint()),
		});
		assert!(matches!(err, StorageError::Internal { .. }));

		let failure = wendelon_client::TableReferenceIntegrityFailure {
			tenant_id: TenantId::new(7),
			table_object_id: TableObjectId::new([0xA5; 16]),
			reference_kind: 1,
			missing_slab_id: 4,
			origin: 2,
		};
		let err = StorageError::from(ClientError::TableReferenceIntegrityFailed {
			failure: failure.clone(),
		});
		match err {
			StorageError::TableReferenceIntegrity {
				failure: actual, ..
			} => assert_eq!(actual, failure),
			other => panic!("integrity diagnosis degraded at Toron boundary: {other:?}"),
		}
	}

	fn descriptor_fingerprint() -> wendelon::schema::PhysicalDescriptorFingerprint {
		let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
		TableSchemaDescriptor::new(
			TableObjectId::new([0xEE; 16]),
			1,
			"local",
			"errors",
			&schema,
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
			LocalityPolicy::default_table_spread(),
			SchemaEvolutionPolicy::AdditiveOnly,
		)
		.expect("descriptor")
		.physical_descriptor_fingerprint
	}

	#[test]
	fn architecture_guard_no_public_result_string_boundaries() {
		let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
		let mut files = Vec::new();
		collect_rs_files(&manifest_dir.join("src"), &mut files);

		let result_marker = ["Result", "<"].concat();
		let string_marker = ["String", ">"].concat();
		let mut offenders = Vec::new();

		for file in files {
			let source = std::fs::read_to_string(&file).expect("read source file");
			for (line_idx, line) in source.lines().enumerate() {
				if line.contains("pub ")
					&& line.contains(&result_marker)
					&& line.contains(&string_marker)
				{
					offenders.push(format!(
						"{}:{}: {}",
						file.strip_prefix(&manifest_dir).unwrap_or(&file).display(),
						line_idx + 1,
						line.trim()
					));
				}
			}
		}

		assert!(
			offenders.is_empty(),
			"public Result<_, String> boundaries found:\n{}",
			offenders.join("\n")
		);
	}

	#[test]
	fn architecture_guard_no_record_batch_name_lookup() {
		let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
		let mut files = Vec::new();
		collect_rs_files(&manifest_dir.join("src"), &mut files);

		let forbidden = ["column", "_by_", "name"].concat();
		let mut offenders = Vec::new();

		for file in files {
			let source = std::fs::read_to_string(&file).expect("read source file");
			for (line_idx, line) in source.lines().enumerate() {
				if line.contains(&forbidden) {
					offenders.push(format!(
						"{}:{}: {}",
						file.strip_prefix(&manifest_dir).unwrap_or(&file).display(),
						line_idx + 1,
						line.trim()
					));
				}
			}
		}

		assert!(
			offenders.is_empty(),
			"RecordBatch column-name runtime lookups found:\n{}",
			offenders.join("\n")
		);
	}

	#[test]
	fn architecture_guard_scan_request_requires_descriptor_pin() {
		let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
		let source = std::fs::read_to_string(manifest_dir.join("src/storage.rs"))
			.expect("read storage source");
		let scan_request = source
			.split_once("pub struct ScanRequest {")
			.and_then(|(_, rest)| rest.split_once("\n}"))
			.map(|(body, _)| body)
			.expect("ScanRequest source body");

		assert!(scan_request.contains("pub table_object_id: TableObjectId,"));
		assert!(
			scan_request
				.contains("pub physical_descriptor_fingerprint: PhysicalDescriptorFingerprint,")
		);
		assert!(!scan_request.contains("table_object_id: Option<"));
		assert!(!scan_request.contains("physical_descriptor_fingerprint: Option<"));
	}

	#[test]
	fn architecture_guard_scan_stream_collection_localized() {
		let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
		let mut files = Vec::new();
		collect_rs_files(&manifest_dir.join("src"), &mut files);
		let mut offenders = Vec::new();
		let try_collect_marker = ["try", "_", "collect"].concat();
		let scan_stream_collect_marker = ["ScanStream", "::", "collect"].concat();
		let batches_collect_marker = [".batches", ".", "collect"].concat();

		for file in files {
			let relative = file.strip_prefix(&manifest_dir).unwrap_or(&file);
			if relative == Path::new("src/error.rs") {
				continue;
			}
			let source = std::fs::read_to_string(&file).expect("read source file");
			for (line_idx, line) in source.lines().enumerate() {
				let stream_collection = line.contains(&try_collect_marker)
					|| line.contains(&scan_stream_collect_marker)
					|| line.contains(&batches_collect_marker);
				if stream_collection && relative != Path::new("src/coordinator.rs") {
					offenders.push(format!(
						"{}:{}: {}",
						relative.display(),
						line_idx + 1,
						line.trim()
					));
				}
			}
		}

		assert!(
			offenders.is_empty(),
			"ScanStream collection outside coordinator:\n{}",
			offenders.join("\n")
		);
	}

	fn collect_rs_files(dir: &Path, files: &mut Vec<PathBuf>) {
		for entry in std::fs::read_dir(dir).expect("read source directory") {
			let entry = entry.expect("source directory entry");
			let path = entry.path();
			if path.is_dir() {
				collect_rs_files(&path, files);
			} else if path.extension().is_some_and(|ext| ext == "rs") {
				files.push(path);
			}
		}
	}
}

#[cfg(test)]
mod adr540_query_status_tests {
	use super::*;

	/// The typed status arm outranks the Unavailable catch-all, the rendering names
	/// the status with its numeric value, and `storage unavailable` is reserved for
	/// failures that genuinely carry no server status.
	#[test]
	fn named_status_maps_typed_and_unavailable_stays_reserved() {
		let status = wendelon::schema::ReadStatus::QUERY_BUDGET_EXCEEDED;
		let mapped = StorageError::from(wendelon_client::ClientError::QueryFailed { status });
		assert!(matches!(
			mapped,
			StorageError::QueryFailed {
				status: wendelon::schema::ReadStatus::QUERY_BUDGET_EXCEEDED,
				..
			}
		));
		assert_eq!(
			mapped.to_string(),
			"wendelon query failed: query_budget_exceeded(11)"
		);

		let transport = StorageError::from(wendelon_client::ClientError::Io(std::io::Error::new(
			std::io::ErrorKind::ConnectionReset,
			"peer reset",
		)));
		assert_eq!(transport.to_string(), "storage unavailable");
	}
}
