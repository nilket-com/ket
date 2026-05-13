use crate::ir::TableId;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageError {
	#[error("table not found: {table:?}")]
	TableNotFound { table: TableId },
	#[error("descriptor evolved during query planning/execution")]
	DescriptorEvolved,
	#[error("read bound unavailable")]
	ReadBoundUnavailable {
		#[source]
		source: wendelon_client::ClientError,
	},
	#[error("invalid PBC program for descriptor")]
	InvalidPbcForDescriptor {
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
			ClientError::TableDescriptorEvolved { .. } => Self::DescriptorEvolved,
			source @ ClientError::QuotaExceeded { .. }
			| source @ ClientError::TenantSuspended { .. }
			| source @ ClientError::TenantUnavailable { .. }
			| source @ ClientError::Io(_) => Self::Unavailable { source },
			source @ ClientError::ReadFailed { .. } => Self::ReadBoundUnavailable { source },
			source @ ClientError::TableWriteRejected { .. } => {
				Self::InvalidPbcForDescriptor { source }
			}
			source @ ClientError::WriteFailed { .. }
			| source @ ClientError::SchemaUpdateRejected { .. } => Self::Client { source },
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
	#[error("type mismatch: {message}")]
	TypeMismatch { message: String },
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
	RuneCompile { message: String },
	#[error("Rune runtime failed: {message}")]
	RuneRuntime { message: String },
	#[error("unsupported frontend construct: {message}")]
	Unsupported { message: String },
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
}

#[derive(Debug, Error)]
pub enum WireError {
	#[error("wire decode failed: {0}")]
	Decode(#[from] postcard::Error),
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
		LocalityPolicyV1, PbcRejectCode, PrimaryKeyDescriptorV1, ReadStatus, RowIdentityPolicy,
		SchemaEvolutionPolicyV1, TableObjectId, TableSchemaDescriptorV1, TenantId,
	};
	use wendelon_client::ClientError;

	#[test]
	fn binding_error_experimental_kernel_is_constructible() {
		let err = BindingError::ExperimentalKernel {
			kernel: "TimeBucket",
		};

		assert!(err.to_string().contains("TimeBucket"));
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
		match err {
			StorageError::InvalidPbcForDescriptor {
				source: ClientError::TableWriteRejected { reject_code },
			} => assert_eq!(reject_code, PbcRejectCode::PBC_REJECT_SCHEMA_STALE),
			other => panic!("unexpected storage error mapping: {other:?}"),
		}

		let err = StorageError::from(ClientError::TableDescriptorEvolved {
			expected: descriptor_fingerprint(),
			observed: Some(descriptor_fingerprint()),
		});
		assert!(matches!(err, StorageError::DescriptorEvolved));
	}

	fn descriptor_fingerprint() -> wendelon::schema::PhysicalDescriptorFingerprint {
		let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
		TableSchemaDescriptorV1::new(
			TableObjectId::new([0xEE; 16]),
			1,
			"local",
			"errors",
			&schema,
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("id")),
			LocalityPolicyV1::default_table_spread(),
			SchemaEvolutionPolicyV1::AdditiveOnly,
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
