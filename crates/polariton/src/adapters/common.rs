use crate::result_artifact::ResultArtifact;
use async_trait::async_trait;
#[cfg(feature = "toron")]
use std::ops::Range;
use std::sync::Arc;

#[derive(Clone, Debug, Default)]
pub enum AdapterStage {
	#[default]
	None,
	Unselected,
	Unconfigured,
	Configured,
	Connected,
}

#[derive(Clone, Debug)]
pub enum AdapterFieldType {
	Text,
}

#[derive(Clone, Debug)]
pub struct AdapterField {
	pub key: &'static str,
	pub placeholder: &'static str,
	pub default_value: Option<&'static str>,
	pub field_type: &'static AdapterFieldType,
	pub is_secure: bool,
	/// ADR 0165 §4: optional fields form one group; the adapter decides
	/// whether a partial group is refused. The form requires every field
	/// that is not optional and not secure.
	pub is_optional: bool,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub enum QueryType {
	Read,
	Write,
	Schema,
	Control,
}

#[allow(dead_code)]
#[derive(Clone, Debug, Default)]
pub enum ExecutionResult {
	#[default]
	None,
	Affected(u64),
	Batch {
		statements: usize,
	},
	CommandCompleted(String),
	Err(String),
	Rows {
		artifact: Arc<ResultArtifact>,
		statements: usize,
	},
}

#[cfg(feature = "toron")]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RuneDispatchFailureKind {
	Unavailable,
	Compile,
	Runtime,
	Sandbox,
	VersionSkew,
	Execution,
	Transport,
}

#[cfg(feature = "toron")]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuneDispatchFailure {
	pub kind: RuneDispatchFailureKind,
	pub message: String,
	pub span: Option<Range<u32>>,
}

/// PRODUCT-0137 §7: what a materialization reports to the host. Built from
/// the wire response at the adapter boundary so `toron_client` types do not
/// leak past `adapters/`. The certificate's format version is reported as
/// recorded; Polariton never decodes certificate bytes.
#[cfg(feature = "toron")]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MaterializationReceipt {
	/// Resolved namespace, carried beside the name rather than dotted into
	/// it — `TableRef::new` does not split a qualified string.
	pub namespace: String,
	pub table_name: String,
	pub certificate_format_version: u16,
	/// Rows written to the new table. A materialization returns no rows to
	/// the caller, so this deliberately does not share the `row_count` name.
	pub rows_written: u64,
}

/// PRODUCT-0137: a Rune dispatch succeeds as a relation or as a
/// materialization. A closed enum rather than an artifact plus an optional
/// receipt: an outcome that can carry both is one that can carry neither
/// honestly, and any `None` to interpret reopens the empty-relation trap
/// TORON-0574 was written to close.
#[cfg(feature = "toron")]
#[derive(Clone, Debug)]
pub enum RuneDispatchSuccess {
	Relation(Arc<ResultArtifact>),
	Materialized(MaterializationReceipt),
}

#[cfg(all(test, feature = "toron"))]
impl RuneDispatchSuccess {
	/// Test accessor for the relation half. Production consumers match both
	/// variants explicitly; a test that expects rows says so.
	pub fn expect_relation(self) -> Arc<ResultArtifact> {
		match self {
			RuneDispatchSuccess::Relation(artifact) => artifact,
			RuneDispatchSuccess::Materialized(receipt) => {
				panic!(
					"expected a relation, got a materialization of {}",
					receipt.table_name
				)
			}
		}
	}
}

#[cfg(feature = "toron")]
pub type RuneDispatchResult = Result<RuneDispatchSuccess, RuneDispatchFailure>;

/// PRODUCT-0135 §2: an engine-side cancellation signal. Implementations must
/// reach their engine over a path that can never wait on the adapter's
/// `RwLock` — the active query owns that lock.
#[async_trait]
pub trait EngineCancel: Send + Sync {
	async fn signal(&self) -> Result<(), String>;
}

/// PRODUCT-0135 §2/§3: the out-of-band cancellation capability, acquired at
/// connect and held beside the connection, never behind it. The variants are
/// the accepted table's honest outcomes.
#[derive(Clone, Default)]
pub enum CancelCapability {
	/// The engine can be told to stop.
	Engine(Arc<dyn EngineCancel>),
	/// Abandon the client future; the engine may keep finishing. The default:
	/// it is the scratch fallback's row.
	#[default]
	StopWaiting,
	/// Neither cancellation nor abandonment takes effect mid-query; the
	/// affordance refuses rather than pretending.
	Refuse,
}

#[async_trait]
pub trait DatabaseAdapter: Send + Sync + 'static {
	async fn dispatch(&mut self, code: &str) -> ExecutionResult;

	#[cfg(feature = "toron")]
	async fn dispatch_rune(&mut self, _source: &str) -> RuneDispatchResult {
		Err(RuneDispatchFailure {
			kind: RuneDispatchFailureKind::Unavailable,
			message: "The selected adapter does not provide Rune relation execution.".to_owned(),
			span: None,
		})
	}
}

#[cfg(all(test, feature = "toron"))]
mod tests {
	use super::*;

	struct NoRuneWorkspace;

	#[async_trait]
	impl DatabaseAdapter for NoRuneWorkspace {
		async fn dispatch(&mut self, _code: &str) -> ExecutionResult {
			ExecutionResult::Err("unused".to_owned())
		}
	}

	#[tokio::test]
	async fn adapter_without_rune_capability_refuses_locally() {
		let error = NoRuneWorkspace
			.dispatch_rune("table(workspace.table(\"test2\"))")
			.await
			.expect_err("adapter without Rune support");
		assert_eq!(error.kind, RuneDispatchFailureKind::Unavailable);
		assert!(error.message.contains("selected adapter"));
	}
}
