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

#[cfg(feature = "toron")]
pub type RuneDispatchResult = Result<Arc<ResultArtifact>, RuneDispatchFailure>;

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
