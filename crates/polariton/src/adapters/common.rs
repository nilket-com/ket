use async_trait::async_trait;
use polars::frame::DataFrame;
#[cfg(feature = "toron")]
use std::ops::Range;

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
	pub value: &'static str,
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
		frame: DataFrame,
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
pub type RuneDispatchResult = Result<DataFrame, RuneDispatchFailure>;

#[async_trait]
pub trait DatabaseAdapter: Send + Sync + 'static {
	async fn dispatch(&mut self, code: &str) -> ExecutionResult;

	#[cfg(feature = "toron")]
	async fn dispatch_rune(&mut self, _source: &str) -> RuneDispatchResult {
		Err(RuneDispatchFailure {
			kind: RuneDispatchFailureKind::Unavailable,
			message: "Local Rune workspace unavailable. Start or attach with `nilket`.".to_owned(),
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
	async fn unavailable_workspace_refuses_locally_with_attach_guidance() {
		let error = NoRuneWorkspace
			.dispatch_rune("table(workspace.table(\"test2\"))")
			.await
			.expect_err("adapter without Rune support");
		assert_eq!(error.kind, RuneDispatchFailureKind::Unavailable);
		assert!(error.message.contains("nilket"));
	}
}
