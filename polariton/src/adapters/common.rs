use async_trait::async_trait;
use polars::frame::DataFrame;

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
	Batch(Vec<ExecutionResult>),
	CommandCompleted(String),
	Err(String),
	Rows(DataFrame),
}

#[async_trait]
pub trait DatabaseAdapter: Send + Sync + 'static {
	async fn dispatch(&mut self, code: &str) -> ExecutionResult;
}
