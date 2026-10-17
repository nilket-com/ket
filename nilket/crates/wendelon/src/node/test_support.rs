//! Feature-only deterministic file-operation injection. No process-global hooks.
use crate::storage::file_ops::AsyncFileOps;
use std::sync::Arc;

/// Opaque test-support handle; equality is allocation identity, never storage state.
#[derive(Clone)]
pub struct TestSupportFileOps(Arc<dyn AsyncFileOps>);

impl TestSupportFileOps {
	pub fn new(file_ops: Arc<dyn AsyncFileOps>) -> Self {
		Self(file_ops)
	}

	pub(super) fn into_inner(self) -> Arc<dyn AsyncFileOps> {
		self.0
	}
}

impl std::fmt::Debug for TestSupportFileOps {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.write_str("TestSupportFileOps(..)")
	}
}

impl PartialEq for TestSupportFileOps {
	fn eq(&self, other: &Self) -> bool {
		Arc::ptr_eq(&self.0, &other.0)
	}
}

impl Eq for TestSupportFileOps {}
