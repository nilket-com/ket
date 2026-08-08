use crate::architecture::support::*;

/// ADR 540: every named `ReadStatus` crosses the boundary typed, and the collapse
/// into a generic error is unrepresentable. Three clauses hold the seam:
/// the internal client wraps failed statuses as a typed source (never a bare
/// string), the wendelon-client downcasts that source into `QueryFailed` before its
/// `Io` fallback, and Toron maps `QueryFailed` ahead of the `Unavailable` catch-all.
/// The totality clause pins the canonical list against the constant count, because
/// `ReadStatus` is a newtype and match exhaustiveness cannot do it.
#[test]
pub(crate) fn adr540_query_statuses_cross_the_boundary_typed() {
	let core =
		std::fs::read_to_string(workspace_root_dir().join("crates/wendelon/src/client/core.rs"))
			.expect("read wendelon client core");
	assert!(core.contains("pub struct QueryStatusError"));
	let failure_fn = &core[core
		.find("fn query_failure_error(")
		.expect("query_failure_error")..];
	let failure_fn = &failure_fn[..failure_fn.find("\n}\n").expect("fn end")];
	assert!(
		failure_fn.contains("QueryStatusError { status }"),
		"query_failure_error must carry the typed status source"
	);
	assert!(
		!failure_fn.contains("format!"),
		"no stringly status construction may return to query_failure_error"
	);

	let client = std::fs::read_to_string(
		workspace_root_dir().join("crates/wendelon/crates/wendelon-client/src/lib.rs"),
	)
	.expect("read wendelon-client");
	let conversion = &client[client
		.find("fn table_query_client_error(")
		.expect("table_query_client_error")..];
	let conversion = &conversion[..conversion.find("\n}\n").expect("fn end")];
	let status_at = conversion
		.find("query_status_failure_from_io")
		.expect("status downcast present");
	let io_at = conversion
		.find("ClientError::Io(error)")
		.expect("io fallback present");
	assert!(
		status_at < io_at,
		"the typed status downcast must precede the Io fallback"
	);

	let toron_error =
		std::fs::read_to_string(manifest_dir().join("src/error.rs")).expect("read toron error");
	let query_failed_at = toron_error
		.find("ClientError::QueryFailed { .. } =>")
		.expect("QueryFailed mapping arm");
	let unavailable_arm_at = toron_error
		.find("| source @ ClientError::Io(_) => Self::Unavailable { source }")
		.expect("Unavailable catch-all arm");
	assert!(
		query_failed_at < unavailable_arm_at,
		"the typed status arm must precede the Unavailable catch-all"
	);

	// Totality: the canonical list names every constant the newtype declares.
	let schema =
		std::fs::read_to_string(workspace_root_dir().join("crates/wendelon/src/schema/core.rs"))
			.expect("read wendelon schema core");
	let block_start = schema.find("pub struct ReadStatus").expect("ReadStatus");
	let block = &schema[block_start..];
	let block = &block[..block
		.find("pub struct TableReferenceIntegrityFailureMeta")
		.expect("block end")];
	let const_count = block.matches(": Self = Self(").count();
	let list = &block[block.find("pub const ALL_NAMED").expect("ALL_NAMED")..];
	let list = &list[..list.find("];").expect("list end")];
	let list_count = list.matches("Self::").count();
	assert_eq!(
		const_count, list_count,
		"a ReadStatus constant exists without an ALL_NAMED entry"
	);
	let named_arms = block.matches("=> \"").count();
	assert_eq!(
		named_arms,
		const_count + 1,
		"every constant needs an as_str name (plus the unknown arm)"
	);
}
