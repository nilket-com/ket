use bytemuck::{Zeroable, bytes_of, from_bytes};
use wendelon::schema::{
	TABLE_QUERY_TRACE_FLAG_PRESENT, TABLE_QUERY_TRACE_FLAG_SAMPLED, TableQueryRequestMeta,
};
use wendelon_client::{TableQueryOptions, TableQueryTraceContext};

#[test]
fn table_query_options_default_trace_context_absent() {
	assert_eq!(
		TableQueryOptions::default().trace_context,
		TableQueryTraceContext::Absent
	);
}

#[test]
fn table_query_meta_absent_roundtrips_zero_flags() {
	let meta = TableQueryRequestMeta::zeroed();
	let roundtrip: TableQueryRequestMeta = *from_bytes(bytes_of(&meta));
	assert_eq!(roundtrip.trace_id, [0; 16]);
	assert_eq!(roundtrip.trace_parent_span_id, [0; 8]);
	assert_eq!(roundtrip.trace_flags, 0);
}

#[test]
fn table_query_meta_sampled_roundtrips_binary_trace_context() {
	let mut meta = TableQueryRequestMeta::zeroed();
	meta.trace_id = [0x11; 16];
	meta.trace_parent_span_id = [0x22; 8];
	meta.trace_flags = TABLE_QUERY_TRACE_FLAG_PRESENT | TABLE_QUERY_TRACE_FLAG_SAMPLED;

	let roundtrip: TableQueryRequestMeta = *from_bytes(bytes_of(&meta));
	assert_eq!(roundtrip.trace_id, [0x11; 16]);
	assert_eq!(roundtrip.trace_parent_span_id, [0x22; 8]);
	assert_eq!(
		roundtrip.trace_flags,
		TABLE_QUERY_TRACE_FLAG_PRESENT | TABLE_QUERY_TRACE_FLAG_SAMPLED
	);
}

#[test]
fn table_query_meta_unsampled_roundtrips_explicit_suppression() {
	let mut meta = TableQueryRequestMeta::zeroed();
	meta.trace_flags = TABLE_QUERY_TRACE_FLAG_PRESENT;

	let roundtrip: TableQueryRequestMeta = *from_bytes(bytes_of(&meta));
	assert_eq!(roundtrip.trace_flags, TABLE_QUERY_TRACE_FLAG_PRESENT);
	assert_eq!(roundtrip.trace_id, [0; 16]);
	assert_eq!(roundtrip.trace_parent_span_id, [0; 8]);
}
