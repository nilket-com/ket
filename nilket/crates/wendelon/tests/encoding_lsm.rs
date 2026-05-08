use std::fs;
use std::path::Path;

#[test]
fn lsm_and_compaction_paths_recompute_encoding_from_writer_config() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR"));
	let manager = fs::read_to_string(root.join("src/storage/manager.rs")).unwrap();

	for token in [
		"pub encoding_writer: EncodingWriterConfig",
		"&self.config.encoding_writer",
		"encode_multirow_from_survivors_with_row_group_encoding_and_chunk_layout_config",
		"pack_multirow_source_rows_with_policy_row_group_encoding_and_chunk_layout_config",
	] {
		assert!(
			manager.contains(token),
			"src/storage/manager.rs missing ADR 057 LSM Encoding token {token}"
		);
	}
	assert!(
		!manager.contains("input.cascades.clone()"),
		"compaction must not copy input Encoding cascade choices"
	);
}
