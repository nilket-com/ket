use std::path::Path;

#[test]
fn lsm_and_compaction_paths_recompute_encoding_from_writer_config() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR"));
	let manager = read_module_concat(root.join("src/storage/manager.rs"));

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

fn read_module_concat(path: impl AsRef<std::path::Path>) -> String {
	let path = path.as_ref();
	let mut source = std::fs::read_to_string(path).unwrap();
	// ADR 078: storage/manager.rs is now a module dir; scan root + non-test submodules.
	if path.file_name().and_then(|s| s.to_str()) == Some("manager.rs")
		&& path
			.parent()
			.and_then(|p| p.file_name())
			.and_then(|s| s.to_str())
			== Some("storage")
	{
		let dir = path.with_extension("");
		if dir.is_dir() {
			let mut subs: Vec<_> = std::fs::read_dir(&dir)
				.unwrap()
				.flatten()
				.map(|e| e.path())
				.filter(|p| {
					p.extension().and_then(|s| s.to_str()) == Some("rs")
						&& p.file_name().and_then(|s| s.to_str()) != Some("tests.rs")
				})
				.collect();
			subs.sort();
			for p in subs {
				source.push('\n');
				source.push_str(&std::fs::read_to_string(p).unwrap());
			}
		}
	}
	source
}
