use crate::core::DocLine;
use crate::ecs::{NodeId, UastRegistry};
use crate::svp::pointer::SvpPointer;
use crate::svp::resolver::DMA_CHUNK_SIZE;
use crate::uast::kind::SemanticKind;
use crate::uast::metrics::SpanMetrics;
use std::time::{SystemTime, UNIX_EPOCH};

pub(super) fn temp_test_path(name: &str) -> std::path::PathBuf {
	let nanos = SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.expect("time should move forward")
		.as_nanos();
	std::env::temp_dir().join(format!("baryon-{}-{}-{}", name, std::process::id(), nanos))
}

pub(super) fn build_source_backed_document(
	text: &str,
) -> (UastRegistry, NodeId, std::path::PathBuf) {
	let path = temp_test_path("folding-source-backed");
	let bytes = text.as_bytes();
	let chunk_count = bytes.len().div_ceil(DMA_CHUNK_SIZE).max(1);
	let newlines = bytes.iter().filter(|&&byte| byte == b'\n').count() as u64;
	let registry = UastRegistry::new(chunk_count as u32 + 4);
	let mut chunk = registry.reserve_chunk(chunk_count as u32 + 1).expect("OOM");
	let root = chunk.spawn_node(
		SemanticKind::RelationalTable,
		None,
		SpanMetrics {
			byte_length: bytes.len() as u64,
			newlines,
		},
	);

	let mut byte_offset = 0usize;
	for fragment in bytes.chunks(DMA_CHUNK_SIZE) {
		let leaf = chunk.spawn_node(
			SemanticKind::Token,
			Some(SvpPointer {
				lba: (byte_offset as u64) / 512,
				byte_length: fragment.len() as u32,
				device_id: 91,
				head_trim: ((byte_offset as u64) % 512) as u16,
			}),
			SpanMetrics {
				byte_length: fragment.len() as u64,
				newlines: fragment.iter().filter(|&&byte| byte == b'\n').count() as u64,
			},
		);
		chunk.append_local_child(root, leaf);
		registry.set_terminal_newline(leaf, fragment.last() == Some(&b'\n'));
		byte_offset += fragment.len();
	}

	std::fs::write(&path, bytes).expect("write temp file");
	registry
		.register_device_path(91, path.to_str().expect("utf8 path"))
		.expect("register backing");
	registry.install_root_child_line_index(root);
	drop(chunk);
	(registry, root, path)
}

pub(super) fn doc_line_of(text: &str, needle: &str) -> DocLine {
	DocLine::new(
		text.lines()
			.position(|line| line.contains(needle))
			.expect("needle line should exist") as u64,
	)
}

pub(super) fn doc_line_of_after(text: &str, anchor: &str, needle: &str) -> DocLine {
	let anchor_offset = text.find(anchor).expect("anchor should exist");
	let needle_offset = text[anchor_offset..]
		.find(needle)
		.expect("needle after anchor should exist");
	DocLine::new(text[..anchor_offset + needle_offset].matches('\n').count() as u64)
}
