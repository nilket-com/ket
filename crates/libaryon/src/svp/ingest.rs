use crate::ecs::id::NodeId;
use crate::ecs::registry::UastRegistry;
use crate::svp::line_index::{SparseLineIndex, cache_path_for_backing};
use crate::svp::pointer::SvpPointer;
use crate::svp::resolver::{RequestPriority, SvpRequest, SvpResolver};
use crate::uast::kind::SemanticKind;
use crate::uast::metrics::SpanMetrics;
use std::path::Path;
use std::sync::Arc;

/// Initial source-backed extent size. Resolver transfers and mutation
/// rechunking are deliberately independent policies.
pub const COLD_INGEST_EXTENT_SIZE: usize = 1024 * 1024;

pub fn ingest_svp_file(
	resolver: &SvpResolver,
	registry: &Arc<UastRegistry>,
	file_size: u64,
	device_id: u16,
	path: String,
) -> Result<NodeId, String> {
	let chunk_count = file_size.div_ceil(COLD_INGEST_EXTENT_SIZE as u64);
	let node_count = chunk_count
		.checked_add(1)
		.and_then(|count| u32::try_from(count).ok())
		.ok_or_else(|| format!("File requires too many extents to load ({chunk_count})"))?;
	let mut chunk = registry.reserve_chunk(node_count).ok_or_else(|| {
		format!(
			"Not enough document-node capacity to load {file_size} bytes ({chunk_count} extents required)"
		)
	})?;

	let root_id = chunk.spawn_node(
		SemanticKind::RelationalTable,
		None,
		SpanMetrics {
			byte_length: file_size,
			newlines: 0,
		},
	);

	let mut first_leaf_id = None;
	let mut last_leaf_id = None;

	for i in 0..chunk_count {
		let byte_offset = i * COLD_INGEST_EXTENT_SIZE as u64;
		let byte_length = (file_size - byte_offset).min(COLD_INGEST_EXTENT_SIZE as u64) as u32;

		let leaf_id = chunk.spawn_node(
			SemanticKind::Token,
			Some(SvpPointer {
				lba: byte_offset / 512,
				byte_length,
				device_id,
				head_trim: (byte_offset % 512) as u16,
			}),
			SpanMetrics {
				byte_length: u64::from(byte_length),
				newlines: 0,
			},
		);

		if first_leaf_id.is_none() {
			first_leaf_id = Some(leaf_id);
		}
		last_leaf_id = Some(leaf_id);

		chunk.append_local_child(root_id, leaf_id);
	}
	registry
		.cold_extent_count
		.fetch_add(chunk_count, std::sync::atomic::Ordering::Relaxed);
	if let Err(error) = resolver.register_device(device_id, &path) {
		registry.note_nodes_retired(u64::from(node_count));
		return Err(format!("Open error: {error}"));
	}
	let warm_index = registry
		.backing_identity(device_id)
		.and_then(|identity| {
			SparseLineIndex::load_validated(&cache_path_for_backing(Path::new(&path)), identity)
				.ok()
		})
		.filter(|index| {
			index.block_size() as usize == COLD_INGEST_EXTENT_SIZE
				&& index.newline_counts().len() as u64 == chunk_count
		});

	if let Some(index) = warm_index.as_ref() {
		let mut leaf = first_leaf_id;
		for (&newline_count, &ends_with_newline) in
			index.newline_counts().iter().zip(index.terminal_newlines())
		{
			let node = leaf.expect("validated index count matches ingested extents");
			let _ = registry.try_complete_metric_inflation(node, newline_count, ends_with_newline);
			leaf = registry.get_next_sibling(node);
		}
	} else if let (Some(first), Some(last)) = (first_leaf_id, last_leaf_id) {
		registry.begin_metric_scan(root_id, first, last);
	}
	registry.install_root_child_line_index(root_id);

	// Saturate the background metric scan while keeping each lane disjoint.
	if warm_index.is_none()
		&& let (Some(first), Some(last)) = (first_leaf_id, last_leaf_id)
	{
		let _ = (first, last);
		for _ in 0..super::resolver::METRIC_SCAN_LANES {
			if let Some(claim) = registry.claim_next_metric_scan(root_id) {
				let request = SvpRequest {
					node_id: claim.node_id,
					pointer: claim.pointer,
					extent_offset: 0,
					request_length: claim.pointer.byte_length,
					priority: RequestPriority::Low,
					scan_root: Some(root_id),
					content_generation: claim.content_generation,
					hydration_buffer: None,
				};
				if resolver.request_queue.push(request).is_err() {
					registry.return_metric_scan_claim(root_id, claim);
					break;
				}
			}
		}
	}

	Ok(root_id)
}

pub(crate) fn persist_line_index(registry: &UastRegistry, root: NodeId) {
	let Some(first) = registry.get_first_child(root) else {
		return;
	};
	let Some(span) = (unsafe { *registry.spans[first.index()].get() }) else {
		return;
	};
	let device_id = span.device_id;
	let Some(identity) = registry.backing_identity(span.device_id) else {
		return;
	};
	let Some(path) = registry
		.device_paths
		.lock()
		.unwrap()
		.get(&span.device_id)
		.cloned()
	else {
		return;
	};
	let mut counts = Vec::new();
	let mut terminal_newlines = Vec::new();
	let mut child = Some(first);
	let mut expected_offset = 0u64;
	while let Some(extent) = child {
		if registry.get_first_child(extent).is_some() {
			return;
		}
		let Some(extent_span) = (unsafe { *registry.spans[extent.index()].get() }) else {
			return;
		};
		let span_offset = extent_span.lba * 512 + u64::from(extent_span.head_trim);
		let expected_len =
			(identity.length - expected_offset).min(COLD_INGEST_EXTENT_SIZE as u64) as u32;
		if extent_span.device_id != device_id
			|| span_offset != expected_offset
			|| extent_span.byte_length != expected_len
		{
			// The persistent format describes immutable fixed-size backing blocks,
			// not the current edit-shaped leaf topology.
			return;
		}
		let metrics = unsafe { *registry.metrics[extent.index()].get() };
		let Ok(count) = u32::try_from(metrics.newlines) else {
			return;
		};
		counts.push(count);
		let Some(ends_with_newline) = registry.terminal_newline(extent) else {
			return;
		};
		terminal_newlines.push(ends_with_newline);
		expected_offset = expected_offset.saturating_add(u64::from(extent_span.byte_length));
		child = registry.get_next_sibling(extent);
	}
	if expected_offset != identity.length {
		return;
	}
	let Ok(index) = SparseLineIndex::new(
		identity,
		COLD_INGEST_EXTENT_SIZE as u32,
		counts,
		terminal_newlines,
	) else {
		return;
	};
	let sidecar = cache_path_for_backing(&path);
	if let Some(parent) = sidecar.parent()
		&& std::fs::create_dir_all(parent).is_err()
	{
		return;
	}
	let _ = index.save_atomic(&sidecar);
}

#[cfg(test)]
mod tests {
	use super::{cache_path_for_backing, ingest_svp_file, persist_line_index};
	use crate::ecs::UastRegistry;
	use crate::svp::pointer::SvpPointer;
	use crate::svp::resolver::SvpResolver;
	use crate::uast::{SemanticKind, SpanMetrics};
	use std::sync::atomic::Ordering;
	use std::sync::{Arc, mpsc};

	#[test]
	fn failed_post_allocation_device_registration_retires_the_candidate_tree() {
		let registry = Arc::new(UastRegistry::new(2));
		let (notify, _notifications) = mpsc::channel();
		let resolver = SvpResolver::new_blocking_for_tests(registry.clone(), notify);
		let missing =
			std::env::temp_dir().join(format!("baryon-missing-ingest-{}", std::process::id()));
		let _ = std::fs::remove_file(&missing);

		let error = ingest_svp_file(
			&resolver,
			&registry,
			1,
			91,
			missing.to_string_lossy().into_owned(),
		)
		.expect_err("missing device path must reject after reservation");

		assert!(error.contains("Open error"));
		assert_eq!(registry.stats().allocated_nodes, 2);
		assert_eq!(registry.stats().retired_nodes, 2);
	}

	#[test]
	fn edited_variable_extents_are_not_persisted_as_fixed_index_blocks() {
		let path = std::env::temp_dir().join(format!(
			"baryon-variable-index-{}-{}",
			std::process::id(),
			std::time::SystemTime::now()
				.duration_since(std::time::UNIX_EPOCH)
				.unwrap()
				.as_nanos()
		));
		std::fs::write(&path, b"abcdefgh").unwrap();
		let sidecar = cache_path_for_backing(&path);
		let _ = std::fs::remove_file(&sidecar);

		let registry = UastRegistry::new(3);
		registry
			.register_device_path(9, path.to_str().unwrap())
			.expect("register backing");
		let mut chunk = registry.reserve_chunk(3).unwrap();
		let root = chunk.spawn_node(
			SemanticKind::RelationalTable,
			None,
			SpanMetrics {
				byte_length: 8,
				newlines: 0,
			},
		);
		for (offset, length) in [(0u64, 4u32), (4, 4)] {
			let leaf = chunk.spawn_node(
				SemanticKind::Token,
				Some(SvpPointer {
					lba: offset / 512,
					byte_length: length,
					device_id: 9,
					head_trim: (offset % 512) as u16,
				}),
				SpanMetrics {
					byte_length: u64::from(length),
					newlines: 0,
				},
			);
			chunk.append_local_child(root, leaf);
			registry.metrics_inflated[leaf.index()].store(true, Ordering::Release);
			registry.set_terminal_newline(leaf, false);
		}

		persist_line_index(&registry, root);
		assert!(!sidecar.exists());
		let _ = std::fs::remove_file(path);
	}
}
