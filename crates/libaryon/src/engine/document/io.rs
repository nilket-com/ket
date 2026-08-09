use crate::core::DocByte;
use crate::ecs::{NodeId, UastRegistry};
use crate::engine::settings::SaveDurability;
use crate::svp::SvpPointer;
use crate::uast::UastProjection;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;

pub(crate) const MAX_WHOLE_DOCUMENT_SNAPSHOT_BYTES: u64 = 256 * 1024 * 1024;
const MAX_IN_PLACE_ROLLBACK_BYTES: u64 = 1024 * 1024;

/// Below this size, a save rewrites the whole document through a temporary file
/// and an atomic rename, so a crash mid-save can never destroy the original.
///
/// The bounded in-place suffix path is not crash-atomic: it writes over the
/// user's file, and its rollback only covers an in-process `io::Error`, not a
/// power loss. That trade is only worth making when copying the unchanged prefix
/// is genuinely prohibitive — rewriting a few hundred megabytes is not.
const MIN_IN_PLACE_SAVE_BYTES: u64 = MAX_WHOLE_DOCUMENT_SNAPSHOT_BYTES;

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct SaveIoStats {
	pub(crate) logical_bytes: u64,
	pub(crate) source_bytes_read: u64,
	pub(crate) target_bytes_written: u64,
}

/// How a save reached the target file, and therefore what it guarantees.
///
/// The two are not interchangeable: one survives a power loss and the other does
/// not. So the durability a save actually achieved is part of its result, rather
/// than something a caller would have to re-derive from the document's size and
/// leaf layout — which is how it would silently go unreported.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SaveMethod {
	/// Written to a temporary file and renamed over the target. A crash at any
	/// point leaves either the old file or the new one, never a mixture.
	AtomicReplace,
	/// The trailing extent was rewritten over the target in place. Bounded, but
	/// not crash-atomic: a power loss mid-write can leave a torn tail.
	InPlaceSuffix,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct SaveOutcome {
	pub(crate) stats: SaveIoStats,
	pub(crate) method: SaveMethod,
}

fn temp_save_path(target_path: &Path) -> Result<PathBuf, String> {
	let file_name = target_path
		.file_name()
		.ok_or_else(|| "Invalid target path".to_string())?;
	let parent = target_path.parent().unwrap_or_else(|| Path::new("."));
	let file_name = file_name.to_string_lossy();
	let pid = std::process::id();

	for attempt in 0..1024u32 {
		let candidate = parent.join(format!(".{}.baryon.{}.{}.tmp", file_name, pid, attempt));
		if !candidate.exists() {
			return Ok(candidate);
		}
	}

	Err("Unable to allocate temporary save path".to_string())
}

fn copy_physical_span_to_writer(
	source_file: &mut File,
	span: SvpPointer,
	writer: &mut impl Write,
	buffer: &mut [u8],
) -> Result<u64, String> {
	let mut remaining = span.byte_length as usize;
	let source_offset = span.lba * 512 + u64::from(span.head_trim);
	source_file
		.seek(SeekFrom::Start(source_offset))
		.map_err(|e| format!("Write error: {}", e))?;

	let mut written = 0u64;
	while remaining > 0 {
		let chunk_len = remaining.min(buffer.len());
		source_file
			.read_exact(&mut buffer[..chunk_len])
			.map_err(|e| format!("Write error: {}", e))?;
		writer
			.write_all(&buffer[..chunk_len])
			.map_err(|e| format!("Write error: {}", e))?;
		remaining -= chunk_len;
		written += chunk_len as u64;
	}

	Ok(written)
}

fn copy_registered_span_to_writer(
	registry: &UastRegistry,
	span: SvpPointer,
	writer: &mut impl Write,
	buffer: &mut [u8],
) -> Result<Option<u64>, String> {
	if registry.backing_identity(span.device_id).is_none() {
		return Ok(None);
	}

	let mut remaining = span.byte_length as usize;
	let mut source_offset = span.lba * 512 + u64::from(span.head_trim);
	let mut written = 0u64;
	while remaining > 0 {
		let chunk_len = remaining.min(buffer.len());
		registry
			.read_backing_exact_at(span.device_id, source_offset, &mut buffer[..chunk_len])
			.map_err(|error| format!("Write error: {error}"))?;
		writer
			.write_all(&buffer[..chunk_len])
			.map_err(|error| format!("Write error: {error}"))?;
		remaining -= chunk_len;
		source_offset = source_offset.saturating_add(chunk_len as u64);
		written = written.saturating_add(chunk_len as u64);
	}
	Ok(Some(written))
}

fn stream_document_to_writer(
	registry: &UastRegistry,
	root: NodeId,
	source_path: Option<&Path>,
	writer: &mut impl Write,
) -> Result<SaveIoStats, String> {
	let _resident_read = registry.resident_data_guard();
	let mut visit = registry.get_first_child(root);
	let mut source_file: Option<File> = None;
	let mut buffer = vec![0u8; 256 * 1024];
	let mut stats = SaveIoStats::default();

	while let Some(node) = visit {
		let idx = node.index();
		let has_children = unsafe { (*registry.edges[idx].get()).first_child.is_some() };

		if has_children {
			visit = registry.get_first_child(node);
			continue;
		}

		unsafe {
			if let Some(v_data) = &*registry.virtual_data[idx].get() {
				writer
					.write_all(v_data)
					.map_err(|e| format!("Write error: {}", e))?;
				let len = v_data.len() as u64;
				stats.logical_bytes += len;
				stats.target_bytes_written += len;
			} else if let Some(span) = *registry.spans[idx].get() {
				let copied = if let Some(copied) =
					copy_registered_span_to_writer(registry, span, writer, &mut buffer)?
				{
					copied
				} else {
					if source_file.is_none() {
						let Some(path) = source_path else {
							return Err(
								"Cannot stream-save physical spans without a backing device or source file"
									.to_string(),
							);
						};
						source_file =
							Some(File::open(path).map_err(|e| format!("Write error: {}", e))?);
					}
					copy_physical_span_to_writer(
						source_file
							.as_mut()
							.expect("source file must be opened for physical span"),
						span,
						writer,
						&mut buffer,
					)?
				};
				stats.logical_bytes += copied;
				stats.source_bytes_read += copied;
				stats.target_bytes_written += copied;
			}
		}

		visit = registry.get_next_node_in_walk(node);
	}

	Ok(stats)
}

fn restore_in_place_suffix(
	file: &mut File,
	rewrite_start: u64,
	original_len: u64,
	original_suffix: &[u8],
) -> Result<(), String> {
	file.seek(SeekFrom::Start(rewrite_start))
		.and_then(|_| file.write_all(original_suffix))
		.and_then(|_| file.set_len(original_len))
		.and_then(|_| file.sync_all())
		.map_err(|error| format!("rollback failed: {error}"))
}

/// Commits a same-file edit confined to the final hot extent without copying
/// the unchanged prefix. The old bounded suffix is retained until the new one
/// is durable so an ordinary write failure can be rolled back in-process.
///
/// More general edits still use the atomic temporary-file path below: moving a
/// physical suffix after a length-changing middle edit requires either copying
/// that suffix or filesystem-specific copy-on-write support.
///
/// This path gives up crash atomicity, so it is taken only when the user has
/// asked for it (`:set savedurability bounded`) *and* the document is large
/// enough that a full rewrite is prohibitive. Anything under
/// `MIN_IN_PLACE_SAVE_BYTES` keeps the temporary-file-and-rename save, where
/// atomicity is essentially free.
fn try_save_bounded_in_place_suffix(
	registry: &UastRegistry,
	root: NodeId,
	source_path: Option<&Path>,
	target_path: &Path,
	durability: SaveDurability,
) -> Result<Option<SaveIoStats>, String> {
	if durability != SaveDurability::Bounded || source_path != Some(target_path) {
		return Ok(None);
	}

	let original_len = std::fs::metadata(target_path)
		.map_err(|error| format!("Write error: {error}"))?
		.len();
	if original_len < MIN_IN_PLACE_SAVE_BYTES
		&& registry.get_total_bytes(root) < MIN_IN_PLACE_SAVE_BYTES
	{
		return Ok(None);
	}
	let _resident_read = registry.resident_data_guard();
	let mut visit = registry.get_first_child(root);
	let mut document_offset = 0u64;
	let mut rewrite: Option<(u64, &[u8])> = None;

	while let Some(node) = visit {
		let idx = node.index();
		if unsafe { (*registry.edges[idx].get()).first_child.is_some() } {
			return Ok(None);
		}
		let byte_length = unsafe { (*registry.metrics[idx].get()).byte_length };
		let span = unsafe { *registry.spans[idx].get() };
		let is_unchanged_physical = span.is_some_and(|span| {
			span.lba * 512 + u64::from(span.head_trim) == document_offset
				&& u64::from(span.byte_length) == byte_length
		});

		if rewrite.is_none() && is_unchanged_physical {
			document_offset = document_offset.saturating_add(byte_length);
			visit = registry.get_next_sibling(node);
			continue;
		}

		// The bounded path supports exactly one final virtual leaf. Seeing a
		// shifted physical span or another leaf after the dirty tail means the
		// ordinary streaming save must preserve document order instead.
		if rewrite.is_some() || span.is_some() || registry.get_next_sibling(node).is_some() {
			return Ok(None);
		}
		let Some(bytes) = (unsafe { (&*registry.virtual_data[idx].get()).as_deref() }) else {
			return Ok(None);
		};
		if bytes.len() as u64 != byte_length {
			return Ok(None);
		}
		rewrite = Some((document_offset, bytes));
		document_offset = document_offset.saturating_add(byte_length);
		visit = registry.get_next_sibling(node);
	}

	let Some((rewrite_start, new_suffix)) = rewrite else {
		return Ok(None);
	};
	let Some(original_suffix_len) = original_len.checked_sub(rewrite_start) else {
		return Ok(None);
	};
	if original_suffix_len > MAX_IN_PLACE_ROLLBACK_BYTES {
		return Ok(None);
	}

	let rollback_len = usize::try_from(original_suffix_len)
		.map_err(|_| "Write error: rollback suffix does not fit memory".to_string())?;
	let mut original_suffix = vec![0u8; rollback_len];
	let mut file = OpenOptions::new()
		.read(true)
		.write(true)
		.open(target_path)
		.map_err(|error| format!("Write error: {error}"))?;
	file.seek(SeekFrom::Start(rewrite_start))
		.and_then(|_| file.read_exact(&mut original_suffix))
		.map_err(|error| format!("Write error: {error}"))?;

	let write_result = file
		.seek(SeekFrom::Start(rewrite_start))
		.and_then(|_| file.write_all(new_suffix))
		.and_then(|_| file.set_len(document_offset))
		.and_then(|_| file.sync_all());
	if let Err(error) = write_result {
		let rollback =
			restore_in_place_suffix(&mut file, rewrite_start, original_len, &original_suffix);
		return Err(match rollback {
			Ok(()) => format!("Write error: {error}"),
			Err(rollback_error) => format!("Write error: {error}; {rollback_error}"),
		});
	}

	Ok(Some(SaveIoStats {
		logical_bytes: document_offset,
		source_bytes_read: original_suffix_len,
		target_bytes_written: new_suffix.len() as u64,
	}))
}

/// Writes the document to `target_path` and reports how it got there.
///
/// A save is atomic unless the user authorised a bounded one and the document
/// qualifies for it, so the method is an outcome the caller must handle rather
/// than a property of this function's name.
pub(crate) fn save_document(
	registry: &UastRegistry,
	root: NodeId,
	source_path: Option<&Path>,
	target_path: &Path,
	durability: SaveDurability,
) -> Result<SaveOutcome, String> {
	if let Some(stats) =
		try_save_bounded_in_place_suffix(registry, root, source_path, target_path, durability)?
	{
		return Ok(SaveOutcome {
			stats,
			method: SaveMethod::InPlaceSuffix,
		});
	}

	let temp_path = temp_save_path(target_path)?;

	let save_result = (|| -> Result<SaveIoStats, String> {
		let temp_file = OpenOptions::new()
			.write(true)
			.create_new(true)
			.open(&temp_path)
			.map_err(|e| format!("Write error: {}", e))?;
		let mut writer = BufWriter::with_capacity(1024 * 1024, temp_file);
		let stats = stream_document_to_writer(registry, root, source_path, &mut writer)?;
		writer.flush().map_err(|e| format!("Write error: {}", e))?;
		let temp_file = writer
			.into_inner()
			.map_err(|e| format!("Write error: {}", e.into_error()))?;
		temp_file
			.sync_all()
			.map_err(|e| format!("Write error: {}", e))?;
		std::fs::rename(&temp_path, target_path).map_err(|e| format!("Write error: {}", e))?;
		if let Some(parent) = target_path.parent()
			&& let Ok(dir) = File::open(parent)
		{
			let _ = dir.sync_all();
		}
		Ok(stats)
	})();

	if save_result.is_err() {
		let _ = std::fs::remove_file(&temp_path);
	}

	save_result.map(|stats| SaveOutcome {
		stats,
		method: SaveMethod::AtomicReplace,
	})
}

pub(crate) fn rebind_document_spans_to_saved_file(
	registry: &UastRegistry,
	root: NodeId,
	device_id: u16,
) {
	let mut visit = registry.get_first_child(root);
	let mut byte_offset = 0u64;

	while let Some(node) = visit {
		let idx = node.index();
		let has_children = unsafe { (*registry.edges[idx].get()).first_child.is_some() };

		if has_children {
			visit = registry.get_first_child(node);
			continue;
		}

		let byte_length = unsafe { (*registry.metrics[idx].get()).byte_length };
		registry.bump_content_generation(node);
		let local_byte_length =
			u32::try_from(byte_length).expect("saved physical leaf length fits SvpPointer");
		let span = if byte_length == 0 {
			None
		} else {
			Some(SvpPointer {
				lba: byte_offset / 512,
				byte_length: local_byte_length,
				device_id,
				head_trim: (byte_offset % 512) as u16,
			})
		};
		registry.rebind_data_to_saved_span(node, span);
		registry.dma_in_flight[idx].store(false, Ordering::Relaxed);

		byte_offset += byte_length;
		visit = registry.get_next_node_in_walk(node);
	}
}

pub(crate) fn read_loaded_document(
	registry: &UastRegistry,
	root: NodeId,
) -> Result<Vec<u8>, String> {
	let total_bytes = registry.get_total_bytes(root);
	if total_bytes > MAX_WHOLE_DOCUMENT_SNAPSHOT_BYTES {
		return Err(format!(
			"Whole-document operation unavailable for {total_bytes} byte document (limit is {MAX_WHOLE_DOCUMENT_SNAPSHOT_BYTES} bytes)"
		));
	}
	registry
		.read_loaded_slice(
			root,
			DocByte::ZERO,
			DocByte::new(registry.get_total_bytes(root)),
		)
		.map_err(|msg| msg.to_string())
}

pub(crate) fn read_document_snapshot(
	registry: &UastRegistry,
	root: NodeId,
	source_path: Option<&Path>,
) -> Result<Vec<u8>, String> {
	let total_bytes = registry.get_total_bytes(root);
	if total_bytes > MAX_WHOLE_DOCUMENT_SNAPSHOT_BYTES {
		return Err(format!(
			"Whole-document operation unavailable for {} byte document (limit is {} bytes)",
			total_bytes, MAX_WHOLE_DOCUMENT_SNAPSHOT_BYTES
		));
	}
	if let Ok(bytes) = read_loaded_document(registry, root) {
		return Ok(bytes);
	}

	let Some(source_path) = source_path else {
		return Err("Document is not fully loaded".to_string());
	};

	let capacity = usize::try_from(total_bytes)
		.map_err(|_| "Document snapshot does not fit this platform's address space".to_string())?;
	let mut bytes = Vec::new();
	bytes
		.try_reserve_exact(capacity)
		.map_err(|_| "Unable to reserve memory for document snapshot".to_string())?;
	let _ = stream_document_to_writer(registry, root, Some(source_path), &mut bytes)?;
	Ok(bytes)
}

#[cfg(test)]
mod tests {
	use super::{
		MIN_IN_PLACE_SAVE_BYTES, SaveDurability, SaveMethod, read_document_snapshot,
		rebind_document_spans_to_saved_file, restore_in_place_suffix, save_document,
		stream_document_to_writer, try_save_bounded_in_place_suffix,
	};
	use crate::ecs::UastRegistry;
	use crate::engine::core::FILE_DEVICE_ID;
	use crate::engine::test_support::{build_mixed_save_document, temp_test_path};
	use crate::uast::{SemanticKind, SpanMetrics, UastMutation};
	use std::fs::{File, OpenOptions};
	use std::io::{self, Read, Seek, SeekFrom, Write};
	use std::sync::atomic::Ordering;
	use std::sync::{Arc, mpsc};
	use std::time::Duration;

	/// Reads `len` bytes at `offset` without pulling the rest of the file into
	/// memory. The bounded-save fixtures are sparse and larger than any buffer a
	/// test should allocate, so verifying them must stay a seek, not a slurp.
	fn read_at(path: &std::path::Path, offset: u64, len: usize) -> Vec<u8> {
		let mut file = File::open(path).expect("open for verification");
		file.seek(SeekFrom::Start(offset)).expect("seek");
		let mut bytes = vec![0u8; len];
		file.read_exact(&mut bytes).expect("read");
		bytes
	}

	struct BlockingWriter {
		bytes: Vec<u8>,
		started: Option<mpsc::Sender<()>>,
		release: mpsc::Receiver<()>,
	}

	impl Write for BlockingWriter {
		fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
			self.bytes.extend_from_slice(bytes);
			if let Some(started) = self.started.take() {
				let _ = started.send(());
				let _ = self.release.recv();
			}
			Ok(bytes.len())
		}

		fn flush(&mut self) -> io::Result<()> {
			Ok(())
		}
	}

	#[test]
	fn save_streams_mixed_physical_and_virtual_leaves() {
		let source_path = temp_test_path("save-source");
		let target_path = temp_test_path("save-target");
		std::fs::write(&source_path, b"alphagamma").expect("write source");

		let (registry, root) = build_mixed_save_document();
		let outcome = save_document(
			&registry,
			root,
			Some(&source_path),
			&target_path,
			SaveDurability::Atomic,
		)
		.expect("streaming save should succeed");
		assert_eq!(outcome.stats.logical_bytes, 12);
		assert_eq!(outcome.method, SaveMethod::AtomicReplace);

		let written = std::fs::read(&target_path).expect("read target");
		assert_eq!(&written, b"alphaZZgamma");

		let _ = std::fs::remove_file(source_path);
		let _ = std::fs::remove_file(target_path);
	}

	#[test]
	fn streaming_prefers_the_immutable_device_over_a_conflicting_source_path() {
		let backing_path = temp_test_path("registered-save-backing");
		let conflicting_path = temp_test_path("conflicting-save-source");
		let target_path = temp_test_path("registered-save-target");
		std::fs::write(&backing_path, b"alphagamma").expect("write registered backing");
		std::fs::write(&conflicting_path, b"BBBBBBBBBB").expect("write conflicting source");

		let (registry, root) = build_mixed_save_document();
		registry
			.register_device_path(FILE_DEVICE_ID, &backing_path)
			.expect("register immutable backing");
		save_document(
			&registry,
			root,
			Some(&conflicting_path),
			&target_path,
			SaveDurability::Atomic,
		)
		.expect("save from registered backing");

		assert_eq!(
			std::fs::read(&target_path).expect("read target"),
			b"alphaZZgamma"
		);
		let _ = std::fs::remove_file(backing_path);
		let _ = std::fs::remove_file(conflicting_path);
		let _ = std::fs::remove_file(target_path);
	}

	#[test]
	fn an_ordinary_small_file_save_keeps_its_atomic_rename() {
		// The in-place suffix path overwrites the user's file and cannot survive a
		// crash. For a small document a full rewrite is cheap, so atomicity must
		// win even when the user asked for bounded saves: the bounded path has to
		// decline on size alone.
		let path = temp_test_path("small-save-stays-atomic");
		std::fs::write(&path, b"alpha").expect("write source");

		let registry = UastRegistry::new(4);
		let mut chunk = registry.reserve_chunk(2).expect("registry chunk");
		let root = chunk.spawn_node(
			SemanticKind::RelationalTable,
			None,
			SpanMetrics {
				byte_length: 5,
				newlines: 0,
			},
		);
		// A small edited file is exactly the shape the bounded path matches:
		// a single trailing virtual leaf with no unchanged physical prefix.
		let leaf = chunk.spawn_node(
			SemanticKind::Token,
			None,
			SpanMetrics {
				byte_length: 5,
				newlines: 0,
			},
		);
		chunk.append_local_child(root, leaf);
		registry.replace_authoritative_data(leaf, b"ALPHA".to_vec());

		let declined = try_save_bounded_in_place_suffix(
			&registry,
			root,
			Some(&path),
			&path,
			SaveDurability::Bounded,
		)
		.expect("bounded save probe");
		assert!(
			declined.is_none(),
			"a {} byte document must save through the atomic temporary file, not in place",
			registry.get_total_bytes(root)
		);

		// And the save still lands, atomically.
		let outcome = save_document(&registry, root, Some(&path), &path, SaveDurability::Bounded)
			.expect("atomic save");
		assert_eq!(outcome.method, SaveMethod::AtomicReplace);
		assert_eq!(std::fs::read(&path).expect("read saved"), b"ALPHA");
		let _ = std::fs::remove_file(&path);
	}

	#[test]
	fn append_only_save_has_bounded_physical_io_amplification() {
		const EXTENT_BYTES: usize = 1024 * 1024;
		// The in-place suffix path trades crash atomicity for a bounded rewrite, so
		// it only engages above MIN_IN_PLACE_SAVE_BYTES. The source is sparse, so
		// this costs logical size, not allocated blocks.
		const EXTENTS: usize = (MIN_IN_PLACE_SAVE_BYTES as usize) / EXTENT_BYTES;
		const ORIGINAL_BYTES: u64 = (EXTENT_BYTES * EXTENTS) as u64;
		let path = temp_test_path("append-only-bounded-save");
		let source = std::fs::File::create(&path).expect("create sparse source");
		source.set_len(ORIGINAL_BYTES).expect("size sparse source");
		drop(source);

		let registry = UastRegistry::new(EXTENTS as u32 + 4);
		let mut chunk = registry
			.reserve_chunk(EXTENTS as u32 + 1)
			.expect("registry chunk");
		let root = chunk.spawn_node(
			SemanticKind::RelationalTable,
			None,
			SpanMetrics {
				byte_length: ORIGINAL_BYTES,
				newlines: 0,
			},
		);
		let mut tail = None;
		for extent in 0..EXTENTS {
			let leaf = chunk.spawn_node(
				SemanticKind::Token,
				Some(crate::svp::SvpPointer {
					lba: (extent * EXTENT_BYTES) as u64 / 512,
					byte_length: EXTENT_BYTES as u32,
					device_id: 1,
					head_trim: 0,
				}),
				SpanMetrics {
					byte_length: EXTENT_BYTES as u64,
					newlines: 0,
				},
			);
			chunk.append_local_child(root, leaf);
			if extent + 1 == EXTENTS {
				tail = Some(leaf);
			}
		}
		let tail = tail.expect("tail extent");
		registry.publish_hydrated_data(tail, vec![0; EXTENT_BYTES]);
		registry.metrics_inflated[tail.index()].store(true, Ordering::Release);
		registry.set_terminal_newline(tail, false);
		let (cursor_node, cursor_offset) = registry.insert_text(tail, EXTENT_BYTES as u32, b"TEST");
		assert_ne!(cursor_node, tail, "the full cold extent must stay bounded");
		assert_eq!(cursor_offset, 4);
		assert!(
			unsafe { (*registry.metrics[cursor_node.index()].get()).byte_length }
				<= EXTENT_BYTES as u64,
			"an EOF edit must not create a leaf that cannot be reloaded after save"
		);

		let outcome = save_document(&registry, root, Some(&path), &path, SaveDurability::Bounded)
			.expect("append-only save");
		// Verifying the tail must itself stay sparse: reading the whole file back
		// would allocate and fault in the entire fixture, which is the cost this
		// save path exists to avoid.
		let saved_tail = read_at(&path, ORIGINAL_BYTES, 4);
		let _ = std::fs::remove_file(&path);

		assert_eq!(saved_tail, b"TEST");
		assert_eq!(outcome.method, SaveMethod::InPlaceSuffix);
		assert!(
			outcome.stats.source_bytes_read <= EXTENT_BYTES as u64,
			"EOF-only edit reread {} bytes from an unchanged prefix",
			outcome.stats.source_bytes_read
		);
		assert!(
			outcome.stats.target_bytes_written <= EXTENT_BYTES as u64 + 4,
			"EOF-only edit rewrote {} bytes instead of only its bounded tail",
			outcome.stats.target_bytes_written
		);
	}

	#[test]
	fn an_atomic_document_never_takes_the_bounded_path() {
		// Same document shape the bounded path accepts, but the user has not asked
		// for reduced durability. Size and leaf layout must not be enough on their
		// own to put a torn tail on someone's file.
		const EXTENT_BYTES: usize = 1024 * 1024;
		const EXTENTS: usize = (MIN_IN_PLACE_SAVE_BYTES as usize) / EXTENT_BYTES;
		const ORIGINAL_BYTES: u64 = (EXTENT_BYTES * EXTENTS) as u64;
		let path = temp_test_path("atomic-declines-bounded-save");
		let source = std::fs::File::create(&path).expect("create sparse source");
		source.set_len(ORIGINAL_BYTES).expect("size sparse source");
		drop(source);

		let registry = UastRegistry::new(EXTENTS as u32 + 4);
		let mut chunk = registry
			.reserve_chunk(EXTENTS as u32 + 1)
			.expect("registry chunk");
		let root = chunk.spawn_node(
			SemanticKind::RelationalTable,
			None,
			SpanMetrics {
				byte_length: ORIGINAL_BYTES,
				newlines: 0,
			},
		);
		let mut tail = None;
		for extent in 0..EXTENTS {
			let leaf = chunk.spawn_node(
				SemanticKind::Token,
				Some(crate::svp::SvpPointer {
					lba: (extent * EXTENT_BYTES) as u64 / 512,
					byte_length: EXTENT_BYTES as u32,
					device_id: 1,
					head_trim: 0,
				}),
				SpanMetrics {
					byte_length: EXTENT_BYTES as u64,
					newlines: 0,
				},
			);
			chunk.append_local_child(root, leaf);
			if extent + 1 == EXTENTS {
				tail = Some(leaf);
			}
		}
		registry.publish_hydrated_data(tail.expect("tail extent"), vec![0; EXTENT_BYTES]);

		let declined = try_save_bounded_in_place_suffix(
			&registry,
			root,
			Some(&path),
			&path,
			SaveDurability::Atomic,
		)
		.expect("bounded save probe");
		let _ = std::fs::remove_file(&path);
		assert!(
			declined.is_none(),
			"a document qualifies for the bounded path only once its owner asks for it"
		);
	}

	#[test]
	fn a_failed_in_place_write_restores_the_original_tail() {
		// The bounded path writes over the user's file, so its only protection
		// against an ordinary write error is putting the old tail back. Tear the
		// tail the way a partial write would and check the rollback undoes both the
		// damaged bytes and the truncated length.
		let path = temp_test_path("in-place-rollback");
		std::fs::write(&path, b"PREFIX-ORIGINALTAIL").expect("write source");
		const REWRITE_START: u64 = 7;
		let original_len = 19u64;
		let original_suffix = b"ORIGINALTAIL";

		let mut file = OpenOptions::new()
			.read(true)
			.write(true)
			.open(&path)
			.expect("open target");
		file.seek(SeekFrom::Start(REWRITE_START))
			.and_then(|_| file.write_all(b"SHORT"))
			.and_then(|_| file.set_len(REWRITE_START + 5))
			.expect("tear the tail");

		restore_in_place_suffix(&mut file, REWRITE_START, original_len, original_suffix)
			.expect("rollback should succeed");

		assert_eq!(
			std::fs::read(&path).expect("read restored"),
			b"PREFIX-ORIGINALTAIL",
			"rollback must restore the original bytes and length"
		);
		let _ = std::fs::remove_file(&path);
	}

	#[test]
	fn resident_publication_waits_for_an_active_streaming_save_reader() {
		let (registry, root) = build_mixed_save_document();
		let first = registry.get_first_child(root).expect("first leaf");
		let middle = registry.get_next_sibling(first).expect("middle leaf");
		let last = registry.get_next_sibling(middle).expect("last leaf");
		registry.publish_hydrated_data(first, b"alpha".to_vec());
		registry.publish_hydrated_data(last, b"gamma".to_vec());
		let registry = Arc::new(registry);
		let (started_tx, started_rx) = mpsc::channel();
		let (release_tx, release_rx) = mpsc::channel();
		let save_registry = registry.clone();
		let save = std::thread::spawn(move || {
			let mut writer = BlockingWriter {
				bytes: Vec::new(),
				started: Some(started_tx),
				release: release_rx,
			};
			stream_document_to_writer(&save_registry, root, None, &mut writer)
				.expect("stream save");
			writer.bytes
		});

		started_rx
			.recv_timeout(Duration::from_secs(1))
			.expect("save reached resident bytes");
		let (published_tx, published_rx) = mpsc::channel();
		let publish_registry = registry.clone();
		let publish = std::thread::spawn(move || {
			publish_registry.publish_hydrated_data(first, b"XXXXX".to_vec());
			let _ = published_tx.send(());
		});
		assert!(
			published_rx
				.recv_timeout(Duration::from_millis(50))
				.is_err(),
			"DMA publication must wait while save holds a resident-data read guard"
		);

		release_tx.send(()).expect("release save writer");
		assert_eq!(save.join().expect("save thread"), b"alphaZZgamma");
		published_rx
			.recv_timeout(Duration::from_secs(1))
			.expect("publication resumes after save");
		publish.join().expect("publication thread");
	}

	#[test]
	fn read_document_snapshot_streams_unloaded_source_backed_documents() {
		let source_path = temp_test_path("snapshot-source");
		std::fs::write(&source_path, b"alphagamma").expect("write source");

		let (registry, root) = build_mixed_save_document();
		let snapshot =
			read_document_snapshot(&registry, root, Some(&source_path)).expect("snapshot bytes");
		assert_eq!(&snapshot, b"alphaZZgamma");

		let _ = std::fs::remove_file(source_path);
	}

	#[test]
	fn whole_document_snapshot_rejects_200_gib_before_allocating() {
		let registry = UastRegistry::new(1);
		let mut chunk = registry.reserve_chunk(1).expect("registry chunk");
		let root = chunk.spawn_node(
			SemanticKind::RelationalTable,
			None,
			SpanMetrics {
				byte_length: 200 * 1024 * 1024 * 1024,
				newlines: 0,
			},
		);
		let error = read_document_snapshot(&registry, root, None).unwrap_err();
		assert!(error.contains("Whole-document operation unavailable"));
	}

	#[test]
	fn rebind_document_spans_to_saved_file_re_sparsifies_saved_leaves() {
		let (registry, root) = build_mixed_save_document();
		registry.dma_in_flight[1].store(true, Ordering::Relaxed);
		registry.dma_in_flight[2].store(true, Ordering::Relaxed);
		registry.dma_in_flight[3].store(true, Ordering::Relaxed);
		rebind_document_spans_to_saved_file(&registry, root, FILE_DEVICE_ID);

		let first = registry.get_first_child(root).expect("first leaf");
		let second = registry.get_next_sibling(first).expect("second leaf");
		let third = registry.get_next_sibling(second).expect("third leaf");

		let first_span = unsafe { (*registry.spans[first.index()].get()).expect("first span") };
		let second_span = unsafe { (*registry.spans[second.index()].get()).expect("second span") };
		let third_span = unsafe { (*registry.spans[third.index()].get()).expect("third span") };

		assert_eq!(first_span.lba * 512 + u64::from(first_span.head_trim), 0);
		assert_eq!(second_span.lba * 512 + u64::from(second_span.head_trim), 5);
		assert_eq!(third_span.lba * 512 + u64::from(third_span.head_trim), 7);

		assert!(unsafe { (*registry.virtual_data[first.index()].get()).is_none() });
		assert!(unsafe { (*registry.virtual_data[second.index()].get()).is_none() });
		assert!(unsafe { (*registry.virtual_data[third.index()].get()).is_none() });

		assert!(!registry.dma_in_flight[first.index()].load(Ordering::Relaxed));
		assert!(!registry.dma_in_flight[second.index()].load(Ordering::Relaxed));
		assert!(!registry.dma_in_flight[third.index()].load(Ordering::Relaxed));

		assert!(!registry.metrics_inflated[first.index()].load(Ordering::Relaxed));
		assert!(registry.metrics_inflated[second.index()].load(Ordering::Relaxed));
		assert!(!registry.metrics_inflated[third.index()].load(Ordering::Relaxed));
	}
}
