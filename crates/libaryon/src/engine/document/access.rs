use super::edit::{apply_preflighted_deltas_to_document, fold_edits_for_deltas};
use crate::core::{BufferId, DocByte, DocLine, StateId, ViewId};
use crate::ecs::{NodeId, UastRegistry};
use crate::engine::session::{EditorSession, TextEditorMut};
use crate::engine::undo::TextDelta;
use crate::svp::resolver::{
	RESOLVER_TRANSFER_SIZE, SvpResolver, WholeLeafHydration, WholeLeafHydrationError,
};
use crate::uast::UastProjection;
use memchr::memchr_iter;
use std::collections::HashSet;

pub const MAX_READ_BYTES: u64 = RESOLVER_TRANSFER_SIZE as u64;
pub const MAX_LINE_SCAN_BYTES: u64 = RESOLVER_TRANSFER_SIZE as u64;
pub const MAX_OPERATION_NODES: usize = 65_536;
pub const MAX_HYDRATION_LEAVES: usize = 256;
pub const MAX_SNAPSHOT_BYTES: u64 = 256 * 1024 * 1024;
pub const MAX_EDIT_BYTES: u64 = RESOLVER_TRANSFER_SIZE as u64;
pub const MAX_BATCH_DELTAS: usize = 1_024;
pub const MAX_BATCH_BYTES: u64 = 16 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Readiness<T> {
	Ready(T),
	NotReady { waiting_on: WaitingOn },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WaitingOn {
	LineIndexBuilding,
	ColdExtent { start: DocByte, end: DocByte },
	LineNewlineHydration { line: DocLine },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadError {
	TooLarge { requested: u64, maximum: u64 },
	InvalidUtf8 { absolute_offset: DocByte },
	QueueFull,
	ScanLimitExceeded { scanned: u64, maximum: u64 },
	ColdReadFailed,
	StaleVersion,
	NotResident,
	InvalidChunkSize,
	LineOutOfRange { line: DocLine, line_count: u64 },
	WorkLimitExceeded { maximum: usize },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EditError {
	NotUtf8Boundary {
		endpoint: DocByte,
	},
	RangeInvalid {
		start: DocByte,
		end: DocByte,
		total: u64,
	},
	TooLarge {
		requested: u64,
		maximum: u64,
	},
	BatchTooLarge {
		requested: u64,
		maximum: u64,
	},
	NotUtf8Text {
		relative_offset: u64,
	},
	WorkLimitExceeded {
		maximum: usize,
	},
	QueueFull,
	ColdReadFailed,
	StaleVersion,
	ViewUnavailable,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BufferEdit {
	Bytes {
		start: DocByte,
		end: DocByte,
		replacement: Vec<u8>,
	},
	Text {
		start: DocByte,
		end: DocByte,
		replacement: Vec<u8>,
	},
}

impl BufferEdit {
	fn range(&self) -> (DocByte, DocByte) {
		match self {
			Self::Bytes { start, end, .. } | Self::Text { start, end, .. } => (*start, *end),
		}
	}

	fn replacement(&self) -> &[u8] {
		match self {
			Self::Bytes { replacement, .. } | Self::Text { replacement, .. } => replacement,
		}
	}

	fn is_text(&self) -> bool {
		matches!(self, Self::Text { .. })
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MutationOutcome {
	pub version: StateId,
	pub delta_count: usize,
	pub deleted_bytes: u64,
	pub inserted_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkCursor {
	pub start: DocByte,
	pub end: DocByte,
	pub position: DocByte,
	pub state: StateId,
	pub max_chunk_len: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkPage {
	pub bytes: Vec<u8>,
	pub next: DocByte,
	pub done: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LineCursor {
	pub next_line: DocLine,
	pub end_line: DocLine,
	pub state: StateId,
	pub max_line_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinePage {
	pub line: DocLine,
	pub start: DocByte,
	pub end: DocByte,
	pub bytes: Vec<u8>,
	pub truncated: bool,
	pub next_line: DocLine,
	pub done: bool,
}

#[derive(Debug, Clone, Copy)]
struct LeafSlice {
	node: NodeId,
	local_start: usize,
	len: usize,
	resident: bool,
}

#[derive(Debug, Default)]
struct WorkBudget {
	visited: usize,
}

impl WorkBudget {
	fn remaining(&self) -> usize {
		MAX_OPERATION_NODES.saturating_sub(self.visited)
	}

	fn charge(&mut self, visits: usize) -> Result<(), ReadError> {
		self.visited = self.visited.saturating_add(visits);
		if self.visited > MAX_OPERATION_NODES {
			Err(ReadError::WorkLimitExceeded {
				maximum: MAX_OPERATION_NODES,
			})
		} else {
			Ok(())
		}
	}
}

fn clamped_range(
	registry: &UastRegistry,
	root: NodeId,
	start: DocByte,
	end: DocByte,
) -> (u64, u64) {
	let total = registry.get_total_bytes(root);
	let start = start.get().min(total);
	let end = end.get().min(total);
	if start > end {
		(end, end)
	} else {
		(start, end)
	}
}

fn describe_range(
	registry: &UastRegistry,
	root: NodeId,
	start: u64,
	end: u64,
) -> Result<Vec<LeafSlice>, ReadError> {
	let mut budget = WorkBudget::default();
	describe_range_with_budget(registry, root, start, end, &mut budget)
}

fn describe_range_with_budget(
	registry: &UastRegistry,
	root: NodeId,
	start: u64,
	end: u64,
	budget: &mut WorkBudget,
) -> Result<Vec<LeafSlice>, ReadError> {
	if start >= end {
		return Ok(Vec::new());
	}
	let _resident_read = registry.resident_data_guard();
	let (target, descent_visits) = registry
		.find_node_at_doc_byte_raw_bounded(root, DocByte::new(start), budget.remaining())
		.map_err(|()| ReadError::WorkLimitExceeded {
			maximum: MAX_OPERATION_NODES,
		})?;
	budget.charge(descent_visits)?;
	let mut visit = Some(target.node_id);
	let mut local_start = target.node_byte.get() as usize;
	let mut remaining = end - start;
	let mut leaves = Vec::new();

	while let Some(node) = visit {
		budget.charge(1)?;
		let idx = node.index();
		let has_children = unsafe { (*registry.edges[idx].get()).first_child.is_some() };
		if has_children {
			visit = registry.get_first_child(node);
			continue;
		}
		let node_len = unsafe { (*registry.metrics[idx].get()).byte_length };
		let available = node_len.saturating_sub(local_start as u64);
		let take = available.min(remaining);
		if take > 0 {
			leaves.push(LeafSlice {
				node,
				local_start,
				len: usize::try_from(take).map_err(|_| ReadError::TooLarge {
					requested: take,
					maximum: usize::MAX as u64,
				})?,
				resident: unsafe { (&*registry.virtual_data[idx].get()).is_some() },
			});
			remaining -= take;
			if remaining == 0 {
				return Ok(leaves);
			}
		}
		local_start = 0;
		visit = registry.get_next_node_in_walk(node);
	}

	Err(ReadError::NotResident)
}

fn collect_resident(
	registry: &UastRegistry,
	leaves: &[LeafSlice],
	requested: usize,
) -> Result<Vec<u8>, ReadError> {
	let _resident_read = registry.resident_data_guard();
	let mut bytes = Vec::new();
	bytes
		.try_reserve_exact(requested)
		.map_err(|_| ReadError::TooLarge {
			requested: requested as u64,
			maximum: usize::MAX as u64,
		})?;
	for leaf in leaves {
		let data = unsafe { (&*registry.virtual_data[leaf.node.index()].get()).as_deref() }
			.ok_or(ReadError::NotResident)?;
		let end = leaf
			.local_start
			.checked_add(leaf.len)
			.ok_or(ReadError::WorkLimitExceeded {
				maximum: MAX_OPERATION_NODES,
			})?;
		let fragment = data
			.get(leaf.local_start..end)
			.ok_or(ReadError::NotResident)?;
		bytes.extend_from_slice(fragment);
	}
	Ok(bytes)
}

fn hydrate_cold_leaves(
	resolver: &SvpResolver,
	leaves: &[LeafSlice],
	hydration_buffer: Option<BufferId>,
) -> Result<bool, ReadError> {
	let cold: Vec<_> = leaves.iter().filter(|leaf| !leaf.resident).collect();
	if cold.len() > MAX_HYDRATION_LEAVES {
		return Err(ReadError::WorkLimitExceeded {
			maximum: MAX_HYDRATION_LEAVES,
		});
	}
	let mut pending = false;
	for leaf in cold {
		let hydration = match hydration_buffer {
			Some(buffer) => resolver.hydrate_whole_leaf_for_buffer(leaf.node, buffer),
			None => resolver.hydrate_whole_leaf(leaf.node),
		};
		match hydration {
			Ok(WholeLeafHydration::Ready) => {}
			Ok(WholeLeafHydration::Pending) => pending = true,
			Ok(WholeLeafHydration::Failed) => return Err(ReadError::ColdReadFailed),
			Err(WholeLeafHydrationError::QueueFull) => return Err(ReadError::QueueFull),
			Err(WholeLeafHydrationError::InvalidRequest) => return Err(ReadError::NotResident),
		}
	}
	Ok(pending)
}

fn read_range_internal(
	registry: &UastRegistry,
	resolver: &SvpResolver,
	root: NodeId,
	hydration_buffer: Option<BufferId>,
	start: u64,
	end: u64,
) -> Result<Readiness<Vec<u8>>, ReadError> {
	let requested = end.saturating_sub(start);
	if requested > MAX_READ_BYTES {
		return Err(ReadError::TooLarge {
			requested,
			maximum: MAX_READ_BYTES,
		});
	}
	let requested_usize = usize::try_from(requested).map_err(|_| ReadError::TooLarge {
		requested,
		maximum: usize::MAX as u64,
	})?;
	let leaves = describe_range(registry, root, start, end)?;
	if leaves.iter().all(|leaf| leaf.resident) {
		return collect_resident(registry, &leaves, requested_usize).map(Readiness::Ready);
	}
	if hydrate_cold_leaves(resolver, &leaves, hydration_buffer)? {
		return Ok(Readiness::NotReady {
			waiting_on: WaitingOn::ColdExtent {
				start: DocByte::new(start),
				end: DocByte::new(end),
			},
		});
	}

	let leaves = describe_range(registry, root, start, end)?;
	if leaves.iter().any(|leaf| !leaf.resident) {
		return Err(ReadError::NotResident);
	}
	collect_resident(registry, &leaves, requested_usize).map(Readiness::Ready)
}

pub fn read_bytes(
	registry: &UastRegistry,
	resolver: &SvpResolver,
	root: NodeId,
	start: DocByte,
	end: DocByte,
) -> Result<Readiness<Vec<u8>>, ReadError> {
	let (start, end) = clamped_range(registry, root, start, end);
	read_range_internal(registry, resolver, root, None, start, end)
}

pub(crate) fn read_bytes_for_buffer(
	registry: &UastRegistry,
	resolver: &SvpResolver,
	buffer: BufferId,
	root: NodeId,
	start: DocByte,
	end: DocByte,
) -> Result<Readiness<Vec<u8>>, ReadError> {
	let (start, end) = clamped_range(registry, root, start, end);
	read_range_internal(registry, resolver, root, Some(buffer), start, end)
}

pub fn decode_strict(bytes: &[u8], absolute_start: DocByte) -> Result<String, ReadError> {
	std::str::from_utf8(bytes)
		.map(str::to_owned)
		.map_err(|error| ReadError::InvalidUtf8 {
			absolute_offset: absolute_start.saturating_add(error.valid_up_to() as u64),
		})
}

pub fn decode_lossy(bytes: &[u8]) -> String {
	String::from_utf8_lossy(bytes).into_owned()
}

pub fn snapshot(
	registry: &UastRegistry,
	root: NodeId,
	max_bytes: u64,
) -> Result<Vec<u8>, ReadError> {
	let total = registry.get_total_bytes(root);
	let maximum = max_bytes.min(MAX_SNAPSHOT_BYTES);
	if total > maximum || max_bytes > MAX_SNAPSHOT_BYTES {
		return Err(ReadError::TooLarge {
			requested: total.max(max_bytes),
			maximum,
		});
	}
	let leaves = describe_range(registry, root, 0, total)?;
	if leaves.iter().any(|leaf| !leaf.resident) {
		return Err(ReadError::NotResident);
	}
	let requested = usize::try_from(total).map_err(|_| ReadError::TooLarge {
		requested: total,
		maximum: usize::MAX as u64,
	})?;
	collect_resident(registry, &leaves, requested)
}

pub fn read_chunk(
	registry: &UastRegistry,
	resolver: &SvpResolver,
	root: NodeId,
	current_state: StateId,
	cursor: &ChunkCursor,
) -> Result<Readiness<ChunkPage>, ReadError> {
	read_chunk_internal(registry, resolver, root, None, current_state, cursor)
}

pub(crate) fn read_chunk_for_buffer(
	registry: &UastRegistry,
	resolver: &SvpResolver,
	buffer: BufferId,
	root: NodeId,
	current_state: StateId,
	cursor: &ChunkCursor,
) -> Result<Readiness<ChunkPage>, ReadError> {
	read_chunk_internal(
		registry,
		resolver,
		root,
		Some(buffer),
		current_state,
		cursor,
	)
}

fn read_chunk_internal(
	registry: &UastRegistry,
	resolver: &SvpResolver,
	root: NodeId,
	hydration_buffer: Option<BufferId>,
	current_state: StateId,
	cursor: &ChunkCursor,
) -> Result<Readiness<ChunkPage>, ReadError> {
	if current_state != cursor.state {
		return Err(ReadError::StaleVersion);
	}
	let (start, end) = clamped_range(registry, root, cursor.start, cursor.end);
	let position = cursor.position.get().clamp(start, end);
	if position == end {
		return Ok(Readiness::Ready(ChunkPage {
			bytes: Vec::new(),
			next: DocByte::new(end),
			done: true,
		}));
	}
	if cursor.max_chunk_len == 0 {
		return Err(ReadError::InvalidChunkSize);
	}
	let chunk_end = position
		.saturating_add(cursor.max_chunk_len.min(MAX_READ_BYTES))
		.min(end);
	match read_range_internal(
		registry,
		resolver,
		root,
		hydration_buffer,
		position,
		chunk_end,
	)? {
		Readiness::Ready(bytes) => Ok(Readiness::Ready(ChunkPage {
			bytes,
			next: DocByte::new(chunk_end),
			done: chunk_end == end,
		})),
		Readiness::NotReady { waiting_on } => Ok(Readiness::NotReady { waiting_on }),
	}
}

pub fn line_count(registry: &UastRegistry, root: NodeId) -> Result<Readiness<u64>, ReadError> {
	if let Some(count) = registry.exact_line_count(root) {
		Ok(Readiness::Ready(count))
	} else if registry.line_metrics_failed(root) {
		Err(ReadError::ColdReadFailed)
	} else {
		Ok(Readiness::NotReady {
			waiting_on: WaitingOn::LineIndexBuilding,
		})
	}
}

pub fn line_start(
	registry: &UastRegistry,
	resolver: &SvpResolver,
	root: NodeId,
	line: DocLine,
) -> Result<Readiness<DocByte>, ReadError> {
	line_start_internal(registry, resolver, root, None, line)
}

pub(crate) fn line_start_for_buffer(
	registry: &UastRegistry,
	resolver: &SvpResolver,
	buffer: BufferId,
	root: NodeId,
	line: DocLine,
) -> Result<Readiness<DocByte>, ReadError> {
	line_start_internal(registry, resolver, root, Some(buffer), line)
}

fn line_start_internal(
	registry: &UastRegistry,
	resolver: &SvpResolver,
	root: NodeId,
	hydration_buffer: Option<BufferId>,
	line: DocLine,
) -> Result<Readiness<DocByte>, ReadError> {
	let count = match line_count(registry, root)? {
		Readiness::Ready(count) => count,
		Readiness::NotReady { waiting_on } => return Ok(Readiness::NotReady { waiting_on }),
	};
	if line.get() >= count {
		return Err(ReadError::LineOutOfRange {
			line,
			line_count: count,
		});
	}
	if line == DocLine::ZERO {
		return Ok(Readiness::Ready(DocByte::ZERO));
	}

	let wanted_newline = line.get() - 1;
	let mut node = root;
	let mut absolute_byte = 0u64;
	let mut newline_prefix = 0u64;
	let mut visits = 0usize;
	loop {
		visits = visits.saturating_add(1);
		if visits > MAX_OPERATION_NODES {
			return Err(ReadError::WorkLimitExceeded {
				maximum: MAX_OPERATION_NODES,
			});
		}
		let idx = node.index();
		if let Some(mut child) = registry.get_first_child(node) {
			loop {
				visits = visits.saturating_add(1);
				if visits > MAX_OPERATION_NODES {
					return Err(ReadError::WorkLimitExceeded {
						maximum: MAX_OPERATION_NODES,
					});
				}
				let metrics = unsafe { &*registry.metrics[child.index()].get() };
				if wanted_newline < newline_prefix.saturating_add(metrics.newlines) {
					node = child;
					break;
				}
				newline_prefix = newline_prefix.saturating_add(metrics.newlines);
				absolute_byte = absolute_byte.saturating_add(metrics.byte_length);
				child = registry
					.get_next_sibling(child)
					.ok_or(ReadError::LineOutOfRange {
						line,
						line_count: count,
					})?;
			}
			continue;
		}

		let leaf_len = unsafe { (*registry.metrics[idx].get()).byte_length };
		if leaf_len > MAX_LINE_SCAN_BYTES {
			return Err(ReadError::ScanLimitExceeded {
				scanned: leaf_len,
				maximum: MAX_LINE_SCAN_BYTES,
			});
		}
		if !registry.whole_leaf_is_resident(node) {
			let hydration = match hydration_buffer {
				Some(buffer) => resolver.hydrate_whole_leaf_for_buffer(node, buffer),
				None => resolver.hydrate_whole_leaf(node),
			};
			match hydration {
				Ok(WholeLeafHydration::Ready) => {}
				Ok(WholeLeafHydration::Pending) => {
					return Ok(Readiness::NotReady {
						waiting_on: WaitingOn::LineNewlineHydration { line },
					});
				}
				Ok(WholeLeafHydration::Failed) => return Err(ReadError::ColdReadFailed),
				Err(WholeLeafHydrationError::QueueFull) => return Err(ReadError::QueueFull),
				Err(WholeLeafHydrationError::InvalidRequest) => {
					return Err(ReadError::NotResident);
				}
			}
		}
		let _resident_read = registry.resident_data_guard();
		let bytes = unsafe { (&*registry.virtual_data[idx].get()).as_deref() }
			.ok_or(ReadError::NotResident)?;
		let local_ordinal = wanted_newline.saturating_sub(newline_prefix) as usize;
		let newline =
			memchr_iter(b'\n', bytes)
				.nth(local_ordinal)
				.ok_or(ReadError::LineOutOfRange {
					line,
					line_count: count,
				})?;
		return Ok(Readiness::Ready(DocByte::new(
			absolute_byte
				.saturating_add(newline as u64)
				.saturating_add(1),
		)));
	}
}

pub fn read_line_page(
	registry: &UastRegistry,
	resolver: &SvpResolver,
	root: NodeId,
	current_state: StateId,
	cursor: &LineCursor,
) -> Result<Readiness<LinePage>, ReadError> {
	read_line_page_internal(registry, resolver, root, None, current_state, cursor)
}

pub(crate) fn read_line_page_for_buffer(
	registry: &UastRegistry,
	resolver: &SvpResolver,
	buffer: BufferId,
	root: NodeId,
	current_state: StateId,
	cursor: &LineCursor,
) -> Result<Readiness<LinePage>, ReadError> {
	read_line_page_internal(
		registry,
		resolver,
		root,
		Some(buffer),
		current_state,
		cursor,
	)
}

fn read_line_page_internal(
	registry: &UastRegistry,
	resolver: &SvpResolver,
	root: NodeId,
	hydration_buffer: Option<BufferId>,
	current_state: StateId,
	cursor: &LineCursor,
) -> Result<Readiness<LinePage>, ReadError> {
	if current_state != cursor.state {
		return Err(ReadError::StaleVersion);
	}
	if cursor.next_line >= cursor.end_line {
		return Ok(Readiness::Ready(LinePage {
			line: cursor.next_line,
			start: DocByte::ZERO,
			end: DocByte::ZERO,
			bytes: Vec::new(),
			truncated: false,
			next_line: cursor.next_line,
			done: true,
		}));
	}
	if cursor.max_line_bytes == 0 {
		return Err(ReadError::InvalidChunkSize);
	}
	let start =
		match line_start_internal(registry, resolver, root, hydration_buffer, cursor.next_line)? {
			Readiness::Ready(start) => start,
			Readiness::NotReady { waiting_on } => return Ok(Readiness::NotReady { waiting_on }),
		};
	let count = match line_count(registry, root)? {
		Readiness::Ready(count) => count,
		Readiness::NotReady { waiting_on } => return Ok(Readiness::NotReady { waiting_on }),
	};
	let next_line = cursor.next_line.saturating_add(1);
	let end = if next_line.get() < count {
		match line_start_internal(registry, resolver, root, hydration_buffer, next_line)? {
			Readiness::Ready(end) => end,
			Readiness::NotReady { waiting_on } => {
				return Ok(Readiness::NotReady { waiting_on });
			}
		}
	} else {
		DocByte::new(registry.get_total_bytes(root))
	};
	let line_len = end.get().saturating_sub(start.get());
	let take = line_len.min(cursor.max_line_bytes).min(MAX_READ_BYTES);
	match read_range_internal(
		registry,
		resolver,
		root,
		hydration_buffer,
		start.get(),
		start.get().saturating_add(take),
	)? {
		Readiness::Ready(bytes) => Ok(Readiness::Ready(LinePage {
			line: cursor.next_line,
			start,
			end,
			bytes,
			truncated: take < line_len,
			next_line,
			done: next_line >= cursor.end_line,
		})),
		Readiness::NotReady { waiting_on } => Ok(Readiness::NotReady { waiting_on }),
	}
}

#[derive(Debug)]
struct PreparedEdits {
	deltas: Vec<TextDelta>,
	base_version: StateId,
	deleted_bytes: u64,
	inserted_bytes: u64,
}

fn edit_read_error(error: ReadError) -> EditError {
	match error {
		ReadError::QueueFull => EditError::QueueFull,
		ReadError::ColdReadFailed | ReadError::NotResident => EditError::ColdReadFailed,
		ReadError::WorkLimitExceeded { maximum } => EditError::WorkLimitExceeded { maximum },
		ReadError::TooLarge { requested, maximum } => EditError::TooLarge { requested, maximum },
		_ => EditError::WorkLimitExceeded {
			maximum: MAX_OPERATION_NODES,
		},
	}
}

fn merge_intervals(mut intervals: Vec<(u64, u64)>) -> Vec<(u64, u64)> {
	intervals.retain(|(start, end)| start < end);
	intervals.sort_unstable_by_key(|(start, _)| *start);
	let mut merged: Vec<(u64, u64)> = Vec::with_capacity(intervals.len());
	for (start, end) in intervals {
		if let Some((_, previous_end)) = merged.last_mut()
			&& start <= *previous_end
		{
			*previous_end = (*previous_end).max(end);
		} else {
			merged.push((start, end));
		}
	}
	merged
}

fn local_utf8_boundary(bytes: &[u8], endpoint: usize) -> bool {
	let preceding_start = endpoint.saturating_sub(3);
	for lead in preceding_start..endpoint {
		let width = match bytes[lead] {
			0xc2..=0xdf => 2,
			0xe0..=0xef => 3,
			0xf0..=0xf4 => 4,
			_ => continue,
		};
		let scalar_end = lead.saturating_add(width);
		if endpoint < scalar_end
			&& scalar_end <= bytes.len()
			&& std::str::from_utf8(&bytes[lead..scalar_end])
				.ok()
				.is_some_and(|text| text.chars().count() == 1)
		{
			return false;
		}
	}
	true
}

fn collect_with_budget(
	registry: &UastRegistry,
	root: NodeId,
	start: u64,
	end: u64,
	budget: &mut WorkBudget,
) -> Result<Vec<u8>, EditError> {
	let leaves =
		describe_range_with_budget(registry, root, start, end, budget).map_err(edit_read_error)?;
	let requested =
		usize::try_from(end.saturating_sub(start)).map_err(|_| EditError::TooLarge {
			requested: end.saturating_sub(start),
			maximum: usize::MAX as u64,
		})?;
	collect_resident(registry, &leaves, requested).map_err(edit_read_error)
}

/// Account for the topology descents that the infallible commit will repeat.
///
/// Edits are ordered from high to low, so an earlier splice cannot make the
/// path to a later, lower byte offset more expensive. One bounded lookup on
/// the pre-mutation tree therefore gives a conservative cost for every commit
/// lookup at that offset. The extra unit accounts for this preflight lookup.
fn preflight_commit_work(
	registry: &UastRegistry,
	root: NodeId,
	total: u64,
	edits: &[BufferEdit],
	budget: &mut WorkBudget,
) -> Result<(), EditError> {
	for edit in edits {
		let (start, end) = edit.range();
		let commit_lookups = usize::from(total > 0)
			+ usize::from(start < end)
			+ usize::from(!edit.replacement().is_empty());
		if commit_lookups == 0 {
			continue;
		}
		let (_, visits) = registry
			.find_node_at_doc_byte_raw_bounded(root, start, budget.remaining())
			.map_err(|()| EditError::WorkLimitExceeded {
				maximum: MAX_OPERATION_NODES,
			})?;
		let charged = visits.checked_mul(commit_lookups.saturating_add(1)).ok_or(
			EditError::WorkLimitExceeded {
				maximum: MAX_OPERATION_NODES,
			},
		)?;
		budget.charge(charged).map_err(edit_read_error)?;
	}
	Ok(())
}

fn prepare_edits(
	registry: &UastRegistry,
	resolver: &SvpResolver,
	buffer: BufferId,
	root: NodeId,
	current_version: StateId,
	edits: &[BufferEdit],
	expected_version: Option<StateId>,
) -> Result<Readiness<PreparedEdits>, EditError> {
	if expected_version.is_some_and(|expected| expected != current_version) {
		return Err(EditError::StaleVersion);
	}
	if edits.len() > MAX_BATCH_DELTAS {
		return Err(EditError::BatchTooLarge {
			requested: edits.len() as u64,
			maximum: MAX_BATCH_DELTAS as u64,
		});
	}
	let total = registry.get_total_bytes(root);
	let mut total_batch_bytes = 0u64;
	let mut total_deleted = 0u64;
	let mut total_inserted = 0u64;
	let mut previous: Option<(u64, u64)> = None;
	let mut intervals = Vec::new();
	for edit in edits {
		let (start, end) = edit.range();
		if start > end || end.get() > total {
			return Err(EditError::RangeInvalid { start, end, total });
		}
		if let Some((previous_start, previous_end)) = previous {
			let repeated_empty =
				start == end && start.get() == previous_start && previous_start == previous_end;
			if end.get() > previous_start || repeated_empty {
				return Err(EditError::RangeInvalid { start, end, total });
			}
		}
		previous = Some((start.get(), end.get()));

		let deleted = end.get() - start.get();
		let inserted = edit.replacement().len() as u64;
		let splice_bytes = deleted.checked_add(inserted).ok_or(EditError::TooLarge {
			requested: u64::MAX,
			maximum: MAX_EDIT_BYTES,
		})?;
		if splice_bytes > MAX_EDIT_BYTES {
			return Err(EditError::TooLarge {
				requested: splice_bytes,
				maximum: MAX_EDIT_BYTES,
			});
		}
		total_batch_bytes =
			total_batch_bytes
				.checked_add(splice_bytes)
				.ok_or(EditError::BatchTooLarge {
					requested: u64::MAX,
					maximum: MAX_BATCH_BYTES,
				})?;
		total_deleted += deleted;
		total_inserted += inserted;
		if total_batch_bytes > MAX_BATCH_BYTES {
			return Err(EditError::BatchTooLarge {
				requested: total_batch_bytes,
				maximum: MAX_BATCH_BYTES,
			});
		}

		if edit.is_text()
			&& let Err(error) = std::str::from_utf8(edit.replacement())
		{
			return Err(EditError::NotUtf8Text {
				relative_offset: error.valid_up_to() as u64,
			});
		}

		// Hydrate the deleted bytes and both possible post-delete insertion
		// neighbors. The exact insertion leaf can be the preceding or following
		// leaf when a deletion removes a whole extent.
		intervals.push((start.get(), end.get()));
		if total > 0 {
			intervals.push((
				start.get().saturating_sub(1),
				start.get().saturating_add(1).min(total),
			));
			intervals.push((end.get().min(total), end.get().saturating_add(1).min(total)));
		}
		if edit.is_text() {
			for endpoint in [start.get(), end.get()] {
				intervals.push((
					endpoint.saturating_sub(3),
					endpoint.saturating_add(3).min(total),
				));
			}
		}
	}

	let intervals = merge_intervals(intervals);
	let mut budget = WorkBudget::default();
	preflight_commit_work(registry, root, total, edits, &mut budget)?;
	let mut cold_nodes = HashSet::new();
	for (start, end) in &intervals {
		let leaves = describe_range_with_budget(registry, root, *start, *end, &mut budget)
			.map_err(edit_read_error)?;
		for leaf in leaves {
			if !leaf.resident {
				cold_nodes.insert(leaf.node);
			}
		}
	}
	if cold_nodes.len() > MAX_HYDRATION_LEAVES {
		return Err(EditError::WorkLimitExceeded {
			maximum: MAX_HYDRATION_LEAVES,
		});
	}
	let mut pending = false;
	for node in cold_nodes {
		match resolver.hydrate_whole_leaf_for_buffer(node, buffer) {
			Ok(WholeLeafHydration::Ready) => {}
			Ok(WholeLeafHydration::Pending) => pending = true,
			Ok(WholeLeafHydration::Failed) => return Err(EditError::ColdReadFailed),
			Err(WholeLeafHydrationError::QueueFull) => return Err(EditError::QueueFull),
			Err(WholeLeafHydrationError::InvalidRequest) => {
				return Err(EditError::ColdReadFailed);
			}
		}
	}
	if pending {
		let start = intervals.first().map_or(0, |interval| interval.0);
		let end = intervals.last().map_or(start, |interval| interval.1);
		return Ok(Readiness::NotReady {
			waiting_on: WaitingOn::ColdExtent {
				start: DocByte::new(start),
				end: DocByte::new(end),
			},
		});
	}

	let mut deltas = Vec::with_capacity(edits.len());
	for edit in edits {
		let (start, end) = edit.range();
		if edit.is_text() {
			for endpoint in [start, end] {
				let window_start = endpoint.get().saturating_sub(3);
				let window_end = endpoint.get().saturating_add(3).min(total);
				let window =
					collect_with_budget(registry, root, window_start, window_end, &mut budget)?;
				let local_endpoint = usize::try_from(endpoint.get() - window_start)
					.expect("UTF-8 boundary window is at most six bytes");
				if !local_utf8_boundary(&window, local_endpoint) {
					return Err(EditError::NotUtf8Boundary { endpoint });
				}
			}
		}
		let deleted_bytes =
			collect_with_budget(registry, root, start.get(), end.get(), &mut budget)?;
		deltas.push(TextDelta {
			global_byte_offset: start,
			deleted_bytes,
			inserted_bytes: edit.replacement().to_vec(),
			state_before: StateId::ZERO,
			state_after: StateId::ZERO,
		});
	}

	Ok(Readiness::Ready(PreparedEdits {
		deltas,
		base_version: current_version,
		deleted_bytes: total_deleted,
		inserted_bytes: total_inserted,
	}))
}

fn commit_prepared_edits(
	registry: &UastRegistry,
	editor: &mut TextEditorMut<'_>,
	prepared: PreparedEdits,
) -> Result<MutationOutcome, EditError> {
	if editor.document.ledger.current_state_id != prepared.base_version {
		return Err(EditError::StaleVersion);
	}
	let state_before = prepared.base_version;
	let cursors_before = editor.cursor.snapshot();
	let fold_edits = fold_edits_for_deltas(&prepared.deltas);
	editor.document.bump_topology_rev();
	let mut root_id = Some(editor.document.root_id);
	let delta_count = prepared.deltas.len();
	apply_preflighted_deltas_to_document(
		registry,
		&mut root_id,
		&mut editor.view.folds,
		&mut editor.document.ledger,
		&mut editor.document.semantic.highlights,
		&mut editor.document.semantic.diagnostics,
		prepared.deltas,
	);
	editor.cursor.rebase_after_edits(&fold_edits);
	let total_bytes = registry.get_total_bytes(editor.document.root_id);
	editor.cursor.clamp_anchors(total_bytes);
	if let Some(points) = editor.cursor.points_mut() {
		points.normalize(registry, editor.document.root_id);
	}
	editor.document.ledger.finish_cursor_transaction(
		state_before,
		cursors_before,
		editor.cursor.snapshot(),
	);
	Ok(MutationOutcome {
		version: editor.document.ledger.current_state_id,
		delta_count,
		deleted_bytes: prepared.deleted_bytes,
		inserted_bytes: prepared.inserted_bytes,
	})
}

pub fn edit_text_view(
	registry: &UastRegistry,
	resolver: &SvpResolver,
	session: &mut EditorSession,
	acting_view_id: ViewId,
	edits: &[BufferEdit],
	expected_version: Option<StateId>,
) -> Result<Readiness<MutationOutcome>, EditError> {
	let (buffer, root, version) = {
		let view = session
			.layout
			.find_view(acting_view_id)
			.ok_or(EditError::ViewUnavailable)?;
		let document = session
			.buffers
			.get(view.buffer_id)
			.and_then(|buffer| buffer.text())
			.ok_or(EditError::ViewUnavailable)?;
		(
			view.buffer_id,
			document.root_id,
			document.ledger.current_state_id,
		)
	};
	let prepared = match prepare_edits(
		registry,
		resolver,
		buffer,
		root,
		version,
		edits,
		expected_version,
	)? {
		Readiness::Ready(prepared) => prepared,
		Readiness::NotReady { waiting_on } => return Ok(Readiness::NotReady { waiting_on }),
	};
	if prepared.deltas.is_empty() {
		return Ok(Readiness::Ready(MutationOutcome {
			version: prepared.base_version,
			delta_count: 0,
			deleted_bytes: 0,
			inserted_bytes: 0,
		}));
	}
	let outcome = EditorSession::mutate_text_buffer(
		registry,
		&mut session.buffers,
		&mut session.layout,
		&mut session.events,
		acting_view_id,
		|editor| commit_prepared_edits(registry, editor, prepared),
	)
	.ok_or(EditError::ViewUnavailable)??;
	Ok(Readiness::Ready(outcome))
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::core::DocLine;
	use crate::engine::event_bus::EngineEvent;
	use crate::engine::settings::SaveDurability;
	use crate::engine::{SplitDirection, UiColorOverrides};
	use crate::svp::backing_store::GeneratedBackingStore;
	use crate::svp::pointer::SvpPointer;
	use crate::uast::kind::SemanticKind;
	use crate::uast::metrics::SpanMetrics;
	use std::collections::BTreeMap;
	use std::sync::{Arc, mpsc};
	use std::time::Duration;

	fn resident_document(bytes: &[u8]) -> (Arc<UastRegistry>, NodeId) {
		let registry = Arc::new(UastRegistry::new(2));
		let mut chunk = registry.reserve_chunk(2).expect("document nodes");
		let newlines = bytes.iter().filter(|&&byte| byte == b'\n').count() as u64;
		let root = chunk.spawn_node(
			SemanticKind::RelationalTable,
			None,
			SpanMetrics {
				byte_length: bytes.len() as u64,
				newlines,
			},
		);
		let leaf = chunk.spawn_node(
			SemanticKind::Token,
			None,
			SpanMetrics {
				byte_length: bytes.len() as u64,
				newlines,
			},
		);
		chunk.append_local_child(root, leaf);
		registry.replace_authoritative_data(leaf, bytes.to_vec());
		registry.metrics_inflated[leaf.index()].store(true, std::sync::atomic::Ordering::Release);
		registry.set_terminal_newline(leaf, bytes.last() == Some(&b'\n'));
		drop(chunk);
		(registry, root)
	}

	fn resolver(registry: Arc<UastRegistry>) -> (SvpResolver, mpsc::Receiver<()>) {
		let (notify, notifications) = mpsc::channel();
		(
			SvpResolver::new_blocking_for_tests(registry, notify),
			notifications,
		)
	}

	fn editor_session(registry: &UastRegistry) -> EditorSession {
		EditorSession::new(
			registry,
			"onedark".to_string(),
			true,
			true,
			true,
			false,
			true,
			true,
			SaveDurability::default(),
			UiColorOverrides::default(),
			BTreeMap::new(),
			None,
		)
	}

	fn text_root_and_version(session: &EditorSession, view_id: ViewId) -> (NodeId, StateId) {
		let view = session.layout.find_view(view_id).expect("text view");
		let document = session
			.buffers
			.get(view.buffer_id)
			.and_then(|buffer| buffer.text())
			.expect("text buffer");
		(document.root_id, document.ledger.current_state_id)
	}

	#[test]
	fn bounded_reads_clamp_ranges_and_preserve_split_utf8_bytes() {
		let (registry, root) = resident_document("aéz".as_bytes());
		let (resolver, _notifications) = resolver(registry.clone());
		assert_eq!(
			read_bytes(&registry, &resolver, root, DocByte::new(2), DocByte::new(3)),
			Ok(Readiness::Ready(vec![0xa9]))
		);
		assert_eq!(
			read_bytes(
				&registry,
				&resolver,
				root,
				DocByte::new(99),
				DocByte::new(1)
			),
			Ok(Readiness::Ready(Vec::new()))
		);
		assert_eq!(
			read_bytes(
				&registry,
				&resolver,
				root,
				DocByte::new(3),
				DocByte::new(99)
			),
			Ok(Readiness::Ready(b"z".to_vec()))
		);
	}

	#[test]
	fn decode_modes_are_explicit_and_strict_offsets_are_absolute() {
		let bytes = b"a\xffz";
		assert_eq!(
			decode_strict(bytes, DocByte::new(40)),
			Err(ReadError::InvalidUtf8 {
				absolute_offset: DocByte::new(41)
			})
		);
		assert_eq!(decode_lossy(bytes), "a�z");
	}

	#[test]
	fn cold_read_is_not_ready_then_wakes_and_returns_exact_bytes() {
		let registry = Arc::new(UastRegistry::new(2));
		let mut chunk = registry.reserve_chunk(2).expect("document nodes");
		let root = chunk.spawn_node(
			SemanticKind::RelationalTable,
			None,
			SpanMetrics {
				byte_length: 4,
				newlines: 0,
			},
		);
		let leaf = chunk.spawn_node(
			SemanticKind::Token,
			Some(SvpPointer {
				lba: 0,
				byte_length: 4,
				device_id: 71,
				head_trim: 0,
			}),
			SpanMetrics {
				byte_length: 4,
				newlines: 0,
			},
		);
		chunk.append_local_child(root, leaf);
		registry.register_backing_store(
			71,
			Arc::new(GeneratedBackingStore::new(4, |offset| b'w' + offset as u8)),
		);
		let (resolver, notifications) = resolver(registry.clone());

		assert_eq!(
			read_bytes(&registry, &resolver, root, DocByte::ZERO, DocByte::new(4)),
			Ok(Readiness::NotReady {
				waiting_on: WaitingOn::ColdExtent {
					start: DocByte::ZERO,
					end: DocByte::new(4)
				}
			})
		);
		notifications
			.recv_timeout(Duration::from_secs(2))
			.expect("hydration refresh");
		assert_eq!(
			read_bytes(&registry, &resolver, root, DocByte::ZERO, DocByte::new(4)),
			Ok(Readiness::Ready(b"wxyz".to_vec()))
		);
	}

	#[test]
	fn snapshot_rejects_cold_content_before_allocating_output() {
		let registry = Arc::new(UastRegistry::new(2));
		let mut chunk = registry.reserve_chunk(2).expect("document nodes");
		let root = chunk.spawn_node(
			SemanticKind::RelationalTable,
			None,
			SpanMetrics {
				byte_length: 4,
				newlines: 0,
			},
		);
		let leaf = chunk.spawn_node(
			SemanticKind::Token,
			Some(SvpPointer {
				lba: 0,
				byte_length: 4,
				device_id: 72,
				head_trim: 0,
			}),
			SpanMetrics {
				byte_length: 4,
				newlines: 0,
			},
		);
		chunk.append_local_child(root, leaf);
		assert_eq!(snapshot(&registry, root, 4), Err(ReadError::NotResident));
	}

	#[test]
	fn oversized_read_and_snapshot_fail_from_metrics_before_traversal() {
		let registry = UastRegistry::new(1);
		let root = registry.alloc_test_node();
		unsafe {
			(*registry.metrics[root.index()].get()).byte_length = 200 * 1024 * 1024 * 1024;
		}
		let registry = Arc::new(registry);
		let (resolver, _notifications) = resolver(registry.clone());
		assert_eq!(
			read_bytes(
				&registry,
				&resolver,
				root,
				DocByte::ZERO,
				DocByte::new(MAX_READ_BYTES + 1),
			),
			Err(ReadError::TooLarge {
				requested: MAX_READ_BYTES + 1,
				maximum: MAX_READ_BYTES,
			})
		);
		assert_eq!(
			snapshot(&registry, root, MAX_SNAPSHOT_BYTES),
			Err(ReadError::TooLarge {
				requested: 200 * 1024 * 1024 * 1024,
				maximum: MAX_SNAPSHOT_BYTES,
			})
		);
	}

	#[test]
	fn chunk_cursor_is_versioned_and_zero_length_only_finishes_an_exhausted_range() {
		let (registry, root) = resident_document(b"abcdef");
		let (resolver, _notifications) = resolver(registry.clone());
		let cursor = ChunkCursor {
			start: DocByte::new(1),
			end: DocByte::new(5),
			position: DocByte::new(1),
			state: StateId::new(7),
			max_chunk_len: 2,
		};
		assert_eq!(
			read_chunk(&registry, &resolver, root, StateId::new(8), &cursor),
			Err(ReadError::StaleVersion)
		);
		assert_eq!(
			read_chunk(&registry, &resolver, root, StateId::new(7), &cursor),
			Ok(Readiness::Ready(ChunkPage {
				bytes: b"bc".to_vec(),
				next: DocByte::new(3),
				done: false,
			}))
		);
		let mut zero = cursor;
		zero.max_chunk_len = 0;
		assert_eq!(
			read_chunk(&registry, &resolver, root, StateId::new(7), &zero),
			Err(ReadError::InvalidChunkSize)
		);
		zero.position = zero.end;
		assert_eq!(
			read_chunk(&registry, &resolver, root, StateId::new(7), &zero),
			Ok(Readiness::Ready(ChunkPage {
				bytes: Vec::new(),
				next: DocByte::new(5),
				done: true,
			}))
		);
	}

	#[test]
	fn line_coordinates_are_zero_based_byte_ranges_and_strict() {
		let (registry, root) = resident_document(b"a\nb\n");
		let (resolver, _notifications) = resolver(registry.clone());
		assert_eq!(line_count(&registry, root), Ok(Readiness::Ready(3)));
		assert_eq!(
			line_start(&registry, &resolver, root, DocLine::ZERO),
			Ok(Readiness::Ready(DocByte::ZERO))
		);
		assert_eq!(
			line_start(&registry, &resolver, root, DocLine::new(1)),
			Ok(Readiness::Ready(DocByte::new(2)))
		);
		assert_eq!(
			line_start(&registry, &resolver, root, DocLine::new(2)),
			Ok(Readiness::Ready(DocByte::new(4)))
		);
		assert_eq!(
			line_start(&registry, &resolver, root, DocLine::new(3)),
			Err(ReadError::LineOutOfRange {
				line: DocLine::new(3),
				line_count: 3
			})
		);

		let page = LineCursor {
			next_line: DocLine::ZERO,
			end_line: DocLine::new(3),
			state: StateId::new(2),
			max_line_bytes: 1,
		};
		assert_eq!(
			read_line_page(&registry, &resolver, root, StateId::new(2), &page),
			Ok(Readiness::Ready(LinePage {
				line: DocLine::ZERO,
				start: DocByte::ZERO,
				end: DocByte::new(2),
				bytes: b"a".to_vec(),
				truncated: true,
				next_line: DocLine::new(1),
				done: false,
			}))
		);
	}

	#[test]
	fn resident_huge_leaf_line_scan_is_rejected_instead_of_retried() {
		let mut bytes = vec![b'x'; MAX_LINE_SCAN_BYTES as usize + 1];
		*bytes.last_mut().expect("large fixture") = b'\n';
		let (registry, root) = resident_document(&bytes);
		let (resolver, _notifications) = resolver(registry.clone());
		assert_eq!(
			line_start(&registry, &resolver, root, DocLine::new(1)),
			Err(ReadError::ScanLimitExceeded {
				scanned: MAX_LINE_SCAN_BYTES + 1,
				maximum: MAX_LINE_SCAN_BYTES
			})
		);
	}

	#[test]
	fn hydration_fan_is_bounded_before_any_request_is_admitted() {
		let leaf_count = MAX_HYDRATION_LEAVES + 1;
		let registry = Arc::new(UastRegistry::new(leaf_count as u32 + 1));
		let mut chunk = registry
			.reserve_chunk(leaf_count as u32 + 1)
			.expect("fan fixture nodes");
		let root = chunk.spawn_node(
			SemanticKind::RelationalTable,
			None,
			SpanMetrics {
				byte_length: leaf_count as u64,
				newlines: 0,
			},
		);
		for index in 0..leaf_count {
			let leaf = chunk.spawn_node(
				SemanticKind::Token,
				Some(SvpPointer {
					lba: index as u64,
					byte_length: 1,
					device_id: 73,
					head_trim: 0,
				}),
				SpanMetrics {
					byte_length: 1,
					newlines: 0,
				},
			);
			chunk.append_local_child(root, leaf);
		}
		let (resolver, _notifications) = resolver(registry.clone());
		assert_eq!(
			read_bytes(
				&registry,
				&resolver,
				root,
				DocByte::ZERO,
				DocByte::new(leaf_count as u64),
			),
			Err(ReadError::WorkLimitExceeded {
				maximum: MAX_HYDRATION_LEAVES
			})
		);
		assert_eq!(resolver.request_queue.len(), 0);
	}

	#[test]
	fn pathological_tiny_leaf_traversal_hits_the_native_work_limit() {
		let leaf_count = MAX_OPERATION_NODES + 1;
		let registry = Arc::new(UastRegistry::new(leaf_count as u32 + 1));
		let mut chunk = registry
			.reserve_chunk(leaf_count as u32 + 1)
			.expect("work-limit fixture nodes");
		let root = chunk.spawn_node(
			SemanticKind::RelationalTable,
			None,
			SpanMetrics {
				byte_length: leaf_count as u64,
				newlines: 0,
			},
		);
		for _ in 0..leaf_count {
			let leaf = chunk.spawn_node(
				SemanticKind::Token,
				None,
				SpanMetrics {
					byte_length: 1,
					newlines: 0,
				},
			);
			chunk.append_local_child(root, leaf);
			unsafe {
				*registry.virtual_data[leaf.index()].get() = Some(vec![b'x']);
			}
		}
		let (resolver, _notifications) = resolver(registry.clone());
		assert_eq!(
			read_bytes(
				&registry,
				&resolver,
				root,
				DocByte::ZERO,
				DocByte::new(leaf_count as u64),
			),
			Err(ReadError::WorkLimitExceeded {
				maximum: MAX_OPERATION_NODES
			})
		);
	}

	#[test]
	fn mutation_preflight_reserves_repeated_commit_descent_work() {
		let leaf_count = MAX_BATCH_DELTAS;
		let registry = Arc::new(UastRegistry::new(leaf_count as u32 + 1));
		let mut chunk = registry
			.reserve_chunk(leaf_count as u32 + 1)
			.expect("commit-work fixture nodes");
		let root = chunk.spawn_node(
			SemanticKind::RelationalTable,
			None,
			SpanMetrics {
				byte_length: leaf_count as u64,
				newlines: 0,
			},
		);
		for _ in 0..leaf_count {
			let leaf = chunk.spawn_node(
				SemanticKind::Token,
				None,
				SpanMetrics {
					byte_length: 1,
					newlines: 0,
				},
			);
			chunk.append_local_child(root, leaf);
			unsafe {
				*registry.virtual_data[leaf.index()].get() = Some(vec![b'x']);
			}
		}
		let (resolver, _notifications) = resolver(registry.clone());
		let edits = (0..leaf_count)
			.rev()
			.map(|offset| BufferEdit::Bytes {
				start: DocByte::new(offset as u64),
				end: DocByte::new(offset as u64),
				replacement: vec![b'y'],
			})
			.collect::<Vec<_>>();

		assert!(matches!(
			prepare_edits(
				&registry,
				&resolver,
				BufferId::new(1),
				root,
				StateId::ZERO,
				&edits,
				None,
			),
			Err(EditError::WorkLimitExceeded {
				maximum: MAX_OPERATION_NODES
			})
		));
	}

	#[test]
	fn byte_and_text_edits_are_exact_versioned_and_boundary_checked() {
		let registry = Arc::new(UastRegistry::new(64));
		let (resolver, _notifications) = resolver(registry.clone());
		let mut session = editor_session(&registry);
		let view_id = ViewId::new(1);

		let inserted = edit_text_view(
			&registry,
			&resolver,
			&mut session,
			view_id,
			&[BufferEdit::Bytes {
				start: DocByte::ZERO,
				end: DocByte::ZERO,
				replacement: "aéz".as_bytes().to_vec(),
			}],
			Some(StateId::ZERO),
		)
		.expect("initial byte insert");
		assert_eq!(
			inserted,
			Readiness::Ready(MutationOutcome {
				version: StateId::new(1),
				delta_count: 1,
				deleted_bytes: 0,
				inserted_bytes: 4,
			})
		);
		let (root, version) = text_root_and_version(&session, view_id);
		assert_eq!(version, StateId::new(1));
		assert_eq!(snapshot(&registry, root, 4), Ok("aéz".as_bytes().to_vec()));

		assert_eq!(
			edit_text_view(
				&registry,
				&resolver,
				&mut session,
				view_id,
				&[BufferEdit::Text {
					start: DocByte::new(2),
					end: DocByte::new(2),
					replacement: b"X".to_vec(),
				}],
				Some(version),
			),
			Err(EditError::NotUtf8Boundary {
				endpoint: DocByte::new(2)
			})
		);
		assert_eq!(
			text_root_and_version(&session, view_id).1,
			version,
			"failed boundary validation must not advance content state"
		);
		assert_eq!(snapshot(&registry, root, 4), Ok("aéz".as_bytes().to_vec()));

		assert_eq!(
			edit_text_view(
				&registry,
				&resolver,
				&mut session,
				view_id,
				&[],
				Some(StateId::ZERO),
			),
			Err(EditError::StaleVersion)
		);
	}

	#[test]
	fn malformed_utf8_near_an_endpoint_does_not_fabricate_a_text_boundary_error() {
		let registry = Arc::new(UastRegistry::new(64));
		let (resolver, _notifications) = resolver(registry.clone());
		let mut session = editor_session(&registry);
		let view_id = ViewId::new(1);
		edit_text_view(
			&registry,
			&resolver,
			&mut session,
			view_id,
			&[BufferEdit::Bytes {
				start: DocByte::ZERO,
				end: DocByte::ZERO,
				replacement: vec![0xe2, b'x'],
			}],
			None,
		)
		.expect("install malformed bytes");

		let version = text_root_and_version(&session, view_id).1;
		edit_text_view(
			&registry,
			&resolver,
			&mut session,
			view_id,
			&[BufferEdit::Text {
				start: DocByte::new(1),
				end: DocByte::new(1),
				replacement: b"Y".to_vec(),
			}],
			Some(version),
		)
		.expect("truncated lead byte must not consume the following boundary");
		let (root, _) = text_root_and_version(&session, view_id);
		assert_eq!(snapshot(&registry, root, 3), Ok(vec![0xe2, b'Y', b'x']));
	}

	#[test]
	fn text_edit_rejects_every_interior_boundary_of_two_three_and_four_byte_scalars() {
		let registry = Arc::new(UastRegistry::new(64));
		let (resolver, _notifications) = resolver(registry.clone());
		let mut session = editor_session(&registry);
		let view_id = ViewId::new(1);
		let text = "¢€𐍈";
		edit_text_view(
			&registry,
			&resolver,
			&mut session,
			view_id,
			&[BufferEdit::Bytes {
				start: DocByte::ZERO,
				end: DocByte::ZERO,
				replacement: text.as_bytes().to_vec(),
			}],
			None,
		)
		.expect("install UTF-8 fixture");
		let version = text_root_and_version(&session, view_id).1;

		for (scalar_start, scalar) in text.char_indices() {
			for interior in 1..scalar.len_utf8() {
				let endpoint = DocByte::new((scalar_start + interior) as u64);
				assert_eq!(
					edit_text_view(
						&registry,
						&resolver,
						&mut session,
						view_id,
						&[BufferEdit::Text {
							start: endpoint,
							end: endpoint,
							replacement: b"X".to_vec(),
						}],
						Some(version),
					),
					Err(EditError::NotUtf8Boundary { endpoint }),
					"accepted interior byte {interior} of {scalar:?}"
				);
			}
		}
		assert_eq!(text_root_and_version(&session, view_id).1, version);
	}

	#[test]
	fn declarative_batch_commits_once_and_overlap_rejects_without_mutation() {
		let registry = Arc::new(UastRegistry::new(64));
		let (resolver, _notifications) = resolver(registry.clone());
		let mut session = editor_session(&registry);
		let view_id = ViewId::new(1);
		edit_text_view(
			&registry,
			&resolver,
			&mut session,
			view_id,
			&[BufferEdit::Bytes {
				start: DocByte::ZERO,
				end: DocByte::ZERO,
				replacement: b"abcdef".to_vec(),
			}],
			None,
		)
		.expect("fixture insert");
		let version = text_root_and_version(&session, view_id).1;

		let outcome = edit_text_view(
			&registry,
			&resolver,
			&mut session,
			view_id,
			&[
				BufferEdit::Bytes {
					start: DocByte::new(4),
					end: DocByte::new(6),
					replacement: b"XY".to_vec(),
				},
				BufferEdit::Bytes {
					start: DocByte::new(1),
					end: DocByte::new(3),
					replacement: b"Q".to_vec(),
				},
			],
			Some(version),
		)
		.expect("non-overlapping descending batch");
		assert_eq!(
			outcome,
			Readiness::Ready(MutationOutcome {
				version: StateId::new(2),
				delta_count: 2,
				deleted_bytes: 4,
				inserted_bytes: 3,
			})
		);
		let (root, version) = text_root_and_version(&session, view_id);
		assert_eq!(snapshot(&registry, root, 5), Ok(b"aQdXY".to_vec()));

		assert!(matches!(
			edit_text_view(
				&registry,
				&resolver,
				&mut session,
				view_id,
				&[
					BufferEdit::Bytes {
						start: DocByte::new(2),
						end: DocByte::new(4),
						replacement: Vec::new(),
					},
					BufferEdit::Bytes {
						start: DocByte::new(1),
						end: DocByte::new(3),
						replacement: Vec::new(),
					},
				],
				Some(version),
			),
			Err(EditError::RangeInvalid { .. })
		));
		assert_eq!(snapshot(&registry, root, 5), Ok(b"aQdXY".to_vec()));
		assert_eq!(text_root_and_version(&session, view_id).1, version);
	}

	#[test]
	fn cold_mutation_preflight_defers_before_bytes_or_state_move() {
		let registry = Arc::new(UastRegistry::new(64));
		let mut chunk = registry.reserve_chunk(2).expect("cold document nodes");
		let root = chunk.spawn_node(
			SemanticKind::RelationalTable,
			None,
			SpanMetrics {
				byte_length: 4,
				newlines: 0,
			},
		);
		let leaf = chunk.spawn_node(
			SemanticKind::Token,
			Some(SvpPointer {
				lba: 0,
				byte_length: 4,
				device_id: 74,
				head_trim: 0,
			}),
			SpanMetrics {
				byte_length: 4,
				newlines: 0,
			},
		);
		chunk.append_local_child(root, leaf);
		registry.register_backing_store(
			74,
			Arc::new(GeneratedBackingStore::new(4, |offset| b'a' + offset as u8)),
		);
		let (resolver, notifications) = resolver(registry.clone());
		let mut session = editor_session(&registry);
		let view_id = ViewId::new(1);
		let buffer_id = session
			.layout
			.find_view(view_id)
			.expect("initial view")
			.buffer_id;
		session
			.buffers
			.get_mut(buffer_id)
			.and_then(|buffer| buffer.text_mut())
			.expect("initial text buffer")
			.root_id = root;

		let edit = [BufferEdit::Bytes {
			start: DocByte::new(1),
			end: DocByte::new(2),
			replacement: b"X".to_vec(),
		}];
		assert!(matches!(
			edit_text_view(
				&registry,
				&resolver,
				&mut session,
				view_id,
				&edit,
				Some(StateId::ZERO),
			),
			Ok(Readiness::NotReady { .. })
		));
		assert_eq!(text_root_and_version(&session, view_id).1, StateId::ZERO);
		assert!(
			session.events.take_pass().is_empty(),
			"a deferred preflight is not a committed transition"
		);
		assert!(!registry.whole_leaf_is_resident(leaf));
		notifications
			.recv_timeout(Duration::from_secs(2))
			.expect("mutation hydration refresh");
		assert!(matches!(
			edit_text_view(
				&registry,
				&resolver,
				&mut session,
				view_id,
				&edit,
				Some(StateId::ZERO),
			),
			Ok(Readiness::Ready(_))
		));
		assert_eq!(snapshot(&registry, root, 4), Ok(b"aXcd".to_vec()));
		assert_eq!(text_root_and_version(&session, view_id).1, StateId::new(1));
		assert!(matches!(
			session.events.take_pass().front(),
			Some(EngineEvent::BufferEdited { buffer, .. }) if *buffer == buffer_id
		));
	}

	#[test]
	fn acting_view_edit_rebases_a_shared_sibling_by_affinity() {
		let registry = Arc::new(UastRegistry::new(64));
		let (resolver, _notifications) = resolver(registry.clone());
		let mut session = editor_session(&registry);
		let acting = ViewId::new(1);
		edit_text_view(
			&registry,
			&resolver,
			&mut session,
			acting,
			&[BufferEdit::Bytes {
				start: DocByte::ZERO,
				end: DocByte::ZERO,
				replacement: b"abc".to_vec(),
			}],
			None,
		)
		.expect("fixture insert");
		assert!(session.split_focused_pane(&registry, SplitDirection::Vertical));
		let sibling = ViewId::new(2);
		let sibling_view = session.layout.find_view_mut(sibling).expect("split view");
		let crate::engine::session::ViewContent::Text(sibling_text) = &mut sibling_view.content
		else {
			panic!("split text view")
		};
		sibling_text
			.cursor
			.primary_mut()
			.set_byte(DocByte::new(3), crate::engine::session::Affinity::After);

		let version = text_root_and_version(&session, acting).1;
		edit_text_view(
			&registry,
			&resolver,
			&mut session,
			acting,
			&[BufferEdit::Bytes {
				start: DocByte::ZERO,
				end: DocByte::ZERO,
				replacement: b"X".to_vec(),
			}],
			Some(version),
		)
		.expect("shared-buffer insert");
		let sibling_view = session.layout.find_view(sibling).expect("split view");
		let crate::engine::session::ViewContent::Text(sibling_text) = &sibling_view.content else {
			panic!("split text view")
		};
		assert_eq!(sibling_text.cursor.primary().byte(), DocByte::new(4));
		let events = session.events.take_pass().into_iter().collect::<Vec<_>>();
		assert!(matches!(
			events.as_slice(),
			[
				EngineEvent::BufferEdited { inserted_bytes: 3, .. },
				EngineEvent::BufferEdited {
					acting_view,
					inserted_bytes: 1,
					..
				},
			] if *acting_view == acting
		));
	}
}
