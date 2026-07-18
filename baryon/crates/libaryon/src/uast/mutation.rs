use crate::ecs::{NodeId, UastRegistry};
use crate::svp::pointer::SvpPointer;
use crate::svp::resolver::{DMA_CHUNK_SIZE, RESOLVER_TRANSFER_SIZE};
use crate::uast::UastProjection;
use crate::uast::kind::SemanticKind;
use std::sync::atomic::Ordering;

pub trait UastMutation {
	fn apply_edit(&self, target: NodeId, added_bytes: i64, added_newlines: i64);
	fn insert_text(&self, target: NodeId, offset_in_node: u32, new_text: &[u8]) -> (NodeId, u32);
	fn delete_backwards(&self, target: NodeId, offset_in_node: u32) -> (NodeId, u32);
	fn delete_bytes_backwards(
		&self,
		target: NodeId,
		offset_in_node: u32,
		max_bytes: u32,
	) -> (NodeId, u32, u32);
	fn delete_bytes_forward(
		&self,
		target: NodeId,
		offset_in_node: u32,
		max_bytes: u32,
	) -> (NodeId, u32, u32);
	fn split_node_pvp(&self, target: NodeId, offset: u32, new_text: &[u8]) -> NodeId;
	fn split_node_pvp_delete(&self, target: NodeId, offset: u32, delete_len: u32) -> NodeId;
}

fn spawn_physical_tail_chain(
	registry: &UastRegistry,
	parent: NodeId,
	start_offset: u64,
	total_len: u32,
	device_id: u16,
	resolved_tail: Option<&[u8]>,
	next_sibling: Option<NodeId>,
) -> Option<(NodeId, NodeId)> {
	if total_len == 0 {
		return None;
	}

	let mut first: Option<NodeId> = None;
	let mut prev: Option<NodeId> = None;
	let mut offset_in_tail = 0u32;

	while offset_in_tail < total_len {
		let chunk_len = (total_len - offset_in_tail).min(DMA_CHUNK_SIZE as u32);
		let chunk_start = start_offset + u64::from(offset_in_tail);
		let node = registry.alloc_node_internal();
		let idx = node.index();

		let mut chunk_newlines = 0u32;
		let resolved_chunk = if let Some(data) = resolved_tail {
			let start = offset_in_tail as usize;
			let end = start + chunk_len as usize;
			let chunk_data = &data[start..end];
			chunk_newlines = chunk_data.iter().filter(|&&b| b == b'\n').count() as u32;
			Some(chunk_data.to_vec())
		} else {
			None
		};

		unsafe {
			*registry.kinds[idx].get() = SemanticKind::Token;
			*registry.spans[idx].get() = Some(SvpPointer {
				lba: chunk_start / 512,
				byte_length: chunk_len,
				device_id,
				head_trim: (chunk_start % 512) as u16,
			});

			let metrics = &mut *registry.metrics[idx].get();
			metrics.byte_length = u64::from(chunk_len);
			metrics.newlines = u64::from(chunk_newlines);

			let edges = &mut *registry.edges[idx].get();
			edges.parent = Some(parent);
			edges.next_sibling = None;
		}
		if let Some(chunk_data) = resolved_chunk {
			registry.hot_swap_virtual_data(node, chunk_data);
			registry.metrics_inflated[idx].store(true, Ordering::Relaxed);
			registry.set_terminal_newline(
				node,
				resolved_tail
					.expect("resolved chunk source")
					.get(offset_in_tail as usize..offset_in_tail as usize + chunk_len as usize)
					.and_then(|chunk| chunk.last())
					== Some(&b'\n'),
			);
		}

		registry.apply_edit(parent, i64::from(chunk_len), i64::from(chunk_newlines));

		if let Some(prev_node) = prev {
			unsafe {
				(*registry.edges[prev_node.index()].get()).next_sibling = Some(node);
			}
		} else {
			first = Some(node);
		}
		prev = Some(node);
		offset_in_tail += chunk_len;
	}

	let first = first.expect("non-empty tail should create a first node");
	let last = prev.expect("non-empty tail should create a last node");
	unsafe {
		(*registry.edges[last.index()].get()).next_sibling = next_sibling;
	}
	Some((first, last))
}

fn node_byte_length(registry: &UastRegistry, node: NodeId) -> u64 {
	unsafe { (*registry.metrics[node.index()].get()).byte_length }
}

/// Node-local byte length, saturated into the `NodeByteOffset` domain.
///
/// Leaves are bounded well below 4 GiB, but a branch (notably the flat document
/// root) aggregates the whole document and must never be narrowed unchecked.
fn node_byte_length_local(registry: &UastRegistry, node: NodeId) -> u32 {
	u32::try_from(node_byte_length(registry, node)).unwrap_or(u32::MAX)
}

/// The previous leaf in document order, or `None` at the start of the document.
///
/// Deleting backwards across a leaf boundary must not walk up through parent
/// offsets: under the flat root those offsets are document-wide and overflow the
/// node-local `u32` domain.
fn previous_leaf_in_document(registry: &UastRegistry, node: NodeId) -> Option<NodeId> {
	let mut curr = node;
	loop {
		if let Some(prev) = registry.get_prev_sibling(curr) {
			let mut leaf = prev;
			while let Some(child) = registry.get_first_child(leaf) {
				let mut last = child;
				while let Some(next) = registry.get_next_sibling(last) {
					last = next;
				}
				leaf = last;
			}
			return Some(leaf);
		}
		curr = unsafe { (*registry.edges[curr.index()].get()).parent }?;
	}
}

fn next_leaf_in_document(registry: &UastRegistry, node: NodeId) -> Option<NodeId> {
	let mut next = registry.get_next_node_in_walk(node)?;
	while let Some(child) = registry.get_first_child(next) {
		next = child;
	}
	Some(next)
}

fn node_has_children(registry: &UastRegistry, node: NodeId) -> bool {
	unsafe { (*registry.edges[node.index()].get()).first_child.is_some() }
}

fn prepare_leaf_metrics_for_mutation(registry: &UastRegistry, node: NodeId) {
	if node_has_children(registry, node) {
		return;
	}
	let idx = node.index();
	let virtual_terminal = {
		let _resident_read = registry.resident_data_guard();
		unsafe {
			(&*registry.virtual_data[idx].get())
				.as_deref()
				.map(|bytes| bytes.last() == Some(&b'\n'))
		}
	};
	if unsafe { (*registry.spans[idx].get()).is_none() }
		&& let Some(ends_with_newline) = virtual_terminal
	{
		registry.metrics_inflated[idx].store(true, Ordering::Release);
		registry.set_terminal_newline(node, ends_with_newline);
		return;
	}
	if unsafe { (*registry.spans[idx].get()).is_some() }
		&& !registry.metrics_inflated[idx].load(Ordering::Acquire)
	{
		registry.ensure_metrics_inflated(node);
	}
	if let Some(ends_with_newline) = virtual_terminal {
		registry.set_terminal_newline(node, ends_with_newline);
	}
}

fn is_prunable_empty_node(registry: &UastRegistry, node: NodeId) -> bool {
	let idx = node.index();
	unsafe {
		let metrics = &*registry.metrics[idx].get();
		metrics.byte_length == 0
			&& metrics.newlines == 0
			&& (*registry.edges[idx].get()).first_child.is_none()
	}
}

fn find_child_delete_target(
	registry: &UastRegistry,
	target: NodeId,
	offset_in_node: u64,
) -> Option<(NodeId, u32)> {
	let mut child = registry.get_first_child(target);
	let mut child_start = 0u64;

	while let Some(node) = child {
		let child_len = node_byte_length(registry, node);
		let child_end = child_start.saturating_add(child_len);

		if child_len > 0 && offset_in_node <= child_end {
			let local = offset_in_node.saturating_sub(child_start).min(child_len);
			return Some((
				node,
				u32::try_from(local).expect("child-local delete offset fits NodeByteOffset"),
			));
		}

		child_start = child_end;
		child = registry.get_next_sibling(node);
	}

	None
}

/// Prunes empty nodes upward and returns the cursor anchor.
///
/// The anchor is the end of the previous leaf rather than a parent-relative
/// offset: it names the same document byte while staying inside the node-local
/// `u32` domain, which a document-wide offset under the flat root would not.
fn prune_empty_ancestors(registry: &UastRegistry, start: NodeId) -> (NodeId, u32) {
	let mut curr = start;
	let mut anchor: Option<(NodeId, u32)> = None;

	loop {
		if !is_prunable_empty_node(registry, curr) {
			break;
		}

		let Some(parent) = (unsafe { (*registry.edges[curr.index()].get()).parent }) else {
			break;
		};
		let parent_is_root = unsafe { (*registry.edges[parent.index()].get()).parent.is_none() };
		let is_only_root_child = parent_is_root
			&& registry.get_first_child(parent) == Some(curr)
			&& registry.get_prev_sibling(curr).is_none()
			&& registry.get_next_sibling(curr).is_none();
		if is_only_root_child {
			// Keep one zero-length effective leaf attached to every empty document.
			// Text anchors can then resolve byte zero without ever treating the root
			// itself as a leaf or manufacturing a placeholder NodeId.
			return (curr, 0);
		}
		let previous = previous_leaf_in_document(registry, curr);
		registry.disconnect_node(curr);
		anchor = Some(match previous {
			Some(leaf) => (leaf, node_byte_length_local(registry, leaf)),
			None => (parent, 0),
		});
		curr = parent;
	}

	anchor.unwrap_or((start, 0))
}

impl UastMutation for UastRegistry {
	fn apply_edit(&self, target: NodeId, added_bytes: i64, added_newlines: i64) {
		let mut curr = Some(target);
		while let Some(node) = curr {
			let idx = node.index();
			unsafe {
				let m = &mut *self.metrics[idx].get();
				m.byte_length = m
					.byte_length
					.checked_add_signed(added_bytes)
					.expect("document byte metric underflow/overflow");
				m.newlines = m
					.newlines
					.checked_add_signed(added_newlines)
					.expect("document newline metric underflow/overflow");

				curr = (*self.edges[idx].get()).parent;
			}
		}
	}

	fn insert_text(&self, target: NodeId, offset_in_node: u32, new_text: &[u8]) -> (NodeId, u32) {
		// Text only ever lives in leaves. A branch target means the caller resolved a
		// coordinate it could not actually resolve — a line lookup whose index is still
		// pending hands back the root — and mutating it would add bytes to the document
		// aggregates while splicing none, permanently desynchronising them from content.
		if node_has_children(self, target) {
			return (target, offset_in_node);
		}
		prepare_leaf_metrics_for_mutation(self, target);
		let added_bytes = i64::try_from(new_text.len()).expect("insert length fits i64");
		let added_newlines = i64::try_from(new_text.iter().filter(|&&b| b == b'\n').count())
			.expect("insert newline count fits i64");

		self.apply_edit(target, added_bytes, added_newlines);

		let idx = target.index();
		let is_virtual = unsafe { (*self.spans[idx].get()).is_none() };

		if is_virtual {
			self.bump_content_generation(target);
			{
				let _resident_write = self.resident_data_write_guard();
				unsafe {
					if let Some(v_data) = &mut *self.virtual_data[idx].get() {
						v_data.splice(
							(offset_in_node as usize)..(offset_in_node as usize),
							new_text.iter().copied(),
						);
					}
				}
			}
			self.refresh_resident_accounting(target);
			self.refresh_terminal_newline_from_resident(target);
			self.refresh_installed_root_child_line_index_for_node(target);
			(target, offset_in_node + new_text.len() as u32)
		} else {
			self.bump_content_generation(target);
			let promoted_in_place = {
				let _resident_write = self.resident_data_write_guard();
				unsafe {
					if let Some(v_data) = &mut *self.virtual_data[idx].get() {
						if v_data
							.len()
							.checked_add(new_text.len())
							.is_none_or(|new_len| new_len > RESOLVER_TRANSFER_SIZE)
						{
							false
						} else {
							*self.spans[idx].get() = None;
							v_data.splice(
								(offset_in_node as usize)..(offset_in_node as usize),
								new_text.iter().copied(),
							);
							self.metrics_inflated[idx]
								.store(true, std::sync::atomic::Ordering::Relaxed);
							true
						}
					} else {
						false
					}
				}
			};
			if promoted_in_place {
				self.refresh_resident_accounting(target);
				self.refresh_terminal_newline_from_resident(target);
				self.refresh_installed_root_child_line_index_for_node(target);
				return (target, offset_in_node + new_text.len() as u32);
			}

			let v_id = self.split_node_pvp(target, offset_in_node, new_text);
			(v_id, new_text.len() as u32)
		}
	}

	fn split_node_pvp(&self, target: NodeId, offset: u32, new_text: &[u8]) -> NodeId {
		self.bump_content_generation(target);
		let target_idx = target.index();

		let (parent, old_next_sibling, old_svp, old_metrics) = unsafe {
			let e = &*self.edges[target_idx].get();
			let s = (*self.spans[target_idx].get()).expect("Split target must be Physical");
			let m = *self.metrics[target_idx].get();
			(e.parent, e.next_sibling, s, m)
		};
		let parent = parent.expect("Cannot split a root node");

		// 1. Subtract original metrics from parent chain
		self.apply_edit(
			parent,
			-i64::try_from(old_metrics.byte_length).expect("leaf byte length fits i64"),
			-i64::try_from(old_metrics.newlines).expect("leaf newline count fits i64"),
		);

		let v_id = self.alloc_node_internal();
		let v_idx = v_id.index();

		let p1_len = offset;
		let mut resolved_data = {
			let _resident_write = self.resident_data_write_guard();
			unsafe { (*self.virtual_data[target_idx].get()).take() }
		};
		if resolved_data.is_none() {
			resolved_data = self.read_node_bytes_sync(target).ok();
		}

		// 2. Update p1 (the original target node)
		let p1_newlines = resolved_data
			.as_ref()
			.map(|data| {
				data[..offset as usize]
					.iter()
					.filter(|&&b| b == b'\n')
					.count() as u32
			})
			.unwrap_or(0);
		unsafe {
			if let Some(s) = &mut *self.spans[target_idx].get() {
				s.byte_length = p1_len;
			}

			let m = &mut *self.metrics[target_idx].get();
			m.byte_length = u64::from(p1_len);
			m.newlines = u64::from(p1_newlines);
			if let Some(data) = resolved_data.as_ref() {
				let p1_data = &data[..offset as usize];
				self.hot_swap_virtual_data(target, p1_data.to_vec());
				self.metrics_inflated[target_idx].store(true, Ordering::Release);
				self.set_terminal_newline(target, p1_data.last() == Some(&b'\n'));
			} else {
				self.metrics_inflated[target_idx].store(false, Ordering::Release);
				self.clear_terminal_newline(target);
				self.refresh_resident_accounting(target);
			}

			let e = &mut *self.edges[target_idx].get();
			e.next_sibling = Some(v_id);
		}
		// Add p1's NEW metrics back to parent chain
		self.apply_edit(parent, i64::from(p1_len), i64::from(p1_newlines));

		// 3. Create v_id (the virtual node containing inserted text)
		let v_len = new_text.len() as u32;
		let v_newlines = new_text.iter().filter(|&&b| b == b'\n').count() as u32;
		unsafe {
			*self.kinds[v_idx].get() = SemanticKind::Token;
			*self.spans[v_idx].get() = None;
			let m = &mut *self.metrics[v_idx].get();
			m.byte_length = u64::from(v_len);
			m.newlines = u64::from(v_newlines);

			let e = &mut *self.edges[v_idx].get();
			e.parent = Some(parent);
			e.next_sibling = old_next_sibling;
		}
		self.hot_swap_virtual_data(v_id, new_text.to_vec());
		self.metrics_inflated[v_idx].store(true, Ordering::Release);
		self.set_terminal_newline(v_id, new_text.last() == Some(&b'\n'));
		// Add virtual node metrics to parent chain
		self.apply_edit(parent, i64::from(v_len), i64::from(v_newlines));

		// 4. Create tail chain (p2)
		let p2_len = old_svp.byte_length.saturating_sub(offset);
		let base_offset = old_svp.lba * 512 + u64::from(old_svp.head_trim);
		let split_offset = base_offset + u64::from(offset);
		let tail_chain = spawn_physical_tail_chain(
			self,
			parent,
			split_offset,
			p2_len,
			old_svp.device_id,
			resolved_data.as_ref().map(|data| &data[offset as usize..]),
			old_next_sibling,
		);
		// Note: spawn_physical_tail_chain already calls apply_edit for its nodes.

		unsafe {
			(*self.edges[v_idx].get()).next_sibling =
				tail_chain.map(|(first, _)| first).or(old_next_sibling);
		}

		let p_idx = parent.index();
		unsafe {
			let tail_ptr = &mut *self.child_tails[p_idx].get();
			if *tail_ptr == Some(target) {
				*tail_ptr = Some(tail_chain.map(|(_, last)| last).unwrap_or(v_id));
			}
		}

		self.refresh_installed_root_child_line_index_for_node(target);
		v_id
	}

	fn delete_backwards(&self, target: NodeId, offset_in_node: u32) -> (NodeId, u32) {
		let offset_in_node = offset_in_node.min(node_byte_length_local(self, target));
		if offset_in_node == 0 {
			// Step to the previous leaf and delete its last byte. Recursing up
			// through the parent would carry a document-wide offset in the
			// node-local u32 domain, which overflows past 4 GiB.
			if let Some(previous) = previous_leaf_in_document(self, target) {
				return self.delete_backwards(previous, node_byte_length_local(self, previous));
			}

			return (target, 0);
		}

		if node_has_children(self, target) {
			if let Some((child, child_offset)) =
				find_child_delete_target(self, target, u64::from(offset_in_node))
			{
				return self.delete_backwards(child, child_offset);
			}

			if let Some(previous) = previous_leaf_in_document(self, target) {
				return self.delete_backwards(previous, node_byte_length_local(self, previous));
			}

			return (target, 0);
		}
		prepare_leaf_metrics_for_mutation(self, target);
		self.bump_content_generation(target);

		let idx = target.index();
		let is_virtual = unsafe { (*self.spans[idx].get()).is_none() };

		if is_virtual {
			let mut bytes_to_remove = 1;
			let mut removed_newlines = 0i32;
			{
				let _resident_write = self.resident_data_write_guard();
				unsafe {
					if let Some(v_data) = &mut *self.virtual_data[idx].get() {
						let mut start = offset_in_node as usize - 1;
						while start > 0 && (v_data[start] & 0xC0) == 0x80 {
							start -= 1;
						}
						bytes_to_remove = offset_in_node as usize - start;
						removed_newlines = v_data[start..offset_in_node as usize]
							.iter()
							.filter(|&&b| b == b'\n')
							.count() as i32;
						v_data.drain(start..offset_in_node as usize);
					}
				}
			}
			self.refresh_resident_accounting(target);
			self.refresh_terminal_newline_from_resident(target);
			self.apply_edit(
				target,
				-i64::try_from(bytes_to_remove).expect("removed byte count fits i64"),
				-i64::from(removed_newlines),
			);

			if node_byte_length(self, target) == 0 {
				let result = prune_empty_ancestors(self, target);
				self.refresh_installed_root_child_line_index_for_node(result.0);
				return result;
			}

			self.refresh_installed_root_child_line_index_for_node(target);
			(target, offset_in_node - bytes_to_remove as u32)
		} else {
			// Physical node — Resolve its data and virtualize it COMPLETELY.
			// This prevents the "fractal split" bug where every byte delete
			// creates a new split/tombstone/tail.
			let mut data = self.read_node_bytes_sync(target).unwrap_or_default();
			if data.is_empty() {
				let delete_start = offset_in_node.saturating_sub(1);
				let tombstone = self.split_node_pvp_delete(target, delete_start, 1);
				return (tombstone, delete_start);
			}

			let mut start = offset_in_node as usize - 1;
			while start > 0 && (data[start] & 0xC0) == 0x80 {
				start -= 1;
			}
			let bytes_to_remove = offset_in_node as usize - start;
			let removed_newlines = data[start..offset_in_node as usize]
				.iter()
				.filter(|&&b| b == b'\n')
				.count() as i32;
			data.drain(start..offset_in_node as usize);

			unsafe {
				*self.spans[idx].get() = None;
				self.metrics_inflated[idx].store(true, Ordering::Relaxed);
			}
			let ends_with_newline = data.last() == Some(&b'\n');
			self.hot_swap_virtual_data(target, data);
			self.set_terminal_newline(target, ends_with_newline);

			self.apply_edit(
				target,
				-i64::try_from(bytes_to_remove).expect("removed byte count fits i64"),
				-i64::from(removed_newlines),
			);

			if node_byte_length(self, target) == 0 {
				let result = prune_empty_ancestors(self, target);
				self.refresh_installed_root_child_line_index_for_node(result.0);
				return result;
			}

			self.refresh_installed_root_child_line_index_for_node(target);
			(target, start as u32)
		}
	}

	fn delete_bytes_backwards(
		&self,
		target: NodeId,
		offset_in_node: u32,
		max_bytes: u32,
	) -> (NodeId, u32, u32) {
		let offset_in_node = offset_in_node.min(node_byte_length_local(self, target));
		if max_bytes == 0 {
			return (target, offset_in_node, 0);
		}
		if offset_in_node == 0 {
			// Resolve an endpoint at a leaf boundary to the preceding leaf. Keeping
			// the returned coordinate leaf-local avoids narrowing a document-wide
			// parent offset into u32 on huge documents.
			if let Some(previous) = previous_leaf_in_document(self, target) {
				return self.delete_bytes_backwards(
					previous,
					node_byte_length_local(self, previous),
					max_bytes,
				);
			}

			return (target, 0, 0);
		}

		if node_has_children(self, target) {
			if let Some((child, child_offset)) =
				find_child_delete_target(self, target, u64::from(offset_in_node))
			{
				return self.delete_bytes_backwards(child, child_offset, max_bytes);
			}

			if let Some(previous) = previous_leaf_in_document(self, target) {
				return self.delete_bytes_backwards(
					previous,
					node_byte_length_local(self, previous),
					max_bytes,
				);
			}

			return (target, 0, 0);
		}

		prepare_leaf_metrics_for_mutation(self, target);
		self.bump_content_generation(target);

		let delete_len = offset_in_node.min(max_bytes);
		let delete_start = offset_in_node - delete_len;
		let idx = target.index();
		let is_virtual = unsafe { (*self.spans[idx].get()).is_none() };

		if is_virtual {
			let removed_newlines = {
				let _resident_write = self.resident_data_write_guard();
				unsafe {
					let v_data = (&mut *self.virtual_data[idx].get())
						.as_mut()
						.expect("virtual leaf must retain resident bytes during exact deletion");
					let range = delete_start as usize..offset_in_node as usize;
					let removed = v_data[range.clone()]
						.iter()
						.filter(|&&byte| byte == b'\n')
						.count();
					v_data.drain(range);
					removed
				}
			};
			self.refresh_resident_accounting(target);
			self.refresh_terminal_newline_from_resident(target);
			self.apply_edit(
				target,
				-i64::from(delete_len),
				-i64::try_from(removed_newlines).expect("removed newline count fits i64"),
			);

			if node_byte_length(self, target) == 0 {
				let result = prune_empty_ancestors(self, target);
				self.refresh_installed_root_child_line_index_for_node(result.0);
				return (result.0, result.1, delete_len);
			}

			self.refresh_installed_root_child_line_index_for_node(target);
			return (target, delete_start, delete_len);
		}

		// A physical leaf may already have resident bytes. Promote the complete
		// leaf and drain the exact suffix in one operation so arbitrary byte
		// endpoints never inherit `delete_backwards`' UTF-8-width grouping.
		let mut data = self.read_node_bytes_sync(target).unwrap_or_default();
		if data.is_empty() {
			let tombstone = self.split_node_pvp_delete(target, delete_start, delete_len);
			return (tombstone, delete_start, delete_len);
		}
		assert!(
			offset_in_node as usize <= data.len(),
			"resident physical leaf is shorter than its byte metrics"
		);
		let range = delete_start as usize..offset_in_node as usize;
		let removed_newlines = data[range.clone()]
			.iter()
			.filter(|&&byte| byte == b'\n')
			.count();
		data.drain(range);

		unsafe {
			*self.spans[idx].get() = None;
			self.metrics_inflated[idx].store(true, Ordering::Relaxed);
		}
		let ends_with_newline = data.last() == Some(&b'\n');
		self.hot_swap_virtual_data(target, data);
		self.set_terminal_newline(target, ends_with_newline);
		self.apply_edit(
			target,
			-i64::from(delete_len),
			-i64::try_from(removed_newlines).expect("removed newline count fits i64"),
		);

		if node_byte_length(self, target) == 0 {
			let result = prune_empty_ancestors(self, target);
			self.refresh_installed_root_child_line_index_for_node(result.0);
			return (result.0, result.1, delete_len);
		}

		self.refresh_installed_root_child_line_index_for_node(target);
		(target, delete_start, delete_len)
	}

	fn delete_bytes_forward(
		&self,
		target: NodeId,
		offset_in_node: u32,
		max_bytes: u32,
	) -> (NodeId, u32, u32) {
		assert!(
			!node_has_children(self, target),
			"exact forward delete requires a leaf target"
		);
		let node_len = node_byte_length_local(self, target);
		let offset_in_node = offset_in_node.min(node_len);
		if max_bytes == 0 || offset_in_node == node_len {
			if offset_in_node == node_len
				&& max_bytes > 0
				&& let Some(next) = next_leaf_in_document(self, target)
			{
				return self.delete_bytes_forward(next, 0, max_bytes);
			}
			return (target, offset_in_node, 0);
		}

		prepare_leaf_metrics_for_mutation(self, target);
		self.bump_content_generation(target);
		let delete_len = max_bytes.min(node_len - offset_in_node);
		let delete_end = offset_in_node + delete_len;
		let deleted_to_end = delete_end == node_len;
		let next_leaf = deleted_to_end
			.then(|| next_leaf_in_document(self, target))
			.flatten();
		let idx = target.index();
		let is_virtual = unsafe { (*self.spans[idx].get()).is_none() };

		if is_virtual {
			let removed_newlines = {
				let _resident_write = self.resident_data_write_guard();
				unsafe {
					let data = (&mut *self.virtual_data[idx].get())
						.as_mut()
						.expect("virtual leaf must retain resident bytes during exact deletion");
					let range = offset_in_node as usize..delete_end as usize;
					let removed = data[range.clone()]
						.iter()
						.filter(|&&byte| byte == b'\n')
						.count();
					data.drain(range);
					removed
				}
			};
			self.refresh_resident_accounting(target);
			self.refresh_terminal_newline_from_resident(target);
			self.apply_edit(
				target,
				-i64::from(delete_len),
				-i64::try_from(removed_newlines).expect("removed newline count fits i64"),
			);

			if node_byte_length(self, target) == 0 {
				let anchor = prune_empty_ancestors(self, target);
				// Reconcile from the surviving predecessor (or the root sentinel),
				// not from `next_leaf`: the removed child can otherwise remain in
				// the optional root-child seek index and win a boundary lookup.
				self.refresh_installed_root_child_line_index_for_node(anchor.0);
				let result = next_leaf.map_or(anchor, |next| (next, 0));
				return (result.0, result.1, delete_len);
			}

			self.refresh_installed_root_child_line_index_for_node(target);
			return next_leaf.map_or((target, offset_in_node, delete_len), |next| {
				(next, 0, delete_len)
			});
		}

		let mut data = self.read_node_bytes_sync(target).unwrap_or_default();
		if data.is_empty() {
			let tombstone = self.split_node_pvp_delete(target, offset_in_node, delete_len);
			return (tombstone, offset_in_node, delete_len);
		}
		assert!(
			delete_end as usize <= data.len(),
			"resident physical leaf is shorter than its byte metrics"
		);
		let range = offset_in_node as usize..delete_end as usize;
		let removed_newlines = data[range.clone()]
			.iter()
			.filter(|&&byte| byte == b'\n')
			.count();
		data.drain(range);
		unsafe {
			*self.spans[idx].get() = None;
			self.metrics_inflated[idx].store(true, Ordering::Relaxed);
		}
		let ends_with_newline = data.last() == Some(&b'\n');
		self.hot_swap_virtual_data(target, data);
		self.set_terminal_newline(target, ends_with_newline);
		self.apply_edit(
			target,
			-i64::from(delete_len),
			-i64::try_from(removed_newlines).expect("removed newline count fits i64"),
		);

		if node_byte_length(self, target) == 0 {
			let anchor = prune_empty_ancestors(self, target);
			self.refresh_installed_root_child_line_index_for_node(anchor.0);
			let result = next_leaf.map_or(anchor, |next| (next, 0));
			return (result.0, result.1, delete_len);
		}

		self.refresh_installed_root_child_line_index_for_node(target);
		next_leaf.map_or((target, offset_in_node, delete_len), |next| {
			(next, 0, delete_len)
		})
	}

	fn split_node_pvp_delete(&self, target: NodeId, offset: u32, delete_len: u32) -> NodeId {
		self.bump_content_generation(target);
		let target_idx = target.index();

		let (parent, old_next_sibling, old_svp, old_metrics) = unsafe {
			let e = &*self.edges[target_idx].get();
			let s = (*self.spans[target_idx].get()).expect("Split target must be Physical");
			let m = *self.metrics[target_idx].get();
			(e.parent, e.next_sibling, s, m)
		};
		let parent = parent.expect("Cannot split a root node");

		// 1. Subtract original metrics from parent chain
		self.apply_edit(
			parent,
			-i64::try_from(old_metrics.byte_length).expect("leaf byte length fits i64"),
			-i64::try_from(old_metrics.newlines).expect("leaf newline count fits i64"),
		);

		let v_id = self.alloc_node_internal();
		let v_idx = v_id.index();

		let mut resolved_data = {
			let _resident_write = self.resident_data_write_guard();
			unsafe { (*self.virtual_data[target_idx].get()).take() }
		};
		if resolved_data.is_none() {
			resolved_data = self.read_node_bytes_sync(target).ok();
		}

		// 2. Update p1 (the original target node, truncated)
		let p1_newlines = resolved_data
			.as_ref()
			.map(|data| {
				data[..offset as usize]
					.iter()
					.filter(|&&b| b == b'\n')
					.count() as u32
			})
			.unwrap_or(0);
		unsafe {
			if let Some(s) = &mut *self.spans[target_idx].get() {
				s.byte_length = offset;
			}

			let m = &mut *self.metrics[target_idx].get();
			m.byte_length = u64::from(offset);
			m.newlines = u64::from(p1_newlines);
			if let Some(data) = resolved_data.as_ref() {
				let p1_data = &data[..offset as usize];
				self.hot_swap_virtual_data(target, p1_data.to_vec());
				self.metrics_inflated[target_idx].store(true, Ordering::Release);
				self.set_terminal_newline(target, p1_data.last() == Some(&b'\n'));
			} else {
				self.metrics_inflated[target_idx].store(false, Ordering::Release);
				self.clear_terminal_newline(target);
				self.refresh_resident_accounting(target);
			}
			let e = &mut *self.edges[target_idx].get();
			e.next_sibling = Some(v_id);
		}
		// Add p1's NEW metrics back to parent chain
		self.apply_edit(parent, i64::from(offset), i64::from(p1_newlines));

		// 3. Create v_id (the tombstone for the deleted byte)
		unsafe {
			*self.kinds[v_idx].get() = SemanticKind::Token;
			*self.spans[v_idx].get() = None;
			*self.virtual_data[v_idx].get() = Some(Vec::new());
			let m = &mut *self.metrics[v_idx].get();
			m.byte_length = 0;
			m.newlines = 0;
			let e = &mut *self.edges[v_idx].get();
			e.parent = Some(parent);
			e.next_sibling = old_next_sibling;
		}
		self.metrics_inflated[v_idx].store(true, Ordering::Release);
		self.set_terminal_newline(v_id, false);

		// 4. Create tail chain (p2, starting after the deleted byte)
		let p2_len = old_svp.byte_length.saturating_sub(offset + delete_len);
		let base_offset = old_svp.lba * 512 + u64::from(old_svp.head_trim);
		let split_offset = base_offset + u64::from(offset + delete_len);
		let tail_chain = spawn_physical_tail_chain(
			self,
			parent,
			split_offset,
			p2_len,
			old_svp.device_id,
			resolved_data
				.as_ref()
				.map(|data| &data[(offset + delete_len) as usize..]),
			old_next_sibling,
		);
		// Note: spawn_physical_tail_chain already calls apply_edit for its nodes.

		unsafe {
			(*self.edges[v_idx].get()).next_sibling =
				tail_chain.map(|(first, _)| first).or(old_next_sibling);
		}

		let p_idx = parent.index();
		unsafe {
			let tail_ptr = &mut *self.child_tails[p_idx].get();
			if *tail_ptr == Some(target) {
				*tail_ptr = Some(tail_chain.map(|(_, last)| last).unwrap_or(v_id));
			}
		}

		self.refresh_installed_root_child_line_index_for_node(target);
		v_id
	}
}

#[cfg(test)]
mod tests {
	use super::UastMutation;
	use crate::ecs::UastRegistry;
	use crate::svp::pointer::SvpPointer;
	use crate::svp::resolver::DMA_CHUNK_SIZE;
	use crate::uast::UastProjection;
	use crate::uast::kind::SemanticKind;
	use crate::uast::metrics::SpanMetrics;
	use std::sync::atomic::Ordering;

	fn build_physical_leaf(registry: &UastRegistry, span: SvpPointer) -> crate::ecs::NodeId {
		let mut chunk = registry.reserve_chunk(2).expect("OOM");
		let root = chunk.spawn_node(
			SemanticKind::RelationalTable,
			None,
			SpanMetrics {
				byte_length: u64::from(span.byte_length),
				newlines: 0,
			},
		);
		let leaf = chunk.spawn_node(
			SemanticKind::Token,
			Some(span),
			SpanMetrics {
				byte_length: u64::from(span.byte_length),
				newlines: 0,
			},
		);
		chunk.append_local_child(root, leaf);
		leaf
	}

	fn parent_of(registry: &UastRegistry, node: crate::ecs::NodeId) -> crate::ecs::NodeId {
		unsafe {
			(*registry.edges[node.index()].get())
				.parent
				.expect("node should have parent")
		}
	}

	#[test]
	fn deleting_the_final_byte_keeps_a_zero_length_leaf_attached() {
		let registry = UastRegistry::new(4);
		let mut chunk = registry.reserve_chunk(2).expect("OOM");
		let root = chunk.spawn_node(
			SemanticKind::RelationalTable,
			None,
			SpanMetrics {
				byte_length: 1,
				newlines: 0,
			},
		);
		let leaf = chunk.spawn_node(
			SemanticKind::Token,
			None,
			SpanMetrics {
				byte_length: 1,
				newlines: 0,
			},
		);
		chunk.append_local_child(root, leaf);
		registry.hot_swap_virtual_data(leaf, b"x".to_vec());

		let anchor = registry.delete_backwards(leaf, 1);

		assert_eq!(anchor, (leaf, 0));
		assert_eq!(registry.get_first_child(root), Some(leaf));
		assert_eq!(registry.get_total_bytes(root), 0);
		assert_eq!(
			registry
				.find_node_at_doc_byte_raw(root, crate::core::DocByte::ZERO)
				.node_id,
			leaf
		);
	}

	fn collect_physical_spans(
		registry: &UastRegistry,
		start: Option<crate::ecs::NodeId>,
	) -> Vec<SvpPointer> {
		let mut spans = Vec::new();
		let mut current = start;
		while let Some(node) = current {
			let span = unsafe { *registry.spans[node.index()].get() };
			let Some(span) = span else {
				break;
			};
			spans.push(span);
			current = registry.get_next_sibling(node);
		}
		spans
	}

	#[test]
	fn split_node_pvp_preserves_512_byte_span_addressing() {
		let registry = UastRegistry::new(8);
		let span = SvpPointer {
			lba: 1,
			byte_length: 10_000,
			device_id: 7,
			head_trim: 88,
		};
		let leaf = build_physical_leaf(&registry, span);

		let inserted = registry.split_node_pvp(leaf, 9_000, b"hi");
		let tail = registry
			.get_next_sibling(inserted)
			.expect("tail span should exist");
		let tail_span = unsafe { (*registry.spans[tail.index()].get()).expect("tail span") };

		let expected_offset = span.lba * 512 + u64::from(span.head_trim) + 9_000;
		assert_eq!(tail_span.lba, expected_offset / 512);
		assert_eq!(u64::from(tail_span.head_trim), expected_offset % 512);
	}

	#[test]
	fn installed_root_index_splices_split_extents_without_a_full_rebuild() {
		let registry = UastRegistry::new(8);
		let leaf = build_physical_leaf(
			&registry,
			SvpPointer {
				lba: 0,
				byte_length: 8,
				device_id: 1,
				head_trim: 0,
			},
		);
		let root = parent_of(&registry, leaf);
		unsafe {
			(*registry.metrics[leaf.index()].get()).newlines = 1;
			(*registry.metrics[root.index()].get()).newlines = 1;
		}
		registry.hot_swap_virtual_data(leaf, b"aaa\nbbbb".to_vec());
		registry.metrics_inflated[leaf.index()].store(true, Ordering::Release);
		registry.set_terminal_newline(leaf, false);
		registry.install_root_child_line_index(root);
		let initial_builds = registry
			.root_child_line_index_full_builds
			.load(Ordering::Acquire);

		let inserted = registry.split_node_pvp(leaf, 4, b"X");

		assert_eq!(
			registry
				.root_child_line_indexes
				.read()
				.unwrap()
				.get(&root)
				.expect("installed index")
				.len(),
			3
		);
		assert_eq!(
			registry
				.root_child_line_index_full_builds
				.load(Ordering::Acquire),
			initial_builds
		);
		assert_eq!(
			registry
				.doc_byte_for_node_offset(root, inserted, 0u32.into())
				.get(),
			4
		);
		assert_eq!(
			registry
				.find_node_at_doc_byte(root, &crate::uast::NoFolds, crate::core::DocByte::new(5))
				.node_start_byte
				.get(),
			5
		);
	}

	#[test]
	fn split_node_pvp_delete_preserves_512_byte_span_addressing() {
		let registry = UastRegistry::new(8);
		let span = SvpPointer {
			lba: 3,
			byte_length: 12_000,
			device_id: 9,
			head_trim: 144,
		};
		let leaf = build_physical_leaf(&registry, span);

		let tombstone = registry.split_node_pvp_delete(leaf, 8_000, 3);
		let tail = registry
			.get_next_sibling(tombstone)
			.expect("tail span should exist");
		let tail_span = unsafe { (*registry.spans[tail.index()].get()).expect("tail span") };

		let expected_offset = span.lba * 512 + u64::from(span.head_trim) + 8_003;
		assert_eq!(tail_span.lba, expected_offset / 512);
		assert_eq!(u64::from(tail_span.head_trim), expected_offset % 512);
	}

	#[test]
	fn resolved_physical_leaf_edits_promote_the_whole_chunk_to_virtual() {
		let registry = UastRegistry::new(8);
		let span = SvpPointer {
			lba: 0,
			byte_length: 6,
			device_id: 1,
			head_trim: 0,
		};
		let leaf = build_physical_leaf(&registry, span);
		unsafe {
			*registry.virtual_data[leaf.index()].get() = Some(b"abcdef".to_vec());
		}

		let (inserted, insert_offset) = registry.insert_text(leaf, 3, b"ZZ");
		assert_eq!(inserted, leaf);
		assert_eq!(insert_offset, 5);
		assert!(registry.get_next_sibling(leaf).is_none());
		assert!(unsafe { (*registry.spans[leaf.index()].get()).is_none() });
		assert_eq!(
			unsafe { (*registry.virtual_data[leaf.index()].get()).as_deref() },
			Some("abcZZdef".as_bytes()),
		);
		assert_eq!(
			unsafe { (*registry.metrics[leaf.index()].get()).byte_length },
			8
		);

		let (deleted, delete_offset) = registry.delete_backwards(leaf, 5);
		assert_eq!(deleted, leaf);
		assert_eq!(delete_offset, 4);
		assert!(registry.get_next_sibling(leaf).is_none());
		assert!(unsafe { (*registry.spans[leaf.index()].get()).is_none() });
		assert_eq!(
			unsafe { (*registry.virtual_data[leaf.index()].get()).as_deref() },
			Some("abcZdef".as_bytes()),
		);
		assert_eq!(
			unsafe { (*registry.metrics[leaf.index()].get()).byte_length },
			7
		);
	}

	#[test]
	fn delete_backwards_uses_utf8_width_for_promoted_physical_leaves() {
		let registry = UastRegistry::new(8);
		let span = SvpPointer {
			lba: 0,
			byte_length: 4,
			device_id: 1,
			head_trim: 0,
		};
		let leaf = build_physical_leaf(&registry, span);
		unsafe {
			*registry.virtual_data[leaf.index()].get() = Some("aéb".as_bytes().to_vec());
		}

		let (deleted, delete_offset) = registry.delete_backwards(leaf, 3);
		assert_eq!(deleted, leaf);
		assert_eq!(delete_offset, 1);
		assert!(registry.get_next_sibling(leaf).is_none());
		assert!(unsafe { (*registry.spans[leaf.index()].get()).is_none() });
		assert_eq!(
			unsafe { (*registry.virtual_data[leaf.index()].get()).as_deref() },
			Some("ab".as_bytes()),
		);
	}

	#[test]
	fn insert_text_rechunks_large_physical_tails_to_dma_sized_spans() {
		let registry = UastRegistry::new(16);
		let span = SvpPointer {
			lba: 2,
			byte_length: (DMA_CHUNK_SIZE as u32) * 2 + 17,
			device_id: 11,
			head_trim: 96,
		};
		let leaf = build_physical_leaf(&registry, span);
		let root = parent_of(&registry, leaf);

		let (inserted, insert_offset) = registry.insert_text(leaf, 1, b"XY");
		assert_eq!(insert_offset, 2);

		let tail_spans = collect_physical_spans(&registry, registry.get_next_sibling(inserted));
		assert_eq!(tail_spans.len(), 3);
		assert!(
			tail_spans
				.iter()
				.all(|span| span.byte_length as usize <= DMA_CHUNK_SIZE)
		);
		assert_eq!(
			tail_spans.iter().map(|span| span.byte_length).sum::<u32>(),
			span.byte_length - 1,
		);

		let expected_offset = span.lba * 512 + u64::from(span.head_trim) + 1;
		assert_eq!(tail_spans[0].lba, expected_offset / 512);
		assert_eq!(u64::from(tail_spans[0].head_trim), expected_offset % 512);
		assert_eq!(
			unsafe { (*registry.metrics[root.index()].get()).byte_length },
			u64::from(span.byte_length) + 2,
		);
	}

	#[test]
	fn delete_backwards_rechunks_large_physical_tails_to_dma_sized_spans() {
		let registry = UastRegistry::new(16);
		let span = SvpPointer {
			lba: 4,
			byte_length: (DMA_CHUNK_SIZE as u32) * 2 + 33,
			device_id: 12,
			head_trim: 144,
		};
		let leaf = build_physical_leaf(&registry, span);
		let root = parent_of(&registry, leaf);

		let (tombstone, delete_offset) = registry.delete_backwards(leaf, 1);
		assert_eq!(delete_offset, 0);

		let tail_spans = collect_physical_spans(&registry, registry.get_next_sibling(tombstone));
		assert_eq!(tail_spans.len(), 3);
		assert!(
			tail_spans
				.iter()
				.all(|span| span.byte_length as usize <= DMA_CHUNK_SIZE)
		);
		assert_eq!(
			tail_spans.iter().map(|span| span.byte_length).sum::<u32>(),
			span.byte_length - 1,
		);

		let expected_offset = span.lba * 512 + u64::from(span.head_trim) + 1;
		assert_eq!(tail_spans[0].lba, expected_offset / 512);
		assert_eq!(u64::from(tail_spans[0].head_trim), expected_offset % 512);
		assert_eq!(
			unsafe { (*registry.metrics[root.index()].get()).byte_length },
			u64::from(span.byte_length) - 1,
		);
	}

	#[test]
	fn delete_backwards_preserves_newlines_via_metric_inflation() {
		let registry = UastRegistry::new(8);
		let span = SvpPointer {
			lba: 4,
			byte_length: 10,
			device_id: 12,
			head_trim: 144,
		};
		// Leaf contains "a\nb\nc\nd\ne"
		let leaf = build_physical_leaf(&registry, span);
		let root = parent_of(&registry, leaf);

		// Manually mark it as not inflated but give it some content we can "resolve"
		unsafe {
			*registry.virtual_data[leaf.index()].get() = Some(b"a\nb\nc\nd\ne".to_vec());
			registry.metrics_inflated[leaf.index()]
				.store(false, std::sync::atomic::Ordering::Relaxed);
			(*registry.metrics[leaf.index()].get()).newlines = 4;
			(*registry.metrics[root.index()].get()).newlines = 4;
		}

		// Delete 'e' at the end (offset 9)
		// This should trigger split_node_pvp_delete, which must resolve the data and find the newlines.
		let (_tombstone, _offset) = registry.delete_backwards(leaf, 9);

		// Root should have 4 newlines still (none were in the deleted 'e')
		assert_eq!(
			unsafe { (*registry.metrics[root.index()].get()).newlines },
			4
		);
	}

	#[test]
	fn editing_during_background_scan_never_publishes_a_partial_count_as_exact() {
		use crate::svp::backing_store::GeneratedBackingStore;
		use std::sync::Arc;

		let registry = UastRegistry::new(8);
		let mut chunk = registry.reserve_chunk(2).expect("registry chunk");
		let root = chunk.spawn_node(
			SemanticKind::RelationalTable,
			None,
			SpanMetrics {
				byte_length: 3,
				newlines: 0,
			},
		);
		let leaf = chunk.spawn_node(
			SemanticKind::Token,
			Some(SvpPointer {
				lba: 0,
				byte_length: 3,
				device_id: 77,
				head_trim: 0,
			}),
			SpanMetrics {
				byte_length: 3,
				newlines: 0,
			},
		);
		chunk.append_local_child(root, leaf);
		registry.register_backing_store(
			77,
			Arc::new(GeneratedBackingStore::new(3, |offset| {
				b"abc"[offset as usize]
			})),
		);
		registry.begin_metric_scan(root, leaf, leaf);
		let _ = registry.insert_text(leaf, 3, b"\n");

		assert_eq!(registry.exact_line_count(root), Some(2));
		assert_eq!(registry.stats().pending_scan_blocks, 0);
		assert_eq!(registry.get_total_bytes(root), 4);
	}
	#[test]
	fn delete_backwards_across_a_leaf_boundary_beyond_4_gib_does_not_panic() {
		// The root of a huge document aggregates a byte_length far beyond u32, and a
		// late leaf's offset within it does too. Backspacing at offset 0 of such a leaf
		// must step to the previous leaf rather than recurse through the root carrying
		// a document-wide offset in the node-local u32 domain.
		const THREE_GIB: u64 = 3 * 1024 * 1024 * 1024;
		const TWO_GIB: u64 = 2 * 1024 * 1024 * 1024;
		let registry = UastRegistry::new(8);
		let mut chunk = registry.reserve_chunk(5).expect("OOM");
		let root = chunk.spawn_node(
			SemanticKind::RelationalTable,
			None,
			SpanMetrics {
				byte_length: THREE_GIB + TWO_GIB + 8,
				newlines: 0,
			},
		);
		// Two cold extents that push the document past 4 GiB, then two small hot leaves.
		let mut cold = |len: u64| {
			chunk.spawn_node(
				SemanticKind::Token,
				Some(SvpPointer {
					lba: 0,
					byte_length: u32::try_from(len).expect("extent fits u32"),
					device_id: 1,
					head_trim: 0,
				}),
				SpanMetrics {
					byte_length: len,
					newlines: 0,
				},
			)
		};
		let big_a = cold(THREE_GIB);
		let big_b = cold(TWO_GIB);
		let prev = chunk.spawn_node(
			SemanticKind::Token,
			None,
			SpanMetrics {
				byte_length: 4,
				newlines: 0,
			},
		);
		let tail = chunk.spawn_node(
			SemanticKind::Token,
			None,
			SpanMetrics {
				byte_length: 4,
				newlines: 0,
			},
		);
		for leaf in [big_a, big_b, prev, tail] {
			chunk.append_local_child(root, leaf);
		}
		registry.hot_swap_virtual_data(prev, b"abcd".to_vec());
		registry.hot_swap_virtual_data(tail, b"wxyz".to_vec());

		// Offset 0 of `tail` sits at document byte 5 GiB + 4. This used to compute a
		// parent offset via child_byte_offset (which truncates to None past 4 GiB),
		// fall back to 0, and then hit u32::try_from(root.byte_length).expect(..).
		let (node, offset) = registry.delete_backwards(tail, 0);

		assert_ne!(node, root, "a delete must never target the document root");
		assert_eq!(node, prev, "backspace should step into the previous leaf");
		assert_eq!(offset, 3, "it should remove the last byte of that leaf");
		assert_eq!(
			unsafe { &*registry.virtual_data[prev.index()].get() }
				.as_deref()
				.expect("prev stays resident"),
			b"abc"
		);
	}
}
