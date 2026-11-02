//! Per-key relocation of indexed versions and atomic removal of an empty
//! key with its locality membership.
use super::*;

/// Decrement one table's count in a slab's reference map, removing the table
/// at zero. The caller holds the slab entry's guard.
pub(super) fn decrement_reference_count(
	references: &mut BTreeMap<(TenantId, TableObjectId), usize>,
	attribution: (TenantId, TableObjectId),
) {
	if let Some(count) = references.get_mut(&attribution) {
		*count = count.saturating_sub(1);
		if *count == 0 {
			references.remove(&attribution);
		}
	}
}

/// One relocated version: `old` is replaced by `new` if `old` is still indexed.
#[derive(Clone, Copy, Debug)]
pub(super) struct RelocationPair {
	pub(super) old: RecordLocation,
	pub(super) new: RecordLocation,
	pub(super) table_object_id: Option<TableObjectId>,
}

/// A version published without a predecessor (tier refresh only).
#[derive(Clone, Copy, Debug)]
pub(super) struct RelocationInsertion {
	pub(super) location: RecordLocation,
	pub(super) table_object_id: Option<TableObjectId>,
}

/// Everything one relocating writer changes for one key.
#[derive(Clone, Debug)]
pub(super) struct KeyRelocation {
	pub(super) routing_locality: Locality,
	pub(super) pairs: Vec<RelocationPair>,
	pub(super) removals: Vec<RecordLocation>,
	pub(super) insertions: Vec<RelocationInsertion>,
	pub(super) publish_redirects: bool,
}

use super::tier_budget::{BudgetedSet, try_bounded_set};

/// The working buffers of a key's relocation: sized before it runs from the
/// largest pair and removal counts it serves, never from the key's version
/// chain, and never grown by it. A tier publication builds one fallibly before
/// its durable boundary and charges it with the plan's scratch.
#[derive(Debug, Default)]
pub(super) struct RelocationScratch {
	/// Chain positions a pair already matched.
	matched: BudgetedSet<usize>,
	applied: Vec<(usize, RelocationPair)>,
	produced: BudgetedSet<RecordLocation>,
	refused: Vec<(RecordLocation, RecordLocation)>,
	removed: Vec<RecordLocation>,
	moved: Vec<usize>,
}

impl RelocationScratch {
	/// Buffers for relocations of at most `pairs` pairs and `removals`
	/// removals each.
	/// `None` when an allocation is refused.
	pub(super) fn try_for_counts(pairs: usize, removals: usize) -> Option<Self> {
		fn vec<T>(capacity: usize) -> Option<Vec<T>> {
			let mut vec = Vec::new();
			vec.try_reserve_exact(capacity).ok().map(|()| vec)
		}
		Some(Self {
			matched: try_bounded_set(pairs).ok()?,
			applied: vec(pairs)?,
			produced: try_bounded_set(pairs).ok()?,
			refused: vec(pairs)?,
			removed: vec(pairs.checked_add(removals)?)?,
			moved: vec(pairs)?,
		})
	}

	/// Buffers for `relocation` alone (callers outside §7 coverage).
	pub(super) fn for_relocation(relocation: &KeyRelocation) -> Self {
		let (pairs, removals) = (relocation.pairs.len(), relocation.removals.len());
		fn set<T>(capacity: usize) -> BudgetedSet<T> {
			BudgetedSet::with_capacity_and_hasher(capacity, Default::default())
		}
		Self {
			matched: set(pairs),
			applied: Vec::with_capacity(pairs),
			produced: set(pairs),
			refused: Vec::with_capacity(pairs),
			removed: Vec::with_capacity(pairs.saturating_add(removals)),
			moved: Vec::with_capacity(pairs),
		}
	}

	/// An upper bound on the bytes `try_for_counts(pairs, removals)` holds.
	pub(super) fn bytes_bound(pairs: usize, removals: usize) -> Option<u64> {
		use super::tier_budget::{checked_sum, hash_table_bytes_bound, vec_bytes};
		checked_sum(&[
			hash_table_bytes_bound::<usize>(pairs),
			vec_bytes::<(usize, RelocationPair)>(pairs),
			hash_table_bytes_bound::<RecordLocation>(pairs),
			vec_bytes::<(RecordLocation, RecordLocation)>(pairs),
			vec_bytes::<RecordLocation>(pairs.checked_add(removals)?),
			vec_bytes::<usize>(pairs),
		])
	}

	/// The bytes these buffers have allocated.
	#[cfg(test)]
	pub(super) fn allocation_for_test(&self) -> usize {
		fn vec<T>(vec: &Vec<T>) -> usize {
			vec.capacity() * std::mem::size_of::<T>()
		}
		self.matched.allocation_size()
			+ vec(&self.applied)
			+ self.produced.allocation_size()
			+ vec(&self.refused)
			+ vec(&self.removed)
			+ vec(&self.moved)
	}

	#[cfg(test)]
	pub(super) fn capacities_for_test(&self) -> [usize; 6] {
		[
			self.matched.capacity(),
			self.applied.capacity(),
			self.produced.capacity(),
			self.refused.capacity(),
			self.removed.capacity(),
			self.moved.capacity(),
		]
	}

	fn clear(&mut self) {
		self.matched.clear();
		self.applied.clear();
		self.produced.clear();
		self.refused.clear();
		self.removed.clear();
		self.moved.clear();
	}
}

impl KeyRelocation {
	pub(super) fn new(routing_locality: Locality, publish_redirects: bool) -> Self {
		Self {
			routing_locality,
			pairs: Vec::new(),
			removals: Vec::new(),
			insertions: Vec::new(),
			publish_redirects,
		}
	}
}

impl SlabManager {
	/// Remove a key whose version chain is empty, together with its locality
	/// membership, atomically with respect to appenders.
	///
	/// The locality write lock is taken first and held across the conditional
	/// removal: an appender that recreates the chain before the removal keeps it
	/// (the chain is not empty), and one that recreates it after the removal
	/// publishes its own locality only once this function releases the lock.
	/// Appenders never hold a key guard while taking the locality lock.
	pub(super) fn remove_key_if_empty(&self, tenant_id: TenantId, key_identity: Identity) {
		let key = (tenant_id, key_identity);
		let mut locality_index = recover_rwlock_write(&self.locality_index, "locality_index");
		if self
			.key_index
			.remove_if(&key, |_, versions| versions.is_empty())
			.is_none()
		{
			return;
		}
		#[cfg(test)]
		self.empty_key_locality_removal_pause_point();
		let routing_locality = self.routing_locality_for_key(tenant_id, key_identity);
		let should_remove_tenant = if let Some(members) = locality_index.get_mut(&tenant_id) {
			members.remove(&(routing_locality, key_identity));
			members.is_empty()
		} else {
			false
		};
		if should_remove_tenant {
			locality_index.remove(&tenant_id);
		}
		self.key_localities.remove(&key);
	}

	/// Relocate one key's indexed versions in a single `key_index` critical
	/// section, so a reader sees each version at its old or its new location,
	/// never at neither and never at both.
	///
	/// Each pair replaces `old` with `new` only while `old` is indexed. A pair
	/// whose new location is already indexed is an idempotent repeat; a pair
	/// whose old and new locations are both absent is refused and returned,
	/// with nothing published for it. The table reference and any redirect for
	/// an applied pair are published under the guard before the edit; the old
	/// location's reference is removed after the guard. A key that keeps a
	/// version never leaves the locality index.
	///
	/// The working buffers are `scratch`'s, which must be sized for
	/// `relocation` (`RelocationScratch`); the refused pairs are returned in
	/// it.
	pub(super) fn relocate_indexed_locations<'s>(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
		relocation: &KeyRelocation,
		scratch: &'s mut RelocationScratch,
	) -> &'s [(RecordLocation, RecordLocation)] {
		let key = (tenant_id, key_identity);
		scratch.clear();
		let RelocationScratch {
			matched,
			applied,
			produced,
			refused,
			removed,
			moved,
		} = scratch;
		let mut created = false;
		let now_empty;
		{
			let mut chain = self.key_index.entry(key).or_insert_with(|| {
				created = true;
				Vec::new()
			});
			let versions = chain.value_mut();
			// Every pair's source is matched against the chain as it stood, and
			// all pairs are substituted at once: one pair's output may be
			// another pair's source (copies of one version shifting through
			// each other's old slots).
			for pair in &relocation.pairs {
				if pair.old == pair.new {
					continue;
				}
				let position = versions
					.iter()
					.enumerate()
					.position(|(index, stored)| *stored == pair.old && !matched.contains(&index));
				match position {
					Some(position) => {
						matched.insert(position);
						applied.push((position, *pair));
					}
					None if versions.contains(&pair.new) => {}
					None => refused.push((pair.old, pair.new)),
				}
			}
			// An absent physical source is an idempotent copy when another
			// source in this same substitution produces the identical output.
			produced.extend(applied.iter().map(|(_, pair)| pair.new));
			refused.retain(|(_, new)| !produced.contains(new));
			for (_, pair) in applied.iter() {
				if let Some(table_object_id) = pair.table_object_id {
					self.add_published_table_reference(tenant_id, table_object_id, pair.new);
				}
				if relocation.publish_redirects {
					self.relocated_reads.insert(pair.old, pair.new);
				}
				// A copy of a conflicting version stays marked wherever it moves.
				if self.conflicted_locations.contains(&pair.old) {
					self.conflicted_locations.insert(pair.new);
				}
			}
			for (position, pair) in applied.iter() {
				if pair.old.hlc == pair.new.hlc {
					versions[*position] = pair.new;
				} else {
					moved.push(*position);
				}
				removed.push(pair.old);
			}
			moved.sort_unstable();
			for position in moved.iter().rev() {
				versions.remove(*position);
			}
			for (_, pair) in applied.iter() {
				if pair.old.hlc != pair.new.hlc {
					insert_version_sorted(versions, pair.new);
				}
			}
			// A surplus copy relocated onto a surviving copy names a row another
			// entry already names: one location is one row, indexed once.
			for (_, pair) in applied.iter() {
				let mut seen = false;
				versions.retain(|stored| {
					if *stored != pair.new {
						return true;
					}
					let first = !seen;
					seen = true;
					first
				});
			}
			for location in &relocation.removals {
				if remove_version_exact(versions, *location) {
					removed.push(*location);
				}
			}
			for insertion in &relocation.insertions {
				if !versions.contains(&insertion.location) {
					if let Some(table_object_id) = insertion.table_object_id {
						self.add_published_table_reference(
							tenant_id,
							table_object_id,
							insertion.location,
						);
					}
					insert_version_sorted(versions, insertion.location);
				}
			}
			now_empty = versions.is_empty();
			// A source that another pair's output now names stays referenced.
			removed.retain(|location| !versions.contains(location));
		}
		for location in removed.iter() {
			self.remove_published_table_reference(*location);
		}
		if now_empty {
			self.remove_key_if_empty(tenant_id, key_identity);
		} else if created {
			self.insert_locality_membership_with_locality(
				tenant_id,
				key_identity,
				relocation.routing_locality,
			);
		}
		if !refused.is_empty() {
			metrics().observe_storage_index_relocation_refused(refused.len() as u64);
			tracing::warn!(
				tenant_id = tenant_id.get(),
				refused = refused.len(),
				"index relocation refused pairs whose source and output are both unindexed"
			);
		}
		refused.as_slice()
	}
}
