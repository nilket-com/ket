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
	pub(super) fn relocate_indexed_locations(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
		relocation: &KeyRelocation,
	) -> Vec<(RecordLocation, RecordLocation)> {
		let key = (tenant_id, key_identity);
		let mut refused = Vec::new();
		let mut removed = Vec::new();
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
			let mut matched = vec![false; versions.len()];
			let mut applied = Vec::with_capacity(relocation.pairs.len());
			for pair in &relocation.pairs {
				if pair.old == pair.new {
					continue;
				}
				let position = versions
					.iter()
					.enumerate()
					.position(|(index, stored)| !matched[index] && *stored == pair.old);
				match position {
					Some(position) => {
						matched[position] = true;
						applied.push((position, *pair));
					}
					None if versions.contains(&pair.new) => {}
					None => refused.push((pair.old, pair.new)),
				}
			}
			// An absent physical source is an idempotent copy when another
			// source in this same substitution produces the identical output.
			let produced = applied
				.iter()
				.map(|(_, pair)| pair.new)
				.collect::<HashSet<_>>();
			refused.retain(|(_, new)| !produced.contains(new));
			for (_, pair) in &applied {
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
			let mut moved = Vec::new();
			for (position, pair) in &applied {
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
			for (_, pair) in &applied {
				if pair.old.hlc != pair.new.hlc {
					insert_version_sorted(versions, pair.new);
				}
			}
			// A surplus copy relocated onto a surviving copy names a row another
			// entry already names: one location is one row, indexed once.
			for (_, pair) in &applied {
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
		for location in removed {
			self.remove_published_table_reference(location);
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
		refused
	}
}
