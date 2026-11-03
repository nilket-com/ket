//! The durable slab id allocator bound. Every id is covered by a committed
//! high-water before it is used, so no reopen ever issues it again.

use super::*;

/// How far ahead of the next id the durable high-water is committed.
pub(super) const SLAB_ID_LEASE: u64 = 64;

pub(super) fn slab_ids_exhausted() -> Error {
	Error::other("slab id space exhausted")
}

impl SlabManager {
	/// The store's incarnation, for tests that follow it across reopens.
	#[cfg(test)]
	pub(crate) fn store_incarnation_for_test(
		&self,
	) -> Option<crate::storage::index::StoreIncarnation> {
		self.persistent_index
			.as_ref()
			.map(|index| index.store_incarnation())
	}

	/// Commit the durable high-water past `last` before any id up to it is
	/// used, and return `last + 1`. The bound is raised a lease ahead so most
	/// allocations need no redb commit. A failed commit issues nothing.
	pub(super) async fn cover_slab_ids_through(
		&self,
		state: &mut ManagerState,
		last: SlabId,
	) -> std::io::Result<SlabId> {
		let end = last
			.get()
			.checked_add(1)
			.map(SlabId::new)
			.ok_or_else(slab_ids_exhausted)?;
		if end <= state.durable_slab_id_bound {
			return Ok(end);
		}
		let Some(index) = &self.persistent_index else {
			return Err(Error::other(
				"slab ids cannot be issued without the durable storage index",
			));
		};
		let bound = end
			.get()
			.checked_add(SLAB_ID_LEASE)
			.ok_or_else(slab_ids_exhausted)?;
		index.persist_slab_id_high_water(bound).await?;
		state.durable_slab_id_bound = SlabId::new(bound);
		Ok(end)
	}
}

/// Every slab id a file name in a store directory carries: slab files, their
/// temporary and quarantine variants, and staged maintenance replacements.
fn slab_ids_named_by_file(name: &str) -> Vec<SlabId> {
	let hex16 = |text: &str| {
		let digits = text.get(..16)?;
		if !digits.bytes().all(|byte| byte.is_ascii_hexdigit()) {
			return None;
		}
		u64::from_str_radix(digits, 16).ok().map(SlabId::new)
	};
	let mut ids = Vec::new();
	if let Some(body) = name.strip_prefix("maintenance-staged-") {
		ids.extend(hex16(body));
		ids.extend(body.get(17..).and_then(hex16));
	}
	let mut rest = name;
	while let Some(at) = rest.find("slab_") {
		rest = &rest[at + "slab_".len()..];
		ids.extend(hex16(rest));
	}
	ids
}

/// The exclusive bound on every slab id this store's own evidence names,
/// read without changing any file or row. Open validates the durable
/// high-water against it before any recovery step may rename, unlink or
/// rewrite that evidence.
pub(super) async fn slab_id_evidence_floor(
	file_ops: &dyn AsyncFileOps,
	dir: &Path,
	snapshot: Option<&crate::storage::index::PersistentIndexSnapshot>,
) -> std::io::Result<SlabId> {
	let mut highest: Option<SlabId> = None;
	for entry in file_ops.read_dir(dir).await? {
		if !entry.is_file {
			continue;
		}
		if let Some(name) = entry.file_name.to_str() {
			highest = highest.max(slab_ids_named_by_file(name).into_iter().max());
		}
	}
	if let Some(snapshot) = snapshot {
		let named = snapshot
			.record_rows
			.iter()
			.chain(snapshot.tiered_records.iter())
			.map(|row| row.location.slab_id)
			.chain(snapshot.lsm_runs.iter().map(|run| run.slab_id))
			.chain(
				snapshot
					.secondary_index_segments
					.iter()
					.map(|row| row.key.source_slab_id),
			)
			.chain(snapshot.tiered_slabs.iter().map(|pointer| pointer.slab_id))
			.chain(snapshot.retired_tiered_slabs.iter().copied())
			.chain(snapshot.lsm_install_intents.iter().flat_map(|intent| {
				intent
					.inputs
					.iter()
					.map(|input| input.slab_id)
					.chain(intent.commit.slabs_to_add.iter().map(|run| run.slab_id))
					.chain(intent.local_output_files.iter().map(|file| file.slab_id))
					.chain(
						intent
							.commit
							.sidecar_swaps
							.iter()
							.map(|swap| swap.output_slab_id),
					)
			}))
			.max();
		highest = highest.max(named);
	}
	match highest {
		None => Ok(SlabId::ZERO),
		Some(id) => id
			.get()
			.checked_add(1)
			.map(SlabId::new)
			.ok_or_else(slab_ids_exhausted),
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn every_id_bearing_store_file_name_is_counted() {
		let named = |name: &str| slab_ids_named_by_file(name);
		let id = |n| SlabId::new(n);
		assert_eq!(named("slab_000000000000002a.wnd"), vec![id(0x2a)]);
		assert_eq!(
			named("slab_000000000000002a.wnd.retention.tmp"),
			vec![id(0x2a)]
		);
		assert_eq!(
			named("slab_000000000000002a.wnd.rehydrate.tmp"),
			vec![id(0x2a)]
		);
		assert_eq!(
			named("maintenance-staged-0000000000000001-0000000000000063.wnd"),
			vec![id(1), id(0x63)]
		);
		assert_eq!(
			named("lsm-quarantine-00000000000000ff-slab_0000000000000007.wnd"),
			vec![id(7)]
		);
		assert_eq!(
			named("compaction-temp-00000000000000ff-slab_0000000000000009.wnd"),
			vec![id(9)]
		);
		assert_eq!(named("slab_ffffffffffffffff.wnd"), vec![id(u64::MAX)]);
		assert!(named("storage_index.redb").is_empty());
		assert!(named("slab_xyz.wnd").is_empty());
	}
}
