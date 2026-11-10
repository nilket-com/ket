//! Local slab files that disagree with the committed tier state, and the
//! ownership rule for every unlink of a slab path.
//!
//! Open classifies a discovered `slab_{id}.wnd` whose id has a committed tier
//! pointer as unverified evidence: a rehydrated cache copy, a leftover of a
//! crash between the first tier commit and its local unlink, or a file nothing
//! classifies. It is never opened, indexed, served, renamed over or unlinked;
//! the slab serves only through its tier generation. The set is fixed before
//! readiness is first exposed, and reporting it never authorizes cleanup.
use super::*;
use std::sync::atomic::AtomicU64;

/// Why open classified a local slab file as disagreeing with the committed
/// state. The label set is stable and bounded.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DisagreeingSlabFileReason {
	/// The slab id has a committed tier pointer.
	TierPointer,
	/// The pointer was removed by a durable, authority-backed retirement.
	RetiredTier,
}

impl DisagreeingSlabFileReason {
	pub const fn label(self) -> &'static str {
		match self {
			Self::TierPointer => "tier_pointer",
			Self::RetiredTier => "retired_tier",
		}
	}
}

/// One local slab file held as evidence.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QuarantinedSlabFile {
	pub slab_id: SlabId,
	/// The file's length when open classified it.
	pub bytes: u64,
	pub reason: DisagreeingSlabFileReason,
}

/// Diagnostic view of the files this manager holds as evidence.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct QuarantinedSlabFilesReport {
	/// Sorted by slab id.
	pub files: Vec<QuarantinedSlabFile>,
	/// Rehydrations refused because the slab's path holds a quarantined file.
	pub rehydrations_skipped: u64,
}

/// The quarantined files of one manager. Its contribution to the process
/// gauges is added at classification and removed when the manager drops.
#[derive(Default)]
pub(super) struct SlabFileQuarantine {
	files: std::sync::RwLock<BTreeMap<SlabId, QuarantinedSlabFile>>,
	rehydrations_skipped: AtomicU64,
}

impl SlabFileQuarantine {
	fn new(files: Vec<QuarantinedSlabFile>) -> Self {
		let files = files
			.into_iter()
			.map(|file| (file.slab_id, file))
			.collect::<BTreeMap<_, _>>();
		for file in files.values() {
			metrics().observe_storage_disagreeing_slab_file(file.reason.label(), file.bytes);
		}
		Self {
			files: std::sync::RwLock::new(files),
			rehydrations_skipped: AtomicU64::new(0),
		}
	}

	/// Only bootstrap may extend this set, while publication and state are locked.
	pub(super) fn insert_bootstrap(&self, file: QuarantinedSlabFile) {
		let mut files = self.files.write().unwrap_or_else(|e| e.into_inner());
		if let std::collections::btree_map::Entry::Vacant(entry) = files.entry(file.slab_id) {
			metrics().observe_storage_disagreeing_slab_file(file.reason.label(), file.bytes);
			entry.insert(file);
		}
	}

	pub(super) fn contains(&self, slab_id: SlabId) -> bool {
		self.files
			.read()
			.unwrap_or_else(|e| e.into_inner())
			.contains_key(&slab_id)
	}

	pub(super) fn note_rehydration_skipped(&self) {
		self.rehydrations_skipped.fetch_add(1, Ordering::Relaxed);
		metrics().observe_storage_rehydration_skipped("quarantined_file");
	}

	fn report(&self) -> QuarantinedSlabFilesReport {
		QuarantinedSlabFilesReport {
			files: self
				.files
				.read()
				.unwrap_or_else(|e| e.into_inner())
				.values()
				.copied()
				.collect(),
			rehydrations_skipped: self.rehydrations_skipped.load(Ordering::Relaxed),
		}
	}
}

impl Drop for SlabFileQuarantine {
	fn drop(&mut self) {
		let files = self.files.get_mut().unwrap_or_else(|e| e.into_inner());
		let bytes = files.values().map(|file| file.bytes).sum();
		metrics().release_storage_quarantined_slab_files(files.len() as u64, bytes);
	}
}

/// Split the discovered slab files at open: an id with a committed tier pointer
/// or explicit retirement evidence is quarantined. Other ids recover from their bytes. Absence
/// from redb is never a reason to quarantine.
pub(super) async fn classify_discovered_slab_files(
	file_ops: &dyn AsyncFileOps,
	dir: &Path,
	discovered: Vec<SlabId>,
	tier_pointer_ids: &BTreeSet<SlabId>,
	retired_tier_ids: &BTreeSet<SlabId>,
) -> std::io::Result<(Vec<SlabId>, SlabFileQuarantine)> {
	let mut local = Vec::with_capacity(discovered.len());
	let mut quarantined = Vec::new();
	for id in discovered {
		if !tier_pointer_ids.contains(&id) && !retired_tier_ids.contains(&id) {
			local.push(id);
			continue;
		}
		let path = slab_path(dir, id);
		let bytes = file_ops.file_len(&path).await?.ok_or_else(|| {
			Error::new(
				ErrorKind::NotFound,
				format!("slab disappeared during recovery: {}", path.display()),
			)
		})?;
		let reason = if tier_pointer_ids.contains(&id) {
			DisagreeingSlabFileReason::TierPointer
		} else {
			DisagreeingSlabFileReason::RetiredTier
		};
		tracing::warn!(
			slab_id = id.get(),
			bytes,
			reason = reason.label(),
			"a local slab file disagrees with the committed tier state; it is kept as evidence and never served"
		);
		quarantined.push(QuarantinedSlabFile {
			slab_id: id,
			bytes,
			reason,
		});
	}
	Ok((local, SlabFileQuarantine::new(quarantined)))
}

impl SlabManager {
	/// The local slab files this manager holds as evidence because they
	/// disagree with the committed tier state. Nothing here authorizes cleanup.
	pub fn quarantined_slab_files(&self) -> QuarantinedSlabFilesReport {
		self.quarantined_slab_files.report()
	}

	/// Record that the caller's operation removed `slab_id`'s local backing (a
	/// sealed slab or a rehydrated copy) from `state`. The caller holds the
	/// `state` guard, so the release is atomic with the removal; the file is
	/// then this process's to unlink, through [`Self::unlink_released_slab_file`],
	/// until an unlink completes.
	pub(super) fn release_slab_file(&self, slab_id: SlabId) {
		if !self.quarantined_slab_files.contains(slab_id) {
			self.released_slab_files.insert(slab_id);
		}
	}

	/// GC releases only the file identified by the open slab handle. The
	/// caller holds X's tier lock and maintenance gate through this operation;
	/// the directory owner and allocator prevent another managed installer from
	/// replacing the checked path between the identity check and unlink.
	pub(super) async fn gc_unlink_owned_slab_file(&self, slab_id: SlabId) -> std::io::Result<()> {
		use std::os::unix::fs::MetadataExt;
		let path = slab_path(&self.config.dir, slab_id);
		if self.quarantined_slab_files.contains(slab_id) {
			return Err(Error::new(
				ErrorKind::WouldBlock,
				"quarantined slab cannot be released by GC",
			));
		}
		let expected = {
			let mut state = self.state.lock().await;
			let sealed = state.sealed.get(&slab_id).map(|slab| slab.value().clone());
			let rehydrated = state
				.rehydrated
				.get(&slab_id)
				.map(|entry| entry.store.clone());
			let expected = match (sealed.as_ref(), rehydrated.as_ref()) {
				(Some(a), Some(b)) => {
					let identity = a.opened_file_identity()?;
					if identity != b.opened_file_identity()? {
						return Err(Error::new(
							ErrorKind::InvalidData,
							"GC slab handles disagree",
						));
					}
					Some(identity)
				}
				(Some(slab), None) | (None, Some(slab)) => Some(slab.opened_file_identity()?),
				(None, None) => match std::fs::symlink_metadata(&path) {
					Err(error) if error.kind() == ErrorKind::NotFound => None,
					Err(error) => return Err(error),
					Ok(_) => {
						return Err(Error::new(
							ErrorKind::InvalidData,
							"GC found a slab path with no owned backing",
						));
					}
				},
			};
			if let Some(expected) = expected {
				let metadata = std::fs::symlink_metadata(&path)?;
				if !metadata.file_type().is_file() || (metadata.dev(), metadata.ino()) != expected {
					return Err(Error::new(
						ErrorKind::InvalidData,
						"GC slab path identity changed",
					));
				}
				state.sealed.remove(&slab_id);
				state.rehydrated.remove(&slab_id);
				self.release_slab_file(slab_id);
			}
			state.hot_read_counts.remove(&slab_id);
			state.rehydrating.remove(&slab_id);
			expected
		};
		if let Some(expected) = expected {
			let matches = std::fs::symlink_metadata(&path).is_ok_and(|metadata| {
				metadata.file_type().is_file() && (metadata.dev(), metadata.ino()) == expected
			});
			if !matches {
				self.released_slab_files.remove(&slab_id);
				return Err(Error::new(
					ErrorKind::InvalidData,
					"GC slab path changed before unlink",
				));
			}
			#[cfg(test)]
			{
				let hold = self
					.continuity_test_holds
					.gc_before_unlink_hold
					.lock()
					.unwrap()
					.clone();
				if let Some((entered, release)) = hold {
					entered.add_permits(1);
					let _ = release.acquire().await;
				}
			}
			if self.released_slab_files.remove(&slab_id).is_none() {
				return Err(Error::new(
					ErrorKind::WouldBlock,
					"GC slab release was not owned",
				));
			}
			self.file_ops.unlink(&path).await?;
			#[cfg(test)]
			{
				let hold = self
					.continuity_test_holds
					.gc_after_unlink_hold
					.lock()
					.unwrap()
					.clone();
				if let Some((entered, release)) = hold {
					entered.add_permits(1);
					let _ = release.acquire().await;
				}
			}
			fsync_parent_dir(self.file_ops.as_ref(), &path).await?;
			#[cfg(test)]
			{
				let hold = self
					.continuity_test_holds
					.gc_after_fsync_hold
					.lock()
					.unwrap()
					.clone();
				if let Some((entered, release)) = hold {
					entered.add_permits(1);
					let _ = release.acquire().await;
				}
			}
		}
		Ok(())
	}

	/// Unlink `slab_id`'s local file only when this process released it and no
	/// unlink has completed since. One caller claims the release; a failed
	/// unlink returns the claim, so a retry finishes it. A slab id or a path is
	/// never authority on its own, and a quarantined file is never released.
	///
	/// The release is keyed by id alone. That is sound only while no other file
	/// can appear at a released id's path before its unlink, which rests on:
	/// 1. the allocator only advances (`ManagerState::advance_slab_allocator`),
	///    so a released id is never allocated again;
	/// 2. every installer of a slab path uses a freshly allocated id or links
	///    without replacing (rehydration's `link_no_replace`); the call sites
	///    are pinned by an architecture census;
	/// 3. one manager per data directory: the only production constructor,
	///    `open_with_owner`, holds the directory's exclusive owner lock.
	/// A change to any of these needs the file's identity (device and inode,
	/// captured at release) in the release as well as its id.
	pub(super) async fn unlink_released_slab_file(&self, slab_id: SlabId) -> std::io::Result<()> {
		if self.released_slab_files.remove(&slab_id).is_none() {
			return Ok(());
		}
		let path = slab_path(&self.config.dir, slab_id);
		match self.file_ops.unlink(&path).await {
			Ok(()) => fsync_parent_dir(self.file_ops.as_ref(), &path).await,
			Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
			Err(err) => {
				// The file is still there, so nothing can have been installed in
				// its place: the release stays owed.
				self.released_slab_files.insert(slab_id);
				Err(err)
			}
		}
	}
}
