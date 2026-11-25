//! Wendelon ADR 0130: a version's stored copies read with a witness of each read, and the
//! later check that the version still resolves to exactly what was read.
//!
//! A certification build proves a surviving version by reading every stored copy and comparing
//! its row digest. It may make those reads early, in an order that meets each frame once, and
//! use the result at the survivor's own turn only if, at that turn, the ordinary proof would
//! read the same bytes: the same listed copies, each resolving to the same frame of the same
//! local store. The witness records what a read resolved to; the check repeats the ordinary
//! proof's listings and resolutions and reads no frame.
//!
//! Only a copy served by a local slab store is witnessed. A published extent of a live local
//! store is immutable: an append reserves a fresh offset (`LocalSlabStore`'s `next_offset` only
//! grows), a failed append zeroes its own reservation before it returns and so before anything
//! names it, and no path truncates or rewrites a slab file. A store leaves service by leaving
//! the backing map, which the check sees as a different or missing store. A rehydrated copy or
//! an L2 object is not witnessed: the version is left to the ordinary proof, which alone
//! reads them, accounts a missing reference and retries a not-found object.

use super::lsm_driver::FrameReuse;
use super::tier_generation::ReadTarget;
use super::tiered_io::SlabBacking;
use super::*;

/// What one witnessed read of a stored copy resolved to.
pub(crate) struct CopyWitness {
	/// The listed location the read was asked for.
	requested: RecordLocation,
	/// The local store that served it. Held weakly: it names the store, owns nothing of it,
	/// and keeps only its allocation (so the address is not given to another store).
	store: std::sync::Weak<LocalSlabStore>,
	/// The location the read resolved to: its frame's extent, row, HLC and kind.
	target: RecordLocation,
	/// The frame was verified to name the version (its slab was tiered at the read).
	verified: bool,
}

impl CopyWitness {
	/// The serving store's address, to count distinct stores.
	pub(crate) fn store_address(&self) -> usize {
		self.store.as_ptr() as usize
	}
}

impl SlabManager {
	/// One listed copy, read through `frames`, with its witness; `None`, with nothing read, when
	/// the copy does not resolve to a local store. The resolution is the ordinary read's
	/// ([`Self::resolve_read_target`]) and the read uses exactly that resolution.
	async fn local_row_witnessed(
		&self,
		location: RecordLocation,
		frames: &mut FrameReuse,
	) -> std::io::Result<Option<(RecordBatch, CopyWitness)>> {
		let resolved = self.resolve_read_target(location, true).await?;
		let SlabBacking::Local(store) = &resolved.backing else {
			return Ok(None);
		};
		let witness = CopyWitness {
			requested: location,
			store: Arc::downgrade(store),
			target: resolved.location,
			verified: resolved.tiered,
		};
		let row = self
			.stored_row_resolved(location, frames, Some(resolved))
			.await?;
		Ok(Some((row, witness)))
	}

	/// [`Self::stored_rows_at_version`] for a build's early proof: the same admission and
	/// listing, each listed copy read through `frames` in the listed order, and one witness per
	/// copy left in `witnesses` in that order. `None`, with `witnesses` empty, when the version
	/// is not to be saved: it lists more copies than `witnesses` has room for (nothing is read),
	/// or a copy does not resolve to a local store (that copy and the later ones are not read).
	/// On an error `witnesses` is empty too.
	pub(crate) async fn stored_rows_witnessed(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
		hlc: HybridTimestamp,
		frames: &mut FrameReuse,
		witnesses: &mut Vec<CopyWitness>,
	) -> std::io::Result<Option<Vec<RecordBatch>>> {
		witnesses.clear();
		let locations = self.same_hlc_locations(tenant_id, key_identity, hlc)?;
		if locations.len() > witnesses.capacity() {
			return Ok(None);
		}
		let mut rows = Vec::with_capacity(locations.len());
		for location in locations {
			match self.local_row_witnessed(location, frames).await {
				Ok(Some((row, witness))) => {
					rows.push(row);
					witnesses.push(witness);
				}
				Ok(None) => {
					witnesses.clear();
					return Ok(None);
				}
				Err(error) => {
					witnesses.clear();
					return Err(error);
				}
			}
		}
		Ok(Some(rows))
	}

	/// Whether the ordinary proof of a version, made now, would read exactly what `witnesses`
	/// record. It repeats that proof's steps but the frame reads: the reader's listing (its
	/// admission and read scope), the data path's listing (its admission), and each listed
	/// copy's resolution (redirect, read scope, pin, backing snapshot, conflict mark, row index).
	///
	/// `Ok(true)` only if the reader's listing still selects `first` and is not the all-tombstone
	/// case; the data path lists exactly the witnessed copies, in order; and every copy resolves
	/// to the witnessed local store and the witnessed target (extent, row, HLC, kind), with no
	/// verification required now that its read did not perform. A refusal of a listing or of a
	/// resolution is returned as it is: it is the ordinary proof's refusal at this point.
	/// `Ok(false)` is a difference: the caller runs the ordinary proof.
	pub(crate) async fn version_resolves_as_witnessed(
		&self,
		tenant_id: TenantId,
		key_identity: Identity,
		hlc: HybridTimestamp,
		first: RecordLocation,
		witnesses: &[CopyWitness],
	) -> std::io::Result<bool> {
		let listed = self.version_locations(tenant_id, key_identity, hlc)?;
		if listed.first() != Some(&first) || listed.iter().all(|location| location.is_tombstone) {
			return Ok(false);
		}
		let locations = self.same_hlc_locations(tenant_id, key_identity, hlc)?;
		if locations.len() != witnesses.len() {
			return Ok(false);
		}
		for (location, witness) in locations.iter().zip(witnesses) {
			if *location != witness.requested {
				return Ok(false);
			}
			let ReadTarget {
				backing,
				location: target,
				tiered,
				_pin,
			} = self.resolve_read_target(*location, true).await?;
			let SlabBacking::Local(store) = &backing else {
				return Ok(false);
			};
			if !std::ptr::eq(witness.store.as_ptr(), Arc::as_ptr(store))
				|| target != witness.target
				|| (tiered && !witness.verified)
			{
				return Ok(false);
			}
		}
		Ok(true)
	}
}

#[cfg(test)]
impl SlabManager {
	/// Test-only: the class of backing one snapshot selects for `location`.
	pub(crate) async fn backing_class_for_test(&self, location: RecordLocation) -> &'static str {
		match self.resolve_read_target(location, true).await {
			Ok(resolved) => match resolved.backing {
				SlabBacking::Local(_) => "local",
				SlabBacking::Rehydrated(_) => "rehydrated",
				SlabBacking::L2(_) => "l2",
				SlabBacking::Missing => "missing",
			},
			Err(_) => "refused",
		}
	}

	/// Test-only: a sealed slab's local store leaves the backing map, with every index entry
	/// that names it left as it is.
	pub(crate) async fn drop_sealed_store_for_test(&self, slab_id: SlabId) -> bool {
		self.state.lock().await.sealed.remove(&slab_id).is_some()
	}
}
