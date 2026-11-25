//! Wendelon ADR 0130: a certification build's survivor proofs read each frame about once.
//!
//! A build proves every surviving version that comes from a child by reading its stored copies
//! and comparing their row digests, in the candidate's logical order. Keys are hashes, so that
//! order visits frames at random and reads and decodes a whole frame for nearly every survivor.
//!
//! Here the reads are made early, a bounded window of the logical order at a time: the window's
//! survivors are ordered by the child they come from and by where their first copy lies, and
//! proved through the node's verification frame set, which then meets each frame once. Nothing
//! else moves. The build's logical loop still visits every survivor in logical order and alone
//! stops, repairs, refuses and positions; at a survivor's turn it uses an early proof only if
//! the ordinary proof made at that moment would read exactly the same bytes
//! ([`SlabManager::version_resolves_as_witnessed`]), and otherwise runs the ordinary proof.
//!
//! A version is proved early only if every copy is served by a local slab store, it has at most
//! [`VERSION_COPIES`] copies, and its witnesses fit the window; any other version is left, as a
//! whole, to the ordinary proof at its turn. All of a window's memory is reserved once, under
//! [`PROOF_SCRATCH_BYTES`], and reused by the next window.

use super::{BuildRefusal, CandidateBuild};
#[cfg(test)]
use crate::network::data_server::child_writes::PauseStage;
use crate::network::data_server::reader::{self, ReadRefusal};
use crate::schema::{Identity, Locality, TableObjectId, TableSchemaDescriptor, TenantId};
use crate::storage::core::RecordLocation;
use crate::storage::index::certified_base::BaseEntry;
use crate::storage::manager::SlabManager;
use crate::storage::manager::{CopyWitness, WEAK_STORE_BYTES};
use std::borrow::Borrow;
use std::collections::BTreeMap;
use std::ops::Bound;

/// A candidate's key: its tenant, table and key identity.
pub(super) type Slot = (TenantId, TableObjectId, [u8; 32]);
/// A candidate's winners in logical order: the entry, and the child it comes from (`None`: the
/// active base).
pub(super) type Winners = BTreeMap<Slot, (BaseEntry, Option<usize>)>;

/// Everything a build keeps for its early proofs, at capacity: a window's slots, hints, saved
/// results and witnesses, and the store identities those witnesses may keep allocated. The
/// decoded frames are apart, under the node's verification frame allowance.
pub(crate) const PROOF_SCRATCH_BYTES: usize = 1024 * 1024;
/// A version of more stored copies than this is proved on the ordinary path.
const VERSION_COPIES: usize = 8;
/// The distinct local stores one window's saved proofs may name; a version that would name
/// another is proved on the ordinary path.
const WINDOW_STORES: usize = 16;

/// Where a survivor's first copy lies, to order a window's reads. A hint schedules; it proves
/// nothing.
struct Hint {
	child: u32,
	slab: u64,
	offset: u64,
	index: u32,
}

/// An early proof: the ordinary proof's answer, and its copies' witnesses in the window's
/// arena, in the order the copies were listed and read.
pub(super) struct SavedProof {
	locality: Locality,
	first: RecordLocation,
	start: u32,
	copies: u16,
}

/// What a build's proofs did. Read only by tests.
#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct ProofReadStats {
	pub(crate) windows: u64,
	/// Survivors whose reads were scheduled early, and those whose early proof was saved.
	pub(crate) scheduled: u64,
	pub(crate) saved: u64,
	/// Proved early but not saved: more copies or stores than the bounds, or no room.
	pub(crate) not_saved: u64,
	/// Saved proofs used at their turn; dropped at their turn because the version no longer
	/// resolved as witnessed; dropped by a repair.
	pub(crate) used: u64,
	pub(crate) fallbacks: u64,
	pub(crate) discarded: u64,
	/// Ordinary proofs made by the logical loop (a repair's second proof included).
	pub(crate) ordinary: u64,
	/// The early proofs' frame set: frames read and decoded, copies served from a held frame.
	pub(crate) frame_reads: u64,
	pub(crate) frame_hits: u64,
	/// The most witnesses and distinct stores one window held, and the scratch reserved.
	pub(crate) witnesses: u64,
	pub(crate) stores: u64,
	pub(crate) reserved_bytes: u64,
	pub(crate) window_rows: u64,
}

/// One window of a build's early proofs, reserved once and reused.
pub(super) struct ProofWindow {
	rows: usize,
	slots: Vec<Slot>,
	hints: Vec<Hint>,
	saved: Vec<Option<SavedProof>>,
	arena: Vec<CopyWitness>,
	stores: Vec<usize>,
	/// One version's witnesses, before they are admitted to the arena.
	sink: Vec<CopyWitness>,
	#[cfg(test)]
	stats: Option<std::sync::Arc<std::sync::Mutex<Vec<ProofReadStats>>>>,
}

impl ProofWindow {
	/// The largest window [`PROOF_SCRATCH_BYTES`] holds, for a candidate of `winners`. A store
	/// identity is counted for every arena store and for every witness waiting in the sink, at
	/// what a weak pointer keeps allocated; a winner is counted with its slot, hint and saved
	/// result and nine eighths of a witness (the arena's room for versions of several copies).
	pub(super) fn reserved(winners: usize) -> Self {
		let fixed = (WINDOW_STORES + VERSION_COPIES)
			* (WEAK_STORE_BYTES + std::mem::size_of::<usize>())
			+ VERSION_COPIES * std::mem::size_of::<CopyWitness>();
		let winner = std::mem::size_of::<Slot>()
			+ std::mem::size_of::<Hint>()
			+ std::mem::size_of::<Option<SavedProof>>()
			+ std::mem::size_of::<CopyWitness>() * 9 / 8
			+ 1;
		let rows = (PROOF_SCRATCH_BYTES.saturating_sub(fixed) / winner)
			.max(1)
			.min(winners.max(1));
		let mut window = Self {
			rows,
			slots: Vec::new(),
			hints: Vec::new(),
			saved: Vec::new(),
			arena: Vec::new(),
			stores: Vec::new(),
			sink: Vec::new(),
			#[cfg(test)]
			stats: None,
		};
		window.slots.reserve_exact(rows);
		window.hints.reserve_exact(rows);
		window.saved.reserve_exact(rows);
		window.arena.reserve_exact(rows + rows / 8);
		window.stores.reserve_exact(WINDOW_STORES);
		window.sink.reserve_exact(VERSION_COPIES);
		window
	}

	/// The winners one window covers.
	pub(super) fn rows(&self) -> usize {
		self.rows
	}

	/// Test-only: where this window's counts go.
	#[cfg(test)]
	pub(super) fn report_to(
		&mut self,
		stats: std::sync::Arc<std::sync::Mutex<Vec<ProofReadStats>>>,
	) {
		stats.lock().unwrap().push(ProofReadStats {
			window_rows: self.rows as u64,
			reserved_bytes: self.reserved_bytes() as u64,
			..ProofReadStats::default()
		});
		self.stats = Some(stats);
	}

	#[cfg(test)]
	pub(super) fn note(&self, count: impl FnOnce(&mut ProofReadStats)) {
		if let Some(stats) = &self.stats
			&& let Some(build) = stats.lock().unwrap().last_mut()
		{
			count(build);
		}
	}

	/// Test-only: the scratch this window holds, at capacity.
	#[cfg(test)]
	fn reserved_bytes(&self) -> usize {
		self.slots.capacity() * std::mem::size_of::<Slot>()
			+ self.hints.capacity() * std::mem::size_of::<Hint>()
			+ self.saved.capacity() * std::mem::size_of::<Option<SavedProof>>()
			+ (self.arena.capacity() + self.sink.capacity()) * std::mem::size_of::<CopyWitness>()
			+ (self.stores.capacity() + self.sink.capacity())
				* (WEAK_STORE_BYTES + std::mem::size_of::<usize>())
	}

	/// The early proofs of the window that follows `from` in logical order. Every child-sourced
	/// survivor with a stored data copy is scheduled by its first copy's place, and the window is
	/// then proved in the order (child, slab, offset) through one verification frame set. A
	/// proof that succeeds with every copy witnessed is saved, if its witnesses fit; anything
	/// else (a refusal, a missing or differing copy, a copy that is not a local store's, too many
	/// copies or stores) saves nothing and is left to the logical loop. The stop is observed
	/// before every listing and every proof.
	pub(super) async fn prove_early<D: Borrow<TableSchemaDescriptor>>(
		&mut self,
		slab: &SlabManager,
		winners: &Winners,
		from: Bound<Slot>,
		descriptors: &BTreeMap<(TenantId, TableObjectId), D>,
		build: &CandidateBuild<'_>,
	) -> Result<(), BuildRefusal> {
		self.slots.clear();
		self.hints.clear();
		self.saved.clear();
		self.arena.clear();
		self.stores.clear();
		for (slot, (entry, source)) in winners
			.range::<Slot, _>((from, Bound::Unbounded))
			.take(self.rows)
		{
			if build.stopping() {
				return Err(BuildRefusal::Stopped);
			}
			let index = self.slots.len() as u32;
			self.slots.push(*slot);
			self.saved.push(None);
			let Some(child) = *source else {
				continue;
			};
			let Ok(locations) =
				slab.version_locations(entry.tenant, Identity::new(entry.key_identity), entry.hlc)
			else {
				continue;
			};
			// A typed tombstone is proved by its listing alone: there is no read to move.
			let Some(first) = locations.first() else {
				continue;
			};
			if locations.iter().all(|location| location.is_tombstone) {
				continue;
			}
			self.hints.push(Hint {
				child: child as u32,
				slab: first.slab_id.get(),
				offset: first.offset,
				index,
			});
		}
		if self.slots.is_empty() {
			return Ok(());
		}
		// In place: no sort workspace.
		self.hints
			.sort_unstable_by_key(|hint| (hint.child, hint.slab, hint.offset));
		let mut frames = slab.verifier_frames();
		for at in 0..self.hints.len() {
			// Test-only: a pause between two early proofs.
			#[cfg(test)]
			if at > 0
				&& let Some(peer) = build.peer
			{
				peer.pause_at(PauseStage::CertificationBetweenEarlyProofs)
					.await;
			}
			if build.stopping() {
				return Err(BuildRefusal::Stopped);
			}
			let index = self.hints[at].index as usize;
			let (entry, _) = &winners[&self.slots[index]];
			let descriptor = descriptors[&(entry.tenant, entry.table)].borrow();
			let proved = reader::proved_location_witnessed(
				slab,
				entry.tenant,
				descriptor,
				Identity::new(entry.key_identity),
				entry.hlc,
				entry.row_digest,
				&mut frames,
				&mut self.sink,
			)
			.await;
			let Ok(Some((locality, first))) = proved else {
				self.sink.clear();
				continue;
			};
			if self.admits() {
				let start = self.arena.len() as u32;
				let copies = self.sink.len() as u16;
				self.arena.append(&mut self.sink);
				self.saved[index] = Some(SavedProof {
					locality,
					first,
					start,
					copies,
				});
				#[cfg(test)]
				self.note(|stats| stats.saved += 1);
			} else {
				self.sink.clear();
				#[cfg(test)]
				self.note(|stats| stats.not_saved += 1);
			}
		}
		#[cfg(test)]
		{
			let (reads, hits) = (frames.stats.reads, frames.stats.reused);
			let (scheduled, witnesses, stores) = (
				self.hints.len() as u64,
				self.arena.len() as u64,
				self.stores.len() as u64,
			);
			self.note(|stats| {
				stats.windows += 1;
				stats.scheduled += scheduled;
				stats.frame_reads += reads;
				stats.frame_hits += hits;
				stats.witnesses = stats.witnesses.max(witnesses);
				stats.stores = stats.stores.max(stores);
			});
		}
		Ok(())
	}

	/// Whether the version whose witnesses wait in the sink fits the window as a whole: room in
	/// the arena for every copy, and no store beyond the window's bound. The stores it names are
	/// entered as it is admitted.
	fn admits(&mut self) -> bool {
		if self.sink.is_empty() || self.arena.len() + self.sink.len() > self.arena.capacity() {
			return false;
		}
		let named = self.stores.len();
		for witness in &self.sink {
			let address = witness.store_address();
			if self.stores.contains(&address) {
				continue;
			}
			if self.stores.len() == self.stores.capacity() {
				self.stores.truncate(named);
				return false;
			}
			self.stores.push(address);
		}
		true
	}

	/// The early proof saved for the window's winner at `at`, taken once.
	pub(super) fn take(&mut self, at: usize) -> Option<SavedProof> {
		self.saved.get_mut(at)?.take()
	}

	/// An early proof at its survivor's logical turn: the ordinary proof's answer if, now, that
	/// proof would read exactly what the early one read; `None` if the version resolves
	/// differently (the caller runs the ordinary proof). A refusal is the ordinary proof's
	/// refusal at this turn.
	pub(super) async fn confirm(
		&self,
		slab: &SlabManager,
		entry: &BaseEntry,
		proof: &SavedProof,
	) -> Result<Option<(Locality, RecordLocation)>, ReadRefusal> {
		let witnesses = &self.arena[proof.start as usize..][..proof.copies as usize];
		let same = reader::still_proved(
			slab,
			entry.tenant,
			Identity::new(entry.key_identity),
			entry.hlc,
			proof.first,
			witnesses,
		)
		.await?;
		Ok(same.then_some((proof.locality, proof.first)))
	}

	/// A repair may have changed what is stored: every early proof still saved is dropped, and
	/// those survivors are proved on the ordinary path.
	pub(super) fn discard(&mut self) {
		#[cfg(test)]
		let mut dropped = 0;
		for saved in &mut self.saved {
			if saved.take().is_some() {
				#[cfg(test)]
				{
					dropped += 1;
				}
			}
		}
		#[cfg(test)]
		self.note(|stats| stats.discarded += dropped);
	}
}
