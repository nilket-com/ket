//! Compaction of the applied log into numbered generations (E3, E5).
//!
//! Generation 0 is `counter-applied.log`, headerless, as F-CTR-8 writes it.
//! Compaction writes `counter-applied.<g>` (`create_new`, never reused): a
//! header `{g, Q, op-id high-water}` and every record E3 keeps. `CURRENT`
//! names the authoritative generation and checks its base: `{g, base_len,
//! blake3(bytes[0..base_len])}`. Frames appended after compaction form the
//! tail beyond `base_len`, parsed under the F-CTR-8 rules.
//!
//! Compaction is two-phase. Under the applied-log mutex it snapshots the live
//! set; outside it, it writes and syncs the new generation; back under the
//! mutex it copies the frames appended since the snapshot, syncs them and the
//! directory, and publishes `CURRENT` (temp file, `sync_all`, rename, directory
//! sync). Still under the mutex it switches the cache, stores `Q_mem` with
//! Release ordering, and only then evicts the deleted write keys. After that
//! it unlinks older numbered generations and replaces generation 0 with the
//! downgrade poison, a single zero-length frame older parsers reject.
//!
//! A write or file-sync failure before the `CURRENT` rename abandons the new
//! generation. Any directory-sync failure (Addendum A6), and any failure at
//! or after the rename, latches the store unsettled, and the data server trips
//! its distrust fence.
use std::collections::HashSet;
use std::io::{Error, ErrorKind};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use crate::storage::counter::applied_log::{self, AppliedLogFault, AppliedLogImage};
use crate::storage::counter::{
	AppliedCounterAdd, AppliedLogState, CounterKey, CounterLeader, CounterParticipant,
	CounterWriteKey, DurableCounterWitnessStore, retention,
};

pub(super) const GENERATION_ZERO_NAME: &str = "counter-applied.log";
const GENERATION_PREFIX: &str = "counter-applied.";
const CURRENT_NAME: &str = "CURRENT";
/// Cannot collide with the witness temp files `.tmp-{op_id:016x}`.
const CURRENT_TEMP_NAME: &str = ".current-tmp";
const POISON_TEMP_NAME: &str = ".applied-poison-tmp";
/// The downgrade poison: one zero-length frame, which every applied-log parser
/// (this one included) rejects.
pub(super) const POISON: [u8; 4] = [0; 4];

const GENERATION_MAGIC: [u8; 8] = *b"WNCTRGEN";
const CURRENT_MAGIC: [u8; 8] = *b"WNCTRCUR";
const HEADER_LEN: usize = 32;
const CURRENT_LEN: usize = 56;

/// Compaction is due when the current generation reaches this size and twice
/// its own base, or when an hour has passed since the last attempt.
const COMPACTION_MIN_BYTES: u64 = 64 * 1024 * 1024;
const COMPACTION_MAX_AGE: Duration = Duration::from_secs(60 * 60);
/// A failed attempt is not retried sooner than this.
const COMPACTION_RETRY_BACKOFF: Duration = Duration::from_secs(60);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct GenerationHeader {
	generation: u64,
	cutoff: u64,
	high_water: u64,
}

impl GenerationHeader {
	fn encode(&self) -> [u8; HEADER_LEN] {
		let mut bytes = [0u8; HEADER_LEN];
		bytes[..8].copy_from_slice(&GENERATION_MAGIC);
		bytes[8..16].copy_from_slice(&self.generation.to_le_bytes());
		bytes[16..24].copy_from_slice(&self.cutoff.to_le_bytes());
		bytes[24..32].copy_from_slice(&self.high_water.to_le_bytes());
		bytes
	}

	fn decode(bytes: &[u8]) -> Option<Self> {
		if bytes.len() < HEADER_LEN || bytes[..8] != GENERATION_MAGIC {
			return None;
		}
		let field = |at: usize| u64::from_le_bytes(bytes[at..at + 8].try_into().expect("field"));
		Some(Self {
			generation: field(8),
			cutoff: field(16),
			high_water: field(24),
		})
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct CurrentPointer {
	generation: u64,
	base_len: u64,
	digest: [u8; 32],
}

impl CurrentPointer {
	fn encode(&self) -> [u8; CURRENT_LEN] {
		let mut bytes = [0u8; CURRENT_LEN];
		bytes[..8].copy_from_slice(&CURRENT_MAGIC);
		bytes[8..16].copy_from_slice(&self.generation.to_le_bytes());
		bytes[16..24].copy_from_slice(&self.base_len.to_le_bytes());
		bytes[24..].copy_from_slice(&self.digest);
		bytes
	}

	fn decode(bytes: &[u8]) -> std::io::Result<Self> {
		if bytes.len() != CURRENT_LEN || bytes[..8] != CURRENT_MAGIC {
			return Err(AppliedLogFault::NoAuthority.io());
		}
		let field = |at: usize| u64::from_le_bytes(bytes[at..at + 8].try_into().expect("field"));
		let pointer = Self {
			generation: field(8),
			base_len: field(16),
			digest: bytes[24..].try_into().expect("digest"),
		};
		if pointer.generation == 0 {
			return Err(AppliedLogFault::NoAuthority.io());
		}
		Ok(pointer)
	}
}

/// Test-only switches that each disable one compaction or recovery rule, so a
/// law test can show the failure the rule prevents.
#[cfg(test)]
#[derive(Default)]
pub(super) struct CompactionSwitches {
	/// Delete by the attempt horizon `H` instead of the published cutoff.
	pub(super) prune_by_horizon: std::sync::atomic::AtomicBool,
	/// Delete unkeyed records whatever their age.
	pub(super) prune_unkeyed: std::sync::atomic::AtomicBool,
	/// Write a zero operation-id high-water.
	pub(super) omit_high_water: std::sync::atomic::AtomicBool,
	/// Write a zero cutoff (`Q` unpersisted).
	pub(super) omit_cutoff: std::sync::atomic::AtomicBool,
	/// Publish `CURRENT` before the new generation's data is synced.
	pub(super) publish_before_sync: std::sync::atomic::AtomicBool,
	/// Carry on as if published when the `CURRENT` rename or its directory
	/// sync fails, instead of latching.
	pub(super) no_publish_latch: std::sync::atomic::AtomicBool,
	/// Recovery leaves stale generations and temp files in place.
	pub(super) skip_recovery_cleanup: std::sync::atomic::AtomicBool,
	/// Recovery treats a lone numbered generation as an empty store.
	pub(super) empty_without_current: std::sync::atomic::AtomicBool,
	/// Check `CURRENT`'s digest over the whole file instead of its base.
	pub(super) whole_file_checksum: std::sync::atomic::AtomicBool,
	/// Poison generation 0 before `CURRENT` is durable.
	pub(super) poison_early: std::sync::atomic::AtomicBool,
	/// A failed phase-3 directory sync abandons instead of latching (the
	/// semantics before Addendum A6).
	pub(super) abandon_on_directory_sync: std::sync::atomic::AtomicBool,
	/// Recovery skips its barrier (Addendum A4) before deleting or poisoning.
	pub(super) skip_recovery_barrier: std::sync::atomic::AtomicBool,
	/// Recovery leaves an unpoisoned generation 0 beside `CURRENT` as it is.
	pub(super) skip_recovery_poison: std::sync::atomic::AtomicBool,
}

#[cfg(test)]
fn switched(flag: &std::sync::atomic::AtomicBool) -> bool {
	flag.load(Ordering::Relaxed)
}

/// What the leader supplies to one compaction.
pub(super) struct CompactionRequest {
	/// `now_c - E`; the store publishes `max(Q, this)`.
	pub(super) proposed_cutoff: u64,
	/// `now - H`, read only by the test switch that deletes by `H`.
	#[cfg_attr(not(test), allow(dead_code))]
	pub(super) horizon_cutoff: u64,
}

/// The leader-side state a compaction reads after its snapshot and updates
/// after its publish.
pub(super) trait CompactionHooks {
	/// The highest operation id allocated so far.
	fn high_water(&self) -> u64;
	/// The counter key of a pending witness, for an unkeyed record of its op.
	fn pending_key(&self, op_id: u64) -> Option<CounterKey>;
	/// Evict deleted write keys from the record map (never a pending one).
	fn evict(&self, deleted: &[CounterWriteKey]);
}

/// What one compaction did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CompactionReport {
	pub(crate) generation: u64,
	pub(crate) cutoff: u64,
	pub(crate) kept: usize,
	pub(crate) deleted: usize,
	/// Nothing to compact: generation 0 does not exist.
	pub(crate) skipped: bool,
}

struct Snapshot {
	generation: u64,
	records: Vec<AppliedCounterAdd>,
	file_len: u64,
	cutoff: u64,
	high_water: u64,
}

/// A failure before the `CURRENT` rename (the new generation is abandoned) or
/// at or after it (the store latches).
enum CompactionFailure {
	Abandoned(Error),
	Latched(Error),
}

impl DurableCounterWitnessStore {
	pub(super) fn generation_path(&self, generation: u64) -> PathBuf {
		if generation == 0 {
			self.dir.join(GENERATION_ZERO_NAME)
		} else {
			self.dir.join(format!("{GENERATION_PREFIX}{generation}"))
		}
	}

	fn current_path(&self) -> PathBuf {
		self.dir.join(CURRENT_NAME)
	}

	/// The in-memory cutoff `Q_mem`, read with Acquire ordering: a reader that
	/// observes an eviction also observes the cutoff published before it.
	pub(super) fn cutoff(&self) -> u64 {
		self.cutoff.load(Ordering::Acquire)
	}

	fn publish_cutoff(&self, cutoff: u64) {
		self.cutoff.fetch_max(cutoff, Ordering::Release);
	}

	/// Every numbered generation file in the directory.
	fn numbered_generations(&self) -> std::io::Result<Vec<(u64, PathBuf)>> {
		let mut found = Vec::new();
		for path in self.ops.read_dir(&self.dir)? {
			let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
				continue;
			};
			let Some(number) = name.strip_prefix(GENERATION_PREFIX) else {
				continue;
			};
			if !number.is_empty() && number.bytes().all(|byte| byte.is_ascii_digit()) {
				if let Ok(generation) = number.parse::<u64>() {
					found.push((generation, path));
				}
			}
		}
		found.sort();
		Ok(found)
	}

	/// The operation ids whose witness file exists.
	fn witnessed_ops(&self) -> std::io::Result<HashSet<u64>> {
		let mut ops = HashSet::new();
		for path in self.ops.read_dir(&self.dir)? {
			let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
				continue;
			};
			let Some(rest) = name.strip_prefix("counter-witness-") else {
				continue;
			};
			let Some(hex) = rest
				.strip_suffix(".pw2")
				.or_else(|| rest.strip_suffix(".pw"))
			else {
				continue;
			};
			let op_id = u64::from_str_radix(hex, 16)
				.map_err(|_| Error::new(ErrorKind::InvalidData, "malformed witness name"))?;
			ops.insert(op_id);
		}
		Ok(ops)
	}

	/// Resolve the authoritative generation (§3.6) and parse it. With `clean`
	/// (recovery), delete what the rules name: every numbered generation other
	/// than the authority, and the `CURRENT` and poison temp files. Before any
	/// of that, when `CURRENT` names a generation, recovery's barrier
	/// (Addendum A4) makes the generation's bytes and every name in the
	/// directory durable: a process that crashed without power loss may have
	/// left `CURRENT` volatile. A barrier failure fails recovery closed with
	/// every file kept.
	pub(super) fn resolve_image(&self, clean: bool) -> std::io::Result<AppliedLogImage> {
		let current = match self.ops.read_file(&self.current_path()) {
			Ok(bytes) => Some(CurrentPointer::decode(&bytes)?),
			Err(error) if error.kind() == ErrorKind::NotFound => None,
			Err(error) => return Err(error),
		};
		let numbered = self.numbered_generations()?;
		let (image, authority) = match current {
			// `CURRENT` is the only authority; generation 0 is never read.
			Some(pointer) => (self.parse_generation(&pointer)?, Some(pointer.generation)),
			None => match self.ops.read_file(&self.generation_path(0)) {
				Ok(bytes) if bytes == POISON => return Err(AppliedLogFault::NoAuthority.io()),
				Ok(bytes) => (applied_log::parse(&bytes)?, None),
				Err(error) if error.kind() == ErrorKind::NotFound => {
					if !numbered.is_empty() {
						#[cfg(test)]
						if switched(&self.switches.empty_without_current) {
							return applied_log::parse(&[]);
						}
						return Err(AppliedLogFault::NoAuthority.io());
					}
					(applied_log::parse(&[])?, None)
				}
				Err(error) => return Err(error),
			},
		};
		if clean {
			if let Some(generation) = authority {
				#[cfg(not(test))]
				let barrier = true;
				#[cfg(test)]
				let barrier = !switched(&self.switches.skip_recovery_barrier);
				if barrier {
					self.ops.sync_data(&self.generation_path(generation))?;
					self.ops.sync_dir(&self.dir)?;
				}
			}
			#[cfg(test)]
			if switched(&self.switches.skip_recovery_cleanup) {
				return Ok(image);
			}
			let stale = numbered
				.into_iter()
				.filter(|(generation, _)| Some(*generation) != authority)
				.map(|(_, path)| path)
				.chain([
					self.dir.join(CURRENT_TEMP_NAME),
					self.dir.join(POISON_TEMP_NAME),
				]);
			let mut removed = false;
			for path in stale {
				match self.ops.unlink(&path) {
					Ok(()) => removed = true,
					Err(error) if error.kind() == ErrorKind::NotFound => (),
					Err(error) => return Err(error),
				}
			}
			if removed {
				self.ops.sync_dir(&self.dir)?;
			}
		}
		Ok(image)
	}

	/// Verify the generation `CURRENT` names against its base and parse it: the
	/// base must be complete, the tail beyond it may end in a torn frame.
	fn parse_generation(&self, pointer: &CurrentPointer) -> std::io::Result<AppliedLogImage> {
		let mismatch = || {
			AppliedLogFault::GenerationMismatch {
				generation: pointer.generation,
			}
			.io()
		};
		let bytes = match self
			.ops
			.read_file(&self.generation_path(pointer.generation))
		{
			Ok(bytes) => bytes,
			Err(error) if error.kind() == ErrorKind::NotFound => return Err(mismatch()),
			Err(error) => return Err(error),
		};
		let base_len = usize::try_from(pointer.base_len).map_err(|_| mismatch())?;
		if base_len < HEADER_LEN || bytes.len() < base_len {
			return Err(mismatch());
		}
		let checked = &bytes[..base_len];
		#[cfg(test)]
		let checked = if switched(&self.switches.whole_file_checksum) {
			&bytes[..]
		} else {
			checked
		};
		if crate::hash::blake3::digest(checked) != pointer.digest {
			return Err(mismatch());
		}
		let header = GenerationHeader::decode(&bytes).ok_or_else(mismatch)?;
		if header.generation != pointer.generation {
			return Err(mismatch());
		}
		let (mut records, base_torn) = applied_log::parse_frames(&bytes[..base_len], HEADER_LEN)?;
		if base_torn.is_some() {
			return Err(mismatch());
		}
		let (tail, torn_tail) = applied_log::parse_frames(&bytes, base_len)?;
		records.extend(tail);
		let mut image = AppliedLogImage::new(records, torn_tail, bytes.len() as u64)?;
		image.generation = pointer.generation;
		image.base_len = pointer.base_len;
		image.cutoff = header.cutoff;
		image.high_water = header.high_water;
		Ok(image)
	}

	/// Whether the current generation has outgrown its trigger size.
	pub(super) fn compaction_due_by_size(&self) -> bool {
		let Ok(state) = self.lock_applied() else {
			return false;
		};
		match &*state {
			AppliedLogState::Loaded(image) => {
				image.file_len >= COMPACTION_MIN_BYTES.max(image.base_len.saturating_mul(2))
			}
			_ => false,
		}
	}

	/// One compaction (E3, E5). Runs on the blocking pool under the owner
	/// write permit; never on an image that holds a torn tail, never while the
	/// store is unsettled.
	pub(super) fn compact(
		&self,
		request: &CompactionRequest,
		hooks: &dyn CompactionHooks,
	) -> std::io::Result<CompactionReport> {
		let _permit = self.permit()?;
		// Phase 1, under the applied-log mutex: snapshot the live set.
		let snapshot = {
			let mut state = self.lock_applied()?;
			self.check_settled()?;
			if matches!(*state, AppliedLogState::Unloaded) {
				*state = AppliedLogState::Loaded(self.read_applied_image()?);
			}
			let AppliedLogState::Loaded(image) = &*state else {
				return Err(AppliedLogFault::Unsettled.io());
			};
			if image.torn_tail.is_some() {
				return Err(AppliedLogFault::CompactionNotReady.io());
			}
			// A first compaction needs generation 0 on disk: a crash before its
			// `CURRENT` publish must leave an authority behind.
			if image.generation == 0 && !self.ops.exists(&self.generation_path(0))? {
				return Ok(CompactionReport {
					generation: 0,
					cutoff: image.cutoff,
					kept: 0,
					deleted: 0,
					skipped: true,
				});
			}
			Snapshot {
				generation: image.generation,
				records: image.records.clone(),
				file_len: image.file_len,
				cutoff: image.cutoff,
				high_water: image.high_water,
			}
		};

		// Phase 2, outside the mutex: decide, write and sync the generation.
		let cutoff = snapshot.cutoff.max(request.proposed_cutoff);
		let prune_cutoff = cutoff;
		#[cfg(test)]
		let prune_cutoff = if switched(&self.switches.prune_by_horizon) {
			request.horizon_cutoff
		} else {
			prune_cutoff
		};
		let witnessed = self.witnessed_ops()?;
		let (mut kept, mut deleted) = (Vec::new(), Vec::new());
		for record in snapshot.records.iter().cloned() {
			let prune = retention::prunable(
				&record.write_key,
				prune_cutoff,
				witnessed.contains(&record.op_id),
			);
			#[cfg(test)]
			let prune = prune
				|| (switched(&self.switches.prune_unkeyed)
					&& record.key.is_none()
					&& !witnessed.contains(&record.op_id));
			if prune {
				deleted.push(record);
			} else {
				kept.push(record);
			}
		}
		let snapshot_high = snapshot.records.iter().map(|r| r.op_id).max().unwrap_or(0);
		let header = GenerationHeader {
			generation: self.next_generation(snapshot.generation),
			cutoff,
			high_water: hooks
				.high_water()
				.max(snapshot.high_water)
				.max(snapshot_high),
		};
		#[cfg(test)]
		let header = GenerationHeader {
			cutoff: if switched(&self.switches.omit_cutoff) {
				0
			} else {
				header.cutoff
			},
			high_water: if switched(&self.switches.omit_high_water) {
				0
			} else {
				header.high_water
			},
			..header
		};
		let mut base = header.encode().to_vec();
		for record in &mut kept {
			// An unkeyed record is rewritten keyed only when its pending witness
			// names the key.
			if record.key.is_none() {
				record.key = hooks.pending_key(record.op_id);
			}
			base.extend(encode_record(record)?);
		}
		let path = self.generation_path(header.generation);
		let pointer = CurrentPointer {
			generation: header.generation,
			base_len: base.len() as u64,
			digest: crate::hash::blake3::digest(&base),
		};
		#[cfg(not(test))]
		let publish_before_sync = false;
		#[cfg(test)]
		let publish_before_sync = switched(&self.switches.publish_before_sync);
		// A refused `create_new` created nothing of ours to remove.
		if let Err(error) = self.ops.create_new_file(&path, &base) {
			let created = error.kind() != ErrorKind::AlreadyExists;
			return Err(self.abandon(created.then_some(path.as_path()), error));
		}
		if !publish_before_sync {
			if let Err(error) = self.ops.sync_data(&path) {
				return Err(self.abandon(Some(&path), error));
			}
		}

		// Phases 3 and 4, back under the mutex.
		let mut state = self.lock_applied()?;
		let published = self.publish(
			&mut state,
			&snapshot,
			&path,
			&pointer,
			kept,
			header,
			publish_before_sync,
		);
		let kept = match published {
			Ok(kept) => kept,
			Err(CompactionFailure::Abandoned(error)) => {
				drop(state);
				return Err(self.abandon(Some(&path), error));
			}
			Err(CompactionFailure::Latched(error)) => {
				*state = AppliedLogState::Unsettled;
				self.unsettle(&self.current_path(), &error);
				return Err(error);
			}
		};
		// The cache names the new generation; publish `Q_mem`, then evict.
		self.publish_cutoff(header.cutoff);
		let deleted_keys: Vec<CounterWriteKey> =
			deleted.iter().map(|record| record.write_key).collect();
		hooks.evict(&deleted_keys);
		drop(state);

		// §3.4: only now that `CURRENT` is durable, retire older generations.
		if let Err(error) = self.retire_older_generations(header.generation) {
			let mut state = self.lock_applied()?;
			*state = AppliedLogState::Unsettled;
			self.unsettle(&self.dir, &error);
			return Err(error);
		}
		Ok(CompactionReport {
			generation: header.generation,
			cutoff: header.cutoff,
			kept,
			deleted: deleted.len(),
			skipped: false,
		})
	}

	/// Phase 3 and the cache switch of phase 4. Returns the number of records
	/// the new generation holds.
	#[allow(clippy::too_many_arguments)]
	fn publish(
		&self,
		state: &mut AppliedLogState,
		snapshot: &Snapshot,
		path: &Path,
		pointer: &CurrentPointer,
		kept: Vec<AppliedCounterAdd>,
		header: GenerationHeader,
		publish_before_sync: bool,
	) -> Result<usize, CompactionFailure> {
		use CompactionFailure::{Abandoned, Latched};
		if self.is_unsettled() {
			return Err(Abandoned(AppliedLogFault::Unsettled.io()));
		}
		let AppliedLogState::Loaded(image) = &*state else {
			return Err(Abandoned(AppliedLogFault::Unsettled.io()));
		};
		if image.generation != snapshot.generation
			|| image.torn_tail.is_some()
			|| image.file_len < snapshot.file_len
		{
			return Err(Abandoned(AppliedLogFault::CompactionNotReady.io()));
		}
		let old_path = self.generation_path(image.generation);
		let observed_len = match self.ops.file_len(&old_path) {
			Ok(len) => Some(len),
			Err(error) if error.kind() == ErrorKind::NotFound => None,
			Err(error) => return Err(Abandoned(error)),
		};
		if observed_len.unwrap_or(0) != image.file_len {
			return Err(Abandoned(
				AppliedLogFault::Diverged {
					expected_len: image.file_len,
					observed_len,
				}
				.io(),
			));
		}
		// Copy the frames appended since the snapshot into the tail.
		let tail: Vec<AppliedCounterAdd> = image.records[snapshot.records.len()..].to_vec();
		let mut tail_bytes = Vec::new();
		for record in &tail {
			tail_bytes.extend(encode_record(record).map_err(Abandoned)?);
		}
		if !tail_bytes.is_empty() {
			self.ops
				.append_file(path, &tail_bytes)
				.and_then(|_| self.ops.sync_data(path))
				.map_err(Abandoned)?;
		}
		// Addendum A6: under fsync-error semantics a failed directory sync may
		// drop other dirty names here (witness creates and unlinks, this
		// generation), so it latches rather than abandoning.
		let synced = self.ops.sync_dir(&self.dir);
		#[cfg(test)]
		let synced = synced.map_err(|error| {
			if switched(&self.switches.abandon_on_directory_sync) {
				Abandoned(error)
			} else {
				Latched(error)
			}
		});
		#[cfg(not(test))]
		let synced = synced.map_err(Latched);
		synced?;
		#[cfg(test)]
		if switched(&self.switches.poison_early) {
			self.poison_generation_zero()
				.and_then(|_| self.ops.sync_dir(&self.dir))
				.map_err(Abandoned)?;
		}
		let temp = self.dir.join(CURRENT_TEMP_NAME);
		self.ops
			.write_file(&temp, &pointer.encode())
			.and_then(|()| self.ops.sync_all(&temp))
			.map_err(Abandoned)?;
		// From the rename on, a failure leaves `CURRENT` in doubt: latch.
		let renamed = self
			.ops
			.rename(&temp, &self.current_path())
			.and_then(|()| self.ops.sync_dir(&self.dir));
		#[cfg(test)]
		let renamed = renamed.or_else(|error| {
			// The negative control carries on as if `CURRENT` were durable.
			if switched(&self.switches.no_publish_latch) {
				Ok(())
			} else {
				Err(error)
			}
		});
		renamed.map_err(Latched)?;
		if publish_before_sync {
			self.ops.sync_data(path).map_err(Latched)?;
		}
		let mut records = kept;
		records.extend(tail);
		let count = records.len();
		let mut next = AppliedLogImage::new(
			records,
			None,
			pointer.base_len.saturating_add(tail_bytes.len() as u64),
		)
		.map_err(Latched)?;
		next.generation = header.generation;
		next.base_len = pointer.base_len;
		next.cutoff = header.cutoff;
		next.high_water = header.high_water;
		*state = AppliedLogState::Loaded(next);
		Ok(count)
	}

	/// A generation number never used before in this process: recovery deleted
	/// every numbered generation but the authority, and an abandoned attempt's
	/// number is skipped even if its file could not be removed.
	fn next_generation(&self, current: u64) -> u64 {
		let next = self
			.attempted_generation
			.load(Ordering::Relaxed)
			.max(current)
			.saturating_add(1);
		self.attempted_generation.fetch_max(next, Ordering::Relaxed);
		next
	}

	/// Best-effort removal of an abandoned generation (when this attempt created
	/// it) and of the `CURRENT` temp file; recovery deletes whatever remains.
	fn abandon(&self, generation: Option<&Path>, error: Error) -> Error {
		let temp = self.dir.join(CURRENT_TEMP_NAME);
		for stale in generation.into_iter().chain([temp.as_path()]) {
			let _ = self.ops.unlink(stale);
		}
		tracing::warn!(
			tenant_id = self.tenant.map(|tenant| tenant.get()),
			generation = ?generation.map(Path::display),
			error = %error,
			"counter applied-log compaction abandoned"
		);
		error
	}

	/// Unlink every numbered generation older than `generation`, replace
	/// generation 0 with the poison, then sync the directory.
	fn retire_older_generations(&self, generation: u64) -> std::io::Result<()> {
		for (older, path) in self.numbered_generations()? {
			if older < generation {
				match self.ops.unlink(&path) {
					Ok(()) => (),
					Err(error) if error.kind() == ErrorKind::NotFound => (),
					Err(error) => return Err(error),
				}
			}
		}
		self.poison_generation_zero()?;
		self.ops.sync_dir(&self.dir)
	}

	/// Addendum A1: with `CURRENT` naming a generation, recovery re-applies the
	/// downgrade poison to a generation 0 a crash left unpoisoned (between the
	/// `CURRENT` directory sync and the poison), then syncs the directory, so a
	/// downgraded binary fails closed. A failure latches the store unsettled.
	pub(super) fn repoison_on_recovery(&self, state: &mut AppliedLogState) {
		#[cfg(test)]
		if switched(&self.switches.skip_recovery_poison) {
			return;
		}
		let poisoned = self.poison_generation_zero().and_then(|renamed| {
			if renamed {
				self.ops.sync_dir(&self.dir)
			} else {
				Ok(())
			}
		});
		if let Err(error) = poisoned {
			*state = AppliedLogState::Unsettled;
			self.unsettle(&self.generation_path(0), &error);
		}
	}

	/// Replace an unpoisoned generation 0 with the poison (temp file,
	/// `sync_all`, rename); the caller syncs the directory. Returns whether it
	/// renamed (false when generation 0 is absent or already the poison).
	fn poison_generation_zero(&self) -> std::io::Result<bool> {
		let zero = self.generation_path(0);
		match self.ops.file_len(&zero) {
			// Only a four-byte file can be the poison; read nothing larger.
			Ok(len) if len == POISON.len() as u64 => {
				if self.ops.read_file(&zero)? == POISON {
					return Ok(false);
				}
			}
			Ok(_) => (),
			Err(error) if error.kind() == ErrorKind::NotFound => return Ok(false),
			Err(error) => return Err(error),
		}
		let temp = self.dir.join(POISON_TEMP_NAME);
		self.ops.write_file(&temp, &POISON)?;
		self.ops.sync_all(&temp)?;
		self.ops.rename(&temp, &zero).map(|()| true)
	}
}

/// A record's frame: keyed when its key is known, unkeyed as written otherwise.
fn encode_record(record: &AppliedCounterAdd) -> std::io::Result<Vec<u8>> {
	if record.key.is_some() {
		applied_log::encode_frame(record)
	} else {
		applied_log::encode_unkeyed_frame(record)
	}
}

/// When the leader last attempted a compaction, and whether one is running.
pub(super) struct CompactionSchedule {
	pub(super) running: std::sync::atomic::AtomicBool,
	last: std::sync::Mutex<(Instant, bool)>,
	/// Treat the next check as due (tests).
	#[cfg(test)]
	pub(super) force_due: std::sync::atomic::AtomicBool,
}

impl Default for CompactionSchedule {
	fn default() -> Self {
		Self {
			running: std::sync::atomic::AtomicBool::new(false),
			last: std::sync::Mutex::new((Instant::now(), true)),
			#[cfg(test)]
			force_due: std::sync::atomic::AtomicBool::new(false),
		}
	}
}

impl CompactionSchedule {
	/// Claim the single compaction slot if a trigger holds.
	fn begin_if_due(&self, store: &DurableCounterWitnessStore) -> bool {
		if self.running.load(Ordering::Acquire) {
			return false;
		}
		let (last, succeeded) = *self
			.last
			.lock()
			.unwrap_or_else(|poison| poison.into_inner());
		let age = last.elapsed();
		if !succeeded && age < COMPACTION_RETRY_BACKOFF {
			return false;
		}
		let due = age >= COMPACTION_MAX_AGE || store.compaction_due_by_size();
		#[cfg(test)]
		let due = due || self.force_due.swap(false, Ordering::AcqRel);
		due && self
			.running
			.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
			.is_ok()
	}

	fn finish(&self, succeeded: bool) {
		*self
			.last
			.lock()
			.unwrap_or_else(|poison| poison.into_inner()) = (Instant::now(), succeeded);
		self.running.store(false, Ordering::Release);
	}
}

/// The leader's side of a compaction.
struct LeaderHooks<P>(Arc<CounterLeader<P>>);

impl<P: CounterParticipant + Clone> CompactionHooks for LeaderHooks<P> {
	fn high_water(&self) -> u64 {
		self.0.op_seq.load(Ordering::Acquire).saturating_sub(1)
	}

	fn pending_key(&self, op_id: u64) -> Option<CounterKey> {
		self.0
			.witnesses
			.get(&op_id)
			.map(|witness| witness.key.clone())
	}

	fn evict(&self, deleted: &[CounterWriteKey]) {
		let pending: HashSet<CounterWriteKey> = self
			.0
			.witnesses
			.iter()
			.map(|witness| witness.write_key)
			.collect();
		for write_key in deleted {
			if !pending.contains(write_key) {
				self.0.idempotency.remove(write_key);
			}
		}
	}
}

impl<P: CounterParticipant + Clone + Send + Sync + 'static> CounterLeader<P> {
	/// Compact the applied log now, on the blocking pool.
	pub(crate) async fn compact(self: &Arc<Self>) -> std::io::Result<CompactionReport> {
		let now_ms = self.clock.now_ms();
		let request = CompactionRequest {
			proposed_cutoff: retention::compaction_cutoff(
				now_ms,
				self.max_issued_accepted.load(Ordering::Acquire),
				&self.retention,
			),
			horizon_cutoff: now_ms.saturating_sub(self.retention.attempt_horizon_ms),
		};
		let hooks = LeaderHooks(self.clone());
		self.witness_store
			.run(move |store| store.compact(&request, &hooks))
			.await
	}

	/// Start a compaction in the background when one is due, then run `fence`
	/// if it left the store latched.
	pub(crate) fn compact_when_due<F, Fence>(self: &Arc<Self>, fence: F)
	where
		F: FnOnce() -> Fence + Send + 'static,
		Fence: std::future::Future<Output = ()> + Send,
	{
		if !self.compaction.begin_if_due(&self.witness_store.0) {
			return;
		}
		let leader = self.clone();
		tokio::spawn(async move {
			let result = leader.compact().await;
			leader.compaction.finish(result.is_ok());
			if let Err(error) = result {
				tracing::warn!(
					tenant_id = leader.witness_store.0.tenant.map(|tenant| tenant.get()),
					error = %error,
					"counter applied-log compaction failed"
				);
			}
			if leader.durability_unsettled() {
				fence().await;
			}
		});
	}
}

#[cfg(test)]
pub(super) const CURRENT_TEMP_FILE: &str = CURRENT_TEMP_NAME;
#[cfg(test)]
pub(super) const POISON_TEMP_FILE: &str = POISON_TEMP_NAME;
