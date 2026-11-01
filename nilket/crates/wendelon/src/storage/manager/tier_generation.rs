//! Tier generations and the read snapshot.
//!
//! A tiered slab keeps its slab id while retention, table-drop and rewrap
//! publications change its layout. Each publication installs, in one `state`
//! critical section, the new backing pointer together with the layout history
//! that maps every earlier location of the slab: redirected to the same version
//! in the current generation, or superseded. A read resolves its location and
//! selects its backing in one `state` snapshot, so it never pairs a location of
//! one generation with the backing of another.
use super::tier_plan::{TierRowOwner, tier_row_owner};
use super::tiered_io::TieredRowRefreshPlan;
use super::*;

/// Why a location cannot be served as the version its holder named.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StaleLocationReason {
	/// The version moved or is gone, and no current location serves it.
	Superseded,
	/// A redirect changed the row index a frame-returning caller decodes at.
	RowIndexMoved,
	/// A publication would make an old location name a different version.
	LayoutCollision,
	/// The logical version no longer exists.
	VersionGone,
	/// Several locations claim the version and none can be proven to be it.
	AmbiguousVersion,
	/// Different payloads claim the same version identity.
	ConflictingVersion,
	/// The slab's committed generation is not yet published on this node.
	Fenced,
}

impl StaleLocationReason {
	pub const fn label(self) -> &'static str {
		match self {
			Self::Superseded => "superseded",
			Self::RowIndexMoved => "row_index_moved",
			Self::LayoutCollision => "layout_collision",
			Self::VersionGone => "version_gone",
			Self::AmbiguousVersion => "ambiguous_version",
			Self::ConflictingVersion => "conflicting_version",
			Self::Fenced => "fenced",
		}
	}
}

/// The typed refusal for a location that cannot be served as its version.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StaleLocation {
	pub slab_id: SlabId,
	pub reason: StaleLocationReason,
}

impl std::fmt::Display for StaleLocation {
	fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(
			formatter,
			"stale location in slab {}: {}",
			self.slab_id,
			self.reason.label()
		)
	}
}

impl std::error::Error for StaleLocation {}

pub fn stale_location_error(slab_id: SlabId, reason: StaleLocationReason) -> Error {
	Error::new(ErrorKind::Other, StaleLocation { slab_id, reason })
}

/// The typed refusal carried by `error`, if any.
pub fn stale_location(error: &Error) -> Option<StaleLocation> {
	error
		.get_ref()
		.and_then(|inner| inner.downcast_ref::<StaleLocation>())
		.copied()
}

/// The history of one tiered slab's published generations, complete for the
/// process lifetime: every location ever served for the slab is current,
/// redirected to the same version, or superseded.
#[derive(Clone, Debug, Default)]
pub(super) struct TierLayout {
	pub(super) generation: u64,
	pub(super) redirects: HashMap<RecordLocation, RecordLocation>,
	pub(super) superseded: HashSet<RecordLocation>,
}

impl TierLayout {
	/// The location in this generation that serves `location`'s version.
	pub(super) fn resolve(
		&self,
		slab_id: SlabId,
		location: RecordLocation,
	) -> std::io::Result<RecordLocation> {
		if let Some(target) = self.redirects.get(&location) {
			return Ok(*target);
		}
		if self.superseded.contains(&location) {
			return Err(stale_location_error(
				slab_id,
				StaleLocationReason::Superseded,
			));
		}
		Ok(location)
	}

	/// The next generation's history after `plan` moves the slab's rows.
	///
	/// Rows are matched by their logical owner (`tier_plan`), never by
	/// location alone. A surviving row's old location redirects to its new one
	/// (earlier redirects compose through the new pairs); an old row is kept
	/// where a row of the same owner stands at its location; every other old
	/// row is pruned and its location superseded.
	///
	/// A `RecordLocation` carries no generation and no key, so a holder of an
	/// old location could not tell its version from a different one published
	/// at the same location. A publication is refused when any new row stands
	/// at a superseded location, or at a location whose version before the
	/// publication had another owner, or when a pair or a redirect would join
	/// two owners.
	pub(super) fn next(
		&self,
		slab_id: SlabId,
		plan: &TieredRowRefreshPlan,
	) -> std::io::Result<TierLayout> {
		let collision = || stale_location_error(slab_id, StaleLocationReason::LayoutCollision);
		let old_owner = plan
			.old_rows
			.iter()
			.map(|row| (row.location, tier_row_owner(row)))
			.collect::<HashMap<_, _>>();
		let new_owner = plan
			.new_rows
			.iter()
			.map(|row| (row.location, tier_row_owner(row)))
			.collect::<HashMap<_, _>>();
		let mut pairs = HashMap::with_capacity(plan.relocated_reads.len());
		for (old, new) in &plan.relocated_reads {
			if old == new {
				continue;
			}
			match (old_owner.get(old), new_owner.get(new)) {
				(Some(from), Some(to)) if from == to => {
					pairs.insert(*old, *new);
				}
				_ => return Err(collision()),
			}
		}
		let kept = |location: &RecordLocation, owner: &TierRowOwner| {
			new_owner.get(location) == Some(owner)
		};
		let pruned = old_owner
			.iter()
			.filter(|(location, owner)| !pairs.contains_key(*location) && !kept(*location, *owner))
			.map(|(location, _)| *location)
			.collect::<HashSet<_>>();
		let mut redirects = HashMap::with_capacity(self.redirects.len() + pairs.len());
		let mut superseded = self.superseded.clone();
		for (from, to) in &self.redirects {
			if let Some(next) = pairs.get(to) {
				redirects.insert(*from, *next);
			} else if old_owner.get(to).is_some_and(|owner| kept(to, owner)) {
				redirects.insert(*from, *to);
			} else {
				// The version it served is pruned, or no longer indexed.
				superseded.insert(*from);
			}
		}
		redirects.extend(pairs.iter().map(|(old, new)| (*old, *new)));
		superseded.extend(pruned);
		for (location, owner) in &new_owner {
			if superseded.contains(location) {
				return Err(collision());
			}
			// The version a holder of this location named before the publication.
			let before = match self.redirects.get(location) {
				Some(target) => old_owner.get(target),
				None => old_owner.get(location),
			};
			if before.is_some_and(|before| before != owner) {
				return Err(collision());
			}
			// The version a holder of this location reads after it.
			if let Some(target) = redirects.get(location)
				&& new_owner.get(target) != Some(owner)
			{
				return Err(collision());
			}
		}
		Ok(TierLayout {
			generation: self.generation.saturating_add(1),
			redirects,
			superseded,
		})
	}
}

/// A read's resolved target: the backing and location of one generation,
/// selected in one snapshot, with the slab pinned for the read.
pub(super) struct ReadTarget {
	pub(super) backing: SlabBacking,
	pub(super) location: RecordLocation,
	/// The slab is tiered: its locations move between generations, so a
	/// frame read for one is verified to hold its version.
	pub(super) tiered: bool,
	pub(super) _pin: SlabPinGuard,
}

/// Map `location` through its slab's current generation and select that
/// generation's backing, under one `state` guard.
fn select_in_snapshot(
	state: &ManagerState,
	location: RecordLocation,
) -> std::io::Result<(SlabBacking, RecordLocation)> {
	if state.active.slab_id() == location.slab_id {
		return Ok((SlabBacking::Local(state.active.clone()), location));
	}
	if let Some(slab) = state.sealed.get(&location.slab_id) {
		return Ok((SlabBacking::Local(slab.clone()), location));
	}
	if let Some(pointer) = state.tiered.get(&location.slab_id) {
		let target = match state.tier_layouts.get(&location.slab_id) {
			Some(layout) => layout.resolve(location.slab_id, location)?,
			None => location,
		};
		let backing = match state.rehydrated.get(&location.slab_id) {
			Some(copy) => SlabBacking::Local(copy.slab.clone()),
			None => SlabBacking::L2(pointer.clone()),
		};
		return Ok((backing, target));
	}
	if let Some(copy) = state.rehydrated.get(&location.slab_id) {
		return Ok((SlabBacking::Local(copy.slab.clone()), location));
	}
	Ok((SlabBacking::Missing, location))
}

impl SlabManager {
	/// Recovery bypasses public readiness and conflict admission, retaining a
	/// backing snapshot and pin while it validates every copy itself.
	pub(super) async fn recovery_read_target(
		&self,
		requested: RecordLocation,
	) -> std::io::Result<(SlabBacking, RecordLocation, SlabPinGuard)> {
		let location = self.relocate_read_location(requested);
		let pin = self.pin_lsm_read_location(location)?;
		let state = self.state.lock().await;
		let (backing, target) = select_in_snapshot(&state, location)?;
		Ok((backing, target, pin))
	}
	/// Resolve `requested` to the location and backing that serve its version.
	///
	/// Cross-slab redirects (maintenance) are followed first; their targets are
	/// immutable local slabs. Then one `state` snapshot maps the location
	/// through its tiered slab's generation history and selects that
	/// generation's backing. A frame-returning caller decodes the row at its
	/// own index, so a target whose row index differs is refused.
	pub(super) async fn resolve_read_target(
		&self,
		requested: RecordLocation,
		caller_decodes_row: bool,
	) -> std::io::Result<ReadTarget> {
		let location = self.relocate_read_location(requested);
		self.wait_tier_read_scope(&[requested.slab_id, location.slab_id])
			.await?;
		let pin = self.pin_lsm_read_location(location)?;
		#[cfg(test)]
		self.read_backing_selection_pause_point().await;
		let (backing, target, tiered) = {
			let state = self.state.lock().await;
			let (backing, target) = select_in_snapshot(&state, location)?;
			(
				backing,
				target,
				state.tiered.contains_key(&location.slab_id),
			)
		};
		self.require_tier_read_scope(&[target.slab_id])?;
		if self.conflicted_locations.contains(&target) {
			return Err(stale_location_error(
				target.slab_id,
				StaleLocationReason::ConflictingVersion,
			));
		}
		if caller_decodes_row && target.record_row_index != requested.record_row_index {
			return Err(stale_location_error(
				location.slab_id,
				StaleLocationReason::RowIndexMoved,
			));
		}
		Ok(ReadTarget {
			backing,
			location: target,
			tiered,
			_pin: pin,
		})
	}

	/// Resolve several locations of one physical record in one snapshot, for
	/// row-aware readers that use each target's own row index.
	pub(super) async fn resolve_read_targets(
		&self,
		requested: &[RecordLocation],
	) -> std::io::Result<(SlabBacking, Vec<RecordLocation>, Vec<SlabPinGuard>)> {
		let relocated = requested
			.iter()
			.map(|location| self.relocate_read_location(*location))
			.collect::<Vec<_>>();
		let scope = requested
			.iter()
			.chain(&relocated)
			.map(|location| location.slab_id)
			.collect::<Vec<_>>();
		self.wait_tier_read_scope(&scope).await?;

		let mut pins = Vec::with_capacity(relocated.len());
		for location in &relocated {
			pins.push(self.pin_lsm_read_location(*location)?);
		}
		let state = self.state.lock().await;
		let mut backing = None;
		let mut targets = Vec::with_capacity(relocated.len());
		for location in relocated {
			let (selected, target) = select_in_snapshot(&state, location)?;
			self.require_tier_read_scope(&[target.slab_id])?;
			if self.conflicted_locations.contains(&target) {
				return Err(stale_location_error(
					target.slab_id,
					StaleLocationReason::ConflictingVersion,
				));
			}
			backing.get_or_insert(selected);
			targets.push(target);
		}
		drop(state);
		Ok((backing.unwrap_or(SlabBacking::Missing), targets, pins))
	}

	/// The per-slab tier publication lock. Only tier publication and tier
	/// retirement take it, and never across a Raft request.
	pub(super) fn tier_publication_lock(&self, slab_id: SlabId) -> Arc<tokio::sync::Mutex<()>> {
		self.tier_publication_locks
			.entry(slab_id)
			.or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
			.clone()
	}
}
