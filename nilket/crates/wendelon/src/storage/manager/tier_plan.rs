//! Tier publication plans pair rows by logical owner, never by position.
//!
//! A tier object holds only ordinary rows: blob recovery refuses every counter
//! class (`recover_slab_entries_from_blob`), so a tiered row's full class
//! identity is `(tenant, key, hlc)`. A `RecordLocation` carries no key: two
//! keys written in one batch share an HLC, and a rewrite can move one of them
//! into the other's old physical slot. Locations of two generations therefore
//! name one version only when their rows share that owner.
//!
//! Copies of one owner pair newest-first, and a surplus copy maps to a
//! surviving one, because byte-identical copies are one version. Copies of a
//! conflicting owner pair by payload, so each copy keeps its own bytes.
use super::tier_budget::{TierHistoryShortfall, tier_history_refusal};
use super::tiered_io::TieredRowRefreshPlan;
use super::version_identity::same_row;
use super::*;

/// The logical owner of a tiered row.
pub(super) type TierRowOwner = (TenantId, Identity, HybridTimestamp);

pub(super) fn tier_row_owner(row: &PersistentRecordRow) -> TierRowOwner {
	(row.tenant_id, row.key_identity, row.location.hlc)
}

/// One generation's rows as `(owner, location)`, sorted by owner and then in
/// physical order, so each owner's locations form one run. Built into a
/// vector sized for `rows` (the caller reserved it, ADR 0692 §7) and sorted
/// without a temporary buffer; equal elements are identical, so the unstable
/// sort is exact.
/// Pairing scratch of exactly `capacity`, or a typed scratch refusal: the
/// plan's charge paid for it (`tier_pairing_scratch_bytes`), and no pairing
/// buffer grows past the capacity it is created with.
fn scratch_vec<T>(slab_id: SlabId, capacity: usize) -> std::io::Result<Vec<T>> {
	let mut vec = Vec::new();
	vec.try_reserve_exact(capacity)
		.map_err(|_| tier_history_refusal(slab_id, TierHistoryShortfall::Scratch))?;
	Ok(vec)
}

fn locations_by_owner(
	slab_id: SlabId,
	rows: &[PersistentRecordRow],
) -> std::io::Result<Vec<(TierRowOwner, RecordLocation)>> {
	let mut by_owner = scratch_vec(slab_id, rows.len())?;
	by_owner.extend(rows.iter().map(|row| (tier_row_owner(row), row.location)));
	by_owner.sort_unstable_by_key(|(owner, location)| {
		(*owner, location.offset, location.record_row_index)
	});
	Ok(by_owner)
}

/// The runs of `by_owner`, one per owner, in owner order.
fn owner_runs(
	by_owner: &[(TierRowOwner, RecordLocation)],
) -> impl Iterator<Item = (TierRowOwner, &[(TierRowOwner, RecordLocation)])> {
	by_owner
		.chunk_by(|left, right| left.0 == right.0)
		.map(|run| (run[0].0, run))
}

/// The run of `owner` in `by_owner`, if any.
fn owner_run(
	by_owner: &[(TierRowOwner, RecordLocation)],
	owner: TierRowOwner,
) -> Option<&[(TierRowOwner, RecordLocation)]> {
	let start = by_owner.partition_point(|(other, _)| *other < owner);
	let len = by_owner[start..].partition_point(|(other, _)| *other == owner);
	(len > 0).then(|| &by_owner[start..start + len])
}

/// The locations of one owner's run.
fn run_locations(
	run: &[(TierRowOwner, RecordLocation)],
) -> impl Iterator<Item = RecordLocation> + '_ {
	run.iter().map(|(_, location)| *location)
}

fn same_run_locations(
	left: &[(TierRowOwner, RecordLocation)],
	right: &[(TierRowOwner, RecordLocation)],
) -> bool {
	left.len() == right.len() && run_locations(left).eq(run_locations(right))
}

/// The bytes `pair_tier_rows` and `tier_pairing_needs_old_payloads` allocate
/// for `old` and `new` rows at most, besides per-frame payload decodes.
pub(super) fn tier_pairing_scratch_bytes(old: usize, new: usize) -> Option<u64> {
	use super::tier_budget::{checked_sum, vec_bytes};
	checked_sum(&[
		vec_bytes::<(TierRowOwner, RecordLocation)>(old),
		vec_bytes::<(TierRowOwner, RecordLocation)>(new),
		// One owner's run copied out per side.
		vec_bytes::<RecordLocation>(old),
		vec_bytes::<RecordLocation>(new),
		// pair_conflicting_copies: its pairs (the outer buffer becomes the
		// plan's, charged with it), payload ids and use marks per new copy,
		// matched payloads per old copy.
		vec_bytes::<(RecordLocation, RecordLocation)>(old),
		vec_bytes::<usize>(new),
		vec_bytes::<bool>(new),
		vec_bytes::<usize>(old),
	])
}

/// Whether the row at `left` in `left_plaintext` and the row at `right` in
/// `right_plaintext` carry the same payload: equal bytes for single-row
/// frames, equal values in every column, compared both ways, for rows of
/// multi-row frames. A frame whose rows cannot be decoded is an error.
fn same_tier_row_payload(
	left_plaintext: &[u8],
	left: RecordLocation,
	right_plaintext: &[u8],
	right: RecordLocation,
) -> std::io::Result<bool> {
	let left_frame = extract_record_from_blob(
		left_plaintext,
		crate::storage::core::FrameLocation::of_point_read(left),
	)?;
	let right_frame = extract_record_from_blob(
		right_plaintext,
		crate::storage::core::FrameLocation::of_point_read(right),
	)?;
	match (
		storage_multirow_index_entries_from_frame(&left_frame)?,
		storage_multirow_index_entries_from_frame(&right_frame)?,
	) {
		(None, None) => Ok(left_frame == right_frame),
		(Some(_), Some(_)) => {
			let left_batch = decode_record_batch(&left_frame)?;
			let right_batch = decode_record_batch(&right_frame)?;
			let (left_row, right_row) = (
				left.record_row_index.get() as usize,
				right.record_row_index.get() as usize,
			);
			Ok(same_row(&left_batch, left_row, &right_batch, right_row)
				&& same_row(&right_batch, right_row, &left_batch, left_row))
		}
		_ => Ok(false),
	}
}

/// Copies of one version: the newest copies survive in physical order, and a
/// surplus older copy maps to the first surviving copy, which serves the same
/// version.
fn pair_copies_of_one_version<'a>(
	old: &'a [RecordLocation],
	new: &'a [RecordLocation],
) -> impl Iterator<Item = (RecordLocation, RecordLocation)> + 'a {
	// No surviving copy pairs nothing.
	let paired = if new.is_empty() {
		0
	} else {
		old.len().min(new.len())
	};
	let surplus = if new.is_empty() {
		0
	} else {
		old.len() - paired
	};
	let first_survivor = new.first().copied();
	old[..surplus]
		.iter()
		.filter_map(move |location| first_survivor.map(|first| (*location, first)))
		.chain(
			old[surplus..]
				.iter()
				.copied()
				.zip(new[new.len() - paired..].iter().copied()),
		)
}

/// Copies of a conflicting version, paired by payload: each old copy maps to
/// a new copy with its own bytes, in physical order, and a surplus
/// byte-identical copy maps to its payload's first surviving copy. A payload
/// the rewrite dropped or introduced is refused, because no rewrite prunes or
/// merges a conflicting version.
fn pair_conflicting_copies(
	slab_id: SlabId,
	old: &[RecordLocation],
	new: &[RecordLocation],
	old_plaintext: &[u8],
	new_plaintext: &[u8],
) -> std::io::Result<Vec<(RecordLocation, RecordLocation)>> {
	let refuse = || stale_location_error(slab_id, StaleLocationReason::ConflictingVersion);
	let mut payload_of_new = scratch_vec(slab_id, new.len())?;
	for (index, location) in new.iter().enumerate() {
		let mut payload = index;
		for (earlier, other) in new[..index].iter().enumerate() {
			if same_tier_row_payload(new_plaintext, *location, new_plaintext, *other)? {
				payload = payload_of_new[earlier];
				break;
			}
		}
		payload_of_new.push(payload);
	}
	let mut used = scratch_vec(slab_id, new.len())?;
	used.resize(new.len(), false);
	let mut pairs = scratch_vec(slab_id, old.len())?;
	let mut matched_payloads = scratch_vec(slab_id, old.len())?;
	for location in old {
		let mut payload = None;
		for (index, candidate) in new.iter().enumerate() {
			if same_tier_row_payload(old_plaintext, *location, new_plaintext, *candidate)? {
				payload = Some(payload_of_new[index]);
				break;
			}
		}
		let payload = payload.ok_or_else(refuse)?;
		matched_payloads.push(payload);
		let target = (0..new.len())
			.find(|index| payload_of_new[*index] == payload && !used[*index])
			.or_else(|| (0..new.len()).find(|index| payload_of_new[*index] == payload))
			.expect("a matched payload has a new copy");
		used[target] = true;
		pairs.push((*location, new[target]));
	}
	if payload_of_new
		.iter()
		.any(|payload| !matched_payloads.contains(payload))
	{
		return Err(refuse());
	}
	Ok(pairs)
}

impl SlabManager {
	/// Whether pairing old and new rows needs the old generation's payloads:
	/// only a conflicting owner whose copies move is paired by payload.
	pub(super) fn tier_pairing_needs_old_payloads(
		&self,
		slab_id: SlabId,
		old_rows: &[PersistentRecordRow],
		new_rows: &[PersistentRecordRow],
	) -> std::io::Result<bool> {
		let old_by_owner = locations_by_owner(slab_id, old_rows)?;
		let new_by_owner = locations_by_owner(slab_id, new_rows)?;
		Ok(owner_runs(&old_by_owner).any(|(owner, old)| {
			owner_run(&new_by_owner, owner).is_some_and(|new| !same_run_locations(new, old))
				&& run_locations(old).any(|location| self.conflicted_locations.contains(&location))
		}))
	}

	/// Pair each old row that survives with the new row that serves its
	/// version. Unchanged locations pair with themselves and are omitted.
	pub(super) fn pair_tier_rows(
		&self,
		slab_id: SlabId,
		old_rows: &[PersistentRecordRow],
		new_rows: &[PersistentRecordRow],
		old_plaintext: Option<&[u8]>,
		new_plaintext: &[u8],
	) -> std::io::Result<Vec<(RecordLocation, RecordLocation)>> {
		let old_by_owner = locations_by_owner(slab_id, old_rows)?;
		let new_by_owner = locations_by_owner(slab_id, new_rows)?;
		let mut pairs = scratch_vec(slab_id, old_rows.len())?;
		let mut push = |pair| {
			if pairs.len() == old_rows.len() {
				// Every old row pairs at most once; more is a broken plan.
				return Err(tier_history_refusal(
					slab_id,
					TierHistoryShortfall::StalePlan,
				));
			}
			pairs.push(pair);
			Ok(())
		};
		for (owner, old_run) in owner_runs(&old_by_owner) {
			let Some(new_run) = owner_run(&new_by_owner, owner) else {
				continue;
			};
			if same_run_locations(old_run, new_run) {
				continue;
			}
			let mut old = scratch_vec(slab_id, old_run.len())?;
			old.extend(run_locations(old_run));
			let mut new = scratch_vec(slab_id, new_run.len())?;
			new.extend(run_locations(new_run));
			if old
				.iter()
				.any(|location| self.conflicted_locations.contains(location))
			{
				let Some(old_plaintext) = old_plaintext else {
					return Err(stale_location_error(
						slab_id,
						StaleLocationReason::AmbiguousVersion,
					));
				};
				for pair in
					pair_conflicting_copies(slab_id, &old, &new, old_plaintext, new_plaintext)?
				{
					push(pair)?;
				}
			} else {
				for pair in pair_copies_of_one_version(&old, &new) {
					push(pair)?;
				}
			}
		}
		pairs.retain(|(old, new)| old != new);
		// Old locations are distinct, so the unstable sort is exact and needs no buffer.
		pairs.sort_unstable_by_key(|(old, _)| (old.slab_id, old.offset, old.record_row_index));
		Ok(pairs)
	}

	/// Build, in preparation, the plan a publication of `new_plaintext` would
	/// apply over the slab's published rows, and check it against the slab's
	/// history. Every refusal happens here, before an upload or a commit.
	/// `new_acknowledged` names the record offsets in `new_plaintext`, a
	/// replacement whose layout the index does not yet describe.
	pub(super) async fn preflight_tier_publication(
		&self,
		slab_id: SlabId,
		old_plaintext: &[u8],
		new_plaintext: &[u8],
		new_acknowledged: &BTreeSet<u64>,
	) -> std::io::Result<()> {
		// Count every refusal once, whichever step refused it (plan scratch,
		// build scratch, history, a collision).
		self.preflight_tier_publication_inner(
			slab_id,
			old_plaintext,
			new_plaintext,
			new_acknowledged,
		)
		.await
		.inspect_err(super::tier_tokens::observe_tier_rewrite_refusal)
	}

	async fn preflight_tier_publication_inner(
		&self,
		slab_id: SlabId,
		old_plaintext: &[u8],
		new_plaintext: &[u8],
		new_acknowledged: &BTreeSet<u64>,
	) -> std::io::Result<()> {
		let scratch = self.reserve_tier_plan_scratch(slab_id, new_plaintext, new_acknowledged)?;
		let new_rows =
			self.tiered_rows_within(new_plaintext, slab_id, new_acknowledged, &scratch)?;
		let old_rows = self.record_rows_for_slab_within_checked(slab_id, &scratch)?;
		let relocated_reads = self.pair_tier_rows(
			slab_id,
			&old_rows,
			&new_rows,
			Some(old_plaintext),
			new_plaintext,
		)?;
		let plan = TieredRowRefreshPlan {
			pointer: None,
			old_rows,
			new_rows,
			relocated_reads,
			scratch: scratch.charge,
		};
		let current = {
			let state = self.state.lock().await;
			state
				.tier_layouts
				.get(&slab_id)
				.map(|layout| layout.value().clone())
		};
		current
			.unwrap_or_default()
			.next(slab_id, &plan, &self.tier_budget)
			.map(|_| ())
	}
}
