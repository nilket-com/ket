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
use super::tiered_io::TieredRowRefreshPlan;
use super::version_identity::same_row;
use super::*;

/// The logical owner of a tiered row.
pub(super) type TierRowOwner = (TenantId, Identity, HybridTimestamp);

pub(super) fn tier_row_owner(row: &PersistentRecordRow) -> TierRowOwner {
	(row.tenant_id, row.key_identity, row.location.hlc)
}

/// Each owner's locations in one generation, in physical order.
fn locations_by_owner(rows: &[PersistentRecordRow]) -> BTreeMap<TierRowOwner, Vec<RecordLocation>> {
	let mut by_owner: BTreeMap<TierRowOwner, Vec<RecordLocation>> = BTreeMap::new();
	for row in rows {
		by_owner
			.entry(tier_row_owner(row))
			.or_default()
			.push(row.location);
	}
	for locations in by_owner.values_mut() {
		locations.sort_by_key(|location| (location.offset, location.record_row_index));
	}
	by_owner
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
	let left_frame = extract_record_from_blob(left_plaintext, left)?;
	let right_frame = extract_record_from_blob(right_plaintext, right)?;
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
fn pair_copies_of_one_version(
	old: &[RecordLocation],
	new: &[RecordLocation],
) -> Vec<(RecordLocation, RecordLocation)> {
	let Some(first_survivor) = new.first().copied() else {
		return Vec::new();
	};
	let paired = old.len().min(new.len());
	let surplus = old.len() - paired;
	old[..surplus]
		.iter()
		.map(|location| (*location, first_survivor))
		.chain(
			old[surplus..]
				.iter()
				.copied()
				.zip(new[new.len() - paired..].iter().copied()),
		)
		.collect()
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
	let mut payload_of_new = Vec::with_capacity(new.len());
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
	let mut used = vec![false; new.len()];
	let mut pairs = Vec::with_capacity(old.len());
	let mut matched_payloads = Vec::with_capacity(old.len());
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
		old_rows: &[PersistentRecordRow],
		new_rows: &[PersistentRecordRow],
	) -> bool {
		let new_by_owner = locations_by_owner(new_rows);
		locations_by_owner(old_rows).iter().any(|(owner, old)| {
			new_by_owner.get(owner).is_some_and(|new| new != old)
				&& old
					.iter()
					.any(|location| self.conflicted_locations.contains(location))
		})
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
		let new_by_owner = locations_by_owner(new_rows);
		let mut pairs = Vec::new();
		for (owner, old) in locations_by_owner(old_rows) {
			let Some(new) = new_by_owner.get(&owner) else {
				continue;
			};
			if &old == new {
				continue;
			}
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
				pairs.extend(pair_conflicting_copies(
					slab_id,
					&old,
					new,
					old_plaintext,
					new_plaintext,
				)?);
			} else {
				pairs.extend(pair_copies_of_one_version(&old, new));
			}
		}
		pairs.retain(|(old, new)| old != new);
		pairs.sort_by_key(|(old, _)| (old.slab_id, old.offset, old.record_row_index));
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
		let new_rows = self.tiered_rows_from_plaintext_with_evidence(
			new_plaintext,
			slab_id,
			new_acknowledged,
		)?;
		let old_rows = self.record_rows_for_slab(slab_id)?;
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
		};
		let current = {
			let state = self.state.lock().await;
			state
				.tier_layouts
				.get(&slab_id)
				.map(|layout| layout.value().clone())
		};
		current.unwrap_or_default().next(slab_id, &plan).map(|_| ())
	}
}
