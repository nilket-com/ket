//! What one tier publication does to its slab's row locations.
//!
//! A slab's tiered row locations are logical and never repeat within its
//! lineage: a successor object is the same object, a proven offset-preserving
//! transform of its source at the source's base (a DEK rewrap or a plaintext
//! to encrypted conversion), or a layout-changing rewrite based at or above
//! its source's full aligned logical end. Anything else would let a holder of
//! a source location read a different row, and is refused typed
//! `LayoutRegression` before anything is proposed or published. The leader
//! classifies before proposing; every node classifies again when it publishes
//! over a locally published source.
use super::{StaleLocationReason, stale_location_error};
use crate::storage::tier_object_layout::{SlabView, TierObjectLayout};
use std::collections::BTreeSet;

/// How a successor object relates to its source.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TierTransition {
	/// The same object bytes (equal content hash): metadata-only or
	/// idempotent; locations and layout are unchanged.
	SameObject,
	/// A different object whose complete decoded plaintext equals its
	/// source's at the same base: every location names the same bytes.
	OffsetPreserving,
	/// A rewrite whose base is at or above its source's logical end: none of
	/// its locations is one its source, or any ancestor, ever issued.
	LayoutChanging,
}

/// One object of a transition: its validated plaintext under its layout, and
/// the content hash of its committed bytes when it has been uploaded.
#[derive(Clone, Copy)]
pub(crate) struct TierTransitionObject<'a> {
	pub(crate) plaintext: SlabView<'a>,
	pub(crate) payload_hash: Option<&'a [u8; 32]>,
}

/// The transition from `source` to `successor`, both validated (decoded
/// under their layouts) and bound by the caller to the exact source
/// generation it will publish over. Refused typed `LayoutRegression`.
pub(crate) fn classify_tier_transition(
	source: TierTransitionObject<'_>,
	successor: TierTransitionObject<'_>,
) -> std::io::Result<TierTransition> {
	let slab_id = successor.plaintext.slab_id();
	let regression = || stale_location_error(slab_id, StaleLocationReason::LayoutRegression);
	if source.plaintext.slab_id() != slab_id {
		return Err(regression());
	}
	let (source_layout, successor_layout) =
		(source.plaintext.layout(), successor.plaintext.layout());
	if let (Some(source_hash), Some(successor_hash)) = (source.payload_hash, successor.payload_hash)
		&& source_hash == successor_hash
	{
		// Equal hashes are equal committed bytes, so equal layouts too.
		return if source_layout == successor_layout {
			Ok(TierTransition::SameObject)
		} else {
			Err(regression())
		};
	}
	if successor_layout.base() == source_layout.base() {
		// Equal offsets prove nothing about owners or payloads: only the
		// complete plaintext does.
		return if successor.plaintext.bytes() == source.plaintext.bytes() {
			Ok(TierTransition::OffsetPreserving)
		} else {
			Err(regression())
		};
	}
	if successor_layout.base() >= source_layout.logical_end() {
		return Ok(TierTransition::LayoutChanging);
	}
	Err(regression())
}

/// A layout-changing rewrite's replacement: `bytes`, written densely from 0
/// by a local store, laid out from exactly its source's logical end (the
/// builder's equality in case 3), and the store's physical append offsets
/// converted to logical ones by that layout.
pub(crate) fn layout_changing_replacement<'a>(
	source: TierObjectLayout,
	bytes: &'a [u8],
	appended: &BTreeSet<u64>,
) -> std::io::Result<(SlabView<'a>, BTreeSet<u64>)> {
	let slab_id = source.slab_id();
	let malformed = || stale_location_error(slab_id, StaleLocationReason::MalformedObjectLayout);
	let len = u64::try_from(bytes.len()).map_err(|_| malformed())?;
	let layout = TierObjectLayout::new(slab_id, source.logical_end(), len)?;
	let offsets = appended
		.iter()
		.map(|physical| {
			usize::try_from(*physical)
				.map_err(|_| malformed())
				.and_then(|physical| layout.logical_of(physical))
		})
		.collect::<std::io::Result<BTreeSet<u64>>>()?;
	Ok((SlabView::new(bytes, layout)?, offsets))
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::schema::SlabId;
	use crate::storage::manager::stale_location;
	use crate::storage::tier_object_layout::TIER_LAYOUT_PAGE_BYTES;

	const PAGE: u64 = TIER_LAYOUT_PAGE_BYTES;

	fn view(bytes: &[u8], base: u64) -> SlabView<'_> {
		SlabView::new(
			bytes,
			TierObjectLayout::new(SlabId::new(5), base, bytes.len() as u64).unwrap(),
		)
		.unwrap()
	}

	fn object<'a>(plaintext: SlabView<'a>, hash: Option<&'a [u8; 32]>) -> TierTransitionObject<'a> {
		TierTransitionObject {
			plaintext,
			payload_hash: hash,
		}
	}

	fn refused(result: std::io::Result<TierTransition>) -> bool {
		result
			.err()
			.and_then(|error| stale_location(&error))
			.is_some_and(|stale| stale.reason == StaleLocationReason::LayoutRegression)
	}

	#[test]
	fn the_four_cases() {
		let bytes = vec![1_u8; 5000];
		let other = {
			let mut other = bytes.clone();
			other[4999] = 2;
			other
		};
		let (a, b) = ([1_u8; 32], [2_u8; 32]);
		let source = object(view(&bytes, PAGE), Some(&a));
		// 1. The same object bytes.
		assert_eq!(
			classify_tier_transition(source, object(view(&bytes, PAGE), Some(&a))).unwrap(),
			TierTransition::SameObject
		);
		// 2. Same base: only a byte-identical plaintext is offset-preserving.
		assert_eq!(
			classify_tier_transition(source, object(view(&bytes, PAGE), Some(&b))).unwrap(),
			TierTransition::OffsetPreserving
		);
		assert!(refused(classify_tier_transition(
			source,
			object(view(&other, PAGE), Some(&b))
		)));
		// 3. At or above the source's aligned logical end (PAGE + 5000 -> 3 pages).
		let end = view(&bytes, PAGE).layout().logical_end();
		assert_eq!(end, 3 * PAGE);
		for base in [end, end + PAGE] {
			assert_eq!(
				classify_tier_transition(source, object(view(&other, base), None)).unwrap(),
				TierTransition::LayoutChanging
			);
		}
		// 4. Anything else: overlapping the source, or below it.
		for base in [2 * PAGE, 0] {
			assert!(refused(classify_tier_transition(
				source,
				object(view(&other, base), None)
			)));
		}
	}
}
