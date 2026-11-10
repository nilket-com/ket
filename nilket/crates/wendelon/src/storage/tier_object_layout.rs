//! The logical layout of one tier object.
//!
//! A tiered slab's row locations are logical: they name positions in the
//! slab's lineage, not bytes of one object. An object stores its rows densely
//! from its `base`, so a record at logical `offset` lies at physical
//! `offset - base` of the object's plaintext. `TierObjectLayout` is the only
//! conversion between the two. It is built only by checking the base and the
//! extent, so every conversion after that is checked against a valid layout,
//! and a logical end is never used as a file or allocation length.
use crate::schema::SlabId;
use crate::storage::core::RecordLocation;
use crate::storage::io_uring::DIRECT_IO_ALIGN;
use crate::storage::manager::{StaleLocationReason, stale_location_error};
use std::ops::Range;

/// Bases and logical ends are aligned to this, the slab record alignment, so
/// a record's alignment is the same in logical and physical terms.
pub const TIER_LAYOUT_PAGE_BYTES: u64 = DIRECT_IO_ALIGN as u64;

/// A record's logical extent: the offset and length a `RecordLocation` names.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LogicalExtent {
	pub offset: u64,
	pub length: u64,
}

impl LogicalExtent {
	pub const fn new(offset: u64, length: u64) -> Self {
		Self { offset, length }
	}

	pub fn of(location: &RecordLocation) -> Self {
		Self::new(location.offset, u64::from(location.length))
	}
}

/// Where one tier object's plaintext sits in its slab's logical offsets:
/// `[base, base + plaintext_len)`, with `base` page-aligned and the aligned
/// end representable.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TierObjectLayout {
	slab_id: SlabId,
	base: u64,
	plaintext_len: u64,
	/// `base + plaintext_len`, checked at construction.
	data_end: u64,
	/// `data_end` rounded up to a page, checked at construction.
	logical_end: u64,
}

impl TierObjectLayout {
	/// The layout of `plaintext_len` bytes stored from logical `base`. Refuses
	/// typed `MalformedObjectLayout` for a misaligned base, a `base +
	/// plaintext_len` that overflows, an aligned end that overflows (even when
	/// the sum fits), or an extent that does not fit in memory.
	pub fn new(slab_id: SlabId, base: u64, plaintext_len: u64) -> std::io::Result<Self> {
		let malformed =
			|| stale_location_error(slab_id, StaleLocationReason::MalformedObjectLayout);
		if base % TIER_LAYOUT_PAGE_BYTES != 0 {
			return Err(malformed());
		}
		let data_end = base.checked_add(plaintext_len).ok_or_else(malformed)?;
		let logical_end = data_end
			.checked_next_multiple_of(TIER_LAYOUT_PAGE_BYTES)
			.ok_or_else(malformed)?;
		usize::try_from(plaintext_len).map_err(|_| malformed())?;
		Ok(Self {
			slab_id,
			base,
			plaintext_len,
			data_end,
			logical_end,
		})
	}

	/// The layout of bytes stored from logical offset 0: a local slab, or an
	/// object whose base is 0.
	pub fn dense(slab_id: SlabId, bytes: &[u8]) -> std::io::Result<Self> {
		let plaintext_len = u64::try_from(bytes.len()).map_err(|_| {
			stale_location_error(slab_id, StaleLocationReason::MalformedObjectLayout)
		})?;
		Self::new(slab_id, 0, plaintext_len)
	}

	pub fn slab_id(&self) -> SlabId {
		self.slab_id
	}

	pub fn base(&self) -> u64 {
		self.base
	}

	pub fn plaintext_len(&self) -> u64 {
		self.plaintext_len
	}

	/// The plaintext length in memory terms; it fits, by construction.
	pub fn plaintext_len_usize(&self) -> usize {
		// Checked in `new`; this cannot truncate.
		self.plaintext_len as usize
	}

	/// The object's full aligned logical end: where a layout-changing
	/// successor of this object starts.
	pub fn logical_end(&self) -> u64 {
		self.logical_end
	}

	/// The physical range of `location`'s record in this object's plaintext.
	/// A location of another slab, below the base or ending beyond the
	/// object refuses typed `OutsideGeneration` before any subtraction.
	pub fn physical_range(&self, location: &RecordLocation) -> std::io::Result<Range<usize>> {
		if location.slab_id != self.slab_id {
			return Err(self.outside());
		}
		self.physical_extent(LogicalExtent::of(location))
	}

	/// The physical range of a logical extent of this object, refused typed
	/// `OutsideGeneration` unless the whole extent lies in the object.
	pub fn physical_extent(&self, extent: LogicalExtent) -> std::io::Result<Range<usize>> {
		let end = extent
			.offset
			.checked_add(extent.length)
			.ok_or_else(|| self.outside())?;
		if extent.offset < self.base || end > self.data_end {
			return Err(self.outside());
		}
		let start = extent
			.offset
			.checked_sub(self.base)
			.ok_or_else(|| self.outside())?;
		let end = end.checked_sub(self.base).ok_or_else(|| self.outside())?;
		// Both are at most `plaintext_len`, which fits in usize (`new`).
		let start = usize::try_from(start).map_err(|_| self.outside())?;
		let end = usize::try_from(end).map_err(|_| self.outside())?;
		Ok(start..end)
	}

	/// Whether the whole logical extent lies in this object.
	pub fn contains(&self, extent: LogicalExtent) -> bool {
		extent
			.offset
			.checked_add(extent.length)
			.is_some_and(|end| extent.offset >= self.base && end <= self.data_end)
	}

	/// The logical offset of physical position `physical`, refused typed
	/// `OutsideGeneration` outside `[0, plaintext_len)`.
	pub fn logical_of(&self, physical: usize) -> std::io::Result<u64> {
		let physical = u64::try_from(physical).map_err(|_| self.outside())?;
		if physical >= self.plaintext_len {
			return Err(self.outside());
		}
		self.base
			.checked_add(physical)
			.ok_or_else(|| self.outside())
	}

	/// An offset index entry of this object must lie inside its extent;
	/// otherwise the object is refused typed `MalformedObjectLayout`.
	pub fn check_index_entry(&self, extent: LogicalExtent) -> std::io::Result<()> {
		if self.contains(extent) {
			Ok(())
		} else {
			Err(stale_location_error(
				self.slab_id,
				StaleLocationReason::MalformedObjectLayout,
			))
		}
	}

	fn outside(&self) -> std::io::Error {
		stale_location_error(self.slab_id, StaleLocationReason::OutsideGeneration)
	}
}

/// Slab bytes together with the layout that describes exactly them. Every
/// walk and record extraction of slab bytes takes one, so bytes are never
/// read at a logical offset without the conversion.
#[derive(Clone, Copy)]
pub struct SlabView<'a> {
	bytes: &'a [u8],
	layout: TierObjectLayout,
}

impl<'a> SlabView<'a> {
	/// `bytes` under `layout`; refused typed `MalformedObjectLayout` unless
	/// the layout describes exactly these bytes.
	pub fn new(bytes: &'a [u8], layout: TierObjectLayout) -> std::io::Result<Self> {
		if u64::try_from(bytes.len()).ok() != Some(layout.plaintext_len()) {
			return Err(stale_location_error(
				layout.slab_id(),
				StaleLocationReason::MalformedObjectLayout,
			));
		}
		Ok(Self { bytes, layout })
	}

	/// Bytes stored from logical offset 0 (`TierObjectLayout::dense`).
	pub fn dense(slab_id: SlabId, bytes: &'a [u8]) -> std::io::Result<Self> {
		Self::new(bytes, TierObjectLayout::dense(slab_id, bytes)?)
	}

	pub fn bytes(&self) -> &'a [u8] {
		self.bytes
	}

	pub fn layout(&self) -> TierObjectLayout {
		self.layout
	}

	pub fn slab_id(&self) -> SlabId {
		self.layout.slab_id()
	}

	/// The bytes of `location`'s record (`TierObjectLayout::physical_range`).
	pub fn record(&self, location: &RecordLocation) -> std::io::Result<&'a [u8]> {
		let range = self.layout.physical_range(location)?;
		Ok(&self.bytes[range])
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::schema::{HybridTimestamp, RecordRowIndex};
	use crate::storage::manager::stale_location;

	const PAGE: u64 = TIER_LAYOUT_PAGE_BYTES;
	fn slab() -> SlabId {
		SlabId::new(70)
	}

	fn reason(error: std::io::Error) -> Option<StaleLocationReason> {
		stale_location(&error).map(|stale| stale.reason)
	}

	fn location(slab_id: SlabId, offset: u64, length: u32) -> RecordLocation {
		RecordLocation {
			slab_id,
			offset,
			length,
			hlc: HybridTimestamp {
				physical_ms: 1,
				logical: 0,
				node_id: 0,
			},
			record_row_index: RecordRowIndex::ZERO,
			is_tombstone: false,
			placement: Default::default(),
			partition: None,
		}
	}

	#[test]
	fn construction_refuses_a_misaligned_base_and_every_overflow_typed() {
		let malformed = Some(StaleLocationReason::MalformedObjectLayout);
		let aligned_max = u64::MAX - (u64::MAX % PAGE);
		for (what, base, len) in [
			("misaligned base", PAGE + 1, 10),
			("base + len overflows", aligned_max, PAGE),
			// The sum fits; rounding it up to a page does not.
			("aligned end overflows", aligned_max, 1),
		] {
			let error = TierObjectLayout::new(slab(), base, len).unwrap_err();
			assert_eq!(reason(error), malformed, "{what}");
		}
		let layout = TierObjectLayout::new(slab(), 3 * PAGE, PAGE + 10).unwrap();
		assert_eq!(layout.base(), 3 * PAGE);
		assert_eq!(layout.plaintext_len(), PAGE + 10);
		assert_eq!(layout.logical_end(), 5 * PAGE);
		let exact = TierObjectLayout::new(slab(), 3 * PAGE, 2 * PAGE).unwrap();
		assert_eq!(exact.logical_end(), 5 * PAGE);
		let empty = TierObjectLayout::new(slab(), aligned_max, 0).unwrap();
		assert_eq!(empty.logical_end(), aligned_max);
	}

	#[test]
	fn physical_range_converts_inside_and_refuses_outside_typed() {
		let outside = Some(StaleLocationReason::OutsideGeneration);
		let base = 4 * PAGE;
		let layout = TierObjectLayout::new(slab(), base, 2 * PAGE + 100).unwrap();
		assert_eq!(
			layout.physical_range(&location(slab(), base, 100)).unwrap(),
			0..100
		);
		assert_eq!(
			layout
				.physical_range(&location(slab(), base + PAGE, (PAGE + 100) as u32))
				.unwrap(),
			PAGE as usize..(2 * PAGE + 100) as usize
		);
		for (what, at) in [
			("below base", location(slab(), 0, 100)),
			("ends past base from below", location(slab(), base - 1, 2)),
			("beyond end", location(slab(), base + 2 * PAGE + 100, 1)),
			("straddles end", location(slab(), base + 2 * PAGE, 101)),
			("overflowing extent", location(slab(), u64::MAX, 1)),
			("another slab", location(SlabId::new(71), base, 100)),
		] {
			assert_eq!(
				reason(layout.physical_range(&at).unwrap_err()),
				outside,
				"{what}"
			);
		}
	}

	#[test]
	fn logical_of_and_index_entries_stay_inside_the_extent() {
		let base = 2 * PAGE;
		let layout = TierObjectLayout::new(slab(), base, PAGE + 1).unwrap();
		assert_eq!(layout.logical_of(0).unwrap(), base);
		assert_eq!(layout.logical_of(PAGE as usize).unwrap(), base + PAGE);
		assert_eq!(
			reason(layout.logical_of((PAGE + 1) as usize).unwrap_err()),
			Some(StaleLocationReason::OutsideGeneration)
		);
		assert!(
			layout
				.check_index_entry(LogicalExtent::new(base, PAGE + 1))
				.is_ok()
		);
		for (what, entry) in [
			("below base", LogicalExtent::new(base - 1, 1)),
			("beyond end", LogicalExtent::new(base + 1, PAGE + 1)),
			("overflow", LogicalExtent::new(u64::MAX, 2)),
		] {
			assert_eq!(
				reason(layout.check_index_entry(entry).unwrap_err()),
				Some(StaleLocationReason::MalformedObjectLayout),
				"{what}"
			);
		}
	}

	#[test]
	fn a_view_is_only_its_layouts_bytes() {
		let bytes = vec![7_u8; 100];
		let layout = TierObjectLayout::new(slab(), PAGE, 100).unwrap();
		let view = SlabView::new(&bytes, layout).unwrap();
		assert_eq!(
			view.record(&location(slab(), PAGE + 10, 5)).unwrap(),
			&[7; 5]
		);
		assert_eq!(
			reason(view.record(&location(slab(), 10, 5)).unwrap_err()),
			Some(StaleLocationReason::OutsideGeneration)
		);
		let short = TierObjectLayout::new(slab(), PAGE, 99).unwrap();
		assert_eq!(
			reason(SlabView::new(&bytes, short).err().unwrap()),
			Some(StaleLocationReason::MalformedObjectLayout)
		);
		let dense = SlabView::dense(slab(), &bytes).unwrap();
		assert_eq!(dense.layout().base(), 0);
		assert_eq!(dense.layout().logical_end(), PAGE);
	}
}
