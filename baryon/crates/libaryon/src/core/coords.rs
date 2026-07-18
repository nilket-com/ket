use std::{fmt, num::NonZeroU16};

macro_rules! impl_unit_base {
	($name:ident, $repr:ty) => {
		#[repr(transparent)]
		#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
		pub struct $name($repr);

		impl $name {
			pub const ZERO: Self = Self(0);

			pub const fn new(value: $repr) -> Self {
				Self(value)
			}

			pub const fn get(self) -> $repr {
				self.0
			}
		}

		impl fmt::Display for $name {
			fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
				self.0.fmt(f)
			}
		}

		impl From<$repr> for $name {
			fn from(value: $repr) -> Self {
				Self(value)
			}
		}

		impl From<$name> for $repr {
			fn from(value: $name) -> Self {
				value.0
			}
		}
	};
}

macro_rules! impl_checked_arithmetic {
	($name:ident, $repr:ty) => {
		impl $name {
			pub const fn saturating_add(self, rhs: $repr) -> Self {
				Self(self.0.saturating_add(rhs))
			}

			pub const fn saturating_sub(self, rhs: $repr) -> Self {
				Self(self.0.saturating_sub(rhs))
			}

			pub const fn checked_add(self, rhs: $repr) -> Option<Self> {
				match self.0.checked_add(rhs) {
					Some(value) => Some(Self(value)),
					None => None,
				}
			}

			pub const fn checked_sub(self, rhs: $repr) -> Option<Self> {
				match self.0.checked_sub(rhs) {
					Some(value) => Some(Self(value)),
					None => None,
				}
			}
		}
	};
}

macro_rules! impl_scalar_ops {
	($name:ident, $repr:ty) => {
		impl std::ops::Add<$repr> for $name {
			type Output = Self;

			#[track_caller]
			fn add(self, rhs: $repr) -> Self::Output {
				self.checked_add(rhs)
					.expect(concat!(stringify!($name), " addition overflow"))
			}
		}

		impl std::ops::AddAssign<$repr> for $name {
			#[track_caller]
			fn add_assign(&mut self, rhs: $repr) {
				*self = self
					.checked_add(rhs)
					.expect(concat!(stringify!($name), " addition overflow"));
			}
		}

		impl std::ops::Sub<$repr> for $name {
			type Output = Self;

			#[track_caller]
			fn sub(self, rhs: $repr) -> Self::Output {
				self.checked_sub(rhs)
					.expect(concat!(stringify!($name), " subtraction underflow"))
			}
		}

		impl std::ops::SubAssign<$repr> for $name {
			#[track_caller]
			fn sub_assign(&mut self, rhs: $repr) {
				*self = self
					.checked_sub(rhs)
					.expect(concat!(stringify!($name), " subtraction underflow"));
			}
		}
	};
}

// Positions keep explicit checked/saturating helpers; extents keep scalar
// operators until each position family has a matching count type.
macro_rules! impl_position_unit {
	($name:ident, $repr:ty) => {
		impl_unit_base!($name, $repr);
		impl_checked_arithmetic!($name, $repr);
	};
}

macro_rules! impl_extent_unit {
	($name:ident, $repr:ty) => {
		impl_unit_base!($name, $repr);
		impl_checked_arithmetic!($name, $repr);
		impl_scalar_ops!($name, $repr);
	};
}

macro_rules! impl_id_unit {
	($name:ident, $repr:ty) => {
		impl_unit_base!($name, $repr);

		impl $name {
			#[track_caller]
			pub fn next(self) -> Self {
				Self(
					self.0
						.checked_add(1)
						.expect(concat!(stringify!($name), " overflow")),
				)
			}
		}
	};
}

impl_position_unit!(DocLine, u64);
impl_extent_unit!(DocLineCount, u64);
impl_position_unit!(DocVisualRow, u64);
impl_extent_unit!(VisualRowCount, u64);
impl_position_unit!(DocDisplayCol, u32);
impl_extent_unit!(DocDisplayColCount, u32);
impl_position_unit!(DisplayRow, u32);
impl_extent_unit!(DisplayRowCount, u32);
impl_position_unit!(NodeByteOffset, u32);
impl_extent_unit!(NodeByteLen, u32);
impl_id_unit!(PaneId, u32);
impl_id_unit!(BufferId, u64);
impl_id_unit!(ViewId, u64);
impl_id_unit!(ViewportId, u64);
impl_extent_unit!(PaneCol, u32);
impl_extent_unit!(PaneRow, u32);
impl_position_unit!(PaneLocalCol, u32);
impl_position_unit!(PaneLocalRow, u32);
impl_position_unit!(ScreenCol, u32);
impl_position_unit!(ScreenRow, u32);
impl_position_unit!(CellCol, u16);
impl_extent_unit!(CellWidth, u16);
impl_position_unit!(CellRow, u16);
impl_extent_unit!(CellHeight, u16);
impl_position_unit!(PixelX, u32);
impl_extent_unit!(PixelWidth, u32);
impl_position_unit!(PixelY, u32);
impl_extent_unit!(PixelHeight, u32);
impl_unit_base!(PixelDelta, i32);
impl_position_unit!(DocByte, u64);
impl_extent_unit!(ByteLen, u64);
impl_unit_base!(ByteDelta, i64);
impl_unit_base!(MinimapFingerprint, u64);
impl_unit_base!(DiffFingerprint, u64);
impl_unit_base!(PreviewFingerprint, u64);
impl_unit_base!(TokenBin, u8);
impl_unit_base!(Coverage, u8);
impl_id_unit!(StateId, u64);
impl_id_unit!(TopologyRev, u64);
impl_id_unit!(RequestId, u64);
impl_id_unit!(RegistryOwnerId, u64);
impl_id_unit!(RegistryRev, u64);
impl_id_unit!(SourceId, u64);
impl_id_unit!(SubscriptionId, u64);
impl_id_unit!(JobId, u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BlendRatio {
	accent: u16,
	total: NonZeroU16,
}

impl BlendRatio {
	#[track_caller]
	pub fn new(accent: u16, total: u16) -> Self {
		assert!(total > 0, "BlendRatio total must be nonzero");
		assert!(
			accent <= total,
			"BlendRatio accent weight must not exceed total weight"
		);
		Self {
			accent,
			total: NonZeroU16::new(total).expect("BlendRatio total checked nonzero"),
		}
	}

	pub const fn accent(self) -> u16 {
		self.accent
	}

	pub const fn base(self) -> u16 {
		self.total.get() - self.accent
	}

	pub const fn total(self) -> u16 {
		self.total.get()
	}
}

impl TokenBin {
	pub const EMPTY: Self = Self(u8::MAX);

	pub const fn index(self) -> usize {
		self.0 as usize
	}

	pub fn as_bytes(bins: &[Self]) -> &[u8] {
		// SAFETY: TokenBin is repr(transparent) over u8, so a TokenBin slice
		// has the same layout and length in bytes as the underlying bin values.
		unsafe { std::slice::from_raw_parts(bins.as_ptr().cast::<u8>(), bins.len()) }
	}
}

impl DocByte {
	pub const fn saturating_offset(self, delta: ByteDelta) -> Self {
		let delta = delta.get();
		if delta >= 0 {
			self.saturating_add(delta as u64)
		} else {
			self.saturating_sub(delta.unsigned_abs())
		}
	}
}

impl DocLineCount {
	#[track_caller]
	pub fn scale_into<E: LineScaleExtent>(self, count: DocLineCount, extent: E) -> E {
		if self > count {
			panic!("DocLineCount boundary outside DocLineCount");
		}
		E::from_scaled_extent(scale_line_value_into(self.get(), count, extent.extent()))
	}
}

impl DocLine {
	#[track_caller]
	pub fn scale_into<E: LineScaleExtent>(self, count: DocLineCount, extent: E) -> E::Position {
		if self.boundary_after() > count {
			panic!("DocLine outside DocLineCount");
		}
		E::position_from_scaled(scale_line_value_into(self.get(), count, extent.extent()))
	}
}

pub trait LineScaleExtent: Copy {
	type Position;

	fn extent(self) -> u32;
	fn from_scaled_extent(value: u32) -> Self;
	fn position_from_scaled(value: u32) -> Self::Position;
}

impl LineScaleExtent for CellHeight {
	type Position = CellRow;

	fn extent(self) -> u32 {
		u32::from(self.get())
	}

	fn from_scaled_extent(value: u32) -> Self {
		Self::new(u16::try_from(value).expect("scaled cell boundary fits CellHeight"))
	}

	fn position_from_scaled(value: u32) -> Self::Position {
		CellRow::new(u16::try_from(value).expect("scaled cell row fits CellRow"))
	}
}

impl LineScaleExtent for PixelHeight {
	type Position = PixelY;

	fn extent(self) -> u32 {
		self.get()
	}

	fn from_scaled_extent(value: u32) -> Self {
		Self::new(value)
	}

	fn position_from_scaled(value: u32) -> Self::Position {
		PixelY::new(value)
	}
}

fn scale_line_value_into(value: u64, count: DocLineCount, extent: u32) -> u32 {
	let scaled = (u128::from(value) * u128::from(extent)) / u128::from(count.get().max(1));
	u32::try_from(scaled).expect("scaled line value fits target extent")
}

macro_rules! impl_position_count_conversions {
	($position:ident, $count:ident) => {
		impl $count {
			pub const fn last_index(self) -> Option<$position> {
				match self.get().checked_sub(1) {
					Some(value) => Some($position::new(value)),
					None => None,
				}
			}

			pub const fn start_position(self) -> $position {
				$position::new(self.get())
			}
		}

		impl $position {
			pub const fn boundary_before(self) -> $count {
				$count::new(self.get())
			}

			#[track_caller]
			pub fn boundary_after(self) -> $count {
				$count::new(
					self.get()
						.checked_add(1)
						.expect(concat!(stringify!($position), " boundary overflow")),
				)
			}
		}
	};
}

macro_rules! impl_position_count_arithmetic {
	($position:ident, $count:ident) => {
		impl std::ops::Add<$count> for $position {
			type Output = Self;

			#[track_caller]
			fn add(self, rhs: $count) -> Self::Output {
				self.checked_add(rhs.get())
					.expect(concat!(stringify!($position), " addition overflow"))
			}
		}

		impl std::ops::AddAssign<$count> for $position {
			#[track_caller]
			fn add_assign(&mut self, rhs: $count) {
				*self = self
					.checked_add(rhs.get())
					.expect(concat!(stringify!($position), " addition overflow"));
			}
		}

		impl std::ops::Sub<$count> for $position {
			type Output = Self;

			#[track_caller]
			fn sub(self, rhs: $count) -> Self::Output {
				self.checked_sub(rhs.get())
					.expect(concat!(stringify!($position), " subtraction underflow"))
			}
		}

		impl std::ops::SubAssign<$count> for $position {
			#[track_caller]
			fn sub_assign(&mut self, rhs: $count) {
				*self = self
					.checked_sub(rhs.get())
					.expect(concat!(stringify!($position), " subtraction underflow"));
			}
		}

		impl std::ops::Sub<$position> for $position {
			type Output = $count;

			#[track_caller]
			fn sub(self, rhs: $position) -> Self::Output {
				$count::new(
					self.get()
						.checked_sub(rhs.get())
						.expect(concat!(stringify!($position), " difference underflow")),
				)
			}
		}
	};
}

macro_rules! impl_position_count_ops {
	($position:ident, $count:ident) => {
		impl_position_count_conversions!($position, $count);
		impl_position_count_arithmetic!($position, $count);
	};
}

impl_position_count_ops!(DocLine, DocLineCount);
impl_position_count_ops!(DocVisualRow, VisualRowCount);
impl_position_count_ops!(CellCol, CellWidth);
impl_position_count_ops!(CellRow, CellHeight);
impl_position_count_ops!(PixelX, PixelWidth);
impl_position_count_ops!(PixelY, PixelHeight);
impl_position_count_ops!(DocByte, ByteLen);
impl_position_count_ops!(DocDisplayCol, DocDisplayColCount);
impl_position_count_ops!(DisplayRow, DisplayRowCount);
impl_position_count_ops!(NodeByteOffset, NodeByteLen);
impl_position_count_ops!(PaneLocalCol, PaneCol);
impl_position_count_ops!(PaneLocalRow, PaneRow);
impl_position_count_arithmetic!(ScreenCol, PaneCol);
impl_position_count_arithmetic!(ScreenRow, PaneRow);

macro_rules! impl_extent_scale_ops {
	($extent:ident) => {
		impl std::ops::Mul<u32> for $extent {
			type Output = Self;

			#[track_caller]
			fn mul(self, rhs: u32) -> Self::Output {
				Self::new(
					self.get()
						.checked_mul(rhs)
						.expect(concat!(stringify!($extent), " multiplication overflow")),
				)
			}
		}

		impl std::ops::Div<u32> for $extent {
			type Output = Self;

			#[track_caller]
			fn div(self, rhs: u32) -> Self::Output {
				Self::new(
					self.get()
						.checked_div(rhs)
						.expect(concat!(stringify!($extent), " division by zero")),
				)
			}
		}
	};
}

impl_extent_scale_ops!(PixelWidth);
impl_extent_scale_ops!(PixelHeight);

macro_rules! impl_pixel_offset_ops {
	($position:ident) => {
		impl $position {
			pub fn checked_offset(self, delta: PixelDelta) -> Option<Self> {
				let value = i64::from(self.get()).checked_add(i64::from(delta.get()))?;
				u32::try_from(value).ok().map(Self::new)
			}
		}
	};
}

impl_pixel_offset_ops!(PixelX);
impl_pixel_offset_ops!(PixelY);

impl PixelDelta {
	pub fn checked_add_unsigned(self, rhs: u32) -> Option<Self> {
		let rhs = i32::try_from(rhs).ok()?;
		self.get().checked_add(rhs).map(Self::new)
	}
}

impl TryFrom<ScreenCol> for CellCol {
	type Error = std::num::TryFromIntError;

	fn try_from(value: ScreenCol) -> Result<Self, Self::Error> {
		Ok(Self::new(u16::try_from(value.get())?))
	}
}

impl TryFrom<ScreenRow> for CellRow {
	type Error = std::num::TryFromIntError;

	fn try_from(value: ScreenRow) -> Result<Self, Self::Error> {
		Ok(Self::new(u16::try_from(value.get())?))
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn id_units_expose_successor_not_arithmetic() {
		assert_eq!(PaneId::new(1).next(), PaneId::new(2));
		assert_eq!(BufferId::new(1).next(), BufferId::new(2));
		assert_eq!(ViewId::new(1).next(), ViewId::new(2));
		assert_eq!(ViewportId::new(1).next(), ViewportId::new(2));
		assert_eq!(StateId::ZERO.next(), StateId::new(1));
		assert_eq!(TopologyRev::ZERO.next(), TopologyRev::new(1));
		assert_eq!(RequestId::new(41).next(), RequestId::new(42));
		assert_eq!(RegistryOwnerId::new(8).next(), RegistryOwnerId::new(9));
		assert_eq!(RegistryRev::new(12).next(), RegistryRev::new(13));
		assert_eq!(SourceId::new(20).next(), SourceId::new(21));
		assert_eq!(SubscriptionId::new(21).next(), SubscriptionId::new(22));
	}

	#[test]
	fn fingerprint_units_are_distinct_identity_values() {
		assert_eq!(MinimapFingerprint::new(7).get(), 7);
		assert_eq!(DiffFingerprint::new(7).get(), 7);
		assert_eq!(PreviewFingerprint::new(7).get(), 7);
	}

	#[test]
	fn rendering_intensity_units_are_distinct_identity_values() {
		assert_eq!(TokenBin::new(3).get(), 3);
		assert_eq!(TokenBin::new(3).index(), 3);
		assert_eq!(TokenBin::EMPTY.get(), u8::MAX);
		assert_eq!(
			TokenBin::as_bytes(&[TokenBin::new(3), TokenBin::EMPTY]),
			&[3, u8::MAX]
		);
		assert_eq!(Coverage::new(127).get(), 127);
		let ratio = BlendRatio::new(5, 8);
		assert_eq!(ratio.accent(), 5);
		assert_eq!(ratio.base(), 3);
		assert_eq!(ratio.total(), 8);
	}

	#[test]
	#[should_panic(expected = "BlendRatio total must be nonzero")]
	fn blend_ratio_rejects_zero_total_weight() {
		let _ = BlendRatio::new(0, 0);
	}

	#[test]
	#[should_panic(expected = "BlendRatio accent weight must not exceed total weight")]
	fn blend_ratio_rejects_accent_greater_than_total() {
		let _ = BlendRatio::new(3, 2);
	}

	#[test]
	fn position_units_expose_checked_arithmetic() {
		assert_eq!(DocLine::new(10).checked_add(3), Some(DocLine::new(13)));
		assert_eq!(DocLine::new(10).checked_sub(3), Some(DocLine::new(7)));
		assert_eq!(DocLine::ZERO.checked_sub(1), None);
		let beyond_u32 = u64::from(u32::MAX) + 17;
		assert_eq!(DocLine::new(beyond_u32).get(), beyond_u32);
		assert_eq!(DocLineCount::new(beyond_u32).get(), beyond_u32);
	}

	#[test]
	#[should_panic(expected = "DocLine subtraction underflow")]
	fn position_sub_count_operator_panics_on_underflow() {
		let _ = DocLine::ZERO - DocLineCount::new(1);
	}

	#[test]
	#[should_panic(expected = "DocLine addition overflow")]
	fn position_add_count_operator_panics_on_overflow() {
		let _ = DocLine::new(u64::MAX) + DocLineCount::new(1);
	}

	#[test]
	fn extent_units_expose_checked_arithmetic() {
		assert_eq!(PaneCol::new(10).checked_add(3), Some(PaneCol::new(13)));
		assert_eq!(PaneRow::new(10).checked_sub(3), Some(PaneRow::new(7)));
		assert_eq!(PaneCol::new(u32::MAX).checked_add(1), None);
	}

	#[test]
	fn count_units_expose_last_index_conversion() {
		assert_eq!(DocLineCount::ZERO.last_index(), None);
		assert_eq!(DocLineCount::new(3).last_index(), Some(DocLine::new(2)));
		assert_eq!(VisualRowCount::ZERO.last_index(), None);
		assert_eq!(
			VisualRowCount::new(3).last_index(),
			Some(DocVisualRow::new(2))
		);
		assert_eq!(CellWidth::new(3).last_index(), Some(CellCol::new(2)));
		assert_eq!(CellHeight::new(3).last_index(), Some(CellRow::new(2)));
		assert_eq!(PixelWidth::new(3).last_index(), Some(PixelX::new(2)));
		assert_eq!(PixelHeight::new(3).last_index(), Some(PixelY::new(2)));
		assert_eq!(PaneCol::new(3).last_index(), Some(PaneLocalCol::new(2)));
		assert_eq!(PaneRow::new(3).last_index(), Some(PaneLocalRow::new(2)));
		assert_eq!(ByteLen::new(3).last_index(), Some(DocByte::new(2)));
		assert_eq!(
			DocDisplayColCount::new(3).last_index(),
			Some(DocDisplayCol::new(2))
		);
		assert_eq!(
			DisplayRowCount::new(3).last_index(),
			Some(DisplayRow::new(2))
		);
		assert_eq!(
			NodeByteLen::new(3).last_index(),
			Some(NodeByteOffset::new(2))
		);
		assert_eq!(DocLineCount::new(3).start_position(), DocLine::new(3));
		assert_eq!(
			VisualRowCount::new(3).start_position(),
			DocVisualRow::new(3)
		);
		assert_eq!(ByteLen::new(3).start_position(), DocByte::new(3));
		assert_eq!(
			DocLine::new(3).boundary_before().start_position(),
			DocLine::new(3)
		);
		assert_eq!(
			DocVisualRow::new(3).boundary_before().start_position(),
			DocVisualRow::new(3)
		);
		assert_eq!(
			CellCol::new(3).boundary_before().start_position(),
			CellCol::new(3)
		);
		assert_eq!(
			CellRow::new(3).boundary_before().start_position(),
			CellRow::new(3)
		);
		assert_eq!(
			PixelX::new(3).boundary_before().start_position(),
			PixelX::new(3)
		);
		assert_eq!(
			PixelY::new(3).boundary_before().start_position(),
			PixelY::new(3)
		);
		assert_eq!(
			PaneLocalCol::new(3).boundary_before().start_position(),
			PaneLocalCol::new(3)
		);
		assert_eq!(
			PaneLocalRow::new(3).boundary_before().start_position(),
			PaneLocalRow::new(3)
		);
		assert_eq!(
			DocByte::new(3).boundary_before().start_position(),
			DocByte::new(3)
		);
		assert_eq!(
			DocDisplayCol::new(3).boundary_before().start_position(),
			DocDisplayCol::new(3)
		);
		assert_eq!(
			DisplayRow::new(3).boundary_before().start_position(),
			DisplayRow::new(3)
		);
		assert_eq!(
			NodeByteOffset::new(3).boundary_before().start_position(),
			NodeByteOffset::new(3)
		);
	}

	#[test]
	fn doc_lines_scale_into_extent_by_position_or_boundary() {
		let count = DocLineCount::new(10);
		let height = PixelHeight::new(40);

		assert_eq!(DocLine::new(5).scale_into(count, height), PixelY::new(20));
		assert_eq!(
			DocLineCount::new(10).scale_into(count, height),
			PixelHeight::new(40)
		);
		assert_eq!(
			DocLineCount::ZERO.scale_into(DocLineCount::ZERO, height),
			PixelHeight::ZERO
		);
		assert_eq!(
			DocLine::new(5).scale_into(count, CellHeight::new(20)),
			CellRow::new(10)
		);
		assert_eq!(DocLine::new(9).boundary_after(), DocLineCount::new(10));
		assert_eq!(CellRow::new(9).boundary_after(), CellHeight::new(10));
		assert_eq!(PixelX::new(9).boundary_after(), PixelWidth::new(10));
		assert_eq!(PixelY::new(9).boundary_after(), PixelHeight::new(10));
	}

	#[test]
	fn pixel_positions_keep_extent_arithmetic_typed() {
		let mut x = PixelX::new(2);
		x += PixelWidth::new(5);
		assert_eq!(x, PixelX::new(7));
		assert_eq!(x - PixelWidth::new(4), PixelX::new(3));
		assert_eq!(PixelX::new(8) - x, PixelWidth::new(1));
		assert_eq!(PixelWidth::new(8) * 3, PixelWidth::new(24));
		assert_eq!(PixelHeight::new(24) / 3, PixelHeight::new(8));
	}

	#[test]
	fn pixel_delta_offsets_nonnegative_positions_checked() {
		assert_eq!(
			PixelX::new(5).checked_offset(PixelDelta::new(-3)),
			Some(PixelX::new(2))
		);
		assert_eq!(PixelX::new(2).checked_offset(PixelDelta::new(-3)), None);
		assert_eq!(
			PixelDelta::new(-3).checked_add_unsigned(5),
			Some(PixelDelta::new(2))
		);
	}

	#[test]
	fn cell_positions_convert_checked_from_screen_positions() {
		assert_eq!(CellCol::try_from(ScreenCol::new(42)), Ok(CellCol::new(42)));
		assert_eq!(CellRow::try_from(ScreenRow::new(24)), Ok(CellRow::new(24)));
		assert!(CellCol::try_from(ScreenCol::new(u32::from(u16::MAX) + 1)).is_err());
		assert!(CellRow::try_from(ScreenRow::new(u32::from(u16::MAX) + 1)).is_err());
	}

	#[test]
	#[should_panic(expected = "DocLine outside DocLineCount")]
	fn doc_line_scale_into_panics_when_position_is_outside_count() {
		let _ = DocLine::new(10).scale_into(DocLineCount::new(10), PixelHeight::new(40));
	}

	#[test]
	#[should_panic(expected = "DocLineCount boundary outside DocLineCount")]
	fn doc_line_count_scale_into_panics_when_boundary_is_outside_count() {
		let _ = DocLineCount::new(11).scale_into(DocLineCount::new(10), PixelHeight::new(40));
	}

	#[test]
	fn line_position_count_ops_keep_domains_typed() {
		let mut line = DocLine::new(10);
		line += DocLineCount::new(3);
		assert_eq!(line, DocLine::new(13));
		assert_eq!(line - DocLineCount::new(5), DocLine::new(8));
		assert_eq!(line - DocLine::new(4), DocLineCount::new(9));
	}

	#[test]
	fn visual_row_position_count_ops_keep_domains_typed() {
		let mut row = DocVisualRow::new(10);
		row += VisualRowCount::new(3);
		assert_eq!(row, DocVisualRow::new(13));
		assert_eq!(row - VisualRowCount::new(5), DocVisualRow::new(8));
		assert_eq!(row - DocVisualRow::new(4), VisualRowCount::new(9));
	}

	#[test]
	fn pane_local_position_extent_ops_keep_domains_typed() {
		let mut col = PaneLocalCol::new(5);
		col += PaneCol::new(2);
		assert_eq!(col, PaneLocalCol::new(7));
		assert_eq!(col - PaneCol::new(3), PaneLocalCol::new(4));
		assert_eq!(PaneLocalRow::new(8) - PaneLocalRow::new(3), PaneRow::new(5));
	}

	#[test]
	fn screen_positions_use_pane_extents_for_rect_boundaries() {
		let rect = PaneRect::new(
			ScreenCol::new(10),
			ScreenRow::new(20),
			PaneCol::new(30),
			PaneRow::new(40),
		);

		assert_eq!(rect.right(), ScreenCol::new(40));
		assert_eq!(rect.bottom(), ScreenRow::new(60));
		assert_eq!(ScreenCol::new(40) - rect.screen_col, PaneCol::new(30));
		assert_eq!(ScreenRow::new(60) - rect.screen_row, PaneRow::new(40));
	}

	#[test]
	fn byte_position_count_ops_keep_domains_typed() {
		let mut byte = DocByte::new(10);
		byte += ByteLen::new(3);
		assert_eq!(byte, DocByte::new(13));
		assert_eq!(byte - ByteLen::new(5), DocByte::new(8));
		assert_eq!(byte - DocByte::new(4), ByteLen::new(9));
		assert_eq!(
			DocByte::new(10).saturating_offset(ByteDelta::new(-12)),
			DocByte::ZERO
		);
		assert_eq!(
			DocByte::new(10).saturating_offset(ByteDelta::new(12)),
			DocByte::new(22)
		);
	}

	#[test]
	#[should_panic(expected = "DocLine difference underflow")]
	fn line_position_difference_panics_when_negative() {
		let _ = DocLine::new(3) - DocLine::new(10);
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CursorPosition {
	pub line: DocLine,
	pub col: DocDisplayCol,
}

impl CursorPosition {
	pub const fn new(line: DocLine, col: DocDisplayCol) -> Self {
		Self { line, col }
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PaneRect {
	pub screen_col: ScreenCol,
	pub screen_row: ScreenRow,
	pub width: PaneCol,
	pub height: PaneRow,
}

impl PaneRect {
	pub const fn new(
		screen_col: ScreenCol,
		screen_row: ScreenRow,
		width: PaneCol,
		height: PaneRow,
	) -> Self {
		Self {
			screen_col,
			screen_row,
			width,
			height,
		}
	}

	pub fn right(self) -> ScreenCol {
		self.screen_col + self.width
	}

	pub fn bottom(self) -> ScreenRow {
		self.screen_row + self.height
	}

	pub fn contains(self, col: ScreenCol, row: ScreenRow) -> bool {
		col.get() >= self.screen_col.get()
			&& col.get() < self.right().get()
			&& row.get() >= self.screen_row.get()
			&& row.get() < self.bottom().get()
	}
}
