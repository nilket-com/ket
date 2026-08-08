//! Coordinate-space vocabulary for widget geometry.
//!
//! iced owns the raw geometry at framework entry and renderer/backend
//! emission. Between those seams, a marker on each point and rectangle makes
//! origin changes explicit and cross-space arithmetic unrepresentable.

use std::{
	marker::PhantomData,
	mem::{align_of, size_of},
	ops::{Add, Sub},
};

use iced::{Point, Rectangle, Size, Vector};

mod sealed {
	pub trait Sealed {}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(dead_code)] // The enumeration is an executable closure gate, not runtime state.
pub enum SpaceName {
	Window,
	Widget,
	Cell,
	Plot,
	Content,
}

pub trait Space: sealed::Sealed + Copy + 'static {
	#[allow(dead_code)]
	const NAME: SpaceName;
}

macro_rules! define_spaces {
	($($space:ident),+ $(,)?) => {
		$(
			#[derive(Clone, Copy, Debug, PartialEq, Eq)]
			pub enum $space {}

			impl sealed::Sealed for $space {}

			impl Space for $space {
				const NAME: SpaceName = SpaceName::$space;
			}

			const _: [(); size_of::<Point>()] = [(); size_of::<Pt<$space>>()];
			const _: [(); align_of::<Point>()] = [(); align_of::<Pt<$space>>()];
			const _: [(); size_of::<Rectangle>()] = [(); size_of::<Rect<$space>>()];
			const _: [(); align_of::<Rectangle>()] = [(); align_of::<Rect<$space>>()];
		)+

		#[allow(dead_code)]
		pub(crate) const ALL_SPACES: &[SpaceName] = &[
			$(<$space as Space>::NAME),+
		];
	};
}

// This invocation is the closed vocabulary. The implementations and the
// enumeration cannot drift because the macro emits both from the same list.
define_spaces!(Window, Widget, Cell, Plot, Content);

#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Pt<S: Space> {
	raw: Point,
	space: PhantomData<S>,
}

#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Rect<S: Space> {
	raw: Rectangle,
	space: PhantomData<S>,
}

impl<S: Space> Pt<S> {
	fn wrap(raw: Point) -> Self {
		Self {
			raw,
			space: PhantomData,
		}
	}

	pub(crate) fn x(self) -> f32 {
		self.raw.x
	}

	pub(crate) fn y(self) -> f32 {
		self.raw.y
	}

	/// Unwrap at an iced renderer/backend emission seam.
	pub(crate) fn into_iced(self) -> Point {
		self.raw
	}

	pub(crate) fn offset(self, x: f32, y: f32) -> Self {
		Self::wrap(Point::new(self.x() + x, self.y() + y))
	}
}

impl<S: Space> Add<Vector> for Pt<S> {
	type Output = Self;

	fn add(self, offset: Vector) -> Self::Output {
		Self::wrap(self.raw + offset)
	}
}

impl<S: Space> Sub<Vector> for Pt<S> {
	type Output = Self;

	fn sub(self, offset: Vector) -> Self::Output {
		Self::wrap(self.raw - offset)
	}
}

impl<S: Space> Sub<Pt<S>> for Pt<S> {
	type Output = Vector;

	fn sub(self, origin: Pt<S>) -> Self::Output {
		self.raw - origin.raw
	}
}

impl<S: Space> Rect<S> {
	fn wrap(raw: Rectangle) -> Self {
		Self {
			raw,
			space: PhantomData,
		}
	}

	pub(crate) fn x(self) -> f32 {
		self.raw.x
	}

	pub(crate) fn y(self) -> f32 {
		self.raw.y
	}

	pub(crate) fn width(self) -> f32 {
		self.raw.width
	}

	pub(crate) fn height(self) -> f32 {
		self.raw.height
	}

	pub(crate) fn right(self) -> f32 {
		self.raw.x + self.raw.width
	}

	pub(crate) fn bottom(self) -> f32 {
		self.raw.y + self.raw.height
	}

	pub(crate) fn size(self) -> Size {
		self.raw.size()
	}

	pub(crate) fn origin(self) -> Pt<S> {
		Pt::wrap(self.raw.position())
	}

	pub(crate) fn contains(self, point: Pt<S>) -> bool {
		self.raw.contains(point.raw)
	}

	/// Derive another rectangle without changing coordinate space.
	pub(crate) fn region(self, x: f32, y: f32, width: f32, height: f32) -> Self {
		Self::wrap(Rectangle::new(Point::new(x, y), Size::new(width, height)))
	}

	/// Unwrap at an iced renderer/backend emission seam.
	pub(crate) fn into_iced(self) -> Rectangle {
		self.raw
	}
}

impl Pt<Window> {
	/// Wrap the point supplied by an iced window-space event seam.
	pub(crate) fn from_iced_event(raw: Point) -> Self {
		Self::wrap(raw)
	}
}

impl Rect<Window> {
	/// Wrap the rectangle supplied by an iced layout seam.
	pub(crate) fn from_iced_layout(raw: Rectangle) -> Self {
		Self::wrap(raw)
	}
}

impl Rect<Widget> {
	/// A canvas frame is local even when iced supplies window-space bounds.
	pub(crate) fn from_iced_frame(raw: Rectangle) -> Self {
		Self::wrap(Rectangle::new(Point::ORIGIN, raw.size()))
	}
}

impl Pt<Widget> {
	pub(crate) fn from_window(point: Pt<Window>, widget: Rect<Window>) -> Self {
		Self::wrap(Point::new(point.x() - widget.x(), point.y() - widget.y()))
	}
}

impl Pt<Cell> {
	pub(crate) fn from_widget(point: Pt<Widget>, cell: Rect<Widget>) -> Self {
		Self::wrap(Point::new(point.x() - cell.x(), point.y() - cell.y()))
	}
}

impl Pt<Plot> {
	pub(crate) fn from_widget(point: Pt<Widget>, plot_area: Rect<Widget>) -> Self {
		Self::wrap(Point::new(
			point.x() - plot_area.x(),
			point.y() - plot_area.y(),
		))
	}
}

impl Pt<Content> {
	pub(crate) fn from_widget(
		point: Pt<Widget>,
		data_origin: Pt<Widget>,
		horizontal_scroll: f32,
		vertical_scroll: f32,
	) -> Self {
		Self::wrap(Point::new(
			point.x() - data_origin.x() + horizontal_scroll,
			point.y() - data_origin.y() + vertical_scroll,
		))
	}
}

// repr(transparent) plus these static assertions make the zero-cost promise
// fail at compile time if either wrapper's representation ever drifts.
const _: [(); size_of::<Point>()] = [(); size_of::<Pt<Window>>()];
const _: [(); align_of::<Point>()] = [(); align_of::<Pt<Window>>()];
const _: [(); size_of::<Rectangle>()] = [(); size_of::<Rect<Window>>()];
const _: [(); align_of::<Rectangle>()] = [(); align_of::<Rect<Window>>()];

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn the_space_vocabulary_is_exactly_the_census() {
		assert_eq!(
			ALL_SPACES,
			&[
				SpaceName::Window,
				SpaceName::Widget,
				SpaceName::Cell,
				SpaceName::Plot,
				SpaceName::Content,
			]
		);
	}
}
