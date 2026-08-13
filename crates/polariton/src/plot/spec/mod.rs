//! PRODUCT-0007's authored, resolved, and layout plot artifacts.
//!
//! Channel resolution is deliberately split from domain resolution: schemas
//! can establish field meaning and scale compatibility, while returned values
//! are required to establish domains and stable categorical color identity.
//! Kernels accept only [`ResolvedPlotSpec`], never the intermediate
//! [`ChannelBinding`].

#![allow(dead_code)] // Stage 1 publishes vocabulary consumed by stages 2 and 3.

mod bubble_fields;

pub use bubble_fields::{BubbleFieldNames, BubbleFields};
pub(crate) use bubble_fields::{BubbleNumericField, BubblePreparationChannel};

use crate::plot::composition::PlotComposition;
use crate::plot::core::PlotType;
use libaryon::core::Rgb;
use libaryon::engine::{ResolvedCategoricalPalette, Theme};
use polars::prelude::{AnyValue, DataType, Schema};
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;

const DEFAULT_CATEGORICAL_LIMIT: usize = 12;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Override<T> {
	Inherit,
	Set(T),
}

impl<T> Override<T> {
	pub fn overlay(self, later: Self) -> Self {
		match later {
			Self::Inherit => self,
			set => set,
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Presence<T> {
	Inherit,
	Show(T),
	Hide,
}

/// A resolved turn-off property. `Fallback` retains the renderer's generated
/// value; unlike `Option`, `Hide` cannot collapse back into that fallback.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResolvedPresence<T> {
	Fallback,
	Show(T),
	Hide,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct LegendPosition {
	x: f32,
	y: f32,
}

impl LegendPosition {
	pub fn new(x: f64, y: f64) -> Option<Self> {
		let x = x as f32;
		let y = y as f32;
		(x.is_finite() && y.is_finite()).then_some(Self { x, y })
	}

	pub const fn x(self) -> f32 {
		self.x
	}

	pub const fn y(self) -> f32 {
		self.y
	}
}

impl<T> Presence<T> {
	pub fn overlay(self, later: Self) -> Self {
		match later {
			Self::Inherit => self,
			setting => setting,
		}
	}
}

/// PRODUCT-0017's unitless, linear scatter-point factor. The private value
/// makes invalid and renderer-hostile sizes unrepresentable after authoring.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PointSize(f32);

// `PointSize::new` excludes NaN, so equality is reflexive for every value the
// private representation can contain.
impl Eq for PointSize {}

impl PointSize {
	pub const MIN_FACTOR: f32 = 0.25;
	pub const MAX_FACTOR: f32 = 8.0;
	pub const BASE_RADIUS_PX: f32 = 3.0;
	pub const ONE: Self = Self(1.0);

	pub fn new(factor: f32) -> Result<Self, PointSizeError> {
		if factor.is_finite() && (Self::MIN_FACTOR..=Self::MAX_FACTOR).contains(&factor) {
			Ok(Self(factor))
		} else {
			Err(PointSizeError { factor })
		}
	}

	pub const fn factor(self) -> f32 {
		self.0
	}

	pub fn radius_px(self) -> f32 {
		Self::BASE_RADIUS_PX * self.0
	}
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PointSizeError {
	factor: f32,
}

impl fmt::Display for PointSizeError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(
			f,
			"point size must be finite and between {} and {}, got {}",
			PointSize::MIN_FACTOR,
			PointSize::MAX_FACTOR,
			self.factor
		)
	}
}

impl std::error::Error for PointSizeError {}

/// PRODUCT-0019's unitless, linear line-stroke factor. Keep this distinct from
/// `PointSize` until a third relative setting establishes a shared abstraction.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct StrokeWidth(f32);

// `StrokeWidth::new` excludes NaN, so equality is reflexive for every value the
// private representation can contain.
impl Eq for StrokeWidth {}

impl StrokeWidth {
	pub const MIN_FACTOR: f32 = 0.25;
	pub const MAX_FACTOR: f32 = 8.0;
	pub const BASE_WIDTH_PX: f32 = 2.0;
	pub const ONE: Self = Self(1.0);

	pub fn new(factor: f32) -> Result<Self, StrokeWidthError> {
		if factor.is_finite() && (Self::MIN_FACTOR..=Self::MAX_FACTOR).contains(&factor) {
			Ok(Self(factor))
		} else {
			Err(StrokeWidthError { factor })
		}
	}

	pub const fn factor(self) -> f32 {
		self.0
	}

	pub fn width_px(self) -> f32 {
		Self::BASE_WIDTH_PX * self.0
	}
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct StrokeWidthError {
	factor: f32,
}

impl fmt::Display for StrokeWidthError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(
			f,
			"stroke width must be finite and between {} and {}, got {}",
			StrokeWidth::MIN_FACTOR,
			StrokeWidth::MAX_FACTOR,
			self.factor
		)
	}
}

impl std::error::Error for StrokeWidthError {}

/// PRODUCT-0021's normalized occupancy of one allocated bar slot. Unlike point
/// and stroke settings this is a proportion, not a relative pixel factor.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct BarWidth(f32);

// `BarWidth::new` excludes NaN, so equality is reflexive for every value the
// private representation can contain.
impl Eq for BarWidth {}

impl BarWidth {
	pub const DEFAULT: Self = Self(0.9);

	pub fn new(fraction: f32) -> Result<Self, BarWidthError> {
		if fraction.is_finite() && fraction > 0.0 && fraction <= 1.0 {
			Ok(Self(fraction))
		} else {
			Err(BarWidthError { fraction })
		}
	}

	pub const fn fraction(self) -> f32 {
		self.0
	}
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct BarWidthError {
	fraction: f32,
}

impl fmt::Display for BarWidthError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(
			f,
			"bar width must be finite, greater than 0, and at most 1, got {}",
			self.fraction
		)
	}
}

impl std::error::Error for BarWidthError {}

/// PRODUCT-0053's normalized reserve outside one centered category cluster.
/// Unlike `BarWidth`, zero is meaningful and one would remove the cluster.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct CategoryGap(f32);

// `CategoryGap::new` excludes NaN, so equality is reflexive for every value
// the private representation can contain.
impl Eq for CategoryGap {}

impl CategoryGap {
	pub const DEFAULT: Self = Self(0.0);

	pub fn new(fraction: f32) -> Result<Self, CategoryGapError> {
		if fraction.is_finite() && (0.0..1.0).contains(&fraction) {
			Ok(Self(fraction))
		} else {
			Err(CategoryGapError { fraction })
		}
	}

	pub const fn fraction(self) -> f32 {
		self.0
	}
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct CategoryGapError {
	fraction: f32,
}

impl fmt::Display for CategoryGapError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(
			f,
			"category gap must be finite, at least 0, and less than 1, got {}",
			self.fraction
		)
	}
}

impl std::error::Error for CategoryGapError {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AuthoredChannel {
	Named(String),
	Auto,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OptionalAuthoredChannel {
	Named(String),
	Auto,
	Absent,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthoredChannelSpec {
	pub field: AuthoredChannel,
	pub semantic_type: Override<SemanticType>,
	pub scale: Override<ScaleFamily>,
}

impl AuthoredChannelSpec {
	pub fn named(field: impl Into<String>) -> Self {
		Self {
			field: AuthoredChannel::Named(field.into()),
			semantic_type: Override::Inherit,
			scale: Override::Inherit,
		}
	}

	pub fn auto() -> Self {
		Self {
			field: AuthoredChannel::Auto,
			semantic_type: Override::Inherit,
			scale: Override::Inherit,
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OptionalAuthoredChannelSpec {
	pub field: OptionalAuthoredChannel,
	pub semantic_type: Override<SemanticType>,
	pub scale: Override<ScaleFamily>,
}

impl OptionalAuthoredChannelSpec {
	pub fn named(field: impl Into<String>) -> Self {
		Self {
			field: OptionalAuthoredChannel::Named(field.into()),
			semantic_type: Override::Inherit,
			scale: Override::Inherit,
		}
	}

	pub fn auto() -> Self {
		Self {
			field: OptionalAuthoredChannel::Auto,
			semantic_type: Override::Inherit,
			scale: Override::Inherit,
		}
	}

	pub fn absent() -> Self {
		Self {
			field: OptionalAuthoredChannel::Absent,
			semantic_type: Override::Inherit,
			scale: Override::Inherit,
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoxPlotFields {
	pub category: String,
	pub whisker_low: String,
	pub q1: String,
	pub median: String,
	pub q3: String,
	pub whisker_high: String,
	pub outlier: String,
}

impl BoxPlotFields {
	pub fn all(&self) -> [&str; 7] {
		[
			&self.category,
			&self.whisker_low,
			&self.q1,
			&self.median,
			&self.q3,
			&self.whisker_high,
			&self.outlier,
		]
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HeatmapFields {
	pub x: String,
	pub y: String,
	pub value: String,
}

impl HeatmapFields {
	pub fn all(&self) -> [&str; 3] {
		[&self.x, &self.y, &self.value]
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MarkFields {
	BoxPlot(BoxPlotFields),
	Heatmap(HeatmapFields),
	Bubble(BubbleFields),
}

impl MarkFields {
	pub fn all(&self) -> impl Iterator<Item = &str> {
		let fields: [Option<&str>; 7] = match self {
			Self::BoxPlot(fields) => [
				Some(&fields.category),
				Some(&fields.whisker_low),
				Some(&fields.q1),
				Some(&fields.median),
				Some(&fields.q3),
				Some(&fields.whisker_high),
				Some(&fields.outlier),
			],
			Self::Heatmap(fields) => [
				Some(&fields.x),
				Some(&fields.y),
				Some(&fields.value),
				None,
				None,
				None,
				None,
			],
			Self::Bubble(fields) => [
				Some(fields.numeric.get(BubbleNumericField::X)),
				Some(fields.numeric.get(BubbleNumericField::Y)),
				Some(fields.numeric.get(BubbleNumericField::Size)),
				Some(fields.numeric.get(BubbleNumericField::Color)),
				fields.label.as_deref(),
				None,
				None,
			],
		};
		fields.into_iter().flatten()
	}
}

#[derive(Clone, Debug, PartialEq)]
pub struct AuthoredPlotSpec {
	pub composition: PlotComposition,
	pub x: AuthoredChannelSpec,
	pub y: AuthoredChannelSpec,
	pub color: OptionalAuthoredChannelSpec,
	pub color_job: Override<ColorJob>,
	/// PRODUCT-0091: authored continuous-colour anchor policy. The job and
	/// anchor are carried separately here only because `color_job` predates the
	/// continuous substrate; resolution reunites them as one intent.
	pub continuous_anchor: crate::plot::continuous_color::ContinuousAnchor,
	/// PRODUCT-0079: the second edge of a histogram interval. The x channel is
	/// the start and the y channel is count; ordinary Cartesian marks keep None.
	histogram_end: Option<String>,
	/// PRODUCT-0094: explicit fields beyond the generic channel vocabulary.
	mark_fields: Option<MarkFields>,
	/// PRODUCT-0064: stacking is authored geometry within the Bar family, not
	/// a fourth authored plot type. Resolution consumes this only after the
	/// x/y pair has selected the physical orientation.
	stacked_bar: bool,
	stack_normalization: StackNormalization,
}

impl AuthoredPlotSpec {
	pub fn with_continuous_intent(
		mut self,
		intent: crate::plot::continuous_color::ContinuousScaleIntent,
	) -> Self {
		self.color_job = Override::Set(intent.job);
		self.continuous_anchor = intent.anchor;
		self
	}

	pub fn automatic(composition: PlotComposition) -> Self {
		Self {
			composition,
			x: AuthoredChannelSpec::auto(),
			y: AuthoredChannelSpec::auto(),
			color: OptionalAuthoredChannelSpec::auto(),
			color_job: Override::Inherit,
			continuous_anchor: crate::plot::continuous_color::ContinuousAnchor::None,
			histogram_end: None,
			mark_fields: None,
			stacked_bar: false,
			stack_normalization: StackNormalization::Absolute,
		}
	}

	pub fn xy(composition: PlotComposition, x: impl Into<String>, y: impl Into<String>) -> Self {
		Self {
			composition,
			x: AuthoredChannelSpec::named(x),
			y: AuthoredChannelSpec::named(y),
			color: OptionalAuthoredChannelSpec::absent(),
			color_job: Override::Inherit,
			continuous_anchor: crate::plot::continuous_color::ContinuousAnchor::None,
			histogram_end: None,
			mark_fields: None,
			stacked_bar: false,
			stack_normalization: StackNormalization::Absolute,
		}
	}

	pub fn xy_color(
		composition: PlotComposition,
		x: impl Into<String>,
		y: impl Into<String>,
		color: Option<String>,
	) -> Self {
		let mut spec = Self::xy(composition, x, y);
		if let Some(color) = color {
			spec.color = OptionalAuthoredChannelSpec::named(color);
		}
		spec
	}

	pub fn histogram(
		bin_start: impl Into<String>,
		bin_end: impl Into<String>,
		count: impl Into<String>,
	) -> Self {
		let mut spec = Self::xy(
			PlotComposition::single(PlotType::Histogram),
			bin_start,
			count,
		);
		spec.histogram_end = Some(bin_end.into());
		spec
	}

	pub fn histogram_end(&self) -> Option<&str> {
		self.histogram_end.as_deref()
	}

	pub fn box_plot(fields: BoxPlotFields) -> Self {
		let mut spec = Self::xy(
			PlotComposition::single(PlotType::BoxPlot),
			fields.category.clone(),
			fields.median.clone(),
		);
		spec.mark_fields = Some(MarkFields::BoxPlot(fields));
		spec
	}

	pub fn box_plot_fields(&self) -> Option<&BoxPlotFields> {
		match self.mark_fields.as_ref() {
			Some(MarkFields::BoxPlot(fields)) => Some(fields),
			_ => None,
		}
	}

	pub fn heatmap(fields: HeatmapFields) -> Self {
		let mut spec = Self::xy(
			PlotComposition::single(PlotType::Heatmap),
			fields.x.clone(),
			fields.y.clone(),
		);
		spec.mark_fields = Some(MarkFields::Heatmap(fields));
		spec
	}

	pub fn heatmap_fields(&self) -> Option<&HeatmapFields> {
		match self.mark_fields.as_ref() {
			Some(MarkFields::Heatmap(fields)) => Some(fields),
			_ => None,
		}
	}

	pub fn bubble(fields: BubbleFields) -> Self {
		let mut spec = Self::xy(
			PlotComposition::single(PlotType::Bubble),
			fields.numeric.get(BubbleNumericField::X),
			fields.numeric.get(BubbleNumericField::Y),
		);
		spec.mark_fields = Some(MarkFields::Bubble(fields));
		spec
	}

	pub fn bubble_fields(&self) -> Option<&BubbleFields> {
		match self.mark_fields.as_ref() {
			Some(MarkFields::Bubble(fields)) => Some(fields),
			_ => None,
		}
	}

	pub fn mark_fields(&self) -> Option<&MarkFields> {
		self.mark_fields.as_ref()
	}

	pub fn with_stacked_bar(mut self) -> Self {
		self.stacked_bar = true;
		self
	}

	pub fn with_percent(mut self) -> Self {
		self.stack_normalization = StackNormalization::Percent;
		self
	}
}

/// A structurally non-empty authored sequence. There is deliberately no
/// `Default`, empty constructor, or fallible `Vec` conversion: callers must
/// possess the first layer before a layered plot can exist.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NonEmpty<T> {
	first: T,
	rest: Vec<T>,
}

impl<T> NonEmpty<T> {
	pub fn new(first: T) -> Self {
		Self {
			first,
			rest: Vec::new(),
		}
	}

	pub fn push(&mut self, item: T) {
		self.rest.push(item);
	}

	pub fn first(&self) -> &T {
		&self.first
	}

	pub fn last(&self) -> &T {
		self.rest.last().unwrap_or(&self.first)
	}

	pub fn len(&self) -> usize {
		1 + self.rest.len()
	}

	pub fn iter(&self) -> impl Iterator<Item = &T> {
		std::iter::once(&self.first).chain(&self.rest)
	}

	pub fn into_parts(self) -> (T, Vec<T>) {
		(self.first, self.rest)
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum LayerMark {
	Line,
	Scatter,
}

impl LayerMark {
	pub const fn plot_type(self) -> PlotType {
		match self {
			Self::Line => PlotType::Line,
			Self::Scatter => PlotType::Scatter,
		}
	}

	pub const fn label(self) -> &'static str {
		match self {
			Self::Line => "Line",
			Self::Scatter => "Scatter",
		}
	}
}

/// PRODUCT-0007 encoding roles owned by one authored layer. This is separate
/// from `AuthoredPlotSpec`: PRODUCT-0013 adds a second specification and does
/// not migrate or reshape the established single-plot contract.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LayerEncoding {
	pub x: AuthoredChannelSpec,
	pub y: AuthoredChannelSpec,
	pub color: OptionalAuthoredChannelSpec,
	pub color_job: Override<ColorJob>,
}

impl LayerEncoding {
	pub fn xy(x: impl Into<String>, y: impl Into<String>) -> Self {
		Self {
			x: AuthoredChannelSpec::named(x),
			y: AuthoredChannelSpec::named(y),
			color: OptionalAuthoredChannelSpec::absent(),
			color_job: Override::Inherit,
		}
	}

	pub fn automatic() -> Self {
		Self {
			x: AuthoredChannelSpec::auto(),
			y: AuthoredChannelSpec::auto(),
			color: OptionalAuthoredChannelSpec::auto(),
			color_job: Override::Inherit,
		}
	}

	fn as_authored_plot(&self, mark: LayerMark) -> AuthoredPlotSpec {
		AuthoredPlotSpec {
			composition: PlotComposition::single(mark.plot_type()),
			x: self.x.clone(),
			y: self.y.clone(),
			color: self.color.clone(),
			color_job: self.color_job.clone(),
			continuous_anchor: crate::plot::continuous_color::ContinuousAnchor::None,
			histogram_end: None,
			mark_fields: None,
			stacked_bar: false,
			stack_normalization: StackNormalization::Absolute,
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LayerSettings {
	pub point_size: Override<PointSize>,
	pub stroke_width: Override<StrokeWidth>,
	pub bar_width: Override<BarWidth>,
	pub category_gap: Override<CategoryGap>,
}

impl Default for LayerSettings {
	fn default() -> Self {
		Self {
			point_size: Override::Inherit,
			stroke_width: Override::Inherit,
			bar_width: Override::Inherit,
			category_gap: Override::Inherit,
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LayerSpec {
	pub mark: LayerMark,
	pub encoding: LayerEncoding,
	pub settings: LayerSettings,
}

impl LayerSpec {
	pub fn xy(mark: LayerMark, x: impl Into<String>, y: impl Into<String>) -> Self {
		Self {
			mark,
			encoding: LayerEncoding::xy(x, y),
			settings: LayerSettings::default(),
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LayeredPlotSpec {
	pub layers: NonEmpty<LayerSpec>,
}

impl LayeredPlotSpec {
	pub fn new(first: LayerSpec) -> Self {
		Self {
			layers: NonEmpty::new(first),
		}
	}

	pub fn push(&mut self, layer: LayerSpec) {
		self.layers.push(layer);
	}

	pub fn title(&self) -> String {
		self.layers
			.iter()
			.map(|layer| layer.mark.label())
			.collect::<Vec<_>>()
			.join(" + ")
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Channel {
	X,
	Y,
	Color,
}

impl fmt::Display for Channel {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		formatter.write_str(match self {
			Self::X => "x",
			Self::Y => "y",
			Self::Color => "color",
		})
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SemanticType {
	Nominal,
	Ordinal,
	Quantitative,
	Temporal,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ScaleFamily {
	Point,
	Band,
	Linear,
	Log,
	Time,
	CategoricalColor,
	SequentialColor,
	DivergingColor,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ColorJob {
	Categorical,
	Sequential,
	Diverging,
	Status,
}

/// PRODUCT-0072's closed value-scale projection for stacked marks. This is
/// authored geometry/scale intent, not a presentation-cascade setting.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum StackNormalization {
	#[default]
	Absolute,
	Percent,
}

impl StackNormalization {
	pub fn project(self, cumulative: f64, total: f64) -> f64 {
		match self {
			Self::Absolute => cumulative,
			Self::Percent if total > 0.0 => 100.0 * cumulative / total,
			Self::Percent => 0.0,
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LayerIdentity {
	pub ordinal: usize,
	pub plot_type: PlotType,
}

/// Stable authored-family identity for categorical colour continuation.
///
/// Known constants are the only identifiers production authoring may originate;
/// checked owned values allow a newer family's continuation to survive an older
/// PRODUCT-0086 reader without entering the renderer vocabulary.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct AuthoredFamilyId(Cow<'static, str>);

impl AuthoredFamilyId {
	pub(crate) const SCATTER: Self = Self(Cow::Borrowed("scatter"));
	pub(crate) const BAR: Self = Self(Cow::Borrowed("bar"));
	pub(crate) const LINE: Self = Self(Cow::Borrowed("line"));
	pub(crate) const AREA: Self = Self(Cow::Borrowed("area"));

	/// Exhaustive catalog of identifiers this binary can originate from authored
	/// source. It is intentionally narrower than the checked persistence grammar.
	pub(crate) const ALL: [Self; 4] = [Self::SCATTER, Self::BAR, Self::LINE, Self::AREA];

	pub(crate) fn parse(value: String) -> Result<Self, String> {
		let bytes = value.as_bytes();
		if !(1..=64).contains(&bytes.len()) {
			return Err("authored family must contain 1 through 64 ASCII bytes".to_owned());
		}
		if !bytes[0].is_ascii_lowercase()
			|| !bytes[1..]
				.iter()
				.all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'_')
		{
			return Err("authored family must match [a-z][a-z0-9_]* using ASCII bytes".to_owned());
		}
		Ok(Self(Cow::Owned(value)))
	}

	pub(crate) fn as_str(&self) -> &str {
		self.0.as_ref()
	}
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct ColorLayerIdentity {
	pub(crate) ordinal: usize,
	pub(crate) family: AuthoredFamilyId,
}

impl ColorLayerIdentity {
	pub(crate) fn from_authored_layer(layer: &LayerIdentity) -> Self {
		let family = match layer.plot_type {
			PlotType::Scatter => AuthoredFamilyId::SCATTER,
			PlotType::Bar | PlotType::HorizontalBar => AuthoredFamilyId::BAR,
			PlotType::Line => AuthoredFamilyId::LINE,
			PlotType::StackedArea => AuthoredFamilyId::AREA,
			PlotType::StackedBar | PlotType::HorizontalStackedBar => unreachable!(
				"PRODUCT-0064 requires stacked bar colour lineage to retain authored Bar identity"
			),
			other => {
				unreachable!("plot type {other} has no live authored categorical-family conversion")
			}
		};
		Self {
			ordinal: layer.ordinal,
			family,
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundChannel {
	pub channel: Channel,
	pub field: String,
	pub semantic_type: SemanticType,
	pub scale: ScaleFamily,
	pub layer: LayerIdentity,
}

/// Schema-resolved channels. This is intentionally not a specification and is
/// not accepted by any renderer or kernel.
#[derive(Clone, Debug, PartialEq)]
pub struct ChannelBinding {
	composition: PlotComposition,
	channels: Vec<BoundChannel>,
	kernel_fields: Box<ResolvedKernelFields>,
	color_job: Option<ColorJob>,
	continuous_anchor: crate::plot::continuous_color::ContinuousAnchor,
	categorical_limit: usize,
	stack_normalization: StackNormalization,
}

#[derive(Clone, Debug, PartialEq)]
pub struct LayerChannelBinding {
	pub mark: LayerMark,
	pub settings: LayerSettings,
	binding: ChannelBinding,
}

impl LayerChannelBinding {
	pub fn channels(&self) -> &[BoundChannel] {
		self.binding.channels()
	}

	pub(crate) fn into_binding(self) -> ChannelBinding {
		self.binding
	}
}

#[derive(Clone, Debug, PartialEq)]
pub struct LayeredChannelBinding {
	pub layers: NonEmpty<LayerChannelBinding>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ResolvedLayerSpec {
	pub mark: LayerMark,
	pub settings: LayerSettings,
	pub spec: ResolvedPlotSpec,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ResolvedLayeredPlotSpec {
	pub layers: NonEmpty<ResolvedLayerSpec>,
}

impl ResolvedLayeredPlotSpec {
	pub fn shared_channel(&self, channel: Channel) -> Option<&ResolvedChannel> {
		self.layers.first().spec.channel(channel)
	}
}

impl ChannelBinding {
	pub fn channels(&self) -> &[BoundChannel] {
		&self.channels
	}
}

#[derive(Clone, Debug, PartialEq)]
pub enum ScaleDomain {
	Categorical(Vec<String>),
	Quantitative { min: f64, max: f64 },
	Temporal { min: i64, max: i64 },
	Empty,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ResolvedColor {
	pub red: u8,
	pub green: u8,
	pub blue: u8,
	pub alpha: u8,
}

/// A validated position in one categorical allocation space.
///
/// The value is continuation identity, not a render color or normalized ramp
/// coordinate. Construction requires the allocation-space length so an invalid
/// position cannot enter production state.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct PalettePosition(usize);

impl PalettePosition {
	pub fn new(index: usize, palette_len: usize) -> Result<Self, PalettePositionError> {
		if index < palette_len {
			Ok(Self(index))
		} else {
			Err(PalettePositionError { index, palette_len })
		}
	}

	pub const fn index(self) -> usize {
		self.0
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PalettePositionError {
	pub index: usize,
	pub palette_len: usize,
}

impl fmt::Display for PalettePositionError {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(
			formatter,
			"categorical palette position {} is outside length {}",
			self.index, self.palette_len
		)
	}
}

impl std::error::Error for PalettePositionError {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CategoryColorAssignment {
	pub category: String,
	pub position: PalettePosition,
	pub color: ResolvedColor,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BandFillAssignment {
	pub category: String,
	pub position: PalettePosition,
	pub color: ResolvedColor,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CategoricalPaletteSelection {
	identity: String,
	palette: ResolvedCategoricalPalette,
}

impl CategoricalPaletteSelection {
	pub fn from_theme_name(identity: &str) -> Result<Self, CategoricalPaletteSelectionError> {
		let theme = Theme::try_new(identity).map_err(|source| {
			CategoricalPaletteSelectionError::UnknownTheme {
				identity: identity.to_owned(),
				source,
			}
		})?;
		let palette = theme.categorical_palette().cloned().ok_or_else(|| {
			CategoricalPaletteSelectionError::NoCategoricalPalette {
				identity: identity.to_owned(),
			}
		})?;
		Ok(Self {
			identity: identity.to_owned(),
			palette,
		})
	}

	pub fn identity(&self) -> &str {
		&self.identity
	}

	pub fn len(&self) -> usize {
		self.palette.len()
	}

	fn color(&self, position: PalettePosition) -> ResolvedColor {
		resolved_color(
			self.palette
				.color(position.index())
				.expect("position was validated against this allocation space"),
		)
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CategoricalPaletteSelectionError {
	UnknownTheme { identity: String, source: String },
	NoCategoricalPalette { identity: String },
}

impl fmt::Display for CategoricalPaletteSelectionError {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::UnknownTheme { source, .. } => formatter.write_str(source),
			Self::NoCategoricalPalette { identity } => write!(
				formatter,
				"theme `{identity}` has no declared categorical palette"
			),
		}
	}
}

impl std::error::Error for CategoricalPaletteSelectionError {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CategoricalCapacityFailure {
	pub channel: Channel,
	pub field: String,
	pub layer: LayerIdentity,
	pub role: &'static str,
	pub retained: usize,
	pub requested: usize,
	pub geometry_limit: usize,
	pub palette_identity: String,
	pub palette_limit: usize,
	pub geometry_exceeded: bool,
	pub palette_exceeded: bool,
}

impl fmt::Display for CategoricalCapacityFailure {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(
			formatter,
			"categorical {} `{}` requests {} identities ({} retained); geometry limit {}, palette `{}` limit {}",
			self.role,
			self.field,
			self.requested,
			self.retained,
			self.geometry_limit,
			self.palette_identity,
			self.palette_limit
		)
	}
}

impl std::error::Error for CategoricalCapacityFailure {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CategoricalResolutionError {
	Domain(DomainError),
	Capacity(CategoricalCapacityFailure),
	IncompatibleAllocationSpace {
		field: String,
		layer: LayerIdentity,
		existing_len: usize,
		selected_len: usize,
	},
}

impl fmt::Display for CategoricalResolutionError {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::Domain(source) => source.fmt(formatter),
			Self::Capacity(source) => source.fmt(formatter),
			Self::IncompatibleAllocationSpace {
				field,
				existing_len,
				selected_len,
				..
			} => write!(
				formatter,
				"categorical continuation for `{field}` uses palette length {existing_len}, not selected length {selected_len}"
			),
		}
	}
}

impl std::error::Error for CategoricalResolutionError {}

#[derive(Clone, Debug, PartialEq)]
pub struct ResolvedChannel {
	pub binding: BoundChannel,
	pub domain: ScaleDomain,
	pub colors: Vec<CategoryColorAssignment>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlotProvenance {
	pub source_name: String,
	pub revision: Option<u64>,
}

/// The complete, viewport-independent specification accepted by rendering.
#[derive(Clone, Debug, PartialEq)]
pub struct ResolvedPlotSpec {
	pub composition: PlotComposition,
	pub channels: Vec<ResolvedChannel>,
	pub band_fills: Vec<BandFillAssignment>,
	pub kernel_fields: Box<ResolvedKernelFields>,
	pub color_job: Option<ColorJob>,
	pub continuous_intent: Option<crate::plot::continuous_color::ContinuousScaleIntent>,
	pub provenance: PlotProvenance,
	pub stack_normalization: StackNormalization,
}

impl ResolvedPlotSpec {
	pub fn channel(&self, channel: Channel) -> Option<&ResolvedChannel> {
		self.channels
			.iter()
			.find(|resolved| resolved.binding.channel == channel)
	}
}

/// Exact field roles consumed by the existing plot kernels. Automatic field
/// choice is resolved into this product-side artifact; the factory only
/// projects these names into kernel preparation calls.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResolvedKernelFields {
	Violin {
		category: Option<String>,
		value: String,
	},
	Hexbin {
		x: String,
		y: String,
	},
	Line {
		series: Option<String>,
		x: String,
		y: String,
	},
	Bar {
		category: String,
		group: Option<String>,
		value: String,
	},
	Scatter {
		color: Option<String>,
		x: String,
		y: String,
	},
	StackedBar {
		category: String,
		group: Option<String>,
		value: String,
	},
	Pie {
		category: Option<String>,
		value: String,
	},
	Bubble {
		x: String,
		y: String,
		size: String,
		color: String,
		label: Option<String>,
	},
	Heatmap {
		x: String,
		y: String,
		value: String,
	},
	Parallel {
		dimensions: Vec<String>,
		category: Option<String>,
	},
	Radar {
		dimensions: Vec<String>,
		category: Option<String>,
	},
	RadialDial {
		label: Option<String>,
		value: String,
		maximum: String,
	},
	Candlestick {
		x: String,
		open: String,
		high: String,
		low: String,
		close: String,
	},
	Funnel {
		stage: Option<String>,
		value: String,
	},
	StackedArea {
		series: Option<String>,
		x: String,
		y: String,
	},
	Histogram {
		bin_start: String,
		bin_end: String,
		count: String,
	},
	BoxPlot {
		category: String,
		whisker_low: String,
		q1: String,
		median: String,
		q3: String,
		whisker_high: String,
		outlier: String,
	},
	FillBetween {
		x: String,
		middle: String,
		lower: String,
		upper: String,
	},
}

#[derive(Clone, Debug, PartialEq)]
pub struct ResolvedLayout {
	pub viewport_width: u32,
	pub viewport_height: u32,
	pub x_range: (f64, f64),
	pub y_range: (f64, f64),
}

impl ResolvedLayout {
	pub fn from_spec(spec: &ResolvedPlotSpec, viewport_width: u32, viewport_height: u32) -> Self {
		let range = |channel: Channel| {
			spec.channel(channel)
				.map(|channel| match &channel.domain {
					ScaleDomain::Categorical(categories) => {
						(0.0, categories.len().saturating_sub(1) as f64)
					}
					ScaleDomain::Quantitative { min, max } => padded_range(*min, *max),
					ScaleDomain::Temporal { min, max } => padded_range(*min as f64, *max as f64),
					ScaleDomain::Empty => (0.0, 1.0),
				})
				.unwrap_or((0.0, 1.0))
		};
		Self {
			viewport_width,
			viewport_height,
			x_range: range(Channel::X),
			y_range: range(Channel::Y),
		}
	}
}

fn padded_range(min: f64, max: f64) -> (f64, f64) {
	let padding = (max - min).max(0.001) * 0.1;
	(min - padding, max + padding)
}

#[derive(Clone, Debug)]
pub struct StampedResult {
	pub data: Arc<crate::result_artifact::ResultArtifact>,
	pub provenance: PlotProvenance,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct ColorIdentityKey {
	field: String,
	layer: ColorLayerIdentity,
	category: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct ColorLineageKey {
	field: String,
	layer: ColorLayerIdentity,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ColorAllocationSpace {
	Legacy { len: usize },
	CategoricalPalette { len: usize },
}

impl ColorAllocationSpace {
	const fn len(self) -> usize {
		match self {
			Self::Legacy { len } | Self::CategoricalPalette { len } => len,
		}
	}
}

/// Per-plot continuation state for live categorical resolution.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ColorIdentity {
	assignments: HashMap<ColorIdentityKey, PalettePosition>,
	spaces: HashMap<ColorLineageKey, ColorAllocationSpace>,
}

impl ColorIdentity {
	pub fn len(&self) -> usize {
		self.assignments.len()
	}

	pub fn is_empty(&self) -> bool {
		self.assignments.is_empty()
	}

	pub(crate) fn persistence_snapshot(&self) -> ColorIdentitySnapshot {
		let mut assignments = self
			.assignments
			.iter()
			.map(|(key, position)| ColorAssignmentSnapshot {
				field: key.field.clone(),
				layer: key.layer.clone(),
				category: key.category.clone(),
				position: position.index(),
			})
			.collect::<Vec<_>>();
		assignments.sort_by(|left, right| {
			(
				&left.field,
				left.layer.ordinal,
				&left.layer.family,
				&left.category,
			)
				.cmp(&(
					&right.field,
					right.layer.ordinal,
					&right.layer.family,
					&right.category,
				))
		});
		let mut spaces = self
			.spaces
			.iter()
			.map(|(key, space)| ColorSpaceSnapshot {
				field: key.field.clone(),
				layer: key.layer.clone(),
				space: match *space {
					ColorAllocationSpace::Legacy { len } => AllocationSpaceSnapshot::Legacy { len },
					ColorAllocationSpace::CategoricalPalette { len } => {
						AllocationSpaceSnapshot::CategoricalPalette { len }
					}
				},
			})
			.collect::<Vec<_>>();
		spaces.sort_by(|left, right| {
			(&left.field, left.layer.ordinal, &left.layer.family).cmp(&(
				&right.field,
				right.layer.ordinal,
				&right.layer.family,
			))
		});
		ColorIdentitySnapshot {
			assignments,
			spaces,
		}
	}

	pub(crate) fn from_persistence_snapshot(
		snapshot: ColorIdentitySnapshot,
	) -> Result<Self, String> {
		let mut spaces = HashMap::new();
		for entry in snapshot.spaces {
			let key = ColorLineageKey {
				field: entry.field,
				layer: entry.layer,
			};
			let space = match entry.space {
				AllocationSpaceSnapshot::Legacy { len } => ColorAllocationSpace::Legacy { len },
				AllocationSpaceSnapshot::CategoricalPalette { len } => {
					ColorAllocationSpace::CategoricalPalette { len }
				}
			};
			if space.len() == 0 {
				return Err("colour allocation space has zero length".to_owned());
			}
			if spaces.insert(key, space).is_some() {
				return Err("duplicate colour allocation space".to_owned());
			}
		}

		let mut assignments = HashMap::new();
		for entry in snapshot.assignments {
			let lineage = ColorLineageKey {
				field: entry.field.clone(),
				layer: entry.layer.clone(),
			};
			let len = spaces
				.get(&lineage)
				.ok_or_else(|| "colour assignment has no allocation space".to_owned())?
				.len();
			let position =
				PalettePosition::new(entry.position, len).map_err(|error| error.to_string())?;
			let key = ColorIdentityKey {
				field: entry.field,
				layer: entry.layer,
				category: entry.category,
			};
			if assignments.insert(key, position).is_some() {
				return Err("duplicate colour assignment".to_owned());
			}
		}

		Ok(Self {
			assignments,
			spaces,
		})
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ColorIdentitySnapshot {
	pub assignments: Vec<ColorAssignmentSnapshot>,
	pub spaces: Vec<ColorSpaceSnapshot>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ColorAssignmentSnapshot {
	pub field: String,
	pub layer: ColorLayerIdentity,
	pub category: String,
	pub position: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ColorSpaceSnapshot {
	pub field: String,
	pub layer: ColorLayerIdentity,
	pub space: AllocationSpaceSnapshot,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AllocationSpaceSnapshot {
	Legacy { len: usize },
	CategoricalPalette { len: usize },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FieldAttribution {
	Channel(Channel),
	Role(&'static str),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChannelError {
	pub attribution: FieldAttribution,
	pub field: String,
	pub reason: String,
}

impl fmt::Display for ChannelError {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self.attribution {
			FieldAttribution::Channel(channel) => write!(
				formatter,
				"plot {channel} channel `{}` could not resolve: {}",
				self.field, self.reason
			),
			FieldAttribution::Role(role) => write!(
				formatter,
				"plot {role} field `{}` could not resolve: {}",
				self.field, self.reason
			),
		}
	}
}

impl std::error::Error for ChannelError {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LayeredPlotError {
	ColorNotAbsent {
		layer: usize,
		authored: String,
	},
	Channel {
		layer: usize,
		source: ChannelError,
	},
	FieldMismatch {
		layer: usize,
		channel: Channel,
		expected: String,
		actual: String,
	},
	ScaleMismatch {
		layer: usize,
		channel: Channel,
		expected: ScaleFamily,
		actual: ScaleFamily,
	},
	Domain {
		layer: usize,
		source: DomainError,
	},
}

impl fmt::Display for LayeredPlotError {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::ColorNotAbsent { layer, authored } => write!(
				formatter,
				"layer {} color channel {authored} is unaccepted; ordered shared-Cartesian layers require Absent color",
				layer + 1
			),
			Self::Channel { layer, source } => {
				write!(formatter, "layer {} could not resolve: {source}", layer + 1)
			}
			Self::FieldMismatch {
				layer,
				channel,
				expected,
				actual,
			} => write!(
				formatter,
				"layer {} {channel} field `{actual}` is incompatible with shared field `{expected}`",
				layer + 1
			),
			Self::ScaleMismatch {
				layer,
				channel,
				expected,
				actual,
			} => write!(
				formatter,
				"layer {} {channel} scale {actual:?} is incompatible with shared scale family {expected:?}",
				layer + 1
			),
			Self::Domain { layer, source } => {
				write!(formatter, "layer {} could not resolve: {source}", layer + 1)
			}
		}
	}
}

impl std::error::Error for LayeredPlotError {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DomainError {
	pub channel: Channel,
	pub field: String,
	pub reason: String,
}

impl fmt::Display for DomainError {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(
			formatter,
			"plot {} channel `{}` has no renderable domain: {}",
			self.channel, self.field, self.reason
		)
	}
}

impl std::error::Error for DomainError {}

pub fn resolve_channels(
	spec: &AuthoredPlotSpec,
	schema: &Schema,
) -> Result<ChannelBinding, ChannelError> {
	let plot_type = spec.composition.primary_plot_type();
	if matches!(spec.stack_normalization, StackNormalization::Percent) {
		let admitted =
			plot_type == PlotType::StackedArea || (plot_type == PlotType::Bar && spec.stacked_bar);
		if !admitted {
			return Err(ChannelError {
				attribution: FieldAttribution::Channel(Channel::X),
				field: "<authored>".to_owned(),
				reason: if plot_type == PlotType::Bar {
					"percent() requires stacked() on bar marks".to_owned()
				} else {
					"percent() is available on stacked bar and area marks".to_owned()
				},
			});
		}
	}
	if plot_type == PlotType::Histogram {
		return resolve_histogram_channels(spec, schema);
	}
	if plot_type == PlotType::BoxPlot {
		return resolve_box_plot_channels(spec, schema);
	}
	if plot_type == PlotType::Heatmap && spec.heatmap_fields().is_some() {
		return resolve_heatmap_channels(spec, schema);
	}
	if plot_type == PlotType::Bubble && spec.bubble_fields().is_some() {
		return resolve_bubble_channels(spec, schema);
	}
	let legacy_automatic_area = plot_type == PlotType::StackedArea
		&& matches!(spec.x.field, AuthoredChannel::Auto)
		&& matches!(spec.y.field, AuthoredChannel::Auto)
		&& matches!(spec.color.field, OptionalAuthoredChannel::Auto);
	if legacy_automatic_area
		|| !matches!(
			plot_type,
			PlotType::Scatter | PlotType::Bar | PlotType::Line | PlotType::StackedArea
		) {
		if !matches!(spec.x.field, AuthoredChannel::Auto)
			|| !matches!(spec.y.field, AuthoredChannel::Auto)
			|| !matches!(spec.color.field, OptionalAuthoredChannel::Auto)
		{
			return Err(ChannelError {
				attribution: FieldAttribution::Channel(Channel::X),
				field: "<authored>".to_owned(),
				reason: format!("{plot_type} does not yet expose authored channel roles"),
			});
		}
		return Ok(ChannelBinding {
			composition: spec.composition.clone(),
			channels: Vec::new(),
			kernel_fields: Box::new(resolve_automatic_kernel_fields(plot_type, schema)?),
			color_job: None,
			continuous_anchor: spec.continuous_anchor,
			categorical_limit: 0,
			stack_normalization: spec.stack_normalization,
		});
	}

	let layer = LayerIdentity {
		ordinal: 0,
		plot_type,
	};
	let pair_resolved_bar = plot_type == PlotType::Bar
		&& matches!(spec.x.field, AuthoredChannel::Named(_))
		&& matches!(spec.y.field, AuthoredChannel::Named(_));
	let mut unavailable_to_auto = HashSet::new();
	let inferred_x = (matches!(spec.x.field, AuthoredChannel::Auto)
		&& matches!(spec.y.field, AuthoredChannel::Auto))
	.then(|| infer_x_preserving_auto_y(plot_type, schema))
	.flatten();
	let x_spec = inferred_x.as_ref().map(|field| AuthoredChannelSpec {
		field: AuthoredChannel::Named(field.clone()),
		semantic_type: spec.x.semantic_type.clone(),
		scale: spec.x.scale.clone(),
	});
	let x = resolve_required(
		Channel::X,
		x_spec.as_ref().unwrap_or(&spec.x),
		plot_type,
		&layer,
		pair_resolved_bar,
		schema,
		&mut unavailable_to_auto,
	)?;
	let y = resolve_required(
		Channel::Y,
		&spec.y,
		plot_type,
		&layer,
		pair_resolved_bar,
		schema,
		&mut unavailable_to_auto,
	)?;
	// PRODUCT-0055: Rune authors the Bar family; the resolved physical channel
	// pair selects its canonical rendering member. Keep `layer` family-keyed so
	// rotating a bar does not begin a new categorical color lineage.
	let oriented_plot_type = if pair_resolved_bar {
		resolve_bar_orientation(&x, &y)?
	} else {
		plot_type
	};
	let resolved_plot_type = if spec.stacked_bar {
		match oriented_plot_type {
			PlotType::Bar => PlotType::StackedBar,
			PlotType::HorizontalBar => PlotType::HorizontalStackedBar,
			_ => {
				return Err(ChannelError {
					attribution: FieldAttribution::Channel(Channel::X),
					field: "<authored>".to_owned(),
					reason: ".stacked() is available on bar marks".to_owned(),
				});
			}
		}
	} else {
		oriented_plot_type
	};
	let mut color = resolve_optional(
		Channel::Color,
		&spec.color,
		plot_type,
		&layer,
		pair_resolved_bar,
		schema,
		&mut unavailable_to_auto,
	)?;
	if matches!(plot_type, PlotType::Line | PlotType::StackedArea)
		&& color.as_ref().is_some_and(|channel| {
			!matches!(
				channel.semantic_type,
				SemanticType::Nominal | SemanticType::Ordinal
			)
		}) {
		let color = color.as_ref().expect("checked as present");
		return Err(ChannelError {
			attribution: FieldAttribution::Channel(Channel::Color),
			field: color.field.clone(),
			reason: format!(
				"{} series must be categorical",
				if plot_type == PlotType::Line {
					"line"
				} else {
					"area"
				}
			),
		});
	}
	let color_job = color.as_ref().map(|channel| match spec.color_job {
		Override::Set(job) => job,
		Override::Inherit => default_color_job(channel.semantic_type),
	});
	if let (Some(color), Some(job)) = (color.as_mut(), color_job) {
		if !color_job_is_compatible(job, color.semantic_type) {
			return Err(ChannelError {
				attribution: FieldAttribution::Channel(Channel::Color),
				field: color.field.clone(),
				reason: format!(
					"color job {job:?} is incompatible with {:?}",
					color.semantic_type
				),
			});
		}
		let job_scale = color_job_scale(job);
		match spec.color.scale {
			Override::Inherit => color.scale = job_scale,
			Override::Set(_) if color.scale != job_scale => {
				return Err(ChannelError {
					attribution: FieldAttribution::Channel(Channel::Color),
					field: color.field.clone(),
					reason: format!("color job {job:?} requires scale {job_scale:?}"),
				});
			}
			Override::Set(_) => {}
		}
	}
	let mut channels = vec![x, y];
	channels.extend(color);
	let kernel_fields = Box::new(kernel_fields_from_channels(resolved_plot_type, &channels)?);
	let mut composition = spec.composition.clone();
	if plot_type == PlotType::Bar {
		composition.set_single(resolved_plot_type);
	}
	Ok(ChannelBinding {
		composition,
		channels,
		kernel_fields,
		color_job,
		continuous_anchor: spec.continuous_anchor,
		categorical_limit: categorical_limit(plot_type),
		stack_normalization: spec.stack_normalization,
	})
}

fn resolve_histogram_channels(
	spec: &AuthoredPlotSpec,
	schema: &Schema,
) -> Result<ChannelBinding, ChannelError> {
	let layer = LayerIdentity {
		ordinal: 0,
		plot_type: PlotType::Histogram,
	};
	let mut used_by_auto = HashSet::new();
	let bin_start = resolve_required(
		Channel::X,
		&spec.x,
		PlotType::Histogram,
		&layer,
		false,
		schema,
		&mut used_by_auto,
	)?;
	let count = resolve_required(
		Channel::Y,
		&spec.y,
		PlotType::Histogram,
		&layer,
		false,
		schema,
		&mut used_by_auto,
	)?;
	let count_dtype = schema
		.get(count.field.as_str())
		.expect("resolved count field");
	if !matches!(
		count_dtype,
		DataType::Int8
			| DataType::Int16
			| DataType::Int32
			| DataType::Int64
			| DataType::UInt8
			| DataType::UInt16
			| DataType::UInt32
			| DataType::UInt64
	) {
		return Err(ChannelError {
			attribution: FieldAttribution::Channel(Channel::Y),
			field: count.field,
			reason: format!(
				"histogram count requires an integer dtype through u64, got {count_dtype:?}"
			),
		});
	}
	let bin_end = spec.histogram_end().ok_or_else(|| ChannelError {
		attribution: FieldAttribution::Channel(Channel::X),
		field: "<authored>".to_owned(),
		reason: "histogram requires an explicit bin_end field".to_owned(),
	})?;
	let end_dtype = schema.get(bin_end).ok_or_else(|| ChannelError {
		attribution: FieldAttribution::Channel(Channel::X),
		field: bin_end.to_owned(),
		reason: "field is absent from the returned schema".to_owned(),
	})?;
	if !matches!(semantic_type(end_dtype), Some(SemanticType::Quantitative)) {
		return Err(ChannelError {
			attribution: FieldAttribution::Channel(Channel::X),
			field: bin_end.to_owned(),
			reason: format!("histogram bin_end requires a quantitative dtype, got {end_dtype:?}"),
		});
	}
	Ok(ChannelBinding {
		composition: spec.composition.clone(),
		channels: vec![bin_start.clone(), count.clone()],
		kernel_fields: Box::new(ResolvedKernelFields::Histogram {
			bin_start: bin_start.field,
			bin_end: bin_end.to_owned(),
			count: count.field,
		}),
		color_job: None,
		continuous_anchor: spec.continuous_anchor,
		categorical_limit: 0,
		stack_normalization: StackNormalization::Absolute,
	})
}

fn resolve_box_plot_channels(
	spec: &AuthoredPlotSpec,
	schema: &Schema,
) -> Result<ChannelBinding, ChannelError> {
	let fields = spec.box_plot_fields().ok_or_else(|| ChannelError {
		attribution: FieldAttribution::Channel(Channel::X),
		field: "<box plot>".to_owned(),
		reason: "Box Plot has no producer; seven explicit Tukey fields are required".to_owned(),
	})?;
	let layer = LayerIdentity {
		ordinal: 0,
		plot_type: PlotType::BoxPlot,
	};
	let mut used_by_auto = HashSet::new();
	let category = resolve_required(
		Channel::X,
		&spec.x,
		PlotType::BoxPlot,
		&layer,
		false,
		schema,
		&mut used_by_auto,
	)?;
	let median = resolve_required(
		Channel::Y,
		&spec.y,
		PlotType::BoxPlot,
		&layer,
		false,
		schema,
		&mut used_by_auto,
	)?;
	for (role, field) in [
		("whisker_low", &fields.whisker_low),
		("q1", &fields.q1),
		("q3", &fields.q3),
		("whisker_high", &fields.whisker_high),
		("outlier", &fields.outlier),
	] {
		let dtype = schema.get(field.as_str()).ok_or_else(|| ChannelError {
			attribution: FieldAttribution::Channel(Channel::Y),
			field: field.clone(),
			reason: format!("box {role} field is absent from the returned schema"),
		})?;
		if !matches!(semantic_type(dtype), Some(SemanticType::Quantitative)) {
			return Err(ChannelError {
				attribution: FieldAttribution::Channel(Channel::Y),
				field: field.clone(),
				reason: format!("box {role} requires a quantitative dtype, got {dtype:?}"),
			});
		}
	}
	Ok(ChannelBinding {
		composition: spec.composition.clone(),
		channels: vec![category.clone(), median.clone()],
		kernel_fields: Box::new(ResolvedKernelFields::BoxPlot {
			category: fields.category.clone(),
			whisker_low: fields.whisker_low.clone(),
			q1: fields.q1.clone(),
			median: fields.median.clone(),
			q3: fields.q3.clone(),
			whisker_high: fields.whisker_high.clone(),
			outlier: fields.outlier.clone(),
		}),
		color_job: None,
		continuous_anchor: spec.continuous_anchor,
		categorical_limit: 0,
		stack_normalization: StackNormalization::Absolute,
	})
}

fn resolve_heatmap_channels(
	spec: &AuthoredPlotSpec,
	schema: &Schema,
) -> Result<ChannelBinding, ChannelError> {
	let fields = spec.heatmap_fields().ok_or_else(|| ChannelError {
		attribution: FieldAttribution::Channel(Channel::X),
		field: "<heatmap>".to_owned(),
		reason: "Heatmap requires explicit x, y, and value fields".to_owned(),
	})?;
	let layer = LayerIdentity {
		ordinal: 0,
		plot_type: PlotType::Heatmap,
	};
	let mut used_by_auto = HashSet::new();
	let x = resolve_required(
		Channel::X,
		&spec.x,
		PlotType::Heatmap,
		&layer,
		false,
		schema,
		&mut used_by_auto,
	)?;
	let y = resolve_required(
		Channel::Y,
		&spec.y,
		PlotType::Heatmap,
		&layer,
		false,
		schema,
		&mut used_by_auto,
	)?;
	let value_dtype = schema
		.get(fields.value.as_str())
		.ok_or_else(|| ChannelError {
			attribution: FieldAttribution::Channel(Channel::Color),
			field: fields.value.clone(),
			reason: "heatmap value field is absent from the returned schema".to_owned(),
		})?;
	let value_semantic = semantic_type(value_dtype).ok_or_else(|| ChannelError {
		attribution: FieldAttribution::Channel(Channel::Color),
		field: fields.value.clone(),
		reason: format!("dtype {value_dtype:?} has no supported semantic type"),
	})?;
	if value_semantic != SemanticType::Quantitative {
		return Err(ChannelError {
			attribution: FieldAttribution::Channel(Channel::Color),
			field: fields.value.clone(),
			reason: format!("heatmap value requires a quantitative dtype, got {value_dtype:?}"),
		});
	}
	Ok(ChannelBinding {
		composition: spec.composition.clone(),
		channels: vec![x, y],
		kernel_fields: Box::new(ResolvedKernelFields::Heatmap {
			x: fields.x.clone(),
			y: fields.y.clone(),
			value: fields.value.clone(),
		}),
		color_job: Some(match spec.color_job {
			Override::Set(job) => job,
			Override::Inherit => ColorJob::Sequential,
		}),
		continuous_anchor: spec.continuous_anchor,
		categorical_limit: 0,
		stack_normalization: StackNormalization::Absolute,
	})
}

fn resolve_bubble_channels(
	spec: &AuthoredPlotSpec,
	schema: &Schema,
) -> Result<ChannelBinding, ChannelError> {
	let fields = spec.bubble_fields().ok_or_else(|| ChannelError {
		attribution: FieldAttribution::Role("bubble"),
		field: "<bubble>".to_owned(),
		reason: "Bubble requires explicit x, y, size, and color fields".to_owned(),
	})?;
	let layer = LayerIdentity {
		ordinal: 0,
		plot_type: PlotType::Bubble,
	};
	let mut used_by_auto = HashSet::new();
	let x = resolve_required(
		Channel::X,
		&spec.x,
		PlotType::Bubble,
		&layer,
		false,
		schema,
		&mut used_by_auto,
	)?;
	let y = resolve_required(
		Channel::Y,
		&spec.y,
		PlotType::Bubble,
		&layer,
		false,
		schema,
		&mut used_by_auto,
	)?;
	let validate_role = |role: BubblePreparationChannel,
	                     field: &str,
	                     required: SemanticType|
	 -> Result<(), ChannelError> {
		let attribution = FieldAttribution::Role(role.as_str());
		let dtype = schema.get(field).ok_or_else(|| ChannelError {
			attribution,
			field: field.to_owned(),
			reason: format!(
				"bubble {} field is absent from the returned schema",
				role.as_str()
			),
		})?;
		let semantic = semantic_type(dtype).ok_or_else(|| ChannelError {
			attribution,
			field: field.to_owned(),
			reason: format!("dtype {dtype:?} has no supported semantic type"),
		})?;
		if semantic != required {
			return Err(ChannelError {
				attribution,
				field: field.to_owned(),
				reason: format!(
					"bubble {} requires a {required:?} dtype, got {dtype:?}",
					role.as_str()
				),
			});
		}
		Ok(())
	};
	let size = fields.numeric.get(BubbleNumericField::Size);
	let color = fields.numeric.get(BubbleNumericField::Color);
	validate_role(
		BubblePreparationChannel::Size,
		size,
		SemanticType::Quantitative,
	)?;
	validate_role(
		BubblePreparationChannel::Color,
		color,
		SemanticType::Quantitative,
	)?;
	if let Some(label) = fields.label.as_deref() {
		validate_role(
			BubblePreparationChannel::Label,
			label,
			SemanticType::Nominal,
		)?;
	}
	Ok(ChannelBinding {
		composition: spec.composition.clone(),
		channels: vec![x, y],
		kernel_fields: Box::new(ResolvedKernelFields::Bubble {
			x: fields.numeric.get(BubbleNumericField::X).to_owned(),
			y: fields.numeric.get(BubbleNumericField::Y).to_owned(),
			size: size.to_owned(),
			color: color.to_owned(),
			label: fields.label.clone(),
		}),
		color_job: Some(match spec.color_job {
			Override::Set(job) => job,
			Override::Inherit => ColorJob::Sequential,
		}),
		continuous_anchor: spec.continuous_anchor,
		categorical_limit: 0,
		stack_normalization: StackNormalization::Absolute,
	})
}

fn resolve_bar_orientation(x: &BoundChannel, y: &BoundChannel) -> Result<PlotType, ChannelError> {
	match (
		x.semantic_type == SemanticType::Quantitative,
		y.semantic_type == SemanticType::Quantitative,
	) {
		(false, true) => Ok(PlotType::Bar),
		(true, false) => Ok(PlotType::HorizontalBar),
		_ => Err(ChannelError {
			// Resolution failures are attributed to the complete Rune sink. The
			// existing carrier requires a channel and field, while the reason names
			// both facts and deliberately assigns blame to neither one alone.
			attribution: FieldAttribution::Channel(Channel::X),
			field: x.field.clone(),
			reason: format!(
				"bar requires exactly one quantitative value axis and one non-quantitative category axis; x `{}` resolved as {:?}, y `{}` resolved as {:?}",
				x.field, x.semantic_type, y.field, y.semantic_type
			),
		}),
	}
}

/// Resolve every authored layer independently, then apply PRODUCT-0013's
/// accepted shared-Cartesian compatibility rule atomically.
pub fn resolve_layered_channels(
	spec: &LayeredPlotSpec,
	schema: &Schema,
) -> Result<LayeredChannelBinding, LayeredPlotError> {
	let mut resolved = Vec::with_capacity(spec.layers.len());
	for (ordinal, layer) in spec.layers.iter().enumerate() {
		let authored_color = match &layer.encoding.color.field {
			OptionalAuthoredChannel::Absent => None,
			OptionalAuthoredChannel::Named(field) => Some(format!("`{field}`")),
			OptionalAuthoredChannel::Auto => Some("<auto>".to_owned()),
		};
		if let Some(authored) = authored_color {
			return Err(LayeredPlotError::ColorNotAbsent {
				layer: ordinal,
				authored,
			});
		}

		let mut binding = resolve_channels(&layer.encoding.as_authored_plot(layer.mark), schema)
			.map_err(|source| LayeredPlotError::Channel {
				layer: ordinal,
				source,
			})?;
		for channel in &mut binding.channels {
			channel.layer = LayerIdentity {
				ordinal,
				plot_type: layer.mark.plot_type(),
			};
		}
		resolved.push(LayerChannelBinding {
			mark: layer.mark,
			settings: layer.settings.clone(),
			binding,
		});
	}

	let first = resolved
		.first()
		.expect("LayeredPlotSpec is structurally non-empty");
	let shared = [Channel::X, Channel::Y].map(|channel| {
		let binding = first
			.channels()
			.iter()
			.find(|binding| binding.channel == channel)
			.expect("line and scatter always resolve x and y channels");
		(channel, binding.field.clone(), binding.scale)
	});
	for (ordinal, layer) in resolved.iter().enumerate().skip(1) {
		for (channel, expected_field, expected_scale) in &shared {
			let actual = layer
				.channels()
				.iter()
				.find(|binding| binding.channel == *channel)
				.expect("line and scatter always resolve x and y channels");
			if actual.field != *expected_field {
				return Err(LayeredPlotError::FieldMismatch {
					layer: ordinal,
					channel: *channel,
					expected: expected_field.clone(),
					actual: actual.field.clone(),
				});
			}
			if actual.scale != *expected_scale {
				return Err(LayeredPlotError::ScaleMismatch {
					layer: ordinal,
					channel: *channel,
					expected: *expected_scale,
					actual: actual.scale,
				});
			}
		}
	}

	let mut resolved = resolved.into_iter();
	let first = resolved
		.next()
		.expect("LayeredPlotSpec is structurally non-empty");
	let mut layers = NonEmpty::new(first);
	for layer in resolved {
		layers.push(layer);
	}
	Ok(LayeredChannelBinding { layers })
}

/// Resolve the common source-column domains before any mark preparation. Only
/// the first binding scans the stamped result; compatible later bindings reuse
/// those same resolved x/y domains while retaining their own layer identity.
pub fn resolve_layered_domains(
	binding: LayeredChannelBinding,
	result: &StampedResult,
) -> Result<ResolvedLayeredPlotSpec, LayeredPlotError> {
	let (first, rest) = binding.layers.into_parts();
	let first_mark = first.mark;
	let first_settings = first.settings.clone();
	let (first_spec, _) = resolve_domains(first.into_binding(), result, &ColorIdentity::default())
		.map_err(|source| LayeredPlotError::Domain { layer: 0, source })?;
	let shared_channels = first_spec.channels.clone();
	let mut layers = NonEmpty::new(ResolvedLayerSpec {
		mark: first_mark,
		settings: first_settings,
		spec: first_spec,
	});

	for layer in rest {
		let LayerChannelBinding {
			mark,
			settings,
			binding,
		} = layer;
		let channels = binding
			.channels
			.into_iter()
			.map(|binding| {
				let shared = shared_channels
					.iter()
					.find(|channel| channel.binding.channel == binding.channel)
					.expect("compatible layered bindings share x and y channels");
				ResolvedChannel {
					binding,
					domain: shared.domain.clone(),
					colors: Vec::new(),
				}
			})
			.collect();
		layers.push(ResolvedLayerSpec {
			mark,
			settings,
			spec: ResolvedPlotSpec {
				composition: binding.composition,
				channels,
				band_fills: Vec::new(),
				kernel_fields: binding.kernel_fields,
				color_job: binding.color_job,
				continuous_intent: None,
				provenance: result.provenance.clone(),
				stack_normalization: binding.stack_normalization,
			},
		});
	}

	Ok(ResolvedLayeredPlotSpec { layers })
}

/// PRODUCT-0029 compatibility entrypoint. It preserves the pre-palette
/// twelve-slot allocation and adds no band fills until PRODUCT-0030 switches
/// the production caller atomically with authored palette selection.
pub fn resolve_domains(
	binding: ChannelBinding,
	result: &StampedResult,
	identity: &ColorIdentity,
) -> Result<(ResolvedPlotSpec, ColorIdentity), DomainError> {
	let mut next_identity = identity.clone();
	let mut channels = Vec::with_capacity(binding.channels.len());
	let color_job = binding.color_job;
	let continuous_intent = continuous_intent(color_job, binding.continuous_anchor);
	for channel in binding.channels {
		let data = result.data.projection().map_err(|error| DomainError {
			channel: channel.channel,
			field: channel.field.clone(),
			reason: error.to_string(),
		})?;
		let column = data.column(&channel.field).map_err(|error| DomainError {
			channel: channel.channel,
			field: channel.field.clone(),
			reason: error.to_string(),
		})?;
		let domain = resolve_domain(column, &channel)?;
		let colors =
			if channel.channel == Channel::Color && color_job == Some(ColorJob::Categorical) {
				let ScaleDomain::Categorical(categories) = &domain else {
					return Err(DomainError {
						channel: channel.channel,
						field: channel.field.clone(),
						reason: "categorical color requires a categorical domain".to_owned(),
					});
				};
				resolve_category_colors(
					categories,
					&channel,
					binding.categorical_limit,
					&mut next_identity,
				)?
			} else {
				Vec::new()
			};
		channels.push(ResolvedChannel {
			binding: channel,
			domain,
			colors,
		});
	}
	Ok((
		ResolvedPlotSpec {
			composition: binding.composition,
			channels,
			band_fills: Vec::new(),
			kernel_fields: binding.kernel_fields,
			color_job,
			continuous_intent,
			provenance: result.provenance.clone(),
			stack_normalization: binding.stack_normalization,
		},
		next_identity,
	))
}

fn continuous_intent(
	job: Option<ColorJob>,
	anchor: crate::plot::continuous_color::ContinuousAnchor,
) -> Option<crate::plot::continuous_color::ContinuousScaleIntent> {
	match job {
		Some(job @ (ColorJob::Sequential | ColorJob::Diverging)) => {
			Some(crate::plot::continuous_color::ContinuousScaleIntent { job, anchor })
		}
		Some(ColorJob::Categorical | ColorJob::Status) | None => None,
	}
}

/// PRODUCT-0029's explicit categorical palette path. PRODUCT-0030 supplies the
/// first production caller and keeps the authored default in the Rune head.
pub fn resolve_domains_with_categorical_palette(
	binding: ChannelBinding,
	result: &StampedResult,
	identity: &ColorIdentity,
	selection: &CategoricalPaletteSelection,
) -> Result<(ResolvedPlotSpec, ColorIdentity), CategoricalResolutionError> {
	let mut next_identity = identity.clone();
	let mut channels = Vec::with_capacity(binding.channels.len());
	let color_job = binding.color_job;
	let continuous_intent = continuous_intent(color_job, binding.continuous_anchor);
	for channel in binding.channels {
		let data = result.data.projection().map_err(|error| {
			CategoricalResolutionError::Domain(DomainError {
				channel: channel.channel,
				field: channel.field.clone(),
				reason: error.to_string(),
			})
		})?;
		let column = data.column(&channel.field).map_err(|error| {
			CategoricalResolutionError::Domain(DomainError {
				channel: channel.channel,
				field: channel.field.clone(),
				reason: error.to_string(),
			})
		})?;
		let domain =
			resolve_domain(column, &channel).map_err(CategoricalResolutionError::Domain)?;
		let colors =
			if channel.channel == Channel::Color && color_job == Some(ColorJob::Categorical) {
				let ScaleDomain::Categorical(categories) = &domain else {
					return Err(CategoricalResolutionError::Domain(DomainError {
						channel: channel.channel,
						field: channel.field.clone(),
						reason: "categorical color requires a categorical domain".to_owned(),
					}));
				};
				resolve_explicit_category_colors(
					categories,
					&channel,
					binding.categorical_limit,
					selection,
					&mut next_identity,
				)?
			} else {
				Vec::new()
			};
		channels.push(ResolvedChannel {
			binding: channel,
			domain,
			colors,
		});
	}
	let band_fills = resolve_explicit_band_fills(
		binding.kernel_fields.as_ref(),
		&channels,
		binding.categorical_limit,
		selection,
		&mut next_identity,
	)?;
	Ok((
		ResolvedPlotSpec {
			composition: binding.composition,
			channels,
			band_fills,
			kernel_fields: binding.kernel_fields,
			color_job,
			continuous_intent,
			provenance: result.provenance.clone(),
			stack_normalization: binding.stack_normalization,
		},
		next_identity,
	))
}

fn kernel_fields_from_channels(
	plot_type: PlotType,
	channels: &[BoundChannel],
) -> Result<ResolvedKernelFields, ChannelError> {
	let field = |channel| {
		channels
			.iter()
			.find(|binding| binding.channel == channel)
			.map(|binding| binding.field.clone())
	};
	let required = |channel| {
		field(channel).ok_or_else(|| ChannelError {
			attribution: FieldAttribution::Channel(channel),
			field: "<resolved>".to_owned(),
			reason: "required channel was not carried into the kernel contract".to_owned(),
		})
	};
	match plot_type {
		PlotType::Scatter => Ok(ResolvedKernelFields::Scatter {
			color: field(Channel::Color),
			x: required(Channel::X)?,
			y: required(Channel::Y)?,
		}),
		PlotType::Bar => Ok(ResolvedKernelFields::Bar {
			category: required(Channel::X)?,
			group: field(Channel::Color),
			value: required(Channel::Y)?,
		}),
		PlotType::HorizontalBar => Ok(ResolvedKernelFields::Bar {
			category: required(Channel::Y)?,
			group: field(Channel::Color),
			value: required(Channel::X)?,
		}),
		PlotType::StackedBar => Ok(ResolvedKernelFields::StackedBar {
			category: required(Channel::X)?,
			group: field(Channel::Color),
			value: required(Channel::Y)?,
		}),
		PlotType::HorizontalStackedBar => Ok(ResolvedKernelFields::StackedBar {
			category: required(Channel::Y)?,
			group: field(Channel::Color),
			value: required(Channel::X)?,
		}),
		PlotType::Line => Ok(ResolvedKernelFields::Line {
			series: field(Channel::Color),
			x: required(Channel::X)?,
			y: required(Channel::Y)?,
		}),
		PlotType::StackedArea => Ok(ResolvedKernelFields::StackedArea {
			series: field(Channel::Color),
			x: required(Channel::X)?,
			y: required(Channel::Y)?,
		}),
		_ => unreachable!(
			"only authored Scatter, Bar-family, Line, and Area channels reach this projection"
		),
	}
}

fn resolve_automatic_kernel_fields(
	plot_type: PlotType,
	schema: &Schema,
) -> Result<ResolvedKernelFields, ChannelError> {
	let nominal = schema
		.iter()
		.filter(|(_, dtype)| {
			matches!(
				semantic_type(dtype),
				Some(SemanticType::Nominal | SemanticType::Ordinal)
			)
		})
		.map(|(name, _)| name.as_str().to_owned())
		.collect::<Vec<_>>();
	let numeric = schema
		.iter()
		.filter(|(_, dtype)| {
			dtype.is_numeric() || matches!(semantic_type(dtype), Some(SemanticType::Temporal))
		})
		.map(|(name, _)| name.as_str().to_owned())
		.collect::<Vec<_>>();
	let number = |index: usize, role: &str| {
		numeric.get(index).cloned().ok_or_else(|| ChannelError {
			attribution: FieldAttribution::Channel(Channel::Y),
			field: format!("<{role}>"),
			reason: format!(
				"{plot_type} requires numeric field {} for {role}",
				index + 1
			),
		})
	};
	let category = |index: usize, role: &str| {
		nominal.get(index).cloned().ok_or_else(|| ChannelError {
			attribution: FieldAttribution::Channel(Channel::X),
			field: format!("<{role}>"),
			reason: format!(
				"{plot_type} requires categorical field {} for {role}",
				index + 1
			),
		})
	};
	let optional_category = |index: usize| nominal.get(index).cloned();

	Ok(match plot_type {
		PlotType::Violin => ResolvedKernelFields::Violin {
			category: optional_category(0),
			value: number(0, "value")?,
		},
		PlotType::Hexbin => ResolvedKernelFields::Hexbin {
			x: number(0, "x")?,
			y: number(1, "y")?,
		},
		PlotType::Line => ResolvedKernelFields::Line {
			series: optional_category(0),
			x: number(0, "x")?,
			y: number(1, "y")?,
		},
		PlotType::HorizontalBar | PlotType::Bar => ResolvedKernelFields::Bar {
			category: category(0, "category")?,
			group: optional_category(1),
			value: number(0, "value")?,
		},
		PlotType::Scatter => unreachable!("Scatter uses the authored channel resolver"),
		PlotType::StackedBar | PlotType::HorizontalStackedBar => ResolvedKernelFields::StackedBar {
			category: category(0, "category")?,
			group: optional_category(1),
			value: number(0, "value")?,
		},
		PlotType::Pie => ResolvedKernelFields::Pie {
			category: optional_category(0),
			value: number(0, "value")?,
		},
		PlotType::BoxPlot => {
			return Err(ChannelError {
				attribution: FieldAttribution::Channel(Channel::Y),
				field: "<box plot>".to_owned(),
				reason: "Box Plot has no producer; explicit Tukey summary fields are required"
					.to_owned(),
			});
		}
		PlotType::Histogram => {
			return Err(ChannelError {
				attribution: FieldAttribution::Channel(Channel::X),
				field: "<histogram>".to_owned(),
				reason: "Histogram Plot has no producer; explicit interval fields are required"
					.to_owned(),
			});
		}
		PlotType::Bubble => ResolvedKernelFields::Bubble {
			x: number(0, "x")?,
			y: number(1, "y")?,
			size: number(2, "size")?,
			color: number(3, "color")?,
			label: optional_category(0),
		},
		PlotType::Heatmap => ResolvedKernelFields::Heatmap {
			x: category(0, "x")?,
			y: category(1, "y")?,
			value: number(0, "value")?,
		},
		PlotType::Parallel => ResolvedKernelFields::Parallel {
			dimensions: (!numeric.is_empty())
				.then(|| numeric.clone())
				.ok_or_else(|| ChannelError {
					attribution: FieldAttribution::Channel(Channel::Y),
					field: "<dimensions>".to_owned(),
					reason: "Parallel requires at least one numeric dimension".to_owned(),
				})?,
			category: optional_category(0),
		},
		PlotType::Radar => ResolvedKernelFields::Radar {
			dimensions: (!numeric.is_empty())
				.then(|| numeric.clone())
				.ok_or_else(|| ChannelError {
					attribution: FieldAttribution::Channel(Channel::Y),
					field: "<dimensions>".to_owned(),
					reason: "Radar requires at least one numeric dimension".to_owned(),
				})?,
			category: optional_category(0),
		},
		PlotType::RadialDial => ResolvedKernelFields::RadialDial {
			label: optional_category(0),
			value: number(0, "value")?,
			maximum: number(1, "maximum")?,
		},
		PlotType::Candlestick => ResolvedKernelFields::Candlestick {
			x: category(0, "x")?,
			open: number(0, "open")?,
			high: number(1, "high")?,
			low: number(2, "low")?,
			close: number(3, "close")?,
		},
		PlotType::Funnel => ResolvedKernelFields::Funnel {
			stage: optional_category(0),
			value: number(0, "value")?,
		},
		PlotType::StackedArea => ResolvedKernelFields::StackedArea {
			series: optional_category(0),
			x: number(0, "x")?,
			y: number(1, "y")?,
		},
		PlotType::FillBetween => ResolvedKernelFields::FillBetween {
			x: number(0, "x")?,
			middle: number(1, "middle")?,
			lower: number(2, "lower")?,
			upper: number(3, "upper")?,
		},
	})
}

fn resolve_required(
	channel: Channel,
	spec: &AuthoredChannelSpec,
	plot_type: PlotType,
	layer: &LayerIdentity,
	pair_resolved_bar: bool,
	schema: &Schema,
	used_by_auto: &mut HashSet<String>,
) -> Result<BoundChannel, ChannelError> {
	let field = match &spec.field {
		AuthoredChannel::Named(field) => field.clone(),
		AuthoredChannel::Auto => {
			infer_field(channel, plot_type, schema, used_by_auto).ok_or_else(|| ChannelError {
				attribution: FieldAttribution::Channel(channel),
				field: "<auto>".to_owned(),
				reason: "no compatible unused field is available for explicit Auto".to_owned(),
			})?
		}
	};
	let dtype = schema.get(field.as_str()).ok_or_else(|| ChannelError {
		attribution: FieldAttribution::Channel(channel),
		field: field.clone(),
		reason: "field is absent from the returned schema".to_owned(),
	})?;
	let inferred_semantic = semantic_type(dtype).ok_or_else(|| ChannelError {
		attribution: FieldAttribution::Channel(channel),
		field: field.clone(),
		reason: format!("dtype {dtype:?} has no supported semantic type"),
	})?;
	let semantic_type = match spec.semantic_type {
		Override::Inherit => inferred_semantic,
		Override::Set(declared) => declared,
	};
	let pair_candidate = pair_resolved_bar
		&& matches!(channel, Channel::X | Channel::Y)
		&& plot_type == PlotType::Bar;
	if !pair_candidate && !semantic_is_compatible(plot_type, channel, semantic_type) {
		return Err(ChannelError {
			attribution: FieldAttribution::Channel(channel),
			field,
			reason: format!("{semantic_type:?} is incompatible with {plot_type:?}"),
		});
	}
	let default_scale = default_scale(plot_type, channel, semantic_type);
	let scale = match spec.scale {
		Override::Inherit => default_scale,
		Override::Set(scale) => scale,
	};
	if !scale_is_compatible(channel, semantic_type, scale) {
		return Err(ChannelError {
			attribution: FieldAttribution::Channel(channel),
			field,
			reason: format!("scale {scale:?} is incompatible with {semantic_type:?}"),
		});
	}
	// Explicitly repeated Named channels are legal, but Auto may never copy a
	// field that an earlier channel already resolved.
	used_by_auto.insert(field.clone());
	Ok(BoundChannel {
		channel,
		field,
		semantic_type,
		scale,
		layer: layer.clone(),
	})
}

fn resolve_optional(
	channel: Channel,
	spec: &OptionalAuthoredChannelSpec,
	plot_type: PlotType,
	layer: &LayerIdentity,
	pair_resolved_bar: bool,
	schema: &Schema,
	used_by_auto: &mut HashSet<String>,
) -> Result<Option<BoundChannel>, ChannelError> {
	let required = match &spec.field {
		OptionalAuthoredChannel::Absent => return Ok(None),
		OptionalAuthoredChannel::Named(field) => AuthoredChannelSpec {
			field: AuthoredChannel::Named(field.clone()),
			semantic_type: spec.semantic_type.clone(),
			scale: spec.scale.clone(),
		},
		OptionalAuthoredChannel::Auto => {
			let Some(field) = infer_field(channel, plot_type, schema, used_by_auto) else {
				return Ok(None);
			};
			AuthoredChannelSpec {
				field: AuthoredChannel::Named(field),
				semantic_type: spec.semantic_type.clone(),
				scale: spec.scale.clone(),
			}
		}
	};
	resolve_required(
		channel,
		&required,
		plot_type,
		layer,
		pair_resolved_bar,
		schema,
		used_by_auto,
	)
	.map(Some)
}

fn infer_field(
	channel: Channel,
	plot_type: PlotType,
	schema: &Schema,
	used: &HashSet<String>,
) -> Option<String> {
	schema.iter().find_map(|(name, dtype)| {
		let semantic = semantic_type(dtype)?;
		(!used.contains(name.as_str()) && semantic_is_compatible(plot_type, channel, semantic))
			.then(|| name.as_str().to_owned())
	})
}

fn infer_x_preserving_auto_y(plot_type: PlotType, schema: &Schema) -> Option<String> {
	if plot_type != PlotType::Scatter {
		return None;
	}
	let candidates = schema
		.iter()
		.filter_map(|(name, dtype)| {
			let semantic = semantic_type(dtype)?;
			semantic_is_compatible(plot_type, Channel::X, semantic)
				.then(|| name.as_str().to_owned())
		})
		.collect::<Vec<_>>();
	candidates.iter().find_map(|candidate| {
		let used = HashSet::from([candidate.clone()]);
		infer_field(Channel::Y, plot_type, schema, &used).map(|_| candidate.clone())
	})
}

fn semantic_type(dtype: &DataType) -> Option<SemanticType> {
	if dtype.is_numeric() {
		return Some(SemanticType::Quantitative);
	}
	match dtype {
		DataType::String
		| DataType::Categorical(_, _)
		| DataType::Enum(_, _)
		| DataType::Boolean => Some(SemanticType::Nominal),
		DataType::Date | DataType::Datetime(_, _) | DataType::Time => Some(SemanticType::Temporal),
		_ => None,
	}
}

fn semantic_is_compatible(plot_type: PlotType, channel: Channel, semantic: SemanticType) -> bool {
	match (plot_type, channel) {
		(PlotType::Scatter, Channel::X) => true,
		(PlotType::Scatter, Channel::Y) => {
			matches!(
				semantic,
				SemanticType::Quantitative | SemanticType::Temporal
			)
		}
		(PlotType::Bar, Channel::X) => !matches!(semantic, SemanticType::Quantitative),
		(PlotType::Bar, Channel::Y) => semantic == SemanticType::Quantitative,
		(PlotType::Line, Channel::X | Channel::Y) => {
			matches!(
				semantic,
				SemanticType::Quantitative | SemanticType::Temporal
			)
		}
		(PlotType::StackedArea, Channel::X) => {
			matches!(
				semantic,
				SemanticType::Quantitative | SemanticType::Temporal
			)
		}
		(PlotType::StackedArea, Channel::Y) => semantic == SemanticType::Quantitative,
		(PlotType::Histogram, Channel::X | Channel::Y) => semantic == SemanticType::Quantitative,
		(PlotType::BoxPlot, Channel::X) => {
			matches!(semantic, SemanticType::Nominal | SemanticType::Ordinal)
		}
		(PlotType::BoxPlot, Channel::Y) => semantic == SemanticType::Quantitative,
		(PlotType::Heatmap, Channel::X | Channel::Y) => semantic == SemanticType::Nominal,
		(PlotType::Bubble, Channel::X | Channel::Y) => semantic == SemanticType::Quantitative,
		(_, Channel::Color) => true,
		_ => false,
	}
}

fn default_scale(plot_type: PlotType, channel: Channel, semantic: SemanticType) -> ScaleFamily {
	if channel == Channel::Color {
		return match semantic {
			SemanticType::Nominal | SemanticType::Ordinal => ScaleFamily::CategoricalColor,
			SemanticType::Quantitative | SemanticType::Temporal => ScaleFamily::SequentialColor,
		};
	}
	match semantic {
		SemanticType::Nominal | SemanticType::Ordinal
			if matches!(
				plot_type,
				PlotType::Bar | PlotType::BoxPlot | PlotType::Heatmap
			) =>
		{
			ScaleFamily::Band
		}
		SemanticType::Nominal | SemanticType::Ordinal => ScaleFamily::Point,
		SemanticType::Quantitative => ScaleFamily::Linear,
		SemanticType::Temporal => ScaleFamily::Time,
	}
}

fn scale_is_compatible(channel: Channel, semantic: SemanticType, scale: ScaleFamily) -> bool {
	if channel == Channel::Color {
		return match semantic {
			SemanticType::Nominal | SemanticType::Ordinal => scale == ScaleFamily::CategoricalColor,
			SemanticType::Quantitative | SemanticType::Temporal => {
				matches!(
					scale,
					ScaleFamily::SequentialColor | ScaleFamily::DivergingColor
				)
			}
		};
	}
	match semantic {
		SemanticType::Nominal | SemanticType::Ordinal => {
			matches!(scale, ScaleFamily::Point | ScaleFamily::Band)
		}
		SemanticType::Quantitative => matches!(scale, ScaleFamily::Linear | ScaleFamily::Log),
		SemanticType::Temporal => scale == ScaleFamily::Time,
	}
}

fn default_color_job(semantic: SemanticType) -> ColorJob {
	match semantic {
		SemanticType::Nominal | SemanticType::Ordinal => ColorJob::Categorical,
		SemanticType::Quantitative | SemanticType::Temporal => ColorJob::Sequential,
	}
}

fn color_job_is_compatible(job: ColorJob, semantic: SemanticType) -> bool {
	match job {
		ColorJob::Categorical | ColorJob::Status => {
			matches!(semantic, SemanticType::Nominal | SemanticType::Ordinal)
		}
		ColorJob::Sequential => {
			matches!(
				semantic,
				SemanticType::Quantitative | SemanticType::Temporal
			)
		}
		ColorJob::Diverging => semantic == SemanticType::Quantitative,
	}
}

const fn color_job_scale(job: ColorJob) -> ScaleFamily {
	match job {
		ColorJob::Categorical | ColorJob::Status => ScaleFamily::CategoricalColor,
		ColorJob::Sequential => ScaleFamily::SequentialColor,
		ColorJob::Diverging => ScaleFamily::DivergingColor,
	}
}

const fn categorical_limit(plot_type: PlotType) -> usize {
	match plot_type {
		PlotType::Scatter | PlotType::Bar | PlotType::Line | PlotType::StackedArea => {
			DEFAULT_CATEGORICAL_LIMIT
		}
		_ => 0,
	}
}

fn resolve_domain(
	column: &polars::prelude::Column,
	channel: &BoundChannel,
) -> Result<ScaleDomain, DomainError> {
	match channel.semantic_type {
		SemanticType::Nominal | SemanticType::Ordinal => {
			let mut seen = HashSet::new();
			let values = column
				.as_materialized_series()
				.iter()
				.filter_map(|value| {
					let value = category_value(value)?;
					seen.insert(value.clone()).then_some(value)
				})
				.collect();
			Ok(ScaleDomain::Categorical(values))
		}
		SemanticType::Quantitative => {
			let cast = column
				.cast(&DataType::Float64)
				.map_err(|error| DomainError {
					channel: channel.channel,
					field: channel.field.clone(),
					reason: error.to_string(),
				})?;
			let values = cast.f64().map_err(|error| DomainError {
				channel: channel.channel,
				field: channel.field.clone(),
				reason: error.to_string(),
			})?;
			let min = values.into_iter().flatten().reduce(f64::min);
			let max = values.into_iter().flatten().reduce(f64::max);
			match (min, max) {
				(Some(min), Some(max)) => Ok(ScaleDomain::Quantitative { min, max }),
				_ => Ok(ScaleDomain::Empty),
			}
		}
		SemanticType::Temporal => {
			let cast = column.cast(&DataType::Int64).map_err(|error| DomainError {
				channel: channel.channel,
				field: channel.field.clone(),
				reason: error.to_string(),
			})?;
			let values = cast.i64().map_err(|error| DomainError {
				channel: channel.channel,
				field: channel.field.clone(),
				reason: error.to_string(),
			})?;
			let min = values.into_iter().flatten().min();
			let max = values.into_iter().flatten().max();
			match (min, max) {
				(Some(min), Some(max)) => Ok(ScaleDomain::Temporal { min, max }),
				_ => Ok(ScaleDomain::Empty),
			}
		}
	}
}

fn category_value(value: AnyValue<'_>) -> Option<String> {
	match value {
		AnyValue::Null => None,
		AnyValue::String(value) => Some(value.to_owned()),
		AnyValue::StringOwned(value) => Some(value.as_str().to_owned()),
		other => Some(other.to_string()),
	}
}

fn resolved_color(color: Rgb) -> ResolvedColor {
	ResolvedColor {
		red: color.r,
		green: color.g,
		blue: color.b,
		alpha: u8::MAX,
	}
}

fn evenly_spread_positions(domain_len: usize, palette_len: usize) -> Vec<PalettePosition> {
	match domain_len {
		0 => Vec::new(),
		1 => vec![
			PalettePosition::new(0, palette_len)
				.expect("a categorical palette is structurally non-empty"),
		],
		_ => (0..domain_len)
			.map(|index| {
				let numerator = index * (palette_len - 1);
				let denominator = domain_len - 1;
				let rounded = (numerator + denominator / 2) / denominator;
				PalettePosition::new(rounded, palette_len)
					.expect("even spread stays inside the palette")
			})
			.collect(),
	}
}

fn maximin_position(
	occupied: &HashSet<PalettePosition>,
	palette_len: usize,
) -> Option<PalettePosition> {
	(0..palette_len)
		.filter_map(|index| {
			let candidate = PalettePosition::new(index, palette_len).ok()?;
			(!occupied.contains(&candidate)).then(|| {
				let score = occupied
					.iter()
					.map(|position| position.index().abs_diff(index))
					.min()
					.unwrap_or(usize::MAX);
				(candidate, score)
			})
		})
		.fold(None, |best, candidate| match best {
			None => Some(candidate),
			Some((_, best_score)) if candidate.1 > best_score => Some(candidate),
			Some(best) => Some(best),
		})
		.map(|(position, _)| position)
}

fn assign_fresh_positions(
	categories: &[String],
	field: &str,
	layer: &ColorLayerIdentity,
	palette_len: usize,
	assignments: &mut HashMap<ColorIdentityKey, PalettePosition>,
) {
	for (category, position) in categories
		.iter()
		.zip(evenly_spread_positions(categories.len(), palette_len))
	{
		assignments.insert(
			ColorIdentityKey {
				field: field.to_owned(),
				layer: layer.clone(),
				category: category.clone(),
			},
			position,
		);
	}
}

fn resolve_explicit_positions(
	categories: &[String],
	channel: &BoundChannel,
	geometry_limit: usize,
	selection: &CategoricalPaletteSelection,
	identity: &mut ColorIdentity,
	role: &'static str,
) -> Result<Vec<(String, PalettePosition)>, CategoricalResolutionError> {
	let color_layer = ColorLayerIdentity::from_authored_layer(&channel.layer);
	let lineage = ColorLineageKey {
		field: channel.field.clone(),
		layer: color_layer.clone(),
	};
	let selected_space = ColorAllocationSpace::CategoricalPalette {
		len: selection.len(),
	};
	if let Some(existing) = identity.spaces.get(&lineage) {
		if *existing != selected_space {
			return Err(CategoricalResolutionError::IncompatibleAllocationSpace {
				field: channel.field.clone(),
				layer: channel.layer.clone(),
				existing_len: existing.len(),
				selected_len: selection.len(),
			});
		}
	}

	let retained = identity
		.assignments
		.keys()
		.filter(|key| key.field == channel.field && key.layer == color_layer)
		.count();
	let missing = categories
		.iter()
		.filter(|category| {
			!identity.assignments.contains_key(&ColorIdentityKey {
				field: channel.field.clone(),
				layer: color_layer.clone(),
				category: (*category).clone(),
			})
		})
		.count();
	let requested = retained.saturating_add(missing);
	let geometry_exceeded = requested > geometry_limit;
	let palette_exceeded = requested > selection.len();
	if geometry_exceeded || palette_exceeded {
		return Err(CategoricalResolutionError::Capacity(
			CategoricalCapacityFailure {
				channel: channel.channel,
				field: channel.field.clone(),
				layer: channel.layer.clone(),
				role,
				retained,
				requested,
				geometry_limit,
				palette_identity: selection.identity.clone(),
				palette_limit: selection.len(),
				geometry_exceeded,
				palette_exceeded,
			},
		));
	}

	identity.spaces.entry(lineage).or_insert(selected_space);
	if retained == 0 {
		assign_fresh_positions(
			categories,
			&channel.field,
			&color_layer,
			selection.len(),
			&mut identity.assignments,
		);
	} else {
		let mut occupied = identity
			.assignments
			.iter()
			.filter(|(key, _)| key.field == channel.field && key.layer == color_layer)
			.map(|(_, position)| *position)
			.collect::<HashSet<_>>();
		for category in categories {
			let key = ColorIdentityKey {
				field: channel.field.clone(),
				layer: color_layer.clone(),
				category: category.clone(),
			};
			if !identity.assignments.contains_key(&key) {
				let position = maximin_position(&occupied, selection.len())
					.expect("capacity validation guarantees an unoccupied position");
				occupied.insert(position);
				identity.assignments.insert(key, position);
			}
		}
	}

	Ok(categories
		.iter()
		.map(|category| {
			let key = ColorIdentityKey {
				field: channel.field.clone(),
				layer: color_layer.clone(),
				category: category.clone(),
			};
			(category.clone(), identity.assignments[&key])
		})
		.collect())
}

fn resolve_explicit_category_colors(
	categories: &[String],
	channel: &BoundChannel,
	geometry_limit: usize,
	selection: &CategoricalPaletteSelection,
	identity: &mut ColorIdentity,
) -> Result<Vec<CategoryColorAssignment>, CategoricalResolutionError> {
	resolve_explicit_positions(
		categories,
		channel,
		geometry_limit,
		selection,
		identity,
		"color channel",
	)
	.map(|assignments| {
		assignments
			.into_iter()
			.map(|(category, position)| CategoryColorAssignment {
				category,
				position,
				color: selection.color(position),
			})
			.collect()
	})
}

fn resolve_explicit_band_fills(
	kernel_fields: &ResolvedKernelFields,
	channels: &[ResolvedChannel],
	geometry_limit: usize,
	selection: &CategoricalPaletteSelection,
	identity: &mut ColorIdentity,
) -> Result<Vec<BandFillAssignment>, CategoricalResolutionError> {
	let category = match kernel_fields {
		ResolvedKernelFields::Bar { category, .. }
		| ResolvedKernelFields::StackedBar { category, .. } => category,
		_ => return Ok(Vec::new()),
	};
	if channels
		.iter()
		.any(|channel| channel.binding.channel == Channel::Color)
	{
		return Ok(Vec::new());
	}
	let Some(channel) = channels
		.iter()
		.find(|channel| channel.binding.field == *category)
	else {
		return Ok(Vec::new());
	};
	let ScaleDomain::Categorical(categories) = &channel.domain else {
		return Ok(Vec::new());
	};
	resolve_explicit_positions(
		categories,
		&channel.binding,
		geometry_limit,
		selection,
		identity,
		"bar band fill",
	)
	.map(|assignments| {
		assignments
			.into_iter()
			.map(|(category, position)| BandFillAssignment {
				category,
				position,
				color: selection.color(position),
			})
			.collect()
	})
}

fn resolve_category_colors(
	categories: &[String],
	channel: &BoundChannel,
	limit: usize,
	identity: &mut ColorIdentity,
) -> Result<Vec<CategoryColorAssignment>, DomainError> {
	let color_layer = ColorLayerIdentity::from_authored_layer(&channel.layer);
	let lineage = ColorLineageKey {
		field: channel.field.clone(),
		layer: color_layer.clone(),
	};
	let expected_space = ColorAllocationSpace::Legacy {
		len: DEFAULT_CATEGORICAL_LIMIT,
	};
	if let Some(actual) = identity.spaces.get(&lineage) {
		if *actual != expected_space {
			return Err(DomainError {
				channel: channel.channel,
				field: channel.field.clone(),
				reason: format!(
					"categorical continuation uses an incompatible allocation space of length {}",
					actual.len()
				),
			});
		}
	} else {
		identity.spaces.insert(lineage, expected_space);
	}
	let lineage_count = identity
		.assignments
		.keys()
		.filter(|key| key.field == channel.field && key.layer == color_layer)
		.count();
	let missing = categories
		.iter()
		.filter(|category| {
			!identity.assignments.contains_key(&ColorIdentityKey {
				field: channel.field.clone(),
				layer: color_layer.clone(),
				category: (*category).clone(),
			})
		})
		.count();
	if lineage_count.saturating_add(missing) > limit {
		return Err(DomainError {
			channel: channel.channel,
			field: channel.field.clone(),
			reason: format!("categorical cardinality exceeds declared limit {limit}"),
		});
	}
	for category in categories {
		let key = ColorIdentityKey {
			field: channel.field.clone(),
			layer: color_layer.clone(),
			category: category.clone(),
		};
		if !identity.assignments.contains_key(&key) {
			let ordinal = identity
				.assignments
				.keys()
				.filter(|candidate| {
					candidate.field == channel.field && candidate.layer == color_layer
				})
				.count();
			let position = PalettePosition::new(ordinal, DEFAULT_CATEGORICAL_LIMIT)
				.expect("legacy capacity was checked before assignment");
			identity.assignments.insert(key, position);
		}
	}
	Ok(categories
		.iter()
		.map(|category| {
			let key = ColorIdentityKey {
				field: channel.field.clone(),
				layer: color_layer.clone(),
				category: category.clone(),
			};
			CategoryColorAssignment {
				category: category.clone(),
				position: identity.assignments[&key],
				color: categorical_color(identity.assignments[&key].index()),
			}
		})
		.collect())
}

fn categorical_color(index: usize) -> ResolvedColor {
	const COLORS: [(u8, u8, u8); DEFAULT_CATEGORICAL_LIMIT] = [
		(76, 120, 168),
		(242, 142, 43),
		(225, 87, 89),
		(118, 183, 178),
		(89, 161, 79),
		(237, 201, 72),
		(176, 122, 161),
		(255, 157, 167),
		(156, 117, 95),
		(186, 176, 172),
		(31, 119, 180),
		(255, 127, 14),
	];
	let (red, green, blue) = COLORS[index];
	ResolvedColor {
		red,
		green,
		blue,
		alpha: u8::MAX,
	}
}

#[cfg(test)]
impl AuthoredPlotSpec {
	pub(crate) fn with_mark_fields_for_test(mut self, fields: MarkFields) -> Self {
		self.mark_fields = Some(fields);
		self
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use polars::df;
	use polars::prelude::{Column, DataFrame, IntoColumn, NamedFrom};

	fn stamped(data: DataFrame) -> StampedResult {
		StampedResult {
			data: crate::result_artifact::ResultArtifact::test_frame(data),
			provenance: PlotProvenance {
				source_name: "fixture".to_owned(),
				revision: Some(7),
			},
		}
	}

	#[test]
	fn product_0078_automatic_histogram_resolution_names_the_missing_producer() {
		let frame = df!("value" => [1.0_f64, 2.0]).expect("histogram schema fixture");
		let error = resolve_automatic_kernel_fields(PlotType::Histogram, frame.schema().as_ref())
			.expect_err("histogram has no automatic raw-value producer");
		assert_eq!(error.field, "<histogram>");
		assert!(error.reason.contains("Histogram Plot has no producer"));
		assert!(
			error
				.reason
				.contains("explicit interval fields are required")
		);
	}

	#[test]
	fn product_0081_automatic_box_plot_resolution_names_the_missing_producer() {
		let frame = df!(
			"category" => ["A", "B"],
			"value" => [1.0_f64, 2.0]
		)
		.expect("box plot schema fixture");
		let error = resolve_automatic_kernel_fields(PlotType::BoxPlot, frame.schema().as_ref())
			.expect_err("box plot has no automatic raw-value producer");
		assert_eq!(error.field, "<box plot>");
		assert!(error.reason.contains("Box Plot has no producer"));
		assert!(
			error
				.reason
				.contains("explicit Tukey summary fields are required")
		);
	}

	fn heatmap_spec() -> AuthoredPlotSpec {
		AuthoredPlotSpec::heatmap(HeatmapFields {
			x: "region".to_owned(),
			y: "quarter".to_owned(),
			value: "margin".to_owned(),
		})
	}

	#[test]
	fn product_0092_heatmap_binds_nominal_bands_and_quantitative_value() {
		for values in [
			Column::new("margin".into(), [1_i64, 2_i64])
				.cast(&DataType::Int64)
				.unwrap(),
			Column::new("margin".into(), [1.0_f64, 2.0_f64]),
		] {
			let frame = DataFrame::new(
				2,
				vec![
					Column::new("region".into(), ["north", "south"]),
					Column::new("quarter".into(), ["q1", "q2"]),
					values,
				],
			)
			.unwrap();
			let binding = resolve_channels(&heatmap_spec(), frame.schema().as_ref())
				.expect("authored heatmap binds");
			assert_eq!(binding.channels.len(), 2);
			assert!(binding.channels.iter().all(|channel| {
				channel.semantic_type == SemanticType::Nominal && channel.scale == ScaleFamily::Band
			}));
			assert_eq!(binding.color_job, Some(ColorJob::Sequential));
			assert_eq!(
				binding.continuous_anchor,
				crate::plot::continuous_color::ContinuousAnchor::None
			);
			assert!(matches!(
				binding.kernel_fields.as_ref(),
				ResolvedKernelFields::Heatmap { x, y, value }
					if (x.as_str(), y.as_str(), value.as_str()) == ("region", "quarter", "margin")
			));
		}
	}

	#[test]
	fn product_0092_heatmap_compatibility_refuses_axes_values_and_ordinal_precisely() {
		let categorical = df!(
			"region" => ["north"],
			"quarter" => ["q1"],
			"margin" => [1.0_f64],
		)
		.unwrap();
		let mut ordinal = heatmap_spec();
		ordinal.x.semantic_type = Override::Set(SemanticType::Ordinal);
		let error = resolve_channels(&ordinal, categorical.schema().as_ref()).unwrap_err();
		assert_eq!(error.attribution, FieldAttribution::Channel(Channel::X));
		assert_eq!(error.field, "region");
		assert!(error.reason.contains("Ordinal") && error.reason.contains("Heatmap"));

		let numeric_axis = df!(
			"region" => [1_i64],
			"quarter" => ["q1"],
			"margin" => [1.0_f64],
		)
		.unwrap();
		let error = resolve_channels(&heatmap_spec(), numeric_axis.schema().as_ref()).unwrap_err();
		assert_eq!(
			(error.attribution, error.field.as_str()),
			(FieldAttribution::Channel(Channel::X), "region")
		);

		for value in [
			Column::new("margin".into(), ["high"]),
			Column::new("margin".into(), [true]),
		] {
			let frame = DataFrame::new(
				1,
				vec![
					Column::new("region".into(), ["north"]),
					Column::new("quarter".into(), ["q1"]),
					value,
				],
			)
			.unwrap();
			let error = resolve_channels(&heatmap_spec(), frame.schema().as_ref()).unwrap_err();
			assert_eq!(
				(error.attribution, error.field.as_str()),
				(FieldAttribution::Channel(Channel::Color), "margin")
			);
			assert!(error.reason.contains("quantitative"));
		}

		let missing = df!("region" => ["north"], "quarter" => ["q1"]).unwrap();
		let error = resolve_channels(&heatmap_spec(), missing.schema().as_ref()).unwrap_err();
		assert_eq!(
			(error.attribution, error.field.as_str()),
			(FieldAttribution::Channel(Channel::Color), "margin")
		);
		assert!(error.reason.contains("absent"));
	}

	#[test]
	fn product_0092_heatmap_surface_emits_only_inherited_semantic_types() {
		let spec = heatmap_spec();
		assert_eq!(spec.x.semantic_type, Override::Inherit);
		assert_eq!(spec.y.semantic_type, Override::Inherit);
		assert_eq!(spec.color.semantic_type, Override::Inherit);
		assert_eq!(spec.color_job, Override::Inherit);
	}

	#[test]
	fn product_0092_heatmap_unsupported_dtype_refuses_before_preparation() {
		let schema: Schema = [
			("region".into(), DataType::String),
			("quarter".into(), DataType::String),
			("margin".into(), DataType::List(Box::new(DataType::Float64))),
		]
		.into_iter()
		.collect();
		let error = resolve_channels(&heatmap_spec(), &schema).unwrap_err();
		assert_eq!(
			(error.attribution, error.field.as_str()),
			(FieldAttribution::Channel(Channel::Color), "margin")
		);
		assert!(error.reason.contains("no supported semantic type"));
	}

	fn bubble_spec(label: Option<&str>) -> AuthoredPlotSpec {
		AuthoredPlotSpec::bubble(BubbleFields::new(
			BubbleFieldNames::new("x", "y", "size", "color"),
			label.map(str::to_owned),
		))
	}

	fn bubble_frame(label: Column) -> DataFrame {
		DataFrame::new(
			2,
			vec![
				Column::new("x".into(), [1.0_f64, 2.0]),
				Column::new("y".into(), [3_i64, 4]),
				Column::new("size".into(), [10.0_f64, 100.0]),
				Column::new("color".into(), [-30.0_f64, 40.0]),
				label,
			],
		)
		.unwrap()
	}

	#[test]
	fn product_0095_bubble_binds_four_or_five_roles_without_categorical_identity() {
		let frame = bubble_frame(Column::new("label".into(), ["first", "second"]));
		for label in [None, Some("label")] {
			let spec = bubble_spec(label);
			assert!(matches!(spec.color.field, OptionalAuthoredChannel::Absent));
			assert_eq!(spec.x.semantic_type, Override::Inherit);
			assert_eq!(spec.y.semantic_type, Override::Inherit);
			let binding = resolve_channels(&spec, frame.schema().as_ref())
				.expect("authored bubble roles bind");
			assert!(binding.channels.iter().all(|channel| {
				channel.semantic_type == SemanticType::Quantitative
					&& channel.scale == ScaleFamily::Linear
			}));
			assert_eq!(binding.color_job, Some(ColorJob::Sequential));
			assert_eq!(
				binding.continuous_anchor,
				crate::plot::continuous_color::ContinuousAnchor::None
			);
			assert!(matches!(
				binding.kernel_fields.as_ref(),
				ResolvedKernelFields::Bubble { x, y, size, color, label: actual }
					if (x.as_str(), y.as_str(), size.as_str(), color.as_str(), actual.as_deref())
						== ("x", "y", "size", "color", label)
			));
		}
	}

	#[test]
	fn product_0095_bubble_resolution_does_not_allocate_colour_identity() {
		let categories = vec!["alpha".to_owned(), "beta".to_owned()];
		let (_, seeded) = resolve(
			&legacy_category_spec(),
			&legacy_category_frame(&categories),
			&ColorIdentity::default(),
		)
		.expect("categorical fixture seeds identity");
		let before = seeded.clone();
		let frame = stamped(bubble_frame(Column::new(
			"label".into(),
			["first", "second"],
		)));
		let (_, after) = resolve(&bubble_spec(Some("label")), &frame, &seeded)
			.expect("authored bubble resolves");
		assert_eq!(after, before);
	}

	#[test]
	fn product_0095_bubble_compatibility_is_quantitative_numeric_and_nominal_label() {
		let valid = bubble_frame(Column::new("label".into(), ["first", "second"]));
		for axis in ["x", "y"] {
			let mut columns = valid.columns().to_vec();
			let index = valid.get_column_index(axis).unwrap();
			columns[index] = Column::new(axis.into(), ["low", "high"]);
			let frame = DataFrame::new(2, columns).unwrap();
			let error =
				resolve_channels(&bubble_spec(Some("label")), frame.schema().as_ref()).unwrap_err();
			assert_eq!(
				error.attribution,
				FieldAttribution::Channel(if axis == "x" { Channel::X } else { Channel::Y })
			);
		}
		for (role, bad) in [
			(
				BubblePreparationChannel::Size,
				Column::new("size".into(), ["small", "large"]),
			),
			(
				BubblePreparationChannel::Size,
				Column::new("size".into(), [true, false]),
			),
			(
				BubblePreparationChannel::Color,
				Column::new("color".into(), ["cold", "hot"]),
			),
			(
				BubblePreparationChannel::Color,
				Column::new("color".into(), [true, false]),
			),
		] {
			let mut columns = valid.columns().to_vec();
			let index = valid.get_column_index(role.as_str()).unwrap();
			columns[index] = bad;
			let frame = DataFrame::new(2, columns).unwrap();
			let error =
				resolve_channels(&bubble_spec(Some("label")), frame.schema().as_ref()).unwrap_err();
			assert_eq!(error.attribution, FieldAttribution::Role(role.as_str()));
			assert!(error.reason.contains("Quantitative"));
		}
		let numeric_label = bubble_frame(Column::new("label".into(), [1_i64, 2]));
		let error = resolve_channels(&bubble_spec(Some("label")), numeric_label.schema().as_ref())
			.unwrap_err();
		assert_eq!(
			error.attribution,
			FieldAttribution::Role(BubblePreparationChannel::Label.as_str())
		);
		assert!(error.reason.contains("Nominal"));
	}

	#[test]
	fn product_0095_field_attribution_preserves_channels_and_distinguishes_roles() {
		let channel = ChannelError {
			attribution: FieldAttribution::Channel(Channel::X),
			field: "x".to_owned(),
			reason: "reason".to_owned(),
		};
		let role = ChannelError {
			attribution: FieldAttribution::Role(BubblePreparationChannel::Size.as_str()),
			field: "size".to_owned(),
			reason: "reason".to_owned(),
		};
		assert_eq!(
			channel.to_string(),
			"plot x channel `x` could not resolve: reason"
		);
		assert_eq!(
			role.to_string(),
			"plot size field `size` could not resolve: reason"
		);
		assert_ne!(channel.attribution, role.attribution);
		for role in [
			BubblePreparationChannel::X,
			BubblePreparationChannel::Y,
			BubblePreparationChannel::Size,
			BubblePreparationChannel::Color,
			BubblePreparationChannel::Label,
		] {
			assert_eq!(role.to_string(), role.as_str());
		}
	}

	fn resolve(
		spec: &AuthoredPlotSpec,
		result: &StampedResult,
		identity: &ColorIdentity,
	) -> Result<(ResolvedPlotSpec, ColorIdentity), String> {
		let frame = result.data.projection().expect("fixture projection");
		let binding =
			resolve_channels(spec, frame.schema().as_ref()).map_err(|error| error.to_string())?;
		resolve_domains(binding, result, identity).map_err(|error| error.to_string())
	}

	fn legacy_category_frame(categories: &[String]) -> StampedResult {
		stamped(
			DataFrame::new(
				categories.len(),
				vec![
					polars::prelude::Column::new("x".into(), vec![1.0; categories.len()]),
					polars::prelude::Column::new("y".into(), vec![2.0; categories.len()]),
					polars::prelude::Column::new("series".into(), categories.to_vec()),
				],
			)
			.expect("legacy categorical frame"),
		)
	}

	fn legacy_category_spec() -> AuthoredPlotSpec {
		AuthoredPlotSpec::xy_color(
			PlotComposition::single(PlotType::Scatter),
			"x",
			"y",
			Some("series".to_owned()),
		)
	}

	fn format_category_colors(assignments: &[CategoryColorAssignment]) -> String {
		assignments
			.iter()
			.map(|assignment| {
				format!(
					"{}=#{:02X}{:02X}{:02X}{:02X}",
					assignment.category,
					assignment.color.red,
					assignment.color.green,
					assignment.color.blue,
					assignment.color.alpha
				)
			})
			.collect::<Vec<_>>()
			.join(",")
	}

	#[test]
	fn product_0029_pre_refactor_legacy_categorical_oracle() {
		let spec = legacy_category_spec();
		let mut actual = String::new();
		for cardinality in 0..=DEFAULT_CATEGORICAL_LIMIT {
			let categories = (0..cardinality)
				.map(|index| format!("c{index}"))
				.collect::<Vec<_>>();
			let (resolved, _) = resolve(
				&spec,
				&legacy_category_frame(&categories),
				&ColorIdentity::default(),
			)
			.expect("legacy domain resolves");
			let colors = &resolved.channel(Channel::Color).expect("color").colors;
			actual.push_str(&format!(
				"n={cardinality} {}\n",
				format_category_colors(colors)
			));
		}

		let first = vec!["a".to_owned(), "b".to_owned()];
		let (first_spec, identity) = resolve(
			&spec,
			&legacy_category_frame(&first),
			&ColorIdentity::default(),
		)
		.expect("first live result");
		actual.push_str(&format!(
			"live=first {}\n",
			format_category_colors(&first_spec.channel(Channel::Color).unwrap().colors)
		));
		let filtered = vec!["b".to_owned()];
		let (filtered_spec, identity) =
			resolve(&spec, &legacy_category_frame(&filtered), &identity)
				.expect("filtered live result");
		actual.push_str(&format!(
			"live=filtered {} identity={}\n",
			format_category_colors(&filtered_spec.channel(Channel::Color).unwrap().colors),
			identity.len()
		));
		let returned = vec!["a".to_owned(), "b".to_owned()];
		let (returned_spec, _) = resolve(&spec, &legacy_category_frame(&returned), &identity)
			.expect("returning category");
		actual.push_str(&format!(
			"live=returned {}\n",
			format_category_colors(&returned_spec.channel(Channel::Color).unwrap().colors)
		));

		let overflow = (0..=DEFAULT_CATEGORICAL_LIMIT)
			.map(|index| format!("c{index}"))
			.collect::<Vec<_>>();
		let error = resolve(
			&spec,
			&legacy_category_frame(&overflow),
			&ColorIdentity::default(),
		)
		.expect_err("legacy capacity");
		actual.push_str(&format!("overflow={error}\n"));

		assert_eq!(
			actual,
			include_str!("snapshots/product-0029-legacy-categorical.txt")
		);
	}

	#[test]
	fn product_0029_palette_positions_are_validated_continuation_identity() {
		assert_eq!(PalettePosition::new(0, 1).unwrap().index(), 0);
		assert_eq!(
			PalettePosition::new(0, 0),
			Err(PalettePositionError {
				index: 0,
				palette_len: 0,
			})
		);
		assert_eq!(
			PalettePosition::new(12, 12),
			Err(PalettePositionError {
				index: 12,
				palette_len: 12,
			})
		);

		let categories = vec!["a".to_owned(), "b".to_owned()];
		let (resolved, identity) = resolve(
			&legacy_category_spec(),
			&legacy_category_frame(&categories),
			&ColorIdentity::default(),
		)
		.expect("legacy resolution through positions");
		assert_eq!(
			identity
				.assignments
				.values()
				.map(|position| position.index())
				.collect::<HashSet<_>>(),
			HashSet::from([0, 1])
		);
		assert_eq!(
			resolved.channel(Channel::Color).unwrap().colors,
			[
				CategoryColorAssignment {
					category: "a".to_owned(),
					position: PalettePosition::new(0, 12).unwrap(),
					color: categorical_color(0),
				},
				CategoryColorAssignment {
					category: "b".to_owned(),
					position: PalettePosition::new(1, 12).unwrap(),
					color: categorical_color(1),
				},
			]
		);
	}

	#[test]
	fn product_0086_authored_family_is_a_discriminating_colour_lineage_dimension() {
		let scatter = ColorLayerIdentity {
			ordinal: 0,
			family: AuthoredFamilyId::SCATTER,
		};
		let line = ColorLayerIdentity {
			ordinal: 0,
			family: AuthoredFamilyId::LINE,
		};
		let mut assignments = HashMap::new();
		assign_fresh_positions(
			&["A".into(), "B".into(), "C".into()],
			"series",
			&scatter,
			5,
			&mut assignments,
		);
		assign_fresh_positions(
			&["C".into(), "A".into()],
			"series",
			&line,
			5,
			&mut assignments,
		);
		let position = |layer: &ColorLayerIdentity, category: &str| {
			assignments[&ColorIdentityKey {
				field: "series".into(),
				layer: layer.clone(),
				category: category.into(),
			}]
				.index()
		};
		assert_eq!(
			[
				position(&scatter, "A"),
				position(&scatter, "B"),
				position(&scatter, "C")
			],
			[0, 2, 4]
		);
		assert_eq!([position(&line, "C"), position(&line, "A")], [0, 4]);
		assert_ne!(position(&scatter, "C"), position(&line, "C"));
		assert_ne!(position(&scatter, "A"), position(&line, "A"));
	}

	#[test]
	fn product_0086_live_family_catalog_is_unique_complete_and_member_normalized() {
		let tokens = AuthoredFamilyId::ALL
			.iter()
			.map(AuthoredFamilyId::as_str)
			.collect::<Vec<_>>();
		assert_eq!(tokens, ["scatter", "bar", "line", "area"]);
		assert_eq!(
			tokens.iter().copied().collect::<HashSet<_>>().len(),
			tokens.len()
		);
		for (plot_type, expected) in [
			(PlotType::Scatter, "scatter"),
			(PlotType::Bar, "bar"),
			(PlotType::HorizontalBar, "bar"),
			(PlotType::Line, "line"),
			(PlotType::StackedArea, "area"),
		] {
			let actual = ColorLayerIdentity::from_authored_layer(&LayerIdentity {
				ordinal: 7,
				plot_type,
			});
			assert_eq!(actual.ordinal, 7);
			assert_eq!(actual.family.as_str(), expected);
			assert!(tokens.contains(&expected));
		}
	}

	#[test]
	fn product_0086_family_tokens_are_bounded_canonical_and_open_for_preservation() {
		for valid in ["a", &"a".repeat(64), "future_family_9"] {
			assert_eq!(
				AuthoredFamilyId::parse(valid.to_owned()).unwrap().as_str(),
				valid
			);
		}
		for invalid in [
			"".to_owned(),
			"a".repeat(65),
			"é".to_owned(),
			"Area".to_owned(),
			"9area".to_owned(),
			"two words".to_owned(),
			"two-words".to_owned(),
			"two.words".to_owned(),
		] {
			assert!(AuthoredFamilyId::parse(invalid).is_err());
		}
	}

	#[test]
	fn product_0086_forbidden_members_hit_the_authored_family_invariant() {
		let valid = ColorLayerIdentity::from_authored_layer(&LayerIdentity {
			ordinal: 0,
			plot_type: PlotType::Bar,
		});
		assert_eq!(valid.family, AuthoredFamilyId::BAR);
		for plot_type in [PlotType::StackedBar, PlotType::HorizontalStackedBar] {
			let panic = std::panic::catch_unwind(|| {
				ColorLayerIdentity::from_authored_layer(&LayerIdentity {
					ordinal: 0,
					plot_type,
				})
			})
			.expect_err("stacked renderer member must not reach authored conversion");
			let message = panic
				.downcast_ref::<&str>()
				.copied()
				.or_else(|| panic.downcast_ref::<String>().map(String::as_str))
				.unwrap_or("non-string panic");
			assert!(message.contains("PRODUCT-0064"), "{message}");
		}
		let unregistered = std::panic::catch_unwind(|| {
			ColorLayerIdentity::from_authored_layer(&LayerIdentity {
				ordinal: 0,
				plot_type: PlotType::Pie,
			})
		})
		.expect_err("unregistered authored family must expose the missing conversion");
		let message = unregistered
			.downcast_ref::<&str>()
			.copied()
			.or_else(|| unregistered.downcast_ref::<String>().map(String::as_str))
			.unwrap_or("non-string panic");
		assert!(message.contains("no live authored categorical-family conversion"));
	}

	fn resolve_explicit(
		spec: &AuthoredPlotSpec,
		result: &StampedResult,
		identity: &ColorIdentity,
		palette: &str,
	) -> Result<(ResolvedPlotSpec, ColorIdentity), CategoricalResolutionError> {
		let frame = result.data.projection().expect("fixture projection");
		let binding = resolve_channels(spec, frame.schema().as_ref())
			.expect("explicit categorical channel binding");
		let selection = CategoricalPaletteSelection::from_theme_name(palette)
			.expect("categorical palette selection");
		resolve_domains_with_categorical_palette(binding, result, identity, &selection)
	}

	fn position_color_mapping(resolved: &ResolvedPlotSpec) -> Vec<(String, usize, ResolvedColor)> {
		resolved
			.channel(Channel::Color)
			.expect("color channel")
			.colors
			.iter()
			.map(|assignment| {
				(
					assignment.category.clone(),
					assignment.position.index(),
					assignment.color,
				)
			})
			.collect()
	}

	#[test]
	fn product_0029_fresh_tab20c_spreads_complete_mapping_and_observes_modifiers() {
		let categories = ["A", "B", "C", "D", "E", "F"]
			.into_iter()
			.map(str::to_owned)
			.collect::<Vec<_>>();
		let result = legacy_category_frame(&categories);
		let spec = legacy_category_spec();
		let expected_positions = [0, 4, 8, 11, 15, 19];
		let expected_base = [
			ResolvedColor {
				red: 49,
				green: 130,
				blue: 189,
				alpha: 255,
			},
			ResolvedColor {
				red: 230,
				green: 85,
				blue: 13,
				alpha: 255,
			},
			ResolvedColor {
				red: 49,
				green: 163,
				blue: 84,
				alpha: 255,
			},
			ResolvedColor {
				red: 199,
				green: 233,
				blue: 192,
				alpha: 255,
			},
			ResolvedColor {
				red: 218,
				green: 218,
				blue: 235,
				alpha: 255,
			},
			ResolvedColor {
				red: 150,
				green: 150,
				blue: 150,
				alpha: 255,
			},
		];

		for palette in ["tab20c", "tab20c_r", "tab20c_s1"] {
			let (resolved, _) =
				resolve_explicit(&spec, &result, &ColorIdentity::default(), palette)
					.expect("fresh categorical palette");
			let mapping = position_color_mapping(&resolved);
			assert_eq!(
				mapping
					.iter()
					.map(|(category, position, _)| (category.as_str(), *position))
					.collect::<Vec<_>>(),
				["A", "B", "C", "D", "E", "F"]
					.into_iter()
					.zip(expected_positions)
					.collect::<Vec<_>>()
			);
			let authority = Theme::try_new(palette).unwrap();
			let authority = authority.categorical_palette().unwrap();
			assert_eq!(
				mapping
					.iter()
					.map(|(_, _, color)| *color)
					.collect::<Vec<_>>(),
				expected_positions
					.into_iter()
					.map(|position| resolved_color(authority.color(position).unwrap()))
					.collect::<Vec<_>>()
			);
			if palette == "tab20c" {
				assert_eq!(
					mapping
						.iter()
						.map(|(_, _, color)| *color)
						.collect::<Vec<_>>(),
					expected_base
				);
			}
		}
	}

	#[test]
	fn product_0029_fresh_spread_covers_admitted_sizes_and_beats_greedy_control() {
		assert!(evenly_spread_positions(0, 20).is_empty());
		assert_eq!(evenly_spread_positions(1, 20)[0].index(), 0);
		for domain_len in 1..=12 {
			let positions = evenly_spread_positions(domain_len, 20);
			assert_eq!(positions.len(), domain_len);
			assert!(
				positions
					.windows(2)
					.all(|pair| pair[0].index() < pair[1].index())
			);
		}
		let spread = evenly_spread_positions(6, 20)
			.into_iter()
			.map(PalettePosition::index)
			.collect::<Vec<_>>();
		assert_eq!(spread, [0, 4, 8, 11, 15, 19]);
		assert_eq!(
			spread.windows(2).map(|pair| pair[1] - pair[0]).min(),
			Some(3)
		);

		let mut greedy = HashSet::new();
		let mut chosen = Vec::new();
		for _ in 0..6 {
			let next = maximin_position(&greedy, 20).unwrap();
			greedy.insert(next);
			chosen.push(next.index());
		}
		assert_eq!(chosen, [0, 19, 9, 14, 4, 2]);
		let mut sorted = chosen;
		sorted.sort_unstable();
		assert_eq!(
			sorted.windows(2).map(|pair| pair[1] - pair[0]).min(),
			Some(2)
		);
	}

	#[test]
	fn product_0029_live_growth_uses_returned_identity_and_fresh_control_differs() {
		let spec = legacy_category_spec();
		let mut identity = ColorIdentity::default();
		for (categories, expected) in [
			(vec!["A"], vec![("A", 0)]),
			(vec!["A", "B"], vec![("A", 0), ("B", 19)]),
			(vec!["A", "B", "C"], vec![("A", 0), ("B", 19), ("C", 9)]),
		] {
			let categories = categories
				.into_iter()
				.map(str::to_owned)
				.collect::<Vec<_>>();
			let (resolved, next) = resolve_explicit(
				&spec,
				&legacy_category_frame(&categories),
				&identity,
				"tab20c",
			)
			.expect("live categorical resolution");
			assert_eq!(
				position_color_mapping(&resolved)
					.into_iter()
					.map(|(category, position, _)| (category, position))
					.collect::<Vec<_>>(),
				expected
					.into_iter()
					.map(|(category, position)| (category.to_owned(), position))
					.collect::<Vec<_>>()
			);
			identity = next;
		}

		let fresh = ["A", "B", "C"]
			.into_iter()
			.map(str::to_owned)
			.collect::<Vec<_>>();
		let (resolved, _) = resolve_explicit(
			&spec,
			&legacy_category_frame(&fresh),
			&ColorIdentity::default(),
			"tab20c",
		)
		.expect("fresh control");
		assert_eq!(
			position_color_mapping(&resolved)
				.into_iter()
				.map(|(_, position, _)| position)
				.collect::<Vec<_>>(),
			[0, 10, 19]
		);
	}

	#[test]
	fn product_0029_filtering_reserves_positions_and_returning_color() {
		let spec = legacy_category_spec();
		let initial = ["A", "B"]
			.into_iter()
			.map(str::to_owned)
			.collect::<Vec<_>>();
		let (first, identity) = resolve_explicit(
			&spec,
			&legacy_category_frame(&initial),
			&ColorIdentity::default(),
			"tab20c",
		)
		.expect("initial");
		let b_color = position_color_mapping(&first)[1].2;
		let filtered = ["A", "C"]
			.into_iter()
			.map(str::to_owned)
			.collect::<Vec<_>>();
		let (second, identity) = resolve_explicit(
			&spec,
			&legacy_category_frame(&filtered),
			&identity,
			"tab20c",
		)
		.expect("filtered extension");
		assert_eq!(
			position_color_mapping(&second)
				.into_iter()
				.map(|(category, position, _)| (category, position))
				.collect::<Vec<_>>(),
			[("A".to_owned(), 0), ("C".to_owned(), 9)]
		);
		let returning = ["B"].into_iter().map(str::to_owned).collect::<Vec<_>>();
		let (third, _) = resolve_explicit(
			&spec,
			&legacy_category_frame(&returning),
			&identity,
			"tab20c",
		)
		.expect("returning category");
		assert_eq!(
			position_color_mapping(&third),
			[("B".to_owned(), 19, b_color)]
		);
	}

	#[test]
	fn product_0029_selection_and_capacity_failures_remain_distinct_and_atomic() {
		assert!(matches!(
			CategoricalPaletteSelection::from_theme_name("not-a-theme"),
			Err(CategoricalPaletteSelectionError::UnknownTheme { .. })
		));
		assert!(matches!(
			CategoricalPaletteSelection::from_theme_name("nord"),
			Err(CategoricalPaletteSelectionError::NoCategoricalPalette { identity })
				if identity == "nord"
		));

		let spec = legacy_category_spec();
		for (palette, cardinality, geometry_exceeded, palette_exceeded) in [
			("dark2", 10, false, true),
			("tab20c", 13, true, false),
			("dark2", 13, true, true),
		] {
			let categories = (0..cardinality)
				.map(|index| format!("c{index}"))
				.collect::<Vec<_>>();
			let original = ColorIdentity::default();
			let error = resolve_explicit(
				&spec,
				&legacy_category_frame(&categories),
				&original,
				palette,
			)
			.expect_err("capacity must refuse");
			let CategoricalResolutionError::Capacity(facts) = error else {
				panic!("wrong capacity outcome: {error}");
			};
			assert_eq!(facts.field, "series");
			assert_eq!(facts.role, "color channel");
			assert_eq!(facts.retained, 0);
			assert_eq!(facts.requested, cardinality);
			assert_eq!(facts.geometry_limit, 12);
			assert_eq!(facts.palette_identity, palette);
			assert_eq!(facts.palette_limit, if palette == "dark2" { 8 } else { 20 });
			assert_eq!(facts.geometry_exceeded, geometry_exceeded);
			assert_eq!(facts.palette_exceeded, palette_exceeded);
			assert!(original.is_empty(), "failure mutated caller identity");

			let control = ["ok"].into_iter().map(str::to_owned).collect::<Vec<_>>();
			resolve_explicit(&spec, &legacy_category_frame(&control), &original, palette)
				.expect("identity remains usable after refusal");
		}
	}

	#[test]
	fn product_0029_allocation_spaces_do_not_mix_silently() {
		let spec = legacy_category_spec();
		let categories = ["A"].into_iter().map(str::to_owned).collect::<Vec<_>>();
		let result = legacy_category_frame(&categories);
		let (_, legacy) = resolve(&spec, &result, &ColorIdentity::default()).unwrap();
		assert!(matches!(
			resolve_explicit(&spec, &result, &legacy, "tab20c"),
			Err(CategoricalResolutionError::IncompatibleAllocationSpace {
				existing_len: 12,
				selected_len: 20,
				..
			})
		));

		let (_, dark2) =
			resolve_explicit(&spec, &result, &ColorIdentity::default(), "dark2").unwrap();
		assert!(matches!(
			resolve_explicit(&spec, &result, &dark2, "tab20c"),
			Err(CategoricalResolutionError::IncompatibleAllocationSpace {
				existing_len: 8,
				selected_len: 20,
				..
			})
		));
	}

	#[test]
	fn product_0029_two_field_bar_resolves_band_fill_without_a_color_channel() {
		let categories = ["A", "B", "C", "D", "E", "F"];
		let result = stamped(
			df!(
				"cat" => categories,
				"val" => [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]
			)
			.expect("bar frame"),
		);
		let spec = AuthoredPlotSpec::xy(PlotComposition::single(PlotType::Bar), "cat", "val");
		let (resolved, _) = resolve_explicit(&spec, &result, &ColorIdentity::default(), "tab20c")
			.expect("band fill resolution");
		assert!(resolved.channel(Channel::Color).is_none());
		assert_eq!(
			resolved
				.band_fills
				.iter()
				.map(|assignment| (
					assignment.category.as_str(),
					assignment.position.index(),
					assignment.color,
				))
				.collect::<Vec<_>>(),
			[
				(
					"A",
					0,
					ResolvedColor {
						red: 49,
						green: 130,
						blue: 189,
						alpha: 255
					}
				),
				(
					"B",
					4,
					ResolvedColor {
						red: 230,
						green: 85,
						blue: 13,
						alpha: 255
					}
				),
				(
					"C",
					8,
					ResolvedColor {
						red: 49,
						green: 163,
						blue: 84,
						alpha: 255
					}
				),
				(
					"D",
					11,
					ResolvedColor {
						red: 199,
						green: 233,
						blue: 192,
						alpha: 255
					}
				),
				(
					"E",
					15,
					ResolvedColor {
						red: 218,
						green: 218,
						blue: 235,
						alpha: 255
					}
				),
				(
					"F",
					19,
					ResolvedColor {
						red: 150,
						green: 150,
						blue: 150,
						alpha: 255
					}
				),
			]
		);
	}

	#[test]
	fn product_0029_authored_bar_color_is_authoritative_and_other_marks_stay_unstyled() {
		let result = stamped(
			df!(
				"cat" => ["A", "B"],
				"val" => [1.0, 2.0],
				"group" => ["g1", "g2"]
			)
			.expect("bar frame"),
		);
		let bar = AuthoredPlotSpec::xy_color(
			PlotComposition::single(PlotType::Bar),
			"cat",
			"val",
			Some("group".to_owned()),
		);
		let (resolved, _) = resolve_explicit(&bar, &result, &ColorIdentity::default(), "tab20c")
			.expect("authored bar color");
		let color = resolved.channel(Channel::Color).expect("authored color");
		assert_eq!(color.binding.field, "group");
		assert_eq!(resolved.color_job, Some(ColorJob::Categorical));
		assert_eq!(color.colors.len(), 2);
		assert!(resolved.band_fills.is_empty());

		let plain = stamped(df!("x" => [1.0, 2.0], "val" => [1.0, 2.0]).unwrap());
		for plot_type in [PlotType::Scatter, PlotType::Line] {
			let mark = AuthoredPlotSpec::xy(PlotComposition::single(plot_type), "x", "val");
			let (resolved, _) =
				resolve_explicit(&mark, &plain, &ColorIdentity::default(), "tab20c")
					.expect("uncolored mark");
			assert!(resolved.channel(Channel::Color).is_none());
			assert!(resolved.band_fills.is_empty());
		}
	}

	#[test]
	fn product_0029_bar_band_capacity_is_attributed_to_styling() {
		let categories = (0..10).map(|index| format!("c{index}")).collect::<Vec<_>>();
		let result = stamped(
			DataFrame::new(
				categories.len(),
				vec![
					polars::prelude::Column::new("cat".into(), categories),
					polars::prelude::Column::new("val".into(), vec![1.0; 10]),
				],
			)
			.unwrap(),
		);
		let bar = AuthoredPlotSpec::xy(PlotComposition::single(PlotType::Bar), "cat", "val");
		let error = resolve_explicit(&bar, &result, &ColorIdentity::default(), "dark2")
			.expect_err("short band palette");
		let CategoricalResolutionError::Capacity(facts) = error else {
			panic!("wrong failure: {error}");
		};
		assert_eq!(facts.field, "cat");
		assert_eq!(facts.role, "bar band fill");
		assert!(!facts.geometry_exceeded);
		assert!(facts.palette_exceeded);
	}

	fn layered(first: LayerSpec, rest: impl IntoIterator<Item = LayerSpec>) -> LayeredPlotSpec {
		let mut spec = LayeredPlotSpec::new(first);
		for layer in rest {
			spec.push(layer);
		}
		spec
	}

	#[test]
	fn product_0013_layered_spec_is_non_empty_ordered_and_mark_bounded() {
		let spec = layered(
			LayerSpec::xy(LayerMark::Line, "x", "y"),
			[LayerSpec::xy(LayerMark::Scatter, "x", "y")],
		);
		assert_eq!(spec.layers.len(), 2);
		assert_eq!(spec.layers.first().mark, LayerMark::Line);
		assert_eq!(spec.layers.last().mark, LayerMark::Scatter);
		assert_eq!(spec.title(), "Line + Scatter");
		assert_eq!(LayerMark::Line.plot_type(), PlotType::Line);
		assert_eq!(LayerMark::Scatter.plot_type(), PlotType::Scatter);
	}

	#[test]
	fn product_0013_resolution_preserves_authored_layer_order() {
		let result = stamped(df!("x" => [1.0, 2.0], "y" => [2.0, 4.0]).expect("frame"));
		let spec = layered(
			LayerSpec::xy(LayerMark::Line, "x", "y"),
			[LayerSpec::xy(LayerMark::Scatter, "x", "y")],
		);
		let frame = result.data.projection().expect("fixture projection");
		let binding =
			resolve_layered_channels(&spec, frame.schema().as_ref()).expect("channel binding");
		let resolved = resolve_layered_domains(binding, &result).expect("domain resolution");
		assert_eq!(
			resolved
				.layers
				.iter()
				.map(|layer| layer.mark)
				.collect::<Vec<_>>(),
			[LayerMark::Line, LayerMark::Scatter]
		);
		assert_eq!(
			resolved
				.layers
				.first()
				.spec
				.channel(Channel::X)
				.unwrap()
				.domain,
			resolved
				.layers
				.last()
				.spec
				.channel(Channel::X)
				.unwrap()
				.domain
		);
	}

	#[test]
	fn product_0013_layer_compatibility_names_both_field_identities() {
		let frame = df!(
			"x" => [1.0], "y" => [2.0],
			"other_x" => [3.0], "other_y" => [4.0]
		)
		.expect("frame");
		let schema = frame.schema();
		for (channel, second) in [
			(
				Channel::X,
				LayerSpec::xy(LayerMark::Scatter, "other_x", "y"),
			),
			(
				Channel::Y,
				LayerSpec::xy(LayerMark::Scatter, "x", "other_y"),
			),
		] {
			let spec = layered(LayerSpec::xy(LayerMark::Line, "x", "y"), [second]);
			let error = resolve_layered_channels(&spec, schema.as_ref())
				.expect_err("different field must refuse");
			let LayeredPlotError::FieldMismatch {
				layer,
				channel: actual_channel,
				expected,
				actual,
			} = &error
			else {
				panic!("wrong refusal: {error}");
			};
			assert_eq!(*layer, 1);
			assert_eq!(*actual_channel, channel);
			assert_eq!(expected, if channel == Channel::X { "x" } else { "y" });
			assert_eq!(
				actual,
				if channel == Channel::X {
					"other_x"
				} else {
					"other_y"
				}
			);
			let message = error.to_string();
			assert!(message.contains("layer 2"));
			assert!(message.contains(expected.as_str()));
			assert!(message.contains(actual.as_str()));
		}
	}

	#[test]
	fn product_0013_layer_scale_families_must_match() {
		let frame = df!("x" => [1.0], "y" => [2.0]).expect("frame");
		let schema = frame.schema();
		let first = LayerSpec::xy(LayerMark::Line, "x", "y");
		let mut second = LayerSpec::xy(LayerMark::Scatter, "x", "y");
		second.encoding.x.scale = Override::Set(ScaleFamily::Log);
		let error = resolve_layered_channels(&layered(first, [second]), schema.as_ref())
			.expect_err("different scale family must refuse");
		assert!(matches!(
			error,
			LayeredPlotError::ScaleMismatch {
				layer: 1,
				channel: Channel::X,
				expected: ScaleFamily::Linear,
				actual: ScaleFamily::Log,
			}
		));
	}

	#[test]
	fn product_0013_layer_color_refuses_named_and_auto_with_absent_control() {
		let frame = df!("x" => [1.0], "y" => [2.0], "series" => ["a"]).expect("frame");
		let schema = frame.schema();
		for (field, expected) in [
			(
				OptionalAuthoredChannel::Named("series".to_owned()),
				"`series`",
			),
			(OptionalAuthoredChannel::Auto, "<auto>"),
		] {
			let mut offending = LayerSpec::xy(LayerMark::Scatter, "x", "y");
			offending.encoding.color.field = field;
			let error = resolve_layered_channels(
				&layered(LayerSpec::xy(LayerMark::Line, "x", "y"), [offending]),
				schema.as_ref(),
			)
			.expect_err("layered color must refuse");
			assert!(matches!(
				&error,
				LayeredPlotError::ColorNotAbsent { layer: 1, authored }
					if authored == expected
			));
			assert!(error.to_string().contains("layer 2"));
		}

		let control = layered(
			LayerSpec::xy(LayerMark::Line, "x", "y"),
			[LayerSpec::xy(LayerMark::Scatter, "x", "y")],
		);
		let resolved =
			resolve_layered_channels(&control, schema.as_ref()).expect("Absent color resolves");
		assert_eq!(resolved.layers.len(), 2);
		assert!(resolved.layers.iter().all(|layer| {
			layer
				.channels()
				.iter()
				.all(|channel| channel.channel != Channel::Color)
		}));
	}

	#[test]
	fn product_0013_layer_channel_failure_is_atomic_and_attributed() {
		let frame = df!("x" => [1.0], "y" => [2.0]).expect("frame");
		let schema = frame.schema();
		let spec = layered(
			LayerSpec::xy(LayerMark::Line, "x", "y"),
			[LayerSpec::xy(LayerMark::Scatter, "x", "missing")],
		);
		let error = resolve_layered_channels(&spec, schema.as_ref())
			.expect_err("one bad layer refuses the artifact");
		let LayeredPlotError::Channel { layer, source } = &error else {
			panic!("wrong refusal: {error}");
		};
		assert_eq!(*layer, 1);
		assert_eq!(source.field, "missing");
		assert!(error.to_string().contains("layer 2"));
	}

	#[test]
	fn named_scatter_preserves_categorical_x_and_quantitative_y() {
		let result = stamped(df!("cat" => ["a", "b"], "val" => [1.0, 2.0]).expect("frame"));
		let spec = AuthoredPlotSpec::xy(PlotComposition::single(PlotType::Scatter), "cat", "val");
		let (resolved, _) = resolve(&spec, &result, &ColorIdentity::default()).expect("resolve");
		let x = resolved.channel(Channel::X).expect("x");
		let y = resolved.channel(Channel::Y).expect("y");
		assert_eq!(x.binding.field, "cat");
		assert_eq!(x.binding.semantic_type, SemanticType::Nominal);
		assert_eq!(x.binding.scale, ScaleFamily::Point);
		assert_eq!(y.binding.field, "val");
		assert_eq!(y.binding.semantic_type, SemanticType::Quantitative);
		assert_eq!(y.binding.scale, ScaleFamily::Linear);
	}

	#[test]
	fn named_line_preserves_exact_xy_and_projects_the_existing_line_contract() {
		let result = stamped(df!("x" => [1_i64, 2], "y" => [3.0, 5.0]).expect("frame"));
		let spec = AuthoredPlotSpec::xy(PlotComposition::single(PlotType::Line), "x", "y");
		let (resolved, _) = resolve(&spec, &result, &ColorIdentity::default()).expect("resolve");

		assert_eq!(resolved.channel(Channel::X).unwrap().binding.field, "x");
		assert_eq!(resolved.channel(Channel::Y).unwrap().binding.field, "y");
		assert!(resolved.channel(Channel::Color).is_none());
		assert_eq!(
			resolved.kernel_fields.as_ref(),
			&ResolvedKernelFields::Line {
				series: None,
				x: "x".to_owned(),
				y: "y".to_owned(),
			}
		);
	}

	#[test]
	fn product_0055_bar_channel_pair_is_a_strict_orientation_widening() {
		let vertical = df!("category" => ["A"], "value" => [1.0]).unwrap();
		let vertical_spec =
			AuthoredPlotSpec::xy(PlotComposition::single(PlotType::Bar), "category", "value");
		let vertical_binding =
			resolve_channels(&vertical_spec, vertical.schema().as_ref()).unwrap();
		assert_eq!(
			vertical_binding.composition.primary_plot_type(),
			PlotType::Bar
		);
		assert_eq!(
			vertical_binding.kernel_fields.as_ref(),
			&ResolvedKernelFields::Bar {
				category: "category".to_owned(),
				group: None,
				value: "value".to_owned(),
			}
		);

		let horizontal_spec =
			AuthoredPlotSpec::xy(PlotComposition::single(PlotType::Bar), "value", "category");
		let horizontal_binding =
			resolve_channels(&horizontal_spec, vertical.schema().as_ref()).unwrap();
		assert_eq!(
			horizontal_binding.composition.primary_plot_type(),
			PlotType::HorizontalBar
		);
		assert_eq!(
			horizontal_binding.kernel_fields.as_ref(),
			&ResolvedKernelFields::Bar {
				category: "category".to_owned(),
				group: None,
				value: "value".to_owned(),
			}
		);
		for channel in horizontal_binding.channels() {
			assert_eq!(
				channel.layer,
				LayerIdentity {
					ordinal: 0,
					plot_type: PlotType::Bar,
				},
				"orientation may change geometry, axes, and guides; it may not change identity"
			);
		}

		for (frame, expected_x, expected_y) in [
			(
				df!("x" => [1.0], "y" => [2.0]).unwrap(),
				SemanticType::Quantitative,
				SemanticType::Quantitative,
			),
			(
				df!("x" => ["A"], "y" => ["B"]).unwrap(),
				SemanticType::Nominal,
				SemanticType::Nominal,
			),
		] {
			let invalid = AuthoredPlotSpec::xy(PlotComposition::single(PlotType::Bar), "x", "y");
			let error = resolve_channels(&invalid, frame.schema().as_ref())
				.expect_err("an invalid pair must refuse after both channels resolve");
			assert_eq!(
				error.reason,
				format!(
					"bar requires exactly one quantitative value axis and one non-quantitative category axis; x `x` resolved as {expected_x:?}, y `y` resolved as {expected_y:?}"
				)
			);
		}
	}

	#[test]
	fn product_0055_canonical_horizontal_output_does_not_create_authoring_authority() {
		let frame = df!("value" => [1.0], "category" => ["A"]).unwrap();
		let named = AuthoredPlotSpec::xy(
			PlotComposition::single(PlotType::HorizontalBar),
			"value",
			"category",
		);
		let error = resolve_channels(&named, frame.schema().as_ref())
			.expect_err("HorizontalBar is resolved output, not an authored constructor");
		assert!(
			error
				.reason
				.contains("does not yet expose authored channel roles")
		);

		let automatic =
			AuthoredPlotSpec::automatic(PlotComposition::single(PlotType::HorizontalBar));
		let binding = resolve_channels(&automatic, frame.schema().as_ref())
			.expect("the dashboard's automatic HorizontalBar remains admitted");
		assert_eq!(
			binding.composition.primary_plot_type(),
			PlotType::HorizontalBar
		);
		assert!(matches!(
			binding.kernel_fields.as_ref(),
			ResolvedKernelFields::Bar { category, value, .. }
				if category == "category" && value == "value"
		));

		let dashboard_vertical =
			AuthoredPlotSpec::automatic(PlotComposition::single(PlotType::Bar));
		let binding = resolve_channels(&dashboard_vertical, frame.schema().as_ref())
			.expect("automatic dashboard Bar keeps its fixed vertical contract");
		assert_eq!(binding.composition.primary_plot_type(), PlotType::Bar);
		assert!(matches!(
			binding.kernel_fields.as_ref(),
			ResolvedKernelFields::Bar { category, value, .. }
				if category == "category" && value == "value"
		));
	}

	#[test]
	fn product_0064_stacking_selects_members_after_the_bar_pair_rule() {
		let frame = df!(
			"category" => ["A"],
			"value" => [1.0],
			"series" => ["one"],
		)
		.unwrap();
		for (x, y, stacked, expected, expected_fields) in [
			(
				"category",
				"value",
				false,
				PlotType::Bar,
				ResolvedKernelFields::Bar {
					category: "category".to_owned(),
					group: Some("series".to_owned()),
					value: "value".to_owned(),
				},
			),
			(
				"value",
				"category",
				false,
				PlotType::HorizontalBar,
				ResolvedKernelFields::Bar {
					category: "category".to_owned(),
					group: Some("series".to_owned()),
					value: "value".to_owned(),
				},
			),
			(
				"category",
				"value",
				true,
				PlotType::StackedBar,
				ResolvedKernelFields::StackedBar {
					category: "category".to_owned(),
					group: Some("series".to_owned()),
					value: "value".to_owned(),
				},
			),
			(
				"value",
				"category",
				true,
				PlotType::HorizontalStackedBar,
				ResolvedKernelFields::StackedBar {
					category: "category".to_owned(),
					group: Some("series".to_owned()),
					value: "value".to_owned(),
				},
			),
		] {
			let authored = AuthoredPlotSpec::xy_color(
				PlotComposition::single(PlotType::Bar),
				x,
				y,
				Some("series".to_owned()),
			);
			let authored = if stacked {
				authored.with_stacked_bar()
			} else {
				authored
			};
			let binding = resolve_channels(&authored, frame.schema().as_ref()).unwrap();
			assert_eq!(binding.composition.primary_plot_type(), expected);
			assert_eq!(binding.kernel_fields.as_ref(), &expected_fields);
			for channel in binding.channels() {
				assert_eq!(
					channel.layer,
					LayerIdentity {
						ordinal: 0,
						plot_type: PlotType::Bar,
					},
					"stacking and rotation may change geometry, axes, and guides; neither changes identity"
				);
			}
		}

		for frame in [
			df!("x" => [1.0], "y" => [2.0]).unwrap(),
			df!("x" => ["A"], "y" => ["B"]).unwrap(),
		] {
			let grouped = AuthoredPlotSpec::xy(PlotComposition::single(PlotType::Bar), "x", "y");
			let stacked = grouped.clone().with_stacked_bar();
			assert_eq!(
				resolve_channels(&grouped, frame.schema().as_ref()).unwrap_err(),
				resolve_channels(&stacked, frame.schema().as_ref()).unwrap_err(),
				"stacking cannot rescue or reattribute an invalid channel pair"
			);
		}
	}

	#[test]
	fn product_0064_stacking_preserves_lineage_and_canonical_members_gain_no_authority() {
		let result = stamped(
			df!(
				"category" => ["A", "A", "B", "B"],
				"value" => [1.0, 2.0, 3.0, 4.0],
				"series" => ["Atlas", "Beacon", "Atlas", "Beacon"],
				"other" => ["North", "South", "North", "South"],
			)
			.unwrap(),
		);
		let grouped = AuthoredPlotSpec::xy_color(
			PlotComposition::single(PlotType::Bar),
			"category",
			"value",
			Some("series".to_owned()),
		);
		let stacked = grouped.clone().with_stacked_bar();
		let (grouped_resolved, identity) =
			resolve(&grouped, &result, &ColorIdentity::default()).unwrap();
		let (stacked_resolved, identity) = resolve(&stacked, &result, &identity).unwrap();
		assert_eq!(
			grouped_resolved.channel(Channel::Color).unwrap().colors,
			stacked_resolved.channel(Channel::Color).unwrap().colors,
			"grouped and stacked geometry share exact categorical continuation"
		);
		let percent = stacked.clone().with_percent();
		let (percent_resolved, identity) = resolve(&percent, &result, &identity).unwrap();
		assert_eq!(
			stacked_resolved.channel(Channel::Color).unwrap().colors,
			percent_resolved.channel(Channel::Color).unwrap().colors,
			"normalization changes scale, never categorical lineage"
		);
		assert_eq!(
			percent_resolved.stack_normalization,
			StackNormalization::Percent
		);

		let changed = AuthoredPlotSpec::xy_color(
			PlotComposition::single(PlotType::Bar),
			"category",
			"value",
			Some("other".to_owned()),
		)
		.with_stacked_bar();
		let (_, changed_identity) = resolve(&changed, &result, &identity).unwrap();
		assert!(
			changed_identity.assignments.len() > identity.assignments.len(),
			"changing the series field, unlike stacking, begins a fresh lineage"
		);

		for member in [PlotType::StackedBar, PlotType::HorizontalStackedBar] {
			let direct = AuthoredPlotSpec::xy(PlotComposition::single(member), "category", "value");
			let projected = result.data.projection().expect("fixture projection");
			let error = resolve_channels(&direct, projected.schema().as_ref())
				.expect_err("canonical stacked members are resolved output, not authored families");
			assert!(
				error
					.reason
					.contains("does not yet expose authored channel roles")
			);
		}
	}

	#[test]
	fn product_0055_rotation_preserves_color_lineage_while_a_series_field_change_does_not() {
		let full = stamped(
			df!(
				"category" => ["East", "West"],
				"value" => [1.0, 2.0],
				"series" => ["Atlas", "Beacon"],
				"other_series" => ["Atlas", "Beacon"],
			)
			.unwrap(),
		);
		let vertical = AuthoredPlotSpec::xy_color(
			PlotComposition::single(PlotType::Bar),
			"category",
			"value",
			Some("series".to_owned()),
		);
		let (vertical_resolved, identity) =
			resolve_explicit(&vertical, &full, &ColorIdentity::default(), "tab20c").unwrap();
		let beacon_before = position_color_mapping(&vertical_resolved)
			.into_iter()
			.find(|(category, _, _)| category == "Beacon")
			.unwrap();

		let filtered = stamped(
			df!(
				"category" => ["West"],
				"value" => [2.0],
				"series" => ["Beacon"],
				"other_series" => ["Beacon"],
			)
			.unwrap(),
		);
		let horizontal = AuthoredPlotSpec::xy_color(
			PlotComposition::single(PlotType::Bar),
			"value",
			"category",
			Some("series".to_owned()),
		);
		let (horizontal_resolved, identity) =
			resolve_explicit(&horizontal, &filtered, &identity, "tab20c").unwrap();
		assert_eq!(
			position_color_mapping(&horizontal_resolved),
			vec![beacon_before.clone()],
			"orientation may change geometry, axes, and guides; it may not change identity"
		);

		let changed_series = AuthoredPlotSpec::xy_color(
			PlotComposition::single(PlotType::Bar),
			"value",
			"category",
			Some("other_series".to_owned()),
		);
		let (changed_resolved, _) =
			resolve_explicit(&changed_series, &filtered, &identity, "tab20c").unwrap();
		assert_ne!(
			position_color_mapping(&changed_resolved),
			vec![beacon_before],
			"changing the series field must begin a new lineage"
		);
	}

	#[test]
	fn product_0054_line_series_is_categorical_bounded_and_exactly_attributed() {
		let accepted = stamped(
			df!(
				"x" => [1.0, 2.0],
				"y" => [3.0, 5.0],
				"series" => ["Atlas", "Beacon"]
			)
			.unwrap(),
		);
		let spec = AuthoredPlotSpec::xy_color(
			PlotComposition::single(PlotType::Line),
			"x",
			"y",
			Some("series".to_owned()),
		);
		let (resolved, _) = resolve(&spec, &accepted, &ColorIdentity::default()).unwrap();
		assert_eq!(resolved.color_job, Some(ColorJob::Categorical));
		assert_eq!(
			resolved
				.channel(Channel::Color)
				.expect("series channel")
				.colors
				.len(),
			2
		);
		assert!(matches!(
			resolved.kernel_fields.as_ref(),
			ResolvedKernelFields::Line { series: Some(series), .. } if series == "series"
		));

		let missing = AuthoredPlotSpec::xy_color(
			PlotComposition::single(PlotType::Line),
			"x",
			"y",
			Some("missing".to_owned()),
		);
		let error = resolve(&missing, &accepted, &ColorIdentity::default()).unwrap_err();
		assert!(error.contains("missing"), "{error}");

		let numeric = stamped(
			df!(
				"x" => [1.0, 2.0],
				"y" => [3.0, 5.0],
				"series" => [10.0, 20.0]
			)
			.unwrap(),
		);
		let error = resolve(&spec, &numeric, &ColorIdentity::default()).unwrap_err();
		assert!(error.contains("series"), "{error}");
		assert!(error.contains("categorical"), "{error}");

		let categories = (0..13).map(|index| format!("s{index}")).collect::<Vec<_>>();
		let over_capacity = stamped(
			DataFrame::new(
				categories.len(),
				vec![
					Column::new("x".into(), (0..13).map(f64::from).collect::<Vec<_>>()),
					Column::new("y".into(), (0..13).map(f64::from).collect::<Vec<_>>()),
					Column::new("series".into(), categories),
				],
			)
			.unwrap(),
		);
		let error = resolve(&spec, &over_capacity, &ColorIdentity::default()).unwrap_err();
		assert!(error.contains("series"), "{error}");
		assert!(
			error.contains("cardinality exceeds declared limit 12"),
			"{error}"
		);
	}

	#[test]
	fn product_0070_area_channels_resolve_to_one_fixed_member_and_family_identity() {
		let frame = stamped(
			df!(
				"x" => [1.0, 2.0],
				"y" => [3.0, 5.0],
				"series" => ["Atlas", "Beacon"],
				"other_series" => ["Atlas", "Beacon"],
			)
			.unwrap(),
		);
		let area = AuthoredPlotSpec::xy_color(
			PlotComposition::single(PlotType::StackedArea),
			"x",
			"y",
			Some("series".to_owned()),
		);
		let projected = frame.data.projection().expect("fixture projection");
		let binding = resolve_channels(&area, projected.schema().as_ref()).unwrap();
		assert_eq!(
			binding.composition.primary_plot_type(),
			PlotType::StackedArea
		);
		for channel in [Channel::X, Channel::Y, Channel::Color] {
			let bound = binding
				.channels()
				.iter()
				.find(|bound| bound.channel == channel)
				.expect("area channel is bound");
			assert_eq!(
				bound.layer,
				LayerIdentity {
					ordinal: 0,
					plot_type: PlotType::StackedArea,
				},
				"area identity names the authored family/member and never aliases line"
			);
		}
		assert!(matches!(
			binding.kernel_fields.as_ref(),
			ResolvedKernelFields::StackedArea { series: Some(series), x, y }
				if series == "series" && x == "x" && y == "y"
		));

		let (_, identity) = resolve(&area, &frame, &ColorIdentity::default()).unwrap();
		let (_, same_identity) = resolve(&area, &frame, &identity).unwrap();
		assert_eq!(
			identity, same_identity,
			"re-running identical area source preserves every palette position"
		);
		let changed = AuthoredPlotSpec::xy_color(
			PlotComposition::single(PlotType::StackedArea),
			"x",
			"y",
			Some("other_series".to_owned()),
		);
		let (_, changed_identity) = resolve(&changed, &frame, &identity).unwrap();
		assert!(
			changed_identity.len() > identity.len(),
			"changing the area series field must begin a fresh color lineage"
		);
	}

	#[test]
	fn product_0070_area_compatibility_is_line_x_quantitative_y_and_categorical_series() {
		let temporal_x = polars::prelude::Series::new("when".into(), &[0_i32, 1_i32])
			.cast(&polars::prelude::DataType::Date)
			.unwrap();
		let accepted = stamped(
			DataFrame::new(
				2,
				vec![
					temporal_x.into_column(),
					Column::new("value".into(), [3.0_f64, 5.0]),
					Column::new("series".into(), ["Atlas", "Beacon"]),
				],
			)
			.unwrap(),
		);
		let temporal = AuthoredPlotSpec::xy_color(
			PlotComposition::single(PlotType::StackedArea),
			"when",
			"value",
			Some("series".to_owned()),
		);
		let (resolved, _) = resolve(&temporal, &accepted, &ColorIdentity::default()).unwrap();
		assert_eq!(
			resolved.channel(Channel::X).unwrap().binding.semantic_type,
			SemanticType::Temporal
		);
		assert_eq!(
			resolved.channel(Channel::Y).unwrap().binding.semantic_type,
			SemanticType::Quantitative
		);

		let numeric = stamped(
			df!(
				"x" => [1.0, 2.0],
				"y" => [3.0, 5.0],
				"category" => ["East", "West"],
				"series_number" => [10.0, 20.0],
			)
			.unwrap(),
		);
		for (spec, field, expected) in [
			(
				AuthoredPlotSpec::xy(
					PlotComposition::single(PlotType::StackedArea),
					"category",
					"y",
				),
				"category",
				"incompatible",
			),
			(
				AuthoredPlotSpec::xy(
					PlotComposition::single(PlotType::StackedArea),
					"x",
					"category",
				),
				"category",
				"incompatible",
			),
			(
				AuthoredPlotSpec::xy_color(
					PlotComposition::single(PlotType::StackedArea),
					"x",
					"y",
					Some("series_number".to_owned()),
				),
				"series_number",
				"categorical",
			),
		] {
			let error = resolve(&spec, &numeric, &ColorIdentity::default()).unwrap_err();
			assert!(error.contains(field), "field attribution missing: {error}");
			assert!(
				error.contains(expected),
				"semantic refusal missing: {error}"
			);
		}
	}

	#[test]
	fn product_0070_automatic_dashboard_area_keeps_its_assignment_free_projection() {
		let frame = stamped(
			df!(
				"series" => ["Atlas", "Beacon"],
				"x" => [1.0, 2.0],
				"y" => [3.0, 5.0],
			)
			.unwrap(),
		);
		let automatic = AuthoredPlotSpec::automatic(PlotComposition::single(PlotType::StackedArea));
		let projected = frame.data.projection().expect("fixture projection");
		let binding = resolve_channels(&automatic, projected.schema().as_ref()).unwrap();
		assert!(
			binding.channels().is_empty(),
			"the dashboard's all-Auto area remains outside authored channel and color resolution"
		);
		assert!(matches!(
			binding.kernel_fields.as_ref(),
			ResolvedKernelFields::StackedArea { series: Some(series), x, y }
				if series == "series" && x == "x" && y == "y"
		));
		let (resolved, identity) = resolve(&automatic, &frame, &ColorIdentity::default()).unwrap();
		assert!(resolved.channel(Channel::Color).is_none());
		assert!(identity.is_empty());
	}

	#[test]
	fn named_channels_never_substitute_and_deliberate_repetition_works() {
		let result = stamped(df!("cat" => ["a"], "val" => [1.0]).expect("frame"));
		let missing =
			AuthoredPlotSpec::xy(PlotComposition::single(PlotType::Scatter), "missing", "val");
		let error = resolve(&missing, &result, &ColorIdentity::default()).expect_err("missing");
		assert!(error.contains("missing"), "unattributed failure: {error}");

		let repeated =
			AuthoredPlotSpec::xy(PlotComposition::single(PlotType::Scatter), "val", "val");
		let (resolved, _) = resolve(&repeated, &result, &ColorIdentity::default()).expect("repeat");
		assert_eq!(resolved.channel(Channel::X).unwrap().binding.field, "val");
		assert_eq!(resolved.channel(Channel::Y).unwrap().binding.field, "val");
	}

	#[test]
	fn only_auto_authorizes_inference_and_optional_absent_stays_absent() {
		let result = stamped(df!("cat" => ["a"], "val" => [1.0]).expect("frame"));
		let automatic = AuthoredPlotSpec::automatic(PlotComposition::single(PlotType::Scatter));
		let (resolved, _) =
			resolve(&automatic, &result, &ColorIdentity::default()).expect("automatic");
		assert_eq!(resolved.channel(Channel::X).unwrap().binding.field, "cat");
		assert_eq!(resolved.channel(Channel::Y).unwrap().binding.field, "val");

		let explicit =
			AuthoredPlotSpec::xy(PlotComposition::single(PlotType::Scatter), "cat", "val");
		let (resolved, _) =
			resolve(&explicit, &result, &ColorIdentity::default()).expect("explicit");
		assert!(resolved.channel(Channel::Color).is_none());
	}

	#[test]
	fn auto_scatter_chooses_a_complete_matching_instead_of_greedy_reuse() {
		let result = stamped(df!("val" => [1.0], "cat" => ["a"]).expect("frame"));
		let automatic = AuthoredPlotSpec::automatic(PlotComposition::single(PlotType::Scatter));
		let (resolved, _) =
			resolve(&automatic, &result, &ColorIdentity::default()).expect("complete matching");
		assert_eq!(resolved.channel(Channel::X).unwrap().binding.field, "cat");
		assert_eq!(resolved.channel(Channel::Y).unwrap().binding.field, "val");
	}

	#[test]
	fn color_identity_survives_filtering_and_resolved_spec_is_self_sufficient() {
		let spec = AuthoredPlotSpec::xy_color(
			PlotComposition::single(PlotType::Scatter),
			"x",
			"y",
			Some("series".to_owned()),
		);
		let first = stamped(
			df!("x" => [1.0, 2.0], "y" => [3.0, 4.0], "series" => ["a", "b"]).expect("frame"),
		);
		let (first_spec, identity) =
			resolve(&spec, &first, &ColorIdentity::default()).expect("first");
		let first_colors = first_spec.channel(Channel::Color).unwrap().colors.clone();
		let continuation = identity.clone();
		drop(identity);
		assert_eq!(first_colors.len(), 2, "resolved spec owns render colors");

		let filtered = stamped(df!("x" => [2.0], "y" => [4.0], "series" => ["b"]).expect("frame"));
		let (second_spec, next) = resolve(&spec, &filtered, &continuation).expect("second");
		let b_before = first_colors
			.iter()
			.find(|item| item.category == "b")
			.unwrap();
		let b_after = &second_spec.channel(Channel::Color).unwrap().colors[0];
		assert_eq!(b_before, b_after);
		assert_eq!(
			continuation.len(),
			next.len(),
			"filter may not shrink identity"
		);

		let (rebuilt, _) =
			resolve(&spec, &filtered, &ColorIdentity::default()).expect("control resolves");
		let rebuilt_b = &rebuilt.channel(Channel::Color).unwrap().colors[0];
		assert_ne!(
			b_before, rebuilt_b,
			"positive control: rebuilding from the latest result must recolor b"
		);
	}

	#[test]
	fn changed_color_field_starts_a_new_lineage() {
		let first = stamped(
			df!(
				"x" => [1.0], "y" => [2.0],
				"series_a" => ["same"], "series_b" => ["same"]
			)
			.expect("frame"),
		);
		let a = AuthoredPlotSpec::xy_color(
			PlotComposition::single(PlotType::Scatter),
			"x",
			"y",
			Some("series_a".to_owned()),
		);
		let (_, identity) = resolve(&a, &first, &ColorIdentity::default()).expect("a");
		let b = AuthoredPlotSpec::xy_color(
			PlotComposition::single(PlotType::Scatter),
			"x",
			"y",
			Some("series_b".to_owned()),
		);
		let (_, identity) = resolve(&b, &first, &identity).expect("b");
		assert_eq!(identity.len(), 2, "field identity separates lineages");
	}

	#[test]
	fn categorical_capacity_refuses_without_reassignment_or_cycling() {
		let categories = (0..DEFAULT_CATEGORICAL_LIMIT)
			.map(|index| format!("c{index}"))
			.collect::<Vec<_>>();
		let full = stamped(
			DataFrame::new(
				categories.len(),
				vec![
					polars::prelude::Column::new("x".into(), vec![1.0; categories.len()]),
					polars::prelude::Column::new("y".into(), vec![2.0; categories.len()]),
					polars::prelude::Column::new("series".into(), categories),
				],
			)
			.expect("frame"),
		);
		let spec = AuthoredPlotSpec::xy_color(
			PlotComposition::single(PlotType::Scatter),
			"x",
			"y",
			Some("series".to_owned()),
		);
		let (resolved, identity) =
			resolve(&spec, &full, &ColorIdentity::default()).expect("fill capacity");
		let original_colors = resolved.channel(Channel::Color).unwrap().colors.clone();
		let overflow =
			stamped(df!("x" => [20.0], "y" => [21.0], "series" => ["overflow"]).expect("frame"));
		let error = resolve(&spec, &overflow, &identity).expect_err("capacity must refuse");
		assert!(error.contains("limit 12"), "unattributed limit: {error}");
		let (again, _) = resolve(&spec, &full, &identity).expect("identity remains usable");
		assert_eq!(
			again.channel(Channel::Color).unwrap().colors,
			original_colors,
			"overflow may not reassign or cycle an existing identity"
		);
	}

	#[test]
	fn auto_never_reuses_a_field_already_named_by_another_channel() {
		let result = stamped(df!("only" => [1.0]).expect("frame"));
		let mut spec = AuthoredPlotSpec::automatic(PlotComposition::single(PlotType::Scatter));
		spec.x = AuthoredChannelSpec::named("only");
		let error = resolve(&spec, &result, &ColorIdentity::default()).expect_err("no auto y");
		assert!(
			error.contains("no compatible unused field"),
			"wrong error: {error}"
		);
	}

	#[test]
	fn color_job_mismatch_refuses_and_matching_job_names_its_scale() {
		let categorical =
			stamped(df!("x" => [1.0], "y" => [2.0], "series" => ["a"]).expect("frame"));
		let mut wrong = AuthoredPlotSpec::xy_color(
			PlotComposition::single(PlotType::Scatter),
			"x",
			"y",
			Some("series".to_owned()),
		);
		wrong.color_job = Override::Set(ColorJob::Sequential);
		let error = resolve(&wrong, &categorical, &ColorIdentity::default())
			.expect_err("job mismatch must refuse");
		assert!(
			error.contains("series"),
			"field missing from error: {error}"
		);
		assert!(
			error.contains("Sequential"),
			"job missing from error: {error}"
		);

		let quantitative =
			stamped(df!("x" => [1.0], "y" => [2.0], "magnitude" => [-3.0]).expect("frame"));
		let mut diverging = AuthoredPlotSpec::xy_color(
			PlotComposition::single(PlotType::Scatter),
			"x",
			"y",
			Some("magnitude".to_owned()),
		);
		diverging.color_job = Override::Set(ColorJob::Diverging);
		let (resolved, identity) =
			resolve(&diverging, &quantitative, &ColorIdentity::default()).expect("diverging");
		assert_eq!(resolved.color_job, Some(ColorJob::Diverging));
		assert_eq!(
			resolved.channel(Channel::Color).unwrap().binding.scale,
			ScaleFamily::DivergingColor
		);
		assert!(
			identity.is_empty(),
			"continuous color has no category identity"
		);
	}

	#[test]
	fn cascade_algebras_are_associative_with_inherit_as_identity() {
		let overrides = [Override::Inherit, Override::Set(1), Override::Set(2)];
		for a in &overrides {
			assert_eq!(a.clone().overlay(Override::Inherit), *a);
			assert_eq!(Override::Inherit.overlay(a.clone()), *a);
			for b in &overrides {
				for c in &overrides {
					assert_eq!(
						a.clone().overlay(b.clone()).overlay(c.clone()),
						a.clone().overlay(b.clone().overlay(c.clone()))
					);
				}
			}
		}

		let presence = [Presence::Inherit, Presence::Show(1), Presence::Hide];
		for a in &presence {
			assert_eq!(a.clone().overlay(Presence::Inherit), *a);
			assert_eq!(Presence::Inherit.overlay(a.clone()), *a);
			for b in &presence {
				for c in &presence {
					assert_eq!(
						a.clone().overlay(b.clone()).overlay(c.clone()),
						a.clone().overlay(b.clone().overlay(c.clone()))
					);
				}
			}
		}

		fn non_associative(left: u8, right: u8) -> u8 {
			left.saturating_sub(right)
		}
		assert_ne!(
			non_associative(non_associative(3, 2), 1),
			non_associative(3, non_associative(2, 1)),
			"positive control must expose a non-associative cascade"
		);
	}

	#[test]
	fn product_0017_point_size_is_bounded_and_layer_constructors_inherit() {
		assert_eq!(PointSize::ONE.radius_px(), 3.0);
		assert_eq!(PointSize::new(2.0).unwrap().radius_px(), 6.0);
		assert!(PointSize::new(PointSize::MIN_FACTOR).is_ok());
		assert!(PointSize::new(PointSize::MAX_FACTOR).is_ok());
		for invalid in [
			0.0,
			-1.0,
			PointSize::MIN_FACTOR - 0.01,
			PointSize::MAX_FACTOR + 0.01,
			f32::NAN,
			f32::INFINITY,
			f32::NEG_INFINITY,
		] {
			assert!(PointSize::new(invalid).is_err(), "accepted {invalid}");
		}

		let line = LayerSpec::xy(LayerMark::Line, "x", "y");
		let scatter = LayerSpec::xy(LayerMark::Scatter, "x", "y");
		assert_eq!(line.settings, LayerSettings::default());
		assert_eq!(scatter.settings, LayerSettings::default());
		assert_eq!(line.settings.point_size, Override::Inherit);
		assert_eq!(scatter.settings.point_size, Override::Inherit);
	}

	#[test]
	fn product_0019_stroke_width_is_bounded_and_layer_constructors_inherit() {
		assert_eq!(StrokeWidth::ONE.width_px(), 2.0);
		assert_eq!(StrokeWidth::new(2.0).unwrap().width_px(), 4.0);
		assert!(StrokeWidth::new(StrokeWidth::MIN_FACTOR).is_ok());
		assert!(StrokeWidth::new(StrokeWidth::MAX_FACTOR).is_ok());
		for invalid in [
			0.0,
			-1.0,
			StrokeWidth::MIN_FACTOR - 0.01,
			StrokeWidth::MAX_FACTOR + 0.01,
			f32::NAN,
			f32::INFINITY,
			f32::NEG_INFINITY,
		] {
			assert!(StrokeWidth::new(invalid).is_err(), "accepted {invalid}");
		}

		let line = LayerSpec::xy(LayerMark::Line, "x", "y");
		let scatter = LayerSpec::xy(LayerMark::Scatter, "x", "y");
		assert_eq!(line.settings.stroke_width, Override::Inherit);
		assert_eq!(scatter.settings.stroke_width, Override::Inherit);
	}

	#[test]
	fn product_0021_bar_width_is_normalized_and_layer_constructors_inherit() {
		assert_eq!(BarWidth::DEFAULT.fraction(), 0.9);
		for accepted in [f32::MIN_POSITIVE, 0.5, 0.9, 1.0] {
			assert_eq!(BarWidth::new(accepted).unwrap().fraction(), accepted);
		}
		for invalid in [0.0, -0.1, 1.01, f32::NAN, f32::INFINITY, f32::NEG_INFINITY] {
			assert!(BarWidth::new(invalid).is_err(), "accepted {invalid}");
		}

		let line = LayerSpec::xy(LayerMark::Line, "x", "y");
		let scatter = LayerSpec::xy(LayerMark::Scatter, "x", "y");
		assert_eq!(line.settings.bar_width, Override::Inherit);
		assert_eq!(scatter.settings.bar_width, Override::Inherit);
		assert_eq!([LayerMark::Line, LayerMark::Scatter].len(), 2);
	}

	#[test]
	fn product_0053_category_gap_is_a_bounded_zero_default_and_constructors_inherit() {
		assert_eq!(CategoryGap::DEFAULT.fraction(), 0.0);
		for accepted in [0.0, f32::MIN_POSITIVE, 0.25, 0.999_999] {
			assert_eq!(CategoryGap::new(accepted).unwrap().fraction(), accepted);
		}
		for invalid in [-0.1, 1.0, 1.01, f32::NAN, f32::INFINITY, f32::NEG_INFINITY] {
			assert!(CategoryGap::new(invalid).is_err(), "accepted {invalid}");
		}

		let line = LayerSpec::xy(LayerMark::Line, "x", "y");
		let scatter = LayerSpec::xy(LayerMark::Scatter, "x", "y");
		assert_eq!(line.settings.category_gap, Override::Inherit);
		assert_eq!(scatter.settings.category_gap, Override::Inherit);
	}
}
