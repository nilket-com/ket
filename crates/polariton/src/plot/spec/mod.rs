//! PRODUCT-0007's authored, resolved, and layout plot artifacts.
//!
//! Channel resolution is deliberately split from domain resolution: schemas
//! can establish field meaning and scale compatibility, while returned values
//! are required to establish domains and stable categorical color identity.
//! Kernels accept only [`ResolvedPlotSpec`], never the intermediate
//! [`ChannelBinding`].

#![allow(dead_code)] // Stage 1 publishes vocabulary consumed by stages 2 and 3.

use crate::plot::composition::PlotComposition;
use crate::plot::core::PlotType;
use polars::frame::DataFrame;
use polars::prelude::{AnyValue, DataType, Schema};
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
pub struct AuthoredPlotSpec {
	pub composition: PlotComposition,
	pub x: AuthoredChannelSpec,
	pub y: AuthoredChannelSpec,
	pub color: OptionalAuthoredChannelSpec,
	pub color_job: Override<ColorJob>,
}

impl AuthoredPlotSpec {
	pub fn automatic(composition: PlotComposition) -> Self {
		Self {
			composition,
			x: AuthoredChannelSpec::auto(),
			y: AuthoredChannelSpec::auto(),
			color: OptionalAuthoredChannelSpec::auto(),
			color_job: Override::Inherit,
		}
	}

	pub fn xy(composition: PlotComposition, x: impl Into<String>, y: impl Into<String>) -> Self {
		Self {
			composition,
			x: AuthoredChannelSpec::named(x),
			y: AuthoredChannelSpec::named(y),
			color: OptionalAuthoredChannelSpec::absent(),
			color_job: Override::Inherit,
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
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LayerSettings {
	pub point_size: Override<PointSize>,
	pub stroke_width: Override<StrokeWidth>,
	pub bar_width: Override<BarWidth>,
}

impl Default for LayerSettings {
	fn default() -> Self {
		Self {
			point_size: Override::Inherit,
			stroke_width: Override::Inherit,
			bar_width: Override::Inherit,
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

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LayerIdentity {
	pub ordinal: usize,
	pub plot_type: PlotType,
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
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChannelBinding {
	composition: PlotComposition,
	channels: Vec<BoundChannel>,
	kernel_fields: Box<ResolvedKernelFields>,
	color_job: Option<ColorJob>,
	categorical_limit: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
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

#[derive(Clone, Debug, PartialEq, Eq)]
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CategoryColorAssignment {
	pub category: String,
	pub color: ResolvedColor,
}

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
	pub kernel_fields: Box<ResolvedKernelFields>,
	pub color_job: Option<ColorJob>,
	pub provenance: PlotProvenance,
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
	BoxPlot {
		category: Option<String>,
		value: String,
	},
	Histogram {
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
	pub data: Arc<DataFrame>,
	pub provenance: PlotProvenance,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct ColorIdentityKey {
	field: String,
	layer: LayerIdentity,
	category: String,
}

/// Per-plot continuation state for live categorical resolution.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ColorIdentity {
	assignments: HashMap<ColorIdentityKey, ResolvedColor>,
}

impl ColorIdentity {
	pub fn len(&self) -> usize {
		self.assignments.len()
	}

	pub fn is_empty(&self) -> bool {
		self.assignments.is_empty()
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChannelError {
	pub channel: Channel,
	pub field: String,
	pub reason: String,
}

impl fmt::Display for ChannelError {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(
			formatter,
			"plot {} channel `{}` could not resolve: {}",
			self.channel, self.field, self.reason
		)
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
	if !matches!(
		plot_type,
		PlotType::Scatter | PlotType::Bar | PlotType::Line
	) {
		if !matches!(spec.x.field, AuthoredChannel::Auto)
			|| !matches!(spec.y.field, AuthoredChannel::Auto)
			|| !matches!(spec.color.field, OptionalAuthoredChannel::Auto)
		{
			return Err(ChannelError {
				channel: Channel::X,
				field: "<authored>".to_owned(),
				reason: format!("{plot_type} does not yet expose authored channel roles"),
			});
		}
		return Ok(ChannelBinding {
			composition: spec.composition.clone(),
			channels: Vec::new(),
			kernel_fields: Box::new(resolve_automatic_kernel_fields(plot_type, schema)?),
			color_job: None,
			categorical_limit: 0,
		});
	}

	let layer = LayerIdentity {
		ordinal: 0,
		plot_type,
	};
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
		schema,
		&mut unavailable_to_auto,
	)?;
	let y = resolve_required(
		Channel::Y,
		&spec.y,
		plot_type,
		&layer,
		schema,
		&mut unavailable_to_auto,
	)?;
	let mut color = resolve_optional(
		Channel::Color,
		&spec.color,
		plot_type,
		&layer,
		schema,
		&mut unavailable_to_auto,
	)?;
	let color_job = color.as_ref().map(|channel| match spec.color_job {
		Override::Set(job) => job,
		Override::Inherit => default_color_job(channel.semantic_type),
	});
	if let (Some(color), Some(job)) = (color.as_mut(), color_job) {
		if !color_job_is_compatible(job, color.semantic_type) {
			return Err(ChannelError {
				channel: Channel::Color,
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
					channel: Channel::Color,
					field: color.field.clone(),
					reason: format!("color job {job:?} requires scale {job_scale:?}"),
				});
			}
			Override::Set(_) => {}
		}
	}
	let mut channels = vec![x, y];
	channels.extend(color);
	let kernel_fields = Box::new(kernel_fields_from_channels(plot_type, &channels)?);
	Ok(ChannelBinding {
		composition: spec.composition.clone(),
		channels,
		kernel_fields,
		color_job,
		categorical_limit: categorical_limit(plot_type),
	})
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
				kernel_fields: binding.kernel_fields,
				color_job: binding.color_job,
				provenance: result.provenance.clone(),
			},
		});
	}

	Ok(ResolvedLayeredPlotSpec { layers })
}

pub fn resolve_domains(
	binding: ChannelBinding,
	result: &StampedResult,
	identity: &ColorIdentity,
) -> Result<(ResolvedPlotSpec, ColorIdentity), DomainError> {
	let mut next_identity = identity.clone();
	let mut channels = Vec::with_capacity(binding.channels.len());
	let color_job = binding.color_job;
	for channel in binding.channels {
		let column = result
			.data
			.column(&channel.field)
			.map_err(|error| DomainError {
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
			kernel_fields: binding.kernel_fields,
			color_job,
			provenance: result.provenance.clone(),
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
			channel,
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
		PlotType::Line => Ok(ResolvedKernelFields::Line {
			series: field(Channel::Color),
			x: required(Channel::X)?,
			y: required(Channel::Y)?,
		}),
		_ => unreachable!("only authored Scatter, Bar, and Line channels reach this projection"),
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
			channel: Channel::Y,
			field: format!("<{role}>"),
			reason: format!(
				"{plot_type} requires numeric field {} for {role}",
				index + 1
			),
		})
	};
	let category = |index: usize, role: &str| {
		nominal.get(index).cloned().ok_or_else(|| ChannelError {
			channel: Channel::X,
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
		PlotType::BoxPlot => ResolvedKernelFields::BoxPlot {
			category: optional_category(0),
			value: number(0, "value")?,
		},
		PlotType::Histogram => ResolvedKernelFields::Histogram {
			value: number(0, "value")?,
		},
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
					channel: Channel::Y,
					field: "<dimensions>".to_owned(),
					reason: "Parallel requires at least one numeric dimension".to_owned(),
				})?,
			category: optional_category(0),
		},
		PlotType::Radar => ResolvedKernelFields::Radar {
			dimensions: (!numeric.is_empty())
				.then(|| numeric.clone())
				.ok_or_else(|| ChannelError {
					channel: Channel::Y,
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
	schema: &Schema,
	used_by_auto: &mut HashSet<String>,
) -> Result<BoundChannel, ChannelError> {
	let field = match &spec.field {
		AuthoredChannel::Named(field) => field.clone(),
		AuthoredChannel::Auto => {
			infer_field(channel, plot_type, schema, used_by_auto).ok_or_else(|| ChannelError {
				channel,
				field: "<auto>".to_owned(),
				reason: "no compatible unused field is available for explicit Auto".to_owned(),
			})?
		}
	};
	let dtype = schema.get(field.as_str()).ok_or_else(|| ChannelError {
		channel,
		field: field.clone(),
		reason: "field is absent from the returned schema".to_owned(),
	})?;
	let inferred_semantic = semantic_type(dtype).ok_or_else(|| ChannelError {
		channel,
		field: field.clone(),
		reason: format!("dtype {dtype:?} has no supported semantic type"),
	})?;
	let semantic_type = match spec.semantic_type {
		Override::Inherit => inferred_semantic,
		Override::Set(declared) => declared,
	};
	if !semantic_is_compatible(plot_type, channel, semantic_type) {
		return Err(ChannelError {
			channel,
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
			channel,
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
	resolve_required(channel, &required, plot_type, layer, schema, used_by_auto).map(Some)
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
		SemanticType::Nominal | SemanticType::Ordinal if plot_type == PlotType::Bar => {
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
		PlotType::Scatter | PlotType::Bar | PlotType::Line => DEFAULT_CATEGORICAL_LIMIT,
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

fn resolve_category_colors(
	categories: &[String],
	channel: &BoundChannel,
	limit: usize,
	identity: &mut ColorIdentity,
) -> Result<Vec<CategoryColorAssignment>, DomainError> {
	let lineage_count = identity
		.assignments
		.keys()
		.filter(|key| key.field == channel.field && key.layer == channel.layer)
		.count();
	let missing = categories
		.iter()
		.filter(|category| {
			!identity.assignments.contains_key(&ColorIdentityKey {
				field: channel.field.clone(),
				layer: channel.layer.clone(),
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
			layer: channel.layer.clone(),
			category: category.clone(),
		};
		if !identity.assignments.contains_key(&key) {
			let ordinal = identity
				.assignments
				.keys()
				.filter(|candidate| {
					candidate.field == channel.field && candidate.layer == channel.layer
				})
				.count();
			identity.assignments.insert(key, categorical_color(ordinal));
		}
	}
	Ok(categories
		.iter()
		.map(|category| {
			let key = ColorIdentityKey {
				field: channel.field.clone(),
				layer: channel.layer.clone(),
				category: category.clone(),
			};
			CategoryColorAssignment {
				category: category.clone(),
				color: identity.assignments[&key],
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
mod tests {
	use super::*;
	use polars::df;

	fn stamped(data: DataFrame) -> StampedResult {
		StampedResult {
			data: Arc::new(data),
			provenance: PlotProvenance {
				source_name: "fixture".to_owned(),
				revision: Some(7),
			},
		}
	}

	fn resolve(
		spec: &AuthoredPlotSpec,
		result: &StampedResult,
		identity: &ColorIdentity,
	) -> Result<(ResolvedPlotSpec, ColorIdentity), String> {
		let binding = resolve_channels(spec, result.data.schema().as_ref())
			.map_err(|error| error.to_string())?;
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
		let binding = resolve_layered_channels(&spec, result.data.schema().as_ref())
			.expect("channel binding");
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
}
