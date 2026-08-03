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
}
