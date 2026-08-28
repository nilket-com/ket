//! Declarative description of Polariton's authored Rune presentation surface.
//!
//! The registry deliberately describes syntax and routing, not evaluator
//! behavior. Validation remains in the handwritten handlers and the plot-spec
//! newtypes they use.

use crate::plot::common::ResolvedPlotSettings;
use crate::plot::core::PlotType;
use crate::plot::spec::{
	BarWidth, CategoryGap, FacetLabelTextSize, HoverTextSize, LegendTextSize, PlotPaddingPixels,
	PointSize, StrokeWidth, SubtitleTextSize, TickRotation, TitleTextSize, XLabelTextSize,
	XTickTextSize, YLabelTextSize, YTickTextSize,
};
use crate::plot::theme::{PlotSettings, PlotSettingsLayer};
use crate::rune_view::LocalViewIdentity;
use libaryon::core::BufferId;
use std::collections::HashSet;
use std::ops::Range;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum CallPosition {
	Free,
	Method,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum RegistrationRoute {
	Free,
	FigureElement,
	Skin,
	Mark,
	Table,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum SemanticReceiver {
	Title,
	Subtitle,
	XAxis,
	YAxis,
	Facet,
	Legend,
	Padding,
	Skin,
	Mark,
	Table,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ArgumentKind {
	Text,
	Number,
	ElementList,
	LayerList,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ArgumentSignature {
	pub(crate) min: u8,
	pub(crate) max: Option<u8>,
	pub(crate) kind: ArgumentKind,
}

/// Stable reference from metadata to a handwritten evaluator handler.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum HandlerId {
	Bar,
	Line,
	Scatter,
	Area,
	Histogram,
	Box,
	Bubble,
	Heatmap,
	Funnel,
	RadialDial,
	Pie,
	Violin,
	Radar,
	Parallel,
	Title,
	Subtitle,
	XAxis,
	YAxis,
	Facet,
	Palette,
	SequentialPalette,
	DivergingPalette,
	Skin,
	Legend,
	HoverSize,
	Padding,
	Table,
	Layers,
	Figure,
	FigureFontSize,
	FacetGutter,
	PaddingTop,
	PaddingRight,
	PaddingBottom,
	PaddingLeft,
	AxisLabel,
	FigureLabelSize,
	AxisBounds,
	AxisTickStep,
	AxisTickEvery,
	AxisMinorTicks,
	AxisTickSize,
	AxisTickRotation,
	LegendAt,
	LegendHide,
	MarkPointSize,
	MarkStrokeWidth,
	MarkBarWidth,
	MarkCategoryGap,
	MarkCategory,
	MarkStacked,
	MarkPercent,
	MarkDiverging,
	MarkDivergingAt,
	SkinDuotone,
	TableSkin,
	TableDuotone,
	TableShowTypes,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct CallableSpec {
	pub(crate) name: &'static str,
	pub(crate) rune_name: &'static str,
	pub(crate) position: CallPosition,
	pub(crate) arguments: ArgumentSignature,
	pub(crate) handler: HandlerId,
	pub(crate) registration_routes: &'static [RegistrationRoute],
	pub(crate) accepted_receivers: &'static [SemanticReceiver],
	pub(crate) doc: &'static str,
}

const FREE: &[RegistrationRoute] = &[RegistrationRoute::Free];
const FIGURE_AND_SKIN: &[RegistrationRoute] =
	&[RegistrationRoute::FigureElement, RegistrationRoute::Skin];
const MARK: &[RegistrationRoute] = &[RegistrationRoute::Mark];
const SKIN_ROUTE: &[RegistrationRoute] = &[RegistrationRoute::Skin];
const TABLE_ROUTE: &[RegistrationRoute] = &[RegistrationRoute::Table];
const NONE: &[SemanticReceiver] = &[];
const FIGURE_TEXT: &[SemanticReceiver] = &[
	SemanticReceiver::Title,
	SemanticReceiver::Subtitle,
	SemanticReceiver::Legend,
];
const AXES: &[SemanticReceiver] = &[SemanticReceiver::XAxis, SemanticReceiver::YAxis];
const X_AXIS: &[SemanticReceiver] = &[SemanticReceiver::XAxis];
const LABELLED: &[SemanticReceiver] = &[
	SemanticReceiver::XAxis,
	SemanticReceiver::YAxis,
	SemanticReceiver::Facet,
];
const PADDING: &[SemanticReceiver] = &[SemanticReceiver::Padding];
const LEGEND: &[SemanticReceiver] = &[SemanticReceiver::Legend];
const SKIN: &[SemanticReceiver] = &[SemanticReceiver::Skin];
const MARK_RECEIVER: &[SemanticReceiver] = &[SemanticReceiver::Mark];
const TABLE_RECEIVER: &[SemanticReceiver] = &[SemanticReceiver::Table];

macro_rules! callable {
	($name:literal, $position:ident, $min:literal $(..= $max:literal)?, $kind:ident,
	 $handler:ident, $routes:expr, $receivers:expr, $doc:literal) => {
		CallableSpec {
			name: $name,
			rune_name: concat!("__", $name),
			position: CallPosition::$position,
			arguments: ArgumentSignature {
				min: $min,
				max: callable!(@max $($max)?),
				kind: ArgumentKind::$kind,
			},
			handler: HandlerId::$handler,
			registration_routes: $routes,
			accepted_receivers: $receivers,
			doc: $doc,
		}
	};
	(@max $max:literal) => { Some($max) };
	(@max) => { None };
}

/// The sole authored inventory of presentation callables. Duplicate spellings
/// are intentional where distinct receiver routes have distinct handlers.
pub(crate) const CALLABLES: &[CallableSpec] = &[
	callable!(
		"bar",
		Free,
		2..=3,
		Text,
		Bar,
		FREE,
		NONE,
		"Construct a bar mark."
	),
	callable!(
		"line",
		Free,
		2..=3,
		Text,
		Line,
		FREE,
		NONE,
		"Construct a line mark."
	),
	callable!(
		"scatter",
		Free,
		2..=3,
		Text,
		Scatter,
		FREE,
		NONE,
		"Construct a scatter mark."
	),
	callable!(
		"area",
		Free,
		2..=3,
		Text,
		Area,
		FREE,
		NONE,
		"Construct an area mark."
	),
	callable!(
		"histogram",
		Free,
		3..=3,
		Text,
		Histogram,
		FREE,
		NONE,
		"Construct a histogram mark."
	),
	callable!(
		"box",
		Free,
		7..=7,
		Text,
		Box,
		FREE,
		NONE,
		"Construct a box mark."
	),
	callable!(
		"bubble",
		Free,
		4..=5,
		Text,
		Bubble,
		FREE,
		NONE,
		"Construct a bubble mark."
	),
	callable!(
		"heatmap",
		Free,
		3..=3,
		Text,
		Heatmap,
		FREE,
		NONE,
		"Construct a heatmap mark."
	),
	callable!(
		"funnel",
		Free,
		2..=2,
		Text,
		Funnel,
		FREE,
		NONE,
		"Construct a funnel mark."
	),
	callable!(
		"radial_dial",
		Free,
		2..=3,
		Text,
		RadialDial,
		FREE,
		NONE,
		"Construct a radial dial mark."
	),
	callable!(
		"pie",
		Free,
		2..=2,
		Text,
		Pie,
		FREE,
		NONE,
		"Construct a pie mark."
	),
	callable!(
		"violin",
		Free,
		4..=4,
		Text,
		Violin,
		FREE,
		NONE,
		"Construct a violin mark."
	),
	callable!(
		"radar",
		Free,
		3,
		Text,
		Radar,
		FREE,
		NONE,
		"Construct a radar mark from one or more fields."
	),
	callable!(
		"parallel",
		Free,
		2,
		Text,
		Parallel,
		FREE,
		NONE,
		"Construct a parallel-coordinate mark from one or more fields."
	),
	callable!(
		"title",
		Free,
		1..=1,
		Text,
		Title,
		FREE,
		NONE,
		"Set the figure title."
	),
	callable!(
		"subtitle",
		Free,
		1..=1,
		Text,
		Subtitle,
		FREE,
		NONE,
		"Set the figure subtitle."
	),
	callable!(
		"x_axis",
		Free,
		0..=0,
		Text,
		XAxis,
		FREE,
		NONE,
		"Configure the horizontal axis."
	),
	callable!(
		"y_axis",
		Free,
		0..=0,
		Text,
		YAxis,
		FREE,
		NONE,
		"Configure the vertical axis."
	),
	callable!(
		"facet",
		Free,
		1..=1,
		Text,
		Facet,
		FREE,
		NONE,
		"Choose the facet column."
	),
	callable!(
		"palette",
		Free,
		1..=1,
		Text,
		Palette,
		FREE,
		NONE,
		"Choose the categorical palette."
	),
	callable!(
		"sequential_palette",
		Free,
		1..=1,
		Text,
		SequentialPalette,
		FREE,
		NONE,
		"Choose the sequential ramp."
	),
	callable!(
		"diverging_palette",
		Free,
		1..=1,
		Text,
		DivergingPalette,
		FREE,
		NONE,
		"Choose the diverging ramp."
	),
	callable!(
		"skin",
		Free,
		1..=1,
		Text,
		Skin,
		FREE,
		NONE,
		"Choose the plot skin."
	),
	callable!(
		"legend",
		Free,
		0..=0,
		Text,
		Legend,
		FREE,
		NONE,
		"Configure the legend."
	),
	callable!(
		"hover_size",
		Free,
		1..=1,
		Number,
		HoverSize,
		FREE,
		NONE,
		"Set hover hit-target size."
	),
	callable!(
		"padding",
		Free,
		0..=0,
		Text,
		Padding,
		FREE,
		NONE,
		"Configure figure padding."
	),
	callable!(
		"table",
		Free,
		0..=0,
		Text,
		Table,
		FREE,
		NONE,
		"Configure a table view."
	),
	callable!(
		"layers",
		Free,
		1..=1,
		LayerList,
		Layers,
		FREE,
		NONE,
		"Compose a non-empty list of mark layers."
	),
	callable!(
		"figure",
		Free,
		1..=1,
		ElementList,
		Figure,
		FREE,
		NONE,
		"Compose figure elements."
	),
	// The registration routes below intentionally reproduce today's broad Rune
	// method tables. accepted_receivers records the narrower evaluator contract.
	callable!(
		"font_size",
		Method,
		1..=1,
		Number,
		FigureFontSize,
		FIGURE_AND_SKIN,
		FIGURE_TEXT,
		"Set title, subtitle, or legend font size."
	),
	callable!(
		"gutter",
		Method,
		1..=1,
		Number,
		FacetGutter,
		FIGURE_AND_SKIN,
		&[SemanticReceiver::Facet],
		"Set facet gutter size."
	),
	callable!(
		"top",
		Method,
		1..=1,
		Number,
		PaddingTop,
		FIGURE_AND_SKIN,
		PADDING,
		"Set top padding."
	),
	callable!(
		"right",
		Method,
		1..=1,
		Number,
		PaddingRight,
		FIGURE_AND_SKIN,
		PADDING,
		"Set right padding."
	),
	callable!(
		"bottom",
		Method,
		1..=1,
		Number,
		PaddingBottom,
		FIGURE_AND_SKIN,
		PADDING,
		"Set bottom padding."
	),
	callable!(
		"left",
		Method,
		1..=1,
		Number,
		PaddingLeft,
		FIGURE_AND_SKIN,
		PADDING,
		"Set left padding."
	),
	callable!(
		"label",
		Method,
		1..=1,
		Text,
		AxisLabel,
		FIGURE_AND_SKIN,
		AXES,
		"Set an axis label."
	),
	callable!(
		"label_size",
		Method,
		1..=1,
		Number,
		FigureLabelSize,
		FIGURE_AND_SKIN,
		LABELLED,
		"Set axis or facet label size."
	),
	callable!(
		"bounds",
		Method,
		2..=2,
		Number,
		AxisBounds,
		FIGURE_AND_SKIN,
		AXES,
		"Set explicit axis bounds."
	),
	callable!(
		"tick_step",
		Method,
		1..=1,
		Number,
		AxisTickStep,
		FIGURE_AND_SKIN,
		AXES,
		"Set numeric tick spacing."
	),
	callable!(
		"tick_every",
		Method,
		1..=1,
		Text,
		AxisTickEvery,
		FIGURE_AND_SKIN,
		AXES,
		"Set temporal tick spacing."
	),
	callable!(
		"minor_ticks",
		Method,
		1..=1,
		Number,
		AxisMinorTicks,
		FIGURE_AND_SKIN,
		AXES,
		"Set minor ticks per interval."
	),
	callable!(
		"tick_size",
		Method,
		1..=1,
		Number,
		AxisTickSize,
		FIGURE_AND_SKIN,
		AXES,
		"Set axis tick-label size."
	),
	callable!(
		"tick_rotation",
		Method,
		1..=1,
		Number,
		AxisTickRotation,
		FIGURE_AND_SKIN,
		X_AXIS,
		"Rotate axis tick labels."
	),
	callable!(
		"at",
		Method,
		2..=2,
		Number,
		LegendAt,
		FIGURE_AND_SKIN,
		LEGEND,
		"Place the legend at normalized coordinates."
	),
	callable!(
		"hide",
		Method,
		0..=0,
		Text,
		LegendHide,
		FIGURE_AND_SKIN,
		LEGEND,
		"Hide the legend."
	),
	callable!(
		"point_size",
		Method,
		1..=1,
		Number,
		MarkPointSize,
		MARK,
		MARK_RECEIVER,
		"Set mark point size."
	),
	callable!(
		"stroke_width",
		Method,
		1..=1,
		Number,
		MarkStrokeWidth,
		MARK,
		MARK_RECEIVER,
		"Set mark stroke width."
	),
	callable!(
		"bar_width",
		Method,
		1..=1,
		Number,
		MarkBarWidth,
		MARK,
		MARK_RECEIVER,
		"Set bar width."
	),
	callable!(
		"category_gap",
		Method,
		1..=1,
		Number,
		MarkCategoryGap,
		MARK,
		MARK_RECEIVER,
		"Set categorical bar gap."
	),
	callable!(
		"category",
		Method,
		1..=1,
		Text,
		MarkCategory,
		MARK,
		MARK_RECEIVER,
		"Set a multivariate category field."
	),
	callable!(
		"stacked",
		Method,
		0..=0,
		Text,
		MarkStacked,
		MARK,
		MARK_RECEIVER,
		"Stack compatible marks."
	),
	callable!(
		"percent",
		Method,
		0..=0,
		Text,
		MarkPercent,
		MARK,
		MARK_RECEIVER,
		"Normalize a stack to percentages."
	),
	callable!(
		"diverging",
		Method,
		0..=0,
		Text,
		MarkDiverging,
		MARK,
		MARK_RECEIVER,
		"Use diverging heatmap color semantics."
	),
	callable!(
		"diverging_at",
		Method,
		1..=1,
		Number,
		MarkDivergingAt,
		MARK,
		MARK_RECEIVER,
		"Set the diverging color midpoint."
	),
	callable!(
		"duotone",
		Method,
		0..=0,
		Text,
		SkinDuotone,
		SKIN_ROUTE,
		SKIN,
		"Use the skin's duotone variant."
	),
	callable!(
		"skin",
		Method,
		1..=1,
		Text,
		TableSkin,
		TABLE_ROUTE,
		TABLE_RECEIVER,
		"Choose a table skin."
	),
	callable!(
		"duotone",
		Method,
		0..=0,
		Text,
		TableDuotone,
		TABLE_ROUTE,
		TABLE_RECEIVER,
		"Use the table skin's duotone variant."
	),
	callable!(
		"show_types",
		Method,
		0..=0,
		Text,
		TableShowTypes,
		TABLE_ROUTE,
		TABLE_RECEIVER,
		"Show table column types."
	),
];

pub(crate) fn is_presentation_callable(name: &str, position: CallPosition) -> bool {
	CALLABLES
		.iter()
		.any(|spec| spec.name == name && spec.position == position)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) enum SettingKey {
	Title,
	Subtitle,
	TitleSize,
	SubtitleSize,
	CategoricalPalette,
	SequentialPalette,
	DivergingPalette,
	PlotSkin,
	SkinDuotone,
	HoverSize,
	FacetGutter,
	FacetLabelSize,
	PaddingTop,
	PaddingRight,
	PaddingBottom,
	PaddingLeft,
	XAxisLabel,
	YAxisLabel,
	XAxisLabelSize,
	YAxisLabelSize,
	XAxisBounds,
	YAxisBounds,
	XAxisTickStep,
	YAxisTickStep,
	XAxisTickEvery,
	YAxisTickEvery,
	XAxisMinorTicks,
	YAxisMinorTicks,
	XAxisTickSize,
	YAxisTickSize,
	XAxisTickRotation,
	LegendPosition,
	LegendPresence,
	LegendSize,
	PointSize,
	StrokeWidth,
	BarWidth,
	CategoryGap,
	Stacked,
	Percent,
	Diverging,
	DivergingAt,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) enum LogicalTarget {
	Figure,
	XAxis,
	YAxis,
	Facet,
	Legend,
	Padding,
	Mark,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) enum InspectorSection {
	Figure,
	Axes,
	Legend,
	Mark,
	Facet,
	Padding,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum NumericDomain {
	PointSize,
	StrokeWidth,
	BarWidth,
	CategoryGap,
	HoverTextSize,
	PlotPadding,
	TickRotation,
	TitleTextSize,
	SubtitleTextSize,
	XLabelTextSize,
	YLabelTextSize,
	XTickTextSize,
	YTickTextSize,
	FacetLabelTextSize,
	LegendTextSize,
	Finite,
	NonNegative,
	NonNegativeInteger,
}

impl NumericDomain {
	/// Validation delegates to the same newtypes used by authored resolution.
	#[cfg_attr(not(test), allow(dead_code))]
	pub(crate) fn accepts(self, value: f64) -> bool {
		let value32 = value as f32;
		match self {
			Self::PointSize => PointSize::new(value32).is_ok(),
			Self::StrokeWidth => StrokeWidth::new(value32).is_ok(),
			Self::BarWidth => BarWidth::new(value32).is_ok(),
			Self::CategoryGap => CategoryGap::new(value32).is_ok(),
			Self::HoverTextSize => HoverTextSize::new(value32).is_ok(),
			Self::PlotPadding => PlotPaddingPixels::new(value32).is_ok(),
			Self::TickRotation => TickRotation::new(value32).is_ok(),
			Self::TitleTextSize => TitleTextSize::new(value32).is_ok(),
			Self::SubtitleTextSize => SubtitleTextSize::new(value32).is_ok(),
			Self::XLabelTextSize => XLabelTextSize::new(value32).is_ok(),
			Self::YLabelTextSize => YLabelTextSize::new(value32).is_ok(),
			Self::XTickTextSize => XTickTextSize::new(value32).is_ok(),
			Self::YTickTextSize => YTickTextSize::new(value32).is_ok(),
			Self::FacetLabelTextSize => FacetLabelTextSize::new(value32).is_ok(),
			Self::LegendTextSize => LegendTextSize::new(value32).is_ok(),
			Self::Finite => value.is_finite(),
			Self::NonNegative => value.is_finite() && value >= 0.0,
			Self::NonNegativeInteger => value.is_finite() && value >= 0.0 && value.fract() == 0.0,
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ValueShape {
	Text,
	Toggle,
	Choice,
	Number(NumericDomain),
	NumberPair(NumericDomain),
	TemporalInterval,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum SettingValue {
	Text(String),
	Number(f64),
	Integer(u32),
	Toggle(bool),
	Pair(f64, f64),
	Choice(String),
	Unavailable,
}

impl std::fmt::Display for SettingValue {
	fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Text(value) | Self::Choice(value) => formatter.write_str(value),
			Self::Number(value) => write!(formatter, "{value}"),
			Self::Integer(value) => write!(formatter, "{value}"),
			Self::Toggle(value) => formatter.write_str(if *value { "on" } else { "off" }),
			Self::Pair(left, right) => write!(formatter, "{left}, {right}"),
			Self::Unavailable => formatter.write_str("—"),
		}
	}
}

pub(crate) type EffectiveProjection = fn(&ResolvedPlotSettings) -> SettingValue;
pub(crate) type AuthoredProjection = fn(&PlotSettingsLayer) -> bool;

#[derive(Clone, Copy, Debug)]
pub(crate) struct SettingSpec {
	pub(crate) key: SettingKey,
	pub(crate) label: &'static str,
	pub(crate) section: InspectorSection,
	pub(crate) target: LogicalTarget,
	pub(crate) callable: HandlerId,
	pub(crate) receiver: Option<SemanticReceiver>,
	#[allow(dead_code)] // ADR 0142 consumes widget and writeback constraints.
	pub(crate) shape: ValueShape,
	pub(crate) applies_to: &'static [PlotType],
	#[allow(dead_code)] // ADR 0142 consumes dependency enforcement.
	pub(crate) requires: &'static [SettingKey],
	#[allow(dead_code)] // ADR 0142 consumes conflict enforcement.
	pub(crate) conflicts: &'static [SettingKey],
	pub(crate) authored_projection: Option<AuthoredProjection>,
	pub(crate) effective_projection: Option<EffectiveProjection>,
}

const ALL_MARKS: &[PlotType] = &[
	PlotType::Bar,
	PlotType::BoxPlot,
	PlotType::Bubble,
	PlotType::Funnel,
	PlotType::Heatmap,
	PlotType::Histogram,
	PlotType::Line,
	PlotType::Parallel,
	PlotType::Pie,
	PlotType::Radar,
	PlotType::RadialDial,
	PlotType::Scatter,
	PlotType::StackedArea,
	PlotType::StackedBar,
	PlotType::Violin,
];
const POINT_MARKS: &[PlotType] = &[PlotType::Bubble, PlotType::Line, PlotType::Scatter];
const STROKE_MARKS: &[PlotType] = &[
	PlotType::Line,
	PlotType::Parallel,
	PlotType::Radar,
	PlotType::StackedArea,
];
const BAR_MARKS: &[PlotType] = &[
	PlotType::Bar,
	PlotType::BoxPlot,
	PlotType::Histogram,
	PlotType::StackedBar,
];
const STACK_MARKS: &[PlotType] = &[PlotType::Bar, PlotType::StackedArea, PlotType::StackedBar];
const HEATMAP: &[PlotType] = &[PlotType::Heatmap];
const NO_DEPENDENCIES: &[SettingKey] = &[];

fn authored_title(layer: &PlotSettingsLayer) -> bool {
	!matches!(layer.title, crate::plot::spec::Presence::Inherit)
}
fn authored_subtitle(layer: &PlotSettingsLayer) -> bool {
	!matches!(layer.subtitle, crate::plot::spec::Presence::Inherit)
}
fn authored_legend(layer: &PlotSettingsLayer) -> bool {
	!matches!(layer.legend, crate::plot::spec::Presence::Inherit)
}
fn authored_x_label(layer: &PlotSettingsLayer) -> bool {
	!matches!(layer.x_label, crate::plot::spec::Presence::Inherit)
}
fn authored_y_label(layer: &PlotSettingsLayer) -> bool {
	!matches!(layer.y_label, crate::plot::spec::Presence::Inherit)
}

macro_rules! authored_override {
	($function:ident, $field:ident) => {
		fn $function(layer: &PlotSettingsLayer) -> bool {
			!matches!(layer.$field, crate::plot::spec::Override::Inherit)
		}
	};
}

authored_override!(authored_title_size, title_size);
authored_override!(authored_subtitle_size, subtitle_size);
authored_override!(authored_palette, categorical_palette);
authored_override!(authored_sequential, sequential_ramp);
authored_override!(authored_diverging_palette, diverging_ramp);
authored_override!(authored_skin, plot_skin);
authored_override!(authored_hover, hover_size);
authored_override!(authored_facet_size, facet_label_size);
authored_override!(authored_legend_size, legend_size);
authored_override!(authored_padding_top, plot_padding_top);
authored_override!(authored_padding_right, plot_padding_right);
authored_override!(authored_padding_bottom, plot_padding_bottom);
authored_override!(authored_padding_left, plot_padding_left);
authored_override!(authored_x_label_size, x_label_size);
authored_override!(authored_y_label_size, y_label_size);
authored_override!(authored_x_tick_size, x_tick_size);
authored_override!(authored_y_tick_size, y_tick_size);
authored_override!(authored_x_rotation, x_tick_rotation);
authored_override!(authored_guides, guides);
authored_override!(authored_domains, domains);
authored_override!(authored_point_size, point_size);
authored_override!(authored_stroke_width, stroke_width);
authored_override!(authored_bar_width, bar_width);
authored_override!(authored_category_gap, category_gap);

fn effective_title(settings: &ResolvedPlotSettings) -> SettingValue {
	presence_text(&settings.title)
}
fn effective_subtitle(settings: &ResolvedPlotSettings) -> SettingValue {
	presence_text(&settings.subtitle)
}
fn effective_x_label(settings: &ResolvedPlotSettings) -> SettingValue {
	presence_text(&settings.x_label)
}
fn effective_y_label(settings: &ResolvedPlotSettings) -> SettingValue {
	presence_text(&settings.y_label)
}
fn presence_text(
	value: &crate::plot::spec::ResolvedPresence<std::sync::Arc<String>>,
) -> SettingValue {
	match value {
		crate::plot::spec::ResolvedPresence::Fallback => SettingValue::Unavailable,
		crate::plot::spec::ResolvedPresence::Show(value) => SettingValue::Text((**value).clone()),
		crate::plot::spec::ResolvedPresence::Hide => SettingValue::Choice("hidden".to_owned()),
	}
}

macro_rules! effective_number {
	($function:ident, $field:ident, $method:ident) => {
		fn $function(settings: &ResolvedPlotSettings) -> SettingValue {
			SettingValue::Number(settings.$field.$method() as f64)
		}
	};
	($function:ident, $field:ident) => {
		fn $function(settings: &ResolvedPlotSettings) -> SettingValue {
			SettingValue::Number(settings.$field as f64)
		}
	};
}

effective_number!(effective_title_size, title_size, pixels);
effective_number!(effective_subtitle_size, subtitle_size, pixels);
effective_number!(effective_hover, hover_size, points);
effective_number!(effective_facet_size, facet_label_size, pixels);
effective_number!(effective_legend_size, legend_size, pixels);
effective_number!(effective_x_label_size, x_label_size, pixels);
effective_number!(effective_y_label_size, y_label_size, pixels);
effective_number!(effective_x_tick_size, x_tick_size, pixels);
effective_number!(effective_y_tick_size, y_tick_size, pixels);
effective_number!(effective_x_rotation, x_tick_rotation, degrees);
effective_number!(effective_point_size, point_size, factor);
effective_number!(effective_stroke_width, stroke_width, factor);
effective_number!(effective_bar_width, bar_width, fraction);
effective_number!(effective_category_gap, category_gap, fraction);
effective_number!(effective_padding_top, plot_padding_top);
effective_number!(effective_padding_right, plot_padding_right);
effective_number!(effective_padding_bottom, plot_padding_bottom);
effective_number!(effective_padding_left, plot_padding_left);

fn effective_x_bounds(settings: &ResolvedPlotSettings) -> SettingValue {
	match (settings.x_min, settings.x_max) {
		(Some(min), Some(max)) => SettingValue::Pair(min, max),
		_ => SettingValue::Unavailable,
	}
}
fn effective_y_bounds(settings: &ResolvedPlotSettings) -> SettingValue {
	match (settings.y_min, settings.y_max) {
		(Some(min), Some(max)) => SettingValue::Pair(min, max),
		_ => SettingValue::Unavailable,
	}
}
fn effective_x_minor(settings: &ResolvedPlotSettings) -> SettingValue {
	SettingValue::Integer(settings.x_minor_ticks)
}
fn effective_y_minor(settings: &ResolvedPlotSettings) -> SettingValue {
	SettingValue::Integer(settings.y_minor_ticks)
}
fn effective_legend(settings: &ResolvedPlotSettings) -> SettingValue {
	match &settings.legend {
		crate::plot::spec::ResolvedPresence::Fallback => SettingValue::Choice("default".to_owned()),
		crate::plot::spec::ResolvedPresence::Show(position) => {
			SettingValue::Pair(position.x() as f64, position.y() as f64)
		}
		crate::plot::spec::ResolvedPresence::Hide => SettingValue::Choice("hidden".to_owned()),
	}
}
fn effective_categorical_palette(settings: &ResolvedPlotSettings) -> SettingValue {
	SettingValue::Choice(settings.color_theme.to_string())
}
fn effective_sequential_palette(settings: &ResolvedPlotSettings) -> SettingValue {
	SettingValue::Choice(settings.continuous_ramps.sequential.to_string())
}
fn effective_diverging_palette(settings: &ResolvedPlotSettings) -> SettingValue {
	SettingValue::Choice(settings.continuous_ramps.diverging.to_string())
}
fn effective_x_ticks(settings: &ResolvedPlotSettings) -> SettingValue {
	SettingValue::Choice(format!("{:?}", settings.x_tick_placement))
}
fn effective_y_ticks(settings: &ResolvedPlotSettings) -> SettingValue {
	SettingValue::Choice(format!("{:?}", settings.y_tick_placement))
}

macro_rules! setting {
	($key:ident, $label:literal, $section:ident, $target:ident, $callable:ident,
	 $receiver:expr, $shape:expr, $applies:expr, $requires:expr, $conflicts:expr,
	 $authored:expr, $effective:expr) => {
		SettingSpec {
			key: SettingKey::$key,
			label: $label,
			section: InspectorSection::$section,
			target: LogicalTarget::$target,
			callable: HandlerId::$callable,
			receiver: $receiver,
			shape: $shape,
			applies_to: $applies,
			requires: $requires,
			conflicts: $conflicts,
			authored_projection: $authored,
			effective_projection: $effective,
		}
	};
}

pub(crate) const SETTINGS: &[SettingSpec] = &[
	setting!(
		Title,
		"Title",
		Figure,
		Figure,
		Title,
		None,
		ValueShape::Text,
		ALL_MARKS,
		NO_DEPENDENCIES,
		NO_DEPENDENCIES,
		Some(authored_title),
		Some(effective_title)
	),
	setting!(
		Subtitle,
		"Subtitle",
		Figure,
		Figure,
		Subtitle,
		None,
		ValueShape::Text,
		ALL_MARKS,
		NO_DEPENDENCIES,
		NO_DEPENDENCIES,
		Some(authored_subtitle),
		Some(effective_subtitle)
	),
	setting!(
		TitleSize,
		"Title size",
		Figure,
		Figure,
		FigureFontSize,
		Some(SemanticReceiver::Title),
		ValueShape::Number(NumericDomain::TitleTextSize),
		ALL_MARKS,
		NO_DEPENDENCIES,
		NO_DEPENDENCIES,
		Some(authored_title_size),
		Some(effective_title_size)
	),
	setting!(
		SubtitleSize,
		"Subtitle size",
		Figure,
		Figure,
		FigureFontSize,
		Some(SemanticReceiver::Subtitle),
		ValueShape::Number(NumericDomain::SubtitleTextSize),
		ALL_MARKS,
		NO_DEPENDENCIES,
		NO_DEPENDENCIES,
		Some(authored_subtitle_size),
		Some(effective_subtitle_size)
	),
	setting!(
		CategoricalPalette,
		"Categorical palette",
		Figure,
		Figure,
		Palette,
		None,
		ValueShape::Choice,
		ALL_MARKS,
		NO_DEPENDENCIES,
		NO_DEPENDENCIES,
		Some(authored_palette),
		Some(effective_categorical_palette)
	),
	setting!(
		SequentialPalette,
		"Sequential ramp",
		Figure,
		Figure,
		SequentialPalette,
		None,
		ValueShape::Choice,
		ALL_MARKS,
		NO_DEPENDENCIES,
		NO_DEPENDENCIES,
		Some(authored_sequential),
		Some(effective_sequential_palette)
	),
	setting!(
		DivergingPalette,
		"Diverging ramp",
		Figure,
		Figure,
		DivergingPalette,
		None,
		ValueShape::Choice,
		ALL_MARKS,
		NO_DEPENDENCIES,
		NO_DEPENDENCIES,
		Some(authored_diverging_palette),
		Some(effective_diverging_palette)
	),
	setting!(
		PlotSkin,
		"Skin",
		Figure,
		Figure,
		Skin,
		None,
		ValueShape::Choice,
		ALL_MARKS,
		NO_DEPENDENCIES,
		NO_DEPENDENCIES,
		Some(authored_skin),
		None
	),
	setting!(
		SkinDuotone,
		"Duotone skin",
		Figure,
		Figure,
		SkinDuotone,
		Some(SemanticReceiver::Skin),
		ValueShape::Toggle,
		ALL_MARKS,
		&[SettingKey::PlotSkin],
		NO_DEPENDENCIES,
		Some(authored_skin),
		None
	),
	setting!(
		HoverSize,
		"Hover text size",
		Figure,
		Figure,
		HoverSize,
		None,
		ValueShape::Number(NumericDomain::HoverTextSize),
		ALL_MARKS,
		NO_DEPENDENCIES,
		NO_DEPENDENCIES,
		Some(authored_hover),
		Some(effective_hover)
	),
	setting!(
		FacetGutter,
		"Facet gutter",
		Facet,
		Facet,
		FacetGutter,
		Some(SemanticReceiver::Facet),
		ValueShape::Number(NumericDomain::NonNegative),
		ALL_MARKS,
		NO_DEPENDENCIES,
		NO_DEPENDENCIES,
		None,
		None
	),
	setting!(
		FacetLabelSize,
		"Facet label size",
		Facet,
		Facet,
		FigureLabelSize,
		Some(SemanticReceiver::Facet),
		ValueShape::Number(NumericDomain::FacetLabelTextSize),
		ALL_MARKS,
		NO_DEPENDENCIES,
		NO_DEPENDENCIES,
		Some(authored_facet_size),
		Some(effective_facet_size)
	),
	setting!(
		PaddingTop,
		"Top",
		Padding,
		Padding,
		PaddingTop,
		Some(SemanticReceiver::Padding),
		ValueShape::Number(NumericDomain::PlotPadding),
		ALL_MARKS,
		NO_DEPENDENCIES,
		NO_DEPENDENCIES,
		Some(authored_padding_top),
		Some(effective_padding_top)
	),
	setting!(
		PaddingRight,
		"Right",
		Padding,
		Padding,
		PaddingRight,
		Some(SemanticReceiver::Padding),
		ValueShape::Number(NumericDomain::PlotPadding),
		ALL_MARKS,
		NO_DEPENDENCIES,
		NO_DEPENDENCIES,
		Some(authored_padding_right),
		Some(effective_padding_right)
	),
	setting!(
		PaddingBottom,
		"Bottom",
		Padding,
		Padding,
		PaddingBottom,
		Some(SemanticReceiver::Padding),
		ValueShape::Number(NumericDomain::PlotPadding),
		ALL_MARKS,
		NO_DEPENDENCIES,
		NO_DEPENDENCIES,
		Some(authored_padding_bottom),
		Some(effective_padding_bottom)
	),
	setting!(
		PaddingLeft,
		"Left",
		Padding,
		Padding,
		PaddingLeft,
		Some(SemanticReceiver::Padding),
		ValueShape::Number(NumericDomain::PlotPadding),
		ALL_MARKS,
		NO_DEPENDENCIES,
		NO_DEPENDENCIES,
		Some(authored_padding_left),
		Some(effective_padding_left)
	),
	setting!(
		XAxisLabel,
		"X label",
		Axes,
		XAxis,
		AxisLabel,
		Some(SemanticReceiver::XAxis),
		ValueShape::Text,
		ALL_MARKS,
		NO_DEPENDENCIES,
		NO_DEPENDENCIES,
		Some(authored_x_label),
		Some(effective_x_label)
	),
	setting!(
		YAxisLabel,
		"Y label",
		Axes,
		YAxis,
		AxisLabel,
		Some(SemanticReceiver::YAxis),
		ValueShape::Text,
		ALL_MARKS,
		NO_DEPENDENCIES,
		NO_DEPENDENCIES,
		Some(authored_y_label),
		Some(effective_y_label)
	),
	setting!(
		XAxisLabelSize,
		"X label size",
		Axes,
		XAxis,
		FigureLabelSize,
		Some(SemanticReceiver::XAxis),
		ValueShape::Number(NumericDomain::XLabelTextSize),
		ALL_MARKS,
		NO_DEPENDENCIES,
		NO_DEPENDENCIES,
		Some(authored_x_label_size),
		Some(effective_x_label_size)
	),
	setting!(
		YAxisLabelSize,
		"Y label size",
		Axes,
		YAxis,
		FigureLabelSize,
		Some(SemanticReceiver::YAxis),
		ValueShape::Number(NumericDomain::YLabelTextSize),
		ALL_MARKS,
		NO_DEPENDENCIES,
		NO_DEPENDENCIES,
		Some(authored_y_label_size),
		Some(effective_y_label_size)
	),
	setting!(
		XAxisBounds,
		"X bounds",
		Axes,
		XAxis,
		AxisBounds,
		Some(SemanticReceiver::XAxis),
		ValueShape::NumberPair(NumericDomain::Finite),
		ALL_MARKS,
		NO_DEPENDENCIES,
		NO_DEPENDENCIES,
		Some(authored_domains),
		Some(effective_x_bounds)
	),
	setting!(
		YAxisBounds,
		"Y bounds",
		Axes,
		YAxis,
		AxisBounds,
		Some(SemanticReceiver::YAxis),
		ValueShape::NumberPair(NumericDomain::Finite),
		ALL_MARKS,
		NO_DEPENDENCIES,
		NO_DEPENDENCIES,
		Some(authored_domains),
		Some(effective_y_bounds)
	),
	setting!(
		XAxisTickStep,
		"X tick step",
		Axes,
		XAxis,
		AxisTickStep,
		Some(SemanticReceiver::XAxis),
		ValueShape::Number(NumericDomain::Finite),
		ALL_MARKS,
		NO_DEPENDENCIES,
		&[SettingKey::XAxisTickEvery],
		Some(authored_guides),
		Some(effective_x_ticks)
	),
	setting!(
		YAxisTickStep,
		"Y tick step",
		Axes,
		YAxis,
		AxisTickStep,
		Some(SemanticReceiver::YAxis),
		ValueShape::Number(NumericDomain::Finite),
		ALL_MARKS,
		NO_DEPENDENCIES,
		&[SettingKey::YAxisTickEvery],
		Some(authored_guides),
		Some(effective_y_ticks)
	),
	setting!(
		XAxisTickEvery,
		"X tick interval",
		Axes,
		XAxis,
		AxisTickEvery,
		Some(SemanticReceiver::XAxis),
		ValueShape::TemporalInterval,
		ALL_MARKS,
		NO_DEPENDENCIES,
		&[SettingKey::XAxisTickStep, SettingKey::XAxisMinorTicks,],
		Some(authored_guides),
		Some(effective_x_ticks)
	),
	setting!(
		YAxisTickEvery,
		"Y tick interval",
		Axes,
		YAxis,
		AxisTickEvery,
		Some(SemanticReceiver::YAxis),
		ValueShape::TemporalInterval,
		ALL_MARKS,
		NO_DEPENDENCIES,
		&[SettingKey::YAxisTickStep, SettingKey::YAxisMinorTicks,],
		Some(authored_guides),
		Some(effective_y_ticks)
	),
	setting!(
		XAxisMinorTicks,
		"X minor ticks",
		Axes,
		XAxis,
		AxisMinorTicks,
		Some(SemanticReceiver::XAxis),
		ValueShape::Number(NumericDomain::NonNegativeInteger),
		ALL_MARKS,
		NO_DEPENDENCIES,
		&[SettingKey::XAxisTickEvery],
		Some(authored_guides),
		Some(effective_x_minor)
	),
	setting!(
		YAxisMinorTicks,
		"Y minor ticks",
		Axes,
		YAxis,
		AxisMinorTicks,
		Some(SemanticReceiver::YAxis),
		ValueShape::Number(NumericDomain::NonNegativeInteger),
		ALL_MARKS,
		NO_DEPENDENCIES,
		&[SettingKey::YAxisTickEvery],
		Some(authored_guides),
		Some(effective_y_minor)
	),
	setting!(
		XAxisTickSize,
		"X tick size",
		Axes,
		XAxis,
		AxisTickSize,
		Some(SemanticReceiver::XAxis),
		ValueShape::Number(NumericDomain::XTickTextSize),
		ALL_MARKS,
		NO_DEPENDENCIES,
		NO_DEPENDENCIES,
		Some(authored_x_tick_size),
		Some(effective_x_tick_size)
	),
	setting!(
		YAxisTickSize,
		"Y tick size",
		Axes,
		YAxis,
		AxisTickSize,
		Some(SemanticReceiver::YAxis),
		ValueShape::Number(NumericDomain::YTickTextSize),
		ALL_MARKS,
		NO_DEPENDENCIES,
		NO_DEPENDENCIES,
		Some(authored_y_tick_size),
		Some(effective_y_tick_size)
	),
	setting!(
		XAxisTickRotation,
		"X tick rotation",
		Axes,
		XAxis,
		AxisTickRotation,
		Some(SemanticReceiver::XAxis),
		ValueShape::Number(NumericDomain::TickRotation),
		ALL_MARKS,
		NO_DEPENDENCIES,
		NO_DEPENDENCIES,
		Some(authored_x_rotation),
		Some(effective_x_rotation)
	),
	setting!(
		LegendPosition,
		"Position",
		Legend,
		Legend,
		LegendAt,
		Some(SemanticReceiver::Legend),
		ValueShape::NumberPair(NumericDomain::Finite),
		ALL_MARKS,
		NO_DEPENDENCIES,
		NO_DEPENDENCIES,
		Some(authored_legend),
		Some(effective_legend)
	),
	setting!(
		LegendPresence,
		"Visible",
		Legend,
		Legend,
		LegendHide,
		Some(SemanticReceiver::Legend),
		ValueShape::Toggle,
		ALL_MARKS,
		NO_DEPENDENCIES,
		NO_DEPENDENCIES,
		Some(authored_legend),
		Some(effective_legend)
	),
	setting!(
		LegendSize,
		"Text size",
		Legend,
		Legend,
		FigureFontSize,
		Some(SemanticReceiver::Legend),
		ValueShape::Number(NumericDomain::LegendTextSize),
		ALL_MARKS,
		NO_DEPENDENCIES,
		NO_DEPENDENCIES,
		Some(authored_legend_size),
		Some(effective_legend_size)
	),
	setting!(
		PointSize,
		"Point size",
		Mark,
		Mark,
		MarkPointSize,
		Some(SemanticReceiver::Mark),
		ValueShape::Number(NumericDomain::PointSize),
		POINT_MARKS,
		NO_DEPENDENCIES,
		NO_DEPENDENCIES,
		Some(authored_point_size),
		Some(effective_point_size)
	),
	setting!(
		StrokeWidth,
		"Stroke width",
		Mark,
		Mark,
		MarkStrokeWidth,
		Some(SemanticReceiver::Mark),
		ValueShape::Number(NumericDomain::StrokeWidth),
		STROKE_MARKS,
		NO_DEPENDENCIES,
		NO_DEPENDENCIES,
		Some(authored_stroke_width),
		Some(effective_stroke_width)
	),
	setting!(
		BarWidth,
		"Bar width",
		Mark,
		Mark,
		MarkBarWidth,
		Some(SemanticReceiver::Mark),
		ValueShape::Number(NumericDomain::BarWidth),
		BAR_MARKS,
		NO_DEPENDENCIES,
		NO_DEPENDENCIES,
		Some(authored_bar_width),
		Some(effective_bar_width)
	),
	setting!(
		CategoryGap,
		"Category gap",
		Mark,
		Mark,
		MarkCategoryGap,
		Some(SemanticReceiver::Mark),
		ValueShape::Number(NumericDomain::CategoryGap),
		BAR_MARKS,
		NO_DEPENDENCIES,
		NO_DEPENDENCIES,
		Some(authored_category_gap),
		Some(effective_category_gap)
	),
	setting!(
		Stacked,
		"Stacked",
		Mark,
		Mark,
		MarkStacked,
		Some(SemanticReceiver::Mark),
		ValueShape::Toggle,
		STACK_MARKS,
		NO_DEPENDENCIES,
		NO_DEPENDENCIES,
		None,
		None
	),
	setting!(
		Percent,
		"Percent",
		Mark,
		Mark,
		MarkPercent,
		Some(SemanticReceiver::Mark),
		ValueShape::Toggle,
		STACK_MARKS,
		&[SettingKey::Stacked],
		NO_DEPENDENCIES,
		None,
		None
	),
	setting!(
		Diverging,
		"Diverging color",
		Mark,
		Mark,
		MarkDiverging,
		Some(SemanticReceiver::Mark),
		ValueShape::Toggle,
		HEATMAP,
		NO_DEPENDENCIES,
		NO_DEPENDENCIES,
		None,
		None
	),
	setting!(
		DivergingAt,
		"Diverging midpoint",
		Mark,
		Mark,
		MarkDivergingAt,
		Some(SemanticReceiver::Mark),
		ValueShape::Number(NumericDomain::Finite),
		HEATMAP,
		&[SettingKey::Diverging],
		NO_DEPENDENCIES,
		None,
		None
	),
];

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn setting_spec(key: SettingKey) -> &'static SettingSpec {
	SETTINGS
		.iter()
		.find(|setting| setting.key == key)
		.expect("every stable SettingKey has one SettingSpec")
}

pub(crate) fn setting_for_call(
	handler: HandlerId,
	receiver: Option<SemanticReceiver>,
) -> Option<&'static SettingSpec> {
	SETTINGS.iter().find(|setting| {
		setting.callable == handler && (setting.receiver.is_none() || setting.receiver == receiver)
	})
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ExpressionShape {
	DirectLiteral,
	ExpressionOrBinding,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SettingOccurrence {
	pub(crate) key: SettingKey,
	pub(crate) target: LogicalTarget,
	pub(crate) call_span: Range<u32>,
	pub(crate) argument_spans: Vec<Range<u32>>,
	pub(crate) argument_text: Vec<String>,
	pub(crate) expression_shape: ExpressionShape,
	/// Non-empty by construction. One source occurrence can feed many views.
	pub(crate) owners: HashSet<LocalViewIdentity>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ContainerKind {
	Figure,
	Layers,
}

/// Factual syntax only. ADR 0142 decides whether any boundary is editable.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PresentationSyntaxBoundary {
	pub(crate) handler: HandlerId,
	pub(crate) call_span: Range<u32>,
	pub(crate) chain_span: Range<u32>,
	pub(crate) argument_list_span: Range<u32>,
	pub(crate) separators: Vec<Range<u32>>,
	pub(crate) container: Option<(ContainerKind, Range<u32>)>,
	pub(crate) owners: HashSet<LocalViewIdentity>,
}

/// The exact source expression occupying one `views([...])` slot. This is a
/// compiler fact, not permission to edit it; ADR 0142 uses it to distinguish
/// an inline bare mark from helper-built furniture.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PresentationViewSlotBoundary {
	pub(crate) expression_span: Range<u32>,
	pub(crate) owner: LocalViewIdentity,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct PresentationSourceModel {
	pub(crate) occurrences: Vec<SettingOccurrence>,
	pub(crate) syntax: Vec<PresentationSyntaxBoundary>,
	pub(crate) view_slots: Vec<PresentationViewSlotBoundary>,
}

/// The compiler emits `PresentationSourceModel` from text alone. The host adds
/// document identity and the generation against which those spans were read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StampedPresentationSourceModel {
	pub(crate) buffer_id: BufferId,
	pub(crate) document_generation: libaryon::core::StateId,
	pub(crate) model: std::sync::Arc<PresentationSourceModel>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PlotSourceAttachment {
	pub(crate) identity: LocalViewIdentity,
	pub(crate) source: StampedPresentationSourceModel,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SourceAttachmentStatus {
	Attached,
	OutOfDate,
	Detached,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ProvenanceTier {
	LibraryDefault,
	ApplicationTheme,
	AuthoredSpec,
	Layer,
	PaneOverride,
}

impl std::fmt::Display for ProvenanceTier {
	fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		formatter.write_str(match self {
			Self::LibraryDefault => "default",
			Self::ApplicationTheme => "theme",
			Self::AuthoredSpec => "authored",
			Self::Layer => "layer",
			Self::PaneOverride => "pane",
		})
	}
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct InspectorSettingRow {
	pub(crate) key: SettingKey,
	pub(crate) section: InspectorSection,
	pub(crate) label: &'static str,
	pub(crate) value: SettingValue,
	pub(crate) provenance: ProvenanceTier,
	pub(crate) expression_authored: bool,
	pub(crate) shared_source: bool,
	/// Whether the static source model attributed this setting. Authored
	/// provenance can still be true from evaluated spec projection when the
	/// scanner is partial; 0142 treats that combination as read-only.
	pub(crate) source_occurrence: bool,
}

/// The authored presentation family for a resolved renderer type. Orientation
/// is decided by resolution after the x/y pair selects it; the settings
/// surface is authored in orientation-free vocabulary, so applicability is
/// judged against the family, never the physical renderer.
fn presentation_family(plot_type: PlotType) -> PlotType {
	match plot_type {
		PlotType::HorizontalBar => PlotType::Bar,
		PlotType::HorizontalStackedBar => PlotType::StackedBar,
		other => other,
	}
}

pub(crate) fn inspector_rows(
	settings: &PlotSettings,
	resolved: &ResolvedPlotSettings,
	plot_type: Option<PlotType>,
	attachment: Option<&PlotSourceAttachment>,
) -> Vec<InspectorSettingRow> {
	let plot_type = plot_type.map(presentation_family);
	SETTINGS
		.iter()
		.filter(|spec| {
			plot_type
				.map(|plot_type| spec.applies_to.contains(&plot_type))
				.unwrap_or(spec.applies_to == ALL_MARKS)
		})
		.filter_map(|spec| {
			let occurrence = attachment.and_then(|attachment| {
				attachment
					.source
					.model
					.occurrences
					.iter()
					.find(|occurrence| {
						occurrence.key == spec.key
							&& occurrence.target == spec.target
							&& occurrence.owners.contains(&attachment.identity)
					})
			});
			let value = spec
				.effective_projection
				.map(|project| project(resolved))
				.or_else(|| special_effective_value(spec.key, settings, occurrence))?;
			let authored_here = occurrence.is_some()
				|| spec
					.authored_projection
					.is_some_and(|project| project(&settings.authored_spec));
			let provenance = if layer_authors(spec, &settings.pane_override) {
				ProvenanceTier::PaneOverride
			} else if layer_authors(spec, &settings.layer) {
				ProvenanceTier::Layer
			} else if authored_here {
				ProvenanceTier::AuthoredSpec
			} else if layer_authors(spec, &settings.application_theme) {
				ProvenanceTier::ApplicationTheme
			} else {
				ProvenanceTier::LibraryDefault
			};
			Some(InspectorSettingRow {
				key: spec.key,
				section: spec.section,
				label: spec.label,
				value,
				provenance,
				expression_authored: occurrence.is_some_and(|occurrence| {
					occurrence.expression_shape == ExpressionShape::ExpressionOrBinding
				}),
				shared_source: occurrence.is_some_and(|occurrence| occurrence.owners.len() > 1),
				source_occurrence: occurrence.is_some(),
			})
		})
		.collect()
}

fn layer_authors(spec: &SettingSpec, layer: &PlotSettingsLayer) -> bool {
	spec.authored_projection
		.is_some_and(|project| project(layer))
		|| (matches!(
			spec.key,
			SettingKey::CategoricalPalette
				| SettingKey::SequentialPalette
				| SettingKey::DivergingPalette
		) && !matches!(layer.theme, crate::plot::spec::Override::Inherit))
}

fn special_effective_value(
	key: SettingKey,
	settings: &PlotSettings,
	occurrence: Option<&SettingOccurrence>,
) -> Option<SettingValue> {
	match key {
		SettingKey::PlotSkin => Some(
			settings
				.resolve_plot_skin()
				.map(|skin| SettingValue::Choice((*skin.name).clone()))
				.unwrap_or_else(|| SettingValue::Choice("default".to_owned())),
		),
		SettingKey::SkinDuotone => Some(SettingValue::Toggle(occurrence.is_some())),
		SettingKey::LegendPresence => occurrence.map(|_| SettingValue::Toggle(false)),
		SettingKey::Stacked | SettingKey::Percent | SettingKey::Diverging => {
			Some(SettingValue::Toggle(occurrence.is_some()))
		}
		_ => occurrence.map(|occurrence| {
			if occurrence.argument_text.is_empty() {
				SettingValue::Unavailable
			} else {
				SettingValue::Text(occurrence.argument_text.join(", "))
			}
		}),
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::collections::HashSet;

	#[test]
	fn registry_has_no_duplicate_handler_routes() {
		let mut seen = HashSet::new();
		for callable in CALLABLES {
			assert!(!callable.registration_routes.is_empty());
			for route in callable.registration_routes {
				assert!(seen.insert((callable.name, callable.handler, *route)));
			}
		}
	}

	#[test]
	fn registration_and_acceptance_are_distinct_facts() {
		let font_size = CALLABLES
			.iter()
			.find(|callable| callable.handler == HandlerId::FigureFontSize)
			.unwrap();
		assert!(
			font_size
				.registration_routes
				.contains(&RegistrationRoute::Skin)
		);
		assert!(
			!font_size
				.accepted_receivers
				.contains(&SemanticReceiver::Skin)
		);
	}

	#[test]
	fn settings_are_unique_and_project_only_registered_semantics() {
		let mut keys = HashSet::new();
		for setting in SETTINGS {
			assert!(keys.insert(setting.key), "duplicate {:?}", setting.key);
			let callable = CALLABLES
				.iter()
				.find(|callable| callable.handler == setting.callable)
				.expect("SettingSpec references a registered callable");
			if let Some(receiver) = setting.receiver {
				assert!(
					callable.accepted_receivers.contains(&receiver),
					"{:?} is not accepted by {}",
					setting.key,
					callable.name
				);
			}
		}
		assert_eq!(keys.len(), SETTINGS.len());
		assert_eq!(
			setting_spec(SettingKey::Percent).requires,
			&[SettingKey::Stacked]
		);
		assert_eq!(
			setting_spec(SettingKey::XAxisTickStep).conflicts,
			&[SettingKey::XAxisTickEvery]
		);
		assert!(
			setting_for_call(HandlerId::Facet, None).is_none(),
			"facet field selection is outside the first settings projection"
		);
		assert!(
			setting_for_call(HandlerId::MarkCategory, Some(SemanticReceiver::Mark)).is_none(),
			"channel bindings are outside the first settings projection"
		);
	}

	#[test]
	fn numeric_domains_use_the_plot_spec_validators() {
		assert_eq!(
			NumericDomain::PointSize.accepts(PointSize::MIN_FACTOR as f64),
			PointSize::new(PointSize::MIN_FACTOR).is_ok()
		);
		assert_eq!(
			NumericDomain::TickRotation.accepts((TickRotation::MAX_DEGREES + 1.0) as f64),
			TickRotation::new(TickRotation::MAX_DEGREES + 1.0).is_ok()
		);
		assert_eq!(
			NumericDomain::BarWidth.accepts(1.25),
			BarWidth::new(1.25).is_ok()
		);
	}

	#[test]
	fn temporal_ticks_and_minor_ticks_conflict_symmetrically() {
		for (temporal, minor) in [
			(SettingKey::XAxisTickEvery, SettingKey::XAxisMinorTicks),
			(SettingKey::YAxisTickEvery, SettingKey::YAxisMinorTicks),
		] {
			assert!(setting_spec(temporal).conflicts.contains(&minor));
			assert!(setting_spec(minor).conflicts.contains(&temporal));
		}
	}

	#[test]
	fn inspector_provenance_obeys_the_five_layer_order() {
		let mut settings = PlotSettings::default();
		settings.authored_spec.stroke_width =
			crate::plot::spec::Override::Set(StrokeWidth::new(2.0).unwrap());
		settings.pane_override.stroke_width =
			crate::plot::spec::Override::Set(StrokeWidth::new(3.0).unwrap());
		let rows = inspector_rows(&settings, &settings.resolve(), Some(PlotType::Line), None);
		let stroke = rows
			.iter()
			.find(|row| row.key == SettingKey::StrokeWidth)
			.unwrap();
		assert_eq!(stroke.value, SettingValue::Number(3.0));
		assert_eq!(stroke.provenance, ProvenanceTier::PaneOverride);

		let mut themed = PlotSettings::default();
		themed.application_theme.theme =
			crate::plot::spec::Override::Set(crate::plot::theme::PlotThemeSpec::default());
		let rows = inspector_rows(&themed, &themed.resolve(), Some(PlotType::Line), None);
		assert_eq!(
			rows.iter()
				.find(|row| row.key == SettingKey::CategoricalPalette)
				.unwrap()
				.provenance,
			ProvenanceTier::ApplicationTheme
		);
		assert_eq!(
			rows.iter()
				.find(|row| row.key == SettingKey::StrokeWidth)
				.unwrap()
				.provenance,
			ProvenanceTier::LibraryDefault,
			"a color theme does not author unrelated numeric settings"
		);
	}

	#[test]
	fn inspector_badges_expression_and_shared_source_per_logical_target() {
		let identity = LocalViewIdentity::Named("first".to_owned());
		let occurrence = SettingOccurrence {
			key: SettingKey::XAxisLabel,
			target: LogicalTarget::XAxis,
			call_span: 10..24,
			argument_spans: vec![16..23],
			argument_text: vec!["column_name".to_owned()],
			expression_shape: ExpressionShape::ExpressionOrBinding,
			owners: HashSet::from([
				identity.clone(),
				LocalViewIdentity::Named("second".to_owned()),
			]),
		};
		let attachment = PlotSourceAttachment {
			identity,
			source: StampedPresentationSourceModel {
				buffer_id: BufferId::new(7),
				document_generation: libaryon::core::StateId::new(3),
				model: std::sync::Arc::new(PresentationSourceModel {
					occurrences: vec![occurrence],
					syntax: Vec::new(),
					view_slots: Vec::new(),
				}),
			},
		};
		let mut settings = PlotSettings::default();
		settings.authored_spec.x_label =
			crate::plot::spec::Presence::Show(std::sync::Arc::new("Evaluated axis".to_owned()));
		let rows = inspector_rows(
			&settings,
			&settings.resolve(),
			Some(PlotType::Bar),
			Some(&attachment),
		);
		let x_label = rows
			.iter()
			.find(|row| row.key == SettingKey::XAxisLabel)
			.unwrap();
		assert_eq!(x_label.provenance, ProvenanceTier::AuthoredSpec);
		assert!(x_label.expression_authored);
		assert!(x_label.shared_source);
		assert_eq!(
			x_label.value,
			SettingValue::Text("Evaluated axis".to_owned()),
			"the inspector displays the effective value rather than expression text"
		);
		assert!(!rows.iter().any(|row| row.key == SettingKey::PointSize));
		let unknown = inspector_rows(&settings, &settings.resolve(), None, None);
		assert!(
			!unknown.iter().any(|row| matches!(
				row.key,
				SettingKey::PointSize | SettingKey::BarWidth | SettingKey::CategoryGap
			)),
			"an unknown or layered mark family exposes only universally applicable settings"
		);
	}

	#[test]
	fn horizontal_renderers_inspect_as_their_authored_family() {
		let settings = PlotSettings::default();
		let resolved = settings.resolve();
		for (renderer, family) in [
			(PlotType::HorizontalBar, PlotType::Bar),
			(PlotType::HorizontalStackedBar, PlotType::StackedBar),
		] {
			let horizontal = inspector_rows(&settings, &resolved, Some(renderer), None);
			let vertical = inspector_rows(&settings, &resolved, Some(family), None);
			assert!(
				!horizontal.is_empty(),
				"{renderer:?} must expose its authored family's settings"
			);
			assert_eq!(
				horizontal.iter().map(|row| row.key).collect::<Vec<_>>(),
				vertical.iter().map(|row| row.key).collect::<Vec<_>>(),
				"orientation must not change the inspected surface"
			);
		}
	}

	#[test]
	fn authored_projection_survives_an_incomplete_source_model() {
		let identity = LocalViewIdentity::Named("plot".to_owned());
		let attachment = PlotSourceAttachment {
			identity,
			source: StampedPresentationSourceModel {
				buffer_id: BufferId::new(9),
				document_generation: libaryon::core::StateId::new(4),
				model: std::sync::Arc::new(PresentationSourceModel::default()),
			},
		};
		let mut settings = PlotSettings::default();
		settings.authored_spec.x_label =
			crate::plot::spec::Presence::Show(std::sync::Arc::new("Revenue".to_owned()));
		let rows = inspector_rows(
			&settings,
			&settings.resolve(),
			Some(PlotType::Bar),
			Some(&attachment),
		);
		let x_label = rows
			.iter()
			.find(|row| row.key == SettingKey::XAxisLabel)
			.expect("x-axis label row");
		assert_eq!(x_label.provenance, ProvenanceTier::AuthoredSpec);
		assert!(!x_label.expression_authored);
		assert!(!x_label.shared_source);
	}
}
