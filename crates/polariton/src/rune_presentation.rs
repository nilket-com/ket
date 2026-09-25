//! PRODUCT-0076's bounded, typed Rune presentation evaluator.
//!
//! Relation planning remains in Toron. This module owns only values which can
//! be produced before a relation schema exists, and every value carries an
//! origin in the author's original buffer.

use crate::plot::chrome::PlotChromeProjection;
use crate::plot::colors::ColorTheme;
use crate::plot::common::{PositiveTemporalInterval, TickPlacement};
use crate::plot::composition::PlotComposition;
use crate::plot::continuous_color::{ContinuousAnchor, ContinuousScaleIntent};
use crate::plot::core::PlotType;
use crate::plot::spec::{
	BarWidth, BoxPlotFields, BubbleFieldNames, BubbleFields, CandlestickFields, CategoryGap,
	FacetLabelTextSize, FillBetweenFields, FunnelFields, HeatmapFields, HexbinFields,
	HoverTextSize, LayerSettings, LayeredPlotSpec, LegendPosition, LegendTextSize,
	MultivariateFields, Override, PieFields, PlotPaddingPixels, PointSize, Presence,
	RadialDialFields, StrokeWidth, SubtitleTextSize, TickRotation, TitleTextSize, ViolinFields,
	XLabelTextSize, XTickTextSize, YLabelTextSize, YTickTextSize,
};
use crate::plot::theme::{
	PlotDomainOverrides, PlotGuideSettings, PlotSettingsLayer, PlotSkinSelection,
};
use crate::presentation_registry::{CALLABLES, HandlerId, RegistrationRoute};
use crate::rune_relation_envelope::{
	BoxPlotFieldOrigins, BubbleFieldOrigins, CandlestickFieldOrigins, DisposableRunePlotSink,
	FillBetweenFieldOrigins, FunnelFieldOrigins, HeatmapFieldOrigins, HexbinFieldOrigins,
	MultivariateFieldOrigins, PieFieldOrigins, RadialDialFieldOrigins, RunePlotBody,
	RunePlotDomainSpans, RunePlotIntent, RunePlotMinorTickSpans, RunePlotTickEverySpans,
	RunePlotTickStepSpans, RuneTableSink, RuneTableSkinIntent, RuneTableSkinProjection,
	RuneViewIntent, SpannedMarkFields, StyledRunePlotSink, ViolinFieldOrigins,
};
use boson::TemporalInterval;
use rune::alloc::limit;
use rune::ast::Spanned;
use rune::diagnostics::{Diagnostic, FatalDiagnosticKind};
use rune::runtime::{InstAddress, Memory, Output, Value, VmResult, budget};
use rune::{Any, Context, Diagnostics, Module, Ref, Source, Sources, Vm};
use std::collections::HashSet;
use std::ops::Range;
use std::sync::Arc;

pub(crate) const PRESENTATION_COMPILE_ALLOCATION_CAP: usize = 256 * 1024 * 1024;
pub(crate) const PRESENTATION_RUNTIME_ALLOCATION_CAP: usize = 256 * 1024 * 1024;
pub(crate) const PRESENTATION_INSTRUCTION_BUDGET: usize = 50_000_000;

const MODULE: &str = "polariton_presentation";

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PresentationFailure {
	pub(crate) span: Range<u32>,
	pub(crate) message: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum PresentationMapKind {
	Copied { original_start: u32 },
	Synthetic { owner: Range<u32> },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PresentationMapSegment {
	pub(crate) generated: Range<u32>,
	pub(crate) kind: PresentationMapKind,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PresentationSourceMap {
	pub(crate) segments: Vec<PresentationMapSegment>,
	pub(crate) fallback: Range<u32>,
}

impl PresentationSourceMap {
	pub(crate) fn map_span(&self, generated: Range<usize>) -> Range<u32> {
		let Ok(start) = u32::try_from(generated.start) else {
			return self.fallback.clone();
		};
		let Ok(end) = u32::try_from(generated.end) else {
			return self.fallback.clone();
		};
		let generated = start..end;
		self.segments
			.iter()
			.find_map(|segment| {
				if generated.start < segment.generated.start
					|| generated.end > segment.generated.end
				{
					return None;
				}
				Some(match &segment.kind {
					PresentationMapKind::Copied { original_start } => {
						let relative_start = generated.start - segment.generated.start;
						let relative_end = generated.end - segment.generated.start;
						original_start + relative_start..original_start + relative_end
					}
					PresentationMapKind::Synthetic { owner } => owner.clone(),
				})
			})
			.or_else(|| {
				let mut mapped = self
					.segments
					.iter()
					.filter(|segment| {
						generated.start < segment.generated.end
							&& generated.end > segment.generated.start
					})
					.map(|segment| match &segment.kind {
						PresentationMapKind::Copied { original_start } => {
							let overlap_start = generated.start.max(segment.generated.start);
							let overlap_end = generated.end.min(segment.generated.end);
							original_start + (overlap_start - segment.generated.start)
								..original_start + (overlap_end - segment.generated.start)
						}
						PresentationMapKind::Synthetic { owner } => owner.clone(),
					});
				let first = mapped.next()?;
				Some(mapped.fold(first, |union, span| {
					union.start.min(span.start)..union.end.max(span.end)
				}))
			})
			.unwrap_or_else(|| self.fallback.clone())
	}

	#[cfg(test)]
	fn identity(source: &str, fallback: Range<u32>) -> Self {
		Self {
			segments: vec![PresentationMapSegment {
				generated: 0..u32::try_from(source.len()).expect("test source fits u32"),
				kind: PresentationMapKind::Copied { original_start: 0 },
			}],
			fallback,
		}
	}
}

impl PresentationFailure {
	fn new(origin: Origin, message: impl Into<String>) -> Self {
		Self {
			span: origin.range(),
			message: message.into(),
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Origin {
	start: u32,
	end: u32,
}

impl Origin {
	fn decode(start: i64, end: i64) -> Result<Self, PresentationFailure> {
		let start = u32::try_from(start).map_err(|_| PresentationFailure {
			span: 0..0,
			message: "generated presentation origin has an invalid start".to_owned(),
		})?;
		let end = u32::try_from(end).map_err(|_| PresentationFailure {
			span: 0..0,
			message: "generated presentation origin has an invalid end".to_owned(),
		})?;
		if start > end {
			return Err(PresentationFailure {
				span: 0..0,
				message: "generated presentation origin is reversed".to_owned(),
			});
		}
		Ok(Self { start, end })
	}

	fn range(self) -> Range<u32> {
		self.start..self.end
	}
}

#[derive(Clone, Debug)]
enum FigureElementKind {
	Title(String),
	Subtitle(String),
	TextHandle(TextHandleKind),
	Axis(Axis),
	Facet(String),
	Palette(String),
	SequentialPalette(ColorTheme),
	DivergingPalette(ColorTheme),
	Legend,
	HoverSize(HoverTextSize),
	Padding,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Axis {
	X,
	Y,
}

impl Axis {
	fn name(self) -> &'static str {
		match self {
			Self::X => "x_axis",
			Self::Y => "y_axis",
		}
	}
}

#[derive(Clone, Debug, Default)]
struct AxisProperties {
	label: Option<(String, Origin)>,
	label_size: Option<(AxisLabelSize, Origin)>,
	bounds: Option<(f64, f64, Origin)>,
	tick_step: Option<(f64, Origin)>,
	tick_every: Option<(PositiveTemporalInterval, Origin)>,
	minor_ticks: Option<(u32, Origin)>,
	tick_size: Option<(AxisTickSize, Origin)>,
	tick_rotation: Option<(TickRotation, Origin)>,
}

impl AxisProperties {
	fn is_empty(&self) -> bool {
		self.label.is_none()
			&& self.label_size.is_none()
			&& self.bounds.is_none()
			&& self.tick_step.is_none()
			&& self.tick_every.is_none()
			&& self.minor_ticks.is_none()
			&& self.tick_size.is_none()
			&& self.tick_rotation.is_none()
	}
}

#[derive(Clone, Copy, Debug)]
enum AxisLabelSize {
	X(XLabelTextSize),
	Y(YLabelTextSize),
}

#[derive(Clone, Copy, Debug)]
enum AxisTickSize {
	X(XTickTextSize),
	Y(YTickTextSize),
}

#[derive(Clone, Copy, Debug, Default)]
struct FigurePadding {
	top: Option<PlotPaddingPixels>,
	right: Option<PlotPaddingPixels>,
	bottom: Option<PlotPaddingPixels>,
	left: Option<PlotPaddingPixels>,
}

#[derive(Clone, Copy, Debug)]
enum PaddingEdge {
	Top,
	Right,
	Bottom,
	Left,
}

impl PaddingEdge {
	fn index(self) -> usize {
		match self {
			Self::Top => 0,
			Self::Right => 1,
			Self::Bottom => 2,
			Self::Left => 3,
		}
	}

	fn name(self) -> &'static str {
		match self {
			Self::Top => "top",
			Self::Right => "right",
			Self::Bottom => "bottom",
			Self::Left => "left",
		}
	}

	fn slot(self, padding: &mut FigurePadding) -> &mut Option<PlotPaddingPixels> {
		match self {
			Self::Top => &mut padding.top,
			Self::Right => &mut padding.right,
			Self::Bottom => &mut padding.bottom,
			Self::Left => &mut padding.left,
		}
	}
}

#[derive(Clone, Copy, Debug)]
enum FigureTextSize {
	Title(TitleTextSize),
	Subtitle(SubtitleTextSize),
	Legend(LegendTextSize),
}

#[derive(Clone, Copy, Debug)]
enum TextHandleKind {
	Title,
	Subtitle,
}

#[derive(Any, Clone, Debug)]
#[rune(item = ::polariton_presentation, name = FigureElement)]
struct RuneFigureElement {
	value: Result<FigureElementKind, PresentationFailure>,
	origin: Origin,
	font_size: Option<FigureTextSize>,
	font_size_origin: Option<Origin>,
	facet_gutter: Option<f32>,
	facet_gutter_origin: Option<Origin>,
	facet_label_size: Option<FacetLabelTextSize>,
	facet_label_size_origin: Option<Origin>,
	legend_presence: Option<Presence<LegendPosition>>,
	legend_presence_origin: Option<Origin>,
	padding: FigurePadding,
	padding_edge_origins: [Option<Origin>; 4],
	axis: AxisProperties,
}

impl RuneFigureElement {
	fn new(value: Result<FigureElementKind, PresentationFailure>, origin: Origin) -> Self {
		Self {
			value,
			origin,
			font_size: None,
			font_size_origin: None,
			facet_gutter: None,
			facet_gutter_origin: None,
			facet_label_size: None,
			facet_label_size_origin: None,
			legend_presence: None,
			legend_presence_origin: None,
			padding: FigurePadding::default(),
			padding_edge_origins: [None; 4],
			axis: AxisProperties::default(),
		}
	}
}

#[derive(Any, Clone, Debug)]
#[rune(item = ::polariton_presentation, name = Skin)]
struct RuneSkin {
	name: Result<String, PresentationFailure>,
	projection: PlotChromeProjection,
	duotone_origin: Option<Origin>,
	origin: Origin,
}

#[derive(Any, Clone, Debug)]
#[rune(item = ::polariton_presentation, name = Mark)]
struct RuneMark {
	sink: Result<StyledRunePlotSink, PresentationFailure>,
	origin: Origin,
	modified: bool,
	point_size_origin: Option<Origin>,
	stroke_width_origin: Option<Origin>,
	bar_width_origin: Option<Origin>,
	category_gap_origin: Option<Origin>,
	stacked_origin: Option<Origin>,
	percent_origin: Option<Origin>,
	continuous_intent: Option<ContinuousScaleIntent>,
	continuous_intent_origin: Option<Origin>,
}

#[derive(Any, Clone, Debug)]
#[rune(item = ::polariton_presentation, name = Layers)]
struct RuneLayers {
	value: Result<LayeredPlotSpec, PresentationFailure>,
	origin: Origin,
}

#[derive(Any, Clone, Debug)]
#[rune(item = ::polariton_presentation, name = Table)]
struct RuneTable {
	skin_name: Option<String>,
	skin_origin: Option<Origin>,
	duotone: bool,
	duotone_origin: Option<Origin>,
	show_types: bool,
	show_types_origin: Option<Origin>,
	failure: Option<PresentationFailure>,
	origin: Origin,
}

#[derive(Any, Clone, Debug)]
#[rune(item = ::polariton_presentation, name = Presentation)]
struct RunePresentation {
	value: Result<RuneViewIntent, PresentationFailure>,
	origin: Origin,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct PresentationLimits {
	pub(crate) compile_allocation: usize,
	pub(crate) runtime_allocation: usize,
	pub(crate) instructions: usize,
}

impl Default for PresentationLimits {
	fn default() -> Self {
		Self {
			compile_allocation: PRESENTATION_COMPILE_ALLOCATION_CAP,
			runtime_allocation: PRESENTATION_RUNTIME_ALLOCATION_CAP,
			instructions: PRESENTATION_INSTRUCTION_BUDGET,
		}
	}
}

/// Compile and execute already-rewritten presentation source.
///
/// The source must expose `main()` and return a vector with one value for each
/// statically discovered slot. Source rewriting and diagnostic remapping remain
/// the envelope's responsibility.
pub(crate) fn execute_generated(
	source: &str,
	slot_spans: &[Range<u32>],
	source_map: &PresentationSourceMap,
	limits: PresentationLimits,
) -> Result<Vec<RuneViewIntent>, PresentationFailure> {
	let context = limit::with(limits.compile_allocation, build_context)
		.call()
		.map_err(|message| PresentationFailure {
			span: slot_spans.first().cloned().unwrap_or(0..0),
			message,
		})?;
	let runtime = Arc::new(context.runtime().map_err(|error| PresentationFailure {
		span: slot_spans.first().cloned().unwrap_or(0..0),
		message: format!("Rune presentation runtime context error: {error}"),
	})?);
	let mut sources = Sources::new();
	sources
		.insert(
			Source::new("polariton-presentation.rn", source).map_err(|error| {
				PresentationFailure {
					span: slot_spans.first().cloned().unwrap_or(0..0),
					message: format!("Rune presentation source error: {error}"),
				}
			})?,
		)
		.map_err(|error| PresentationFailure {
			span: slot_spans.first().cloned().unwrap_or(0..0),
			message: format!("Rune presentation source error: {error}"),
		})?;
	let unit = limit::with(limits.compile_allocation, || {
		let mut diagnostics = Diagnostics::new();
		let built = rune::prepare(&mut sources)
			.with_context(&context)
			.with_diagnostics(&mut diagnostics)
			.build();
		if let Some((span, message)) = diagnostics.diagnostics().iter().find_map(|diagnostic| {
			let Diagnostic::Fatal(fatal) = diagnostic else {
				return None;
			};
			match fatal.kind() {
				FatalDiagnosticKind::CompileError(error) => Some((
					source_map.map_span(error.span().range()),
					format!("Rune presentation compile error: {error}"),
				)),
				FatalDiagnosticKind::LinkError(rune::compile::LinkerError::MissingFunction {
					spans,
					..
				}) => Some((
					spans
						.first()
						.map(|(span, _)| source_map.map_span(span.range()))
						.unwrap_or_else(|| source_map.fallback.clone()),
					format!("Rune presentation compile error: {}", fatal.kind()),
				)),
				other => Some((
					source_map.fallback.clone(),
					format!("Rune presentation compile error: {other}"),
				)),
			}
		}) {
			return Err(PresentationFailure { span, message });
		}
		built.map_err(|error| PresentationFailure {
			span: source_map.fallback.clone(),
			message: format!("Rune presentation compile error: {error}"),
		})
	})
	.call()?;
	let mut vm = Vm::new(runtime, Arc::new(unit));
	let returned = limit::with(limits.runtime_allocation, || {
		budget::with(limits.instructions, || vm.call(["main"], ())).call()
	})
	.call()
	.map_err(|error| {
		let span = error
			.first_location()
			.and_then(|location| location.unit.debug_info()?.instruction_at(location.ip))
			.map(|instruction| source_map.map_span(instruction.span.range()))
			.unwrap_or_else(|| source_map.fallback.clone());
		let error = error.to_string();
		PresentationFailure {
			span,
			message: if error.contains("`limited`") {
				"Rune presentation instruction budget exhausted".to_owned()
			} else {
				format!("Rune presentation runtime error: {error}")
			},
		}
	})?;
	let values = returned
		.borrow_ref::<rune::runtime::Vec>()
		.map_err(|_| PresentationFailure {
			span: slot_spans.first().cloned().unwrap_or(0..0),
			message: "generated presentation entrypoint must return a vector".to_owned(),
		})?;
	if values.len() != slot_spans.len() {
		return Err(PresentationFailure {
			span: slot_spans.first().cloned().unwrap_or(0..0),
			message: format!(
				"generated presentation returned {} values for {} static slots",
				values.len(),
				slot_spans.len()
			),
		});
	}
	values
		.iter()
		.zip(slot_spans)
		.map(|(value, span)| decode_slot(value, span.clone()))
		.collect()
}

fn build_context() -> Result<Context, String> {
	let mut context =
		Context::with_config(false).map_err(|error| format!("Rune context error: {error}"))?;
	let mut module =
		Module::with_crate(MODULE).map_err(|error| format!("Rune module error: {error}"))?;
	install_vocabulary(&mut module)?;
	context.install(module).map_err(module_error)?;
	Ok(context)
}

/// The presentation value types, listed once. Registration and the local
/// method guard both expand from this list, so they cannot drift apart.
macro_rules! presentation_types {
	($apply:ident) => {
		$apply!(
			RuneFigureElement,
			RuneSkin,
			RuneMark,
			RuneLayers,
			RuneTable,
			RunePresentation
		)
	};
}

fn presentation_type_hashes() -> &'static [rune::Hash] {
	use rune::TypeHash;
	macro_rules! hashes {
		($($ty:ty),*) => {
			&[$(<$ty as TypeHash>::HASH),*]
		};
	}
	presentation_types!(hashes)
}

/// Whether a value is one of the presentation types, without borrowing it.
pub(crate) fn is_presentation_value(value: &Value) -> bool {
	presentation_type_hashes().contains(&value.type_hash())
}

/// The presentation vocabulary: its types and every registry callable, free
/// or associated. The Toron evaluator installs it into its sandboxed context;
/// a local source installs the same module beside rnx's batteries.
pub(crate) fn install_vocabulary(module: &mut Module) -> Result<(), String> {
	macro_rules! register {
		($($ty:ty),*) => {
			$(module.ty::<$ty>().map_err(module_error)?;)*
		};
	}
	presentation_types!(register);

	for callable in CALLABLES {
		let function = raw_handler(callable.handler);
		for route in callable.registration_routes {
			match route {
				RegistrationRoute::Free => module
					.raw_function(callable.rune_name, function)
					.build()
					.map_err(module_error)?,
				RegistrationRoute::FigureElement => module
					.raw_function(callable.rune_name, function)
					.build_associated::<RuneFigureElement>()
					.map_err(module_error)?,
				RegistrationRoute::Skin => module
					.raw_function(callable.rune_name, function)
					.build_associated::<RuneSkin>()
					.map_err(module_error)?,
				RegistrationRoute::Mark => module
					.raw_function(callable.rune_name, function)
					.build_associated::<RuneMark>()
					.map_err(module_error)?,
				RegistrationRoute::Table => module
					.raw_function(callable.rune_name, function)
					.build_associated::<RuneTable>()
					.map_err(module_error)?,
			};
		}
	}
	Ok(())
}

/// A local source's `view(name, body)`: the name is any nonempty string
/// value (it may be computed), the body any presentation value. In a view
/// source the compiler reads `view(...)` statically; a local source builds
/// views at run time, so it carries them as values instead.
#[derive(Any, Clone)]
#[rune(item = ::polariton_presentation, name = LocalView)]
pub(crate) struct LocalView {
	name: Result<String, PresentationFailure>,
	body: Value,
	origin: Origin,
}

/// Register the local-only `__view` carrier and the method guard.
pub(crate) fn install_local_views(module: &mut Module) -> Result<(), String> {
	module.ty::<LocalView>().map_err(module_error)?;
	module
		.function("__view", local_view)
		.build()
		.map_err(module_error)?;
	module
		.function("__is_presentation", |value: Value| is_presentation_value(&value))
		.build()
		.map_err(module_error)?;
	Ok(())
}

fn local_view(name: Value, body: Value, start: i64, end: i64) -> LocalView {
	let origin = Origin::decode(start, end).unwrap_or(Origin { start: 0, end: 0 });
	let name = match name.borrow_string_ref() {
		Ok(name) if !name.trim().is_empty() => Ok(name.to_string()),
		_ => Err(PresentationFailure::new(
			origin,
			"view(...) requires a nonempty string name",
		)),
	};
	LocalView { name, body, origin }
}

/// Decode one entry of a local `views([...])` list into its name, origin and
/// intent, through the same slot decoding a view source uses.
pub(crate) fn decode_local_view(
	value: &Value,
) -> Result<(String, Range<u32>, RuneViewIntent), PresentationFailure> {
	let Ok(view) = value.borrow_ref::<LocalView>() else {
		return Err(PresentationFailure {
			span: 0..0,
			message: "views([...]) takes view(\"name\", figure([...])) entries".to_owned(),
		});
	};
	let span = view.origin.range();
	let name = view.name.clone()?;
	let intent = decode_slot(&view.body, span.clone())?;
	Ok((name, span, intent))
}

fn module_error(error: impl std::fmt::Display) -> String {
	format!("Rune module error: {error}")
}

type RawFunction = fn(&mut dyn Memory, InstAddress, usize, Output) -> VmResult<()>;

fn raw_handler(handler: HandlerId) -> RawFunction {
	match handler {
		HandlerId::Bar => raw_bar,
		HandlerId::Line => raw_line,
		HandlerId::Scatter => raw_scatter,
		HandlerId::Area => raw_area,
		HandlerId::Histogram => raw_histogram,
		HandlerId::Box => raw_box,
		HandlerId::Bubble => raw_bubble,
		HandlerId::Heatmap => raw_heatmap,
		HandlerId::Hexbin => raw_hexbin,
		HandlerId::FillBetween => raw_fill_between,
		HandlerId::Candlestick => raw_candlestick,
		HandlerId::Funnel => raw_funnel,
		HandlerId::RadialDial => raw_radial_dial,
		HandlerId::Pie => raw_pie,
		HandlerId::Violin => raw_violin,
		HandlerId::Radar => raw_radar,
		HandlerId::Parallel => raw_parallel,
		HandlerId::Title => raw_title,
		HandlerId::Subtitle => raw_subtitle,
		HandlerId::XAxis => raw_x_axis,
		HandlerId::YAxis => raw_y_axis,
		HandlerId::Facet => raw_facet,
		HandlerId::Palette => raw_palette,
		HandlerId::SequentialPalette => raw_sequential_palette,
		HandlerId::DivergingPalette => raw_diverging_palette,
		HandlerId::Skin => raw_skin,
		HandlerId::Legend => raw_legend,
		HandlerId::HoverSize => raw_hover_size,
		HandlerId::Padding => raw_padding,
		HandlerId::Table => raw_table,
		HandlerId::Layers => raw_layers,
		HandlerId::Figure => raw_figure,
		HandlerId::FigureFontSize => raw_figure_font_size,
		HandlerId::FacetGutter => raw_figure_facet_gutter,
		HandlerId::PaddingTop => raw_figure_padding_top,
		HandlerId::PaddingRight => raw_figure_padding_right,
		HandlerId::PaddingBottom => raw_figure_padding_bottom,
		HandlerId::PaddingLeft => raw_figure_padding_left,
		HandlerId::AxisLabel => raw_axis_label,
		HandlerId::FigureLabelSize => raw_figure_label_size,
		HandlerId::AxisBounds => raw_axis_bounds,
		HandlerId::AxisTickStep => raw_axis_tick_step,
		HandlerId::AxisTickEvery => raw_axis_tick_every,
		HandlerId::AxisMinorTicks => raw_axis_minor_ticks,
		HandlerId::AxisTickSize => raw_axis_tick_size,
		HandlerId::AxisTickRotation => raw_axis_tick_rotation,
		HandlerId::LegendAt => raw_legend_at,
		HandlerId::LegendHide => raw_legend_hide,
		HandlerId::MarkPointSize => raw_mark_point_size,
		HandlerId::MarkStrokeWidth => raw_mark_stroke_width,
		HandlerId::MarkBarWidth => raw_mark_bar_width,
		HandlerId::MarkCategoryGap => raw_mark_category_gap,
		HandlerId::MarkCategory => raw_mark_category,
		HandlerId::MarkStacked => raw_mark_stacked,
		HandlerId::MarkPercent => raw_mark_percent,
		HandlerId::MarkDiverging => raw_mark_diverging,
		HandlerId::MarkDivergingAt => raw_mark_diverging_at,
		HandlerId::SkinDuotone => raw_skin_duotone,
		HandlerId::TableSkin => raw_table_skin,
		HandlerId::TableDuotone => raw_table_duotone,
		HandlerId::TableShowTypes => raw_table_show_types,
	}
}

fn raw_bar(stack: &mut dyn Memory, addr: InstAddress, args: usize, out: Output) -> VmResult<()> {
	raw_mark(PlotType::Bar, "bar", stack, addr, args, out)
}

fn raw_line(stack: &mut dyn Memory, addr: InstAddress, args: usize, out: Output) -> VmResult<()> {
	raw_mark(PlotType::Line, "line", stack, addr, args, out)
}

fn raw_scatter(
	stack: &mut dyn Memory,
	addr: InstAddress,
	args: usize,
	out: Output,
) -> VmResult<()> {
	raw_mark(PlotType::Scatter, "scatter", stack, addr, args, out)
}

fn raw_area(stack: &mut dyn Memory, addr: InstAddress, args: usize, out: Output) -> VmResult<()> {
	raw_mark(PlotType::StackedArea, "area", stack, addr, args, out)
}

fn raw_histogram(
	stack: &mut dyn Memory,
	addr: InstAddress,
	args: usize,
	out: Output,
) -> VmResult<()> {
	raw_mark(PlotType::Histogram, "histogram", stack, addr, args, out)
}

fn raw_radar(stack: &mut dyn Memory, addr: InstAddress, args: usize, out: Output) -> VmResult<()> {
	raw_multivariate_mark(PlotType::Radar, "radar", stack, addr, args, out)
}

fn raw_parallel(
	stack: &mut dyn Memory,
	addr: InstAddress,
	args: usize,
	out: Output,
) -> VmResult<()> {
	raw_multivariate_mark(PlotType::Parallel, "parallel", stack, addr, args, out)
}

fn raw_multivariate_mark(
	plot_type: PlotType,
	name: &'static str,
	stack: &mut dyn Memory,
	addr: InstAddress,
	args: usize,
	out: Output,
) -> VmResult<()> {
	let values = rune::vm_try!(stack.slice_at(addr, args));
	let origin = match raw_origin(values) {
		Ok(origin) => origin,
		Err(failure) => return store(out, stack, RuneMark::failed(failure)),
	};
	if values.len() < 2 || (values.len() - 2) % 3 != 0 {
		return store(
			out,
			stack,
			RuneMark::failed(PresentationFailure::new(
				origin,
				format!(
					"{name}(...) requires n dimension strings, two injected origins per dimension, and the mark origin (3n + 2 values)"
				),
			)),
		);
	}
	let field_count = (values.len() - 2) / 3;
	if let Err(reason) = plot_type.validate_multivariate_dimension_count(field_count) {
		return store(
			out,
			stack,
			RuneMark::failed(PresentationFailure::new(origin, reason)),
		);
	}
	let mut dimensions = Vec::with_capacity(field_count);
	for value in &values[..field_count] {
		match rune::from_value::<Ref<str>>(value.clone()) {
			Ok(field) => dimensions.push(field.to_owned()),
			Err(_) => {
				return store(
					out,
					stack,
					RuneMark::failed(PresentationFailure::new(
						origin,
						format!("{name}(...) dimension arguments must evaluate to strings"),
					)),
				);
			}
		}
	}
	let decode_span = |index: usize| -> Result<Range<u32>, PresentationFailure> {
		let start = rune::from_value::<i64>(values[index].clone()).map_err(|_| {
			PresentationFailure::new(
				origin,
				format!("{name}(...) has an invalid injected dimension origin"),
			)
		})?;
		let end = rune::from_value::<i64>(values[index + 1].clone()).map_err(|_| {
			PresentationFailure::new(
				origin,
				format!("{name}(...) has an invalid injected dimension origin"),
			)
		})?;
		Origin::decode(start, end).map(Origin::range)
	};
	let mut dimension_origins = Vec::with_capacity(field_count);
	for index in 0..field_count {
		match decode_span(field_count + index * 2) {
			Ok(span) => dimension_origins.push(span),
			Err(failure) => return store(out, stack, RuneMark::failed(failure)),
		}
	}
	let mut seen = HashSet::new();
	for (index, dimension) in dimensions.iter().enumerate() {
		if !seen.insert(dimension.as_str()) {
			return store(
				out,
				stack,
				RuneMark::failed(PresentationFailure {
					span: dimension_origins[index].clone(),
					message: format!(
						"{name}(...) dimension `{dimension}` is duplicated; each dimension field must be unique"
					),
				}),
			);
		}
	}
	let fields = MultivariateFields::new(dimensions, None);
	let origins = MultivariateFieldOrigins {
		dimensions: dimension_origins,
		category: None,
	};
	let spanned = match plot_type {
		PlotType::Radar => SpannedMarkFields::Radar {
			fields: fields.clone(),
			origins,
		},
		PlotType::Parallel => SpannedMarkFields::Parallel {
			fields: fields.clone(),
			origins,
		},
		_ => unreachable!("multivariate Rune constructor admitted {plot_type:?}"),
	};
	let sink = StyledRunePlotSink {
		sink: DisposableRunePlotSink {
			composition: PlotComposition::single(plot_type),
			x: fields.dimensions()[0].clone(),
			y: fields.dimensions()[1].clone(),
			category: None,
			interval_end: None,
			mark_fields: Some(spanned),
			origin: origin.range(),
		},
		settings: LayerSettings::default(),
		stacked: false,
		percent: false,
	};
	store(
		out,
		stack,
		RuneMark {
			sink: Ok(sink),
			origin,
			modified: false,
			point_size_origin: None,
			stroke_width_origin: None,
			bar_width_origin: None,
			category_gap_origin: None,
			stacked_origin: None,
			percent_origin: None,
			continuous_intent: None,
			continuous_intent_origin: None,
		},
	)
}

fn raw_box(stack: &mut dyn Memory, addr: InstAddress, args: usize, out: Output) -> VmResult<()> {
	let values = rune::vm_try!(stack.slice_at(addr, args));
	let origin = match raw_origin(values) {
		Ok(origin) => origin,
		Err(failure) => return store(out, stack, RuneMark::failed(failure)),
	};
	if values.len() != 23 {
		return store(
			out,
			stack,
			RuneMark::failed(PresentationFailure::new(
				origin,
				"box(...) takes category, whisker_low, q1, median, q3, whisker_high, and outlier strings",
			)),
		);
	}
	let mut fields = Vec::with_capacity(7);
	for value in &values[..7] {
		match rune::from_value::<Ref<str>>(value.clone()) {
			Ok(field) => fields.push(field.to_owned()),
			Err(_) => {
				return store(
					out,
					stack,
					RuneMark::failed(PresentationFailure::new(
						origin,
						"box(...) field arguments must evaluate to strings",
					)),
				);
			}
		}
	}
	let decode_span = |index: usize| -> Result<Range<u32>, PresentationFailure> {
		let start = rune::from_value::<i64>(values[index].clone()).map_err(|_| {
			PresentationFailure::new(origin, "box(...) has an invalid injected argument origin")
		})?;
		let end = rune::from_value::<i64>(values[index + 1].clone()).map_err(|_| {
			PresentationFailure::new(origin, "box(...) has an invalid injected argument origin")
		})?;
		Origin::decode(start, end).map(Origin::range)
	};
	let spans = (|| {
		Ok(BoxPlotFieldOrigins {
			category: decode_span(7)?,
			whisker_low: decode_span(9)?,
			q1: decode_span(11)?,
			median: decode_span(13)?,
			q3: decode_span(15)?,
			whisker_high: decode_span(17)?,
			outlier: decode_span(19)?,
		})
	})();
	let spans = match spans {
		Ok(spans) => spans,
		Err(failure) => return store(out, stack, RuneMark::failed(failure)),
	};
	let box_fields = BoxPlotFields {
		category: fields[0].clone(),
		whisker_low: fields[1].clone(),
		q1: fields[2].clone(),
		median: fields[3].clone(),
		q3: fields[4].clone(),
		whisker_high: fields[5].clone(),
		outlier: fields[6].clone(),
	};
	let sink = StyledRunePlotSink {
		sink: DisposableRunePlotSink {
			composition: PlotComposition::single(PlotType::BoxPlot),
			x: box_fields.category.clone(),
			y: box_fields.median.clone(),
			category: None,
			interval_end: None,
			mark_fields: Some(SpannedMarkFields::BoxPlot {
				fields: box_fields,
				origins: spans,
			}),
			origin: origin.range(),
		},
		settings: LayerSettings::default(),
		stacked: false,
		percent: false,
	};
	store(
		out,
		stack,
		RuneMark {
			sink: Ok(sink),
			origin,
			modified: false,
			point_size_origin: None,
			stroke_width_origin: None,
			bar_width_origin: None,
			category_gap_origin: None,
			stacked_origin: None,
			percent_origin: None,
			continuous_intent: None,
			continuous_intent_origin: None,
		},
	)
}

fn raw_bubble(stack: &mut dyn Memory, addr: InstAddress, args: usize, out: Output) -> VmResult<()> {
	let values = rune::vm_try!(stack.slice_at(addr, args));
	let origin = match raw_origin(values) {
		Ok(origin) => origin,
		Err(failure) => return store(out, stack, RuneMark::failed(failure)),
	};
	let field_count = match values.len() {
		14 => 4,
		17 => 5,
		_ => {
			return store(
				out,
				stack,
				RuneMark::failed(PresentationFailure::new(
					origin,
					"bubble(...) takes x, y, size, and color strings, with an optional label string",
				)),
			);
		}
	};
	let mut fields = Vec::with_capacity(field_count);
	for value in &values[..field_count] {
		match rune::from_value::<Ref<str>>(value.clone()) {
			Ok(field) => fields.push(field.to_owned()),
			Err(_) => {
				return store(
					out,
					stack,
					RuneMark::failed(PresentationFailure::new(
						origin,
						"bubble(...) field arguments must evaluate to strings",
					)),
				);
			}
		}
	}
	let decode_span = |index: usize| -> Result<Range<u32>, PresentationFailure> {
		let start = rune::from_value::<i64>(values[index].clone()).map_err(|_| {
			PresentationFailure::new(
				origin,
				"bubble(...) has an invalid injected argument origin",
			)
		})?;
		let end = rune::from_value::<i64>(values[index + 1].clone()).map_err(|_| {
			PresentationFailure::new(
				origin,
				"bubble(...) has an invalid injected argument origin",
			)
		})?;
		Origin::decode(start, end).map(Origin::range)
	};
	let origin_base = field_count;
	let origins = match (|| {
		Ok(BubbleFieldOrigins {
			x: decode_span(origin_base)?,
			y: decode_span(origin_base + 2)?,
			size: decode_span(origin_base + 4)?,
			color: decode_span(origin_base + 6)?,
			label: (field_count == 5)
				.then(|| decode_span(origin_base + 8))
				.transpose()?,
		})
	})() {
		Ok(origins) => origins,
		Err(failure) => return store(out, stack, RuneMark::failed(failure)),
	};
	let bubble_fields = BubbleFields::new(
		BubbleFieldNames::new(&fields[0], &fields[1], &fields[2], &fields[3]),
		(field_count == 5).then(|| fields[4].clone()),
	);
	let sink = StyledRunePlotSink {
		sink: DisposableRunePlotSink {
			composition: PlotComposition::single(PlotType::Bubble),
			x: fields[0].clone(),
			y: fields[1].clone(),
			category: None,
			interval_end: None,
			mark_fields: Some(SpannedMarkFields::Bubble {
				fields: bubble_fields,
				origins,
			}),
			origin: origin.range(),
		},
		settings: LayerSettings::default(),
		stacked: false,
		percent: false,
	};
	store(
		out,
		stack,
		RuneMark {
			sink: Ok(sink),
			origin,
			modified: false,
			point_size_origin: None,
			stroke_width_origin: None,
			bar_width_origin: None,
			category_gap_origin: None,
			stacked_origin: None,
			percent_origin: None,
			continuous_intent: None,
			continuous_intent_origin: None,
		},
	)
}

fn raw_hexbin(stack: &mut dyn Memory, addr: InstAddress, args: usize, out: Output) -> VmResult<()> {
	let values = rune::vm_try!(stack.slice_at(addr, args));
	let origin = match raw_origin(values) {
		Ok(origin) => origin,
		Err(failure) => return store(out, stack, RuneMark::failed(failure)),
	};
	if values.len() != 8 {
		return store(
			out,
			stack,
			RuneMark::failed(PresentationFailure::new(
				origin,
				"hexbin(...) takes x and y strings",
			)),
		);
	}
	let mut fields = Vec::with_capacity(2);
	for value in &values[..2] {
		match rune::from_value::<Ref<str>>(value.clone()) {
			Ok(field) => fields.push(field.to_owned()),
			Err(_) => {
				return store(
					out,
					stack,
					RuneMark::failed(PresentationFailure::new(
						origin,
						"hexbin(...) field arguments must evaluate to strings",
					)),
				);
			}
		}
	}
	let decode_span = |index: usize| -> Result<Range<u32>, PresentationFailure> {
		let start = rune::from_value::<i64>(values[index].clone()).map_err(|_| {
			PresentationFailure::new(
				origin,
				"hexbin(...) has an invalid injected argument origin",
			)
		})?;
		let end = rune::from_value::<i64>(values[index + 1].clone()).map_err(|_| {
			PresentationFailure::new(
				origin,
				"hexbin(...) has an invalid injected argument origin",
			)
		})?;
		Origin::decode(start, end).map(Origin::range)
	};
	let origins = match (|| {
		Ok(HexbinFieldOrigins {
			x: decode_span(2)?,
			y: decode_span(4)?,
		})
	})() {
		Ok(origins) => origins,
		Err(failure) => return store(out, stack, RuneMark::failed(failure)),
	};
	let fields = HexbinFields {
		x: fields[0].clone(),
		y: fields[1].clone(),
	};
	let sink = StyledRunePlotSink {
		sink: DisposableRunePlotSink {
			composition: PlotComposition::single(PlotType::Hexbin),
			x: fields.x.clone(),
			y: fields.y.clone(),
			category: None,
			interval_end: None,
			mark_fields: Some(SpannedMarkFields::Hexbin { fields, origins }),
			origin: origin.range(),
		},
		settings: LayerSettings::default(),
		stacked: false,
		percent: false,
	};
	store(
		out,
		stack,
		RuneMark {
			sink: Ok(sink),
			origin,
			modified: false,
			point_size_origin: None,
			stroke_width_origin: None,
			bar_width_origin: None,
			category_gap_origin: None,
			stacked_origin: None,
			percent_origin: None,
			continuous_intent: None,
			continuous_intent_origin: None,
		},
	)
}

fn raw_fill_between(
	stack: &mut dyn Memory,
	addr: InstAddress,
	args: usize,
	out: Output,
) -> VmResult<()> {
	let values = rune::vm_try!(stack.slice_at(addr, args));
	let origin = match raw_origin(values) {
		Ok(origin) => origin,
		Err(failure) => return store(out, stack, RuneMark::failed(failure)),
	};
	let field_count = match values.len() {
		11 => 3,
		14 => 4,
		_ => {
			return store(
				out,
				stack,
				RuneMark::failed(PresentationFailure::new(
					origin,
					"fill_between(...) takes x, lower, upper, and optional middle strings",
				)),
			);
		}
	};
	let mut fields = Vec::with_capacity(field_count);
	for value in &values[..field_count] {
		match rune::from_value::<Ref<str>>(value.clone()) {
			Ok(field) => fields.push(field.to_owned()),
			Err(_) => {
				return store(
					out,
					stack,
					RuneMark::failed(PresentationFailure::new(
						origin,
						"fill_between(...) field arguments must evaluate to strings",
					)),
				);
			}
		}
	}
	let decode_span = |index: usize| -> Result<Range<u32>, PresentationFailure> {
		let start = rune::from_value::<i64>(values[index].clone()).map_err(|_| {
			PresentationFailure::new(
				origin,
				"fill_between(...) has an invalid injected argument origin",
			)
		})?;
		let end = rune::from_value::<i64>(values[index + 1].clone()).map_err(|_| {
			PresentationFailure::new(
				origin,
				"fill_between(...) has an invalid injected argument origin",
			)
		})?;
		Origin::decode(start, end).map(Origin::range)
	};
	let origin_base = field_count;
	let origins = match (|| {
		Ok(FillBetweenFieldOrigins {
			x: decode_span(origin_base)?,
			lower: decode_span(origin_base + 2)?,
			upper: decode_span(origin_base + 4)?,
			middle: (field_count == 4)
				.then(|| decode_span(origin_base + 6))
				.transpose()?,
		})
	})() {
		Ok(origins) => origins,
		Err(failure) => return store(out, stack, RuneMark::failed(failure)),
	};
	let fields = FillBetweenFields {
		x: fields[0].clone(),
		lower: fields[1].clone(),
		upper: fields[2].clone(),
		middle: (field_count == 4).then(|| fields[3].clone()),
	};
	let sink = StyledRunePlotSink {
		sink: DisposableRunePlotSink {
			composition: PlotComposition::single(PlotType::FillBetween),
			x: fields.x.clone(),
			y: fields.lower.clone(),
			category: None,
			interval_end: None,
			mark_fields: Some(SpannedMarkFields::FillBetween { fields, origins }),
			origin: origin.range(),
		},
		settings: LayerSettings::default(),
		stacked: false,
		percent: false,
	};
	store(
		out,
		stack,
		RuneMark {
			sink: Ok(sink),
			origin,
			modified: false,
			point_size_origin: None,
			stroke_width_origin: None,
			bar_width_origin: None,
			category_gap_origin: None,
			stacked_origin: None,
			percent_origin: None,
			continuous_intent: None,
			continuous_intent_origin: None,
		},
	)
}

fn raw_candlestick(
	stack: &mut dyn Memory,
	addr: InstAddress,
	args: usize,
	out: Output,
) -> VmResult<()> {
	let values = rune::vm_try!(stack.slice_at(addr, args));
	let origin = match raw_origin(values) {
		Ok(origin) => origin,
		Err(failure) => return store(out, stack, RuneMark::failed(failure)),
	};
	if values.len() != 17 {
		return store(
			out,
			stack,
			RuneMark::failed(PresentationFailure::new(
				origin,
				"candlestick(...) takes x, open, high, low, and close strings",
			)),
		);
	}
	let mut fields = Vec::with_capacity(5);
	for value in &values[..5] {
		match rune::from_value::<Ref<str>>(value.clone()) {
			Ok(field) => fields.push(field.to_owned()),
			Err(_) => {
				return store(
					out,
					stack,
					RuneMark::failed(PresentationFailure::new(
						origin,
						"candlestick(...) field arguments must evaluate to strings",
					)),
				);
			}
		}
	}
	let decode_span = |index: usize| -> Result<Range<u32>, PresentationFailure> {
		let start = rune::from_value::<i64>(values[index].clone()).map_err(|_| {
			PresentationFailure::new(
				origin,
				"candlestick(...) has an invalid injected argument origin",
			)
		})?;
		let end = rune::from_value::<i64>(values[index + 1].clone()).map_err(|_| {
			PresentationFailure::new(
				origin,
				"candlestick(...) has an invalid injected argument origin",
			)
		})?;
		Origin::decode(start, end).map(Origin::range)
	};
	let origins = match (|| {
		Ok(CandlestickFieldOrigins {
			x: decode_span(5)?,
			open: decode_span(7)?,
			high: decode_span(9)?,
			low: decode_span(11)?,
			close: decode_span(13)?,
		})
	})() {
		Ok(origins) => origins,
		Err(failure) => return store(out, stack, RuneMark::failed(failure)),
	};
	let fields = CandlestickFields {
		x: fields[0].clone(),
		open: fields[1].clone(),
		high: fields[2].clone(),
		low: fields[3].clone(),
		close: fields[4].clone(),
	};
	let sink = StyledRunePlotSink {
		sink: DisposableRunePlotSink {
			composition: PlotComposition::single(PlotType::Candlestick),
			x: fields.x.clone(),
			y: fields.open.clone(),
			category: None,
			interval_end: None,
			mark_fields: Some(SpannedMarkFields::Candlestick { fields, origins }),
			origin: origin.range(),
		},
		settings: LayerSettings::default(),
		stacked: false,
		percent: false,
	};
	store(
		out,
		stack,
		RuneMark {
			sink: Ok(sink),
			origin,
			modified: false,
			point_size_origin: None,
			stroke_width_origin: None,
			bar_width_origin: None,
			category_gap_origin: None,
			stacked_origin: None,
			percent_origin: None,
			continuous_intent: None,
			continuous_intent_origin: None,
		},
	)
}

fn raw_funnel(stack: &mut dyn Memory, addr: InstAddress, args: usize, out: Output) -> VmResult<()> {
	let values = rune::vm_try!(stack.slice_at(addr, args));
	let origin = match raw_origin(values) {
		Ok(origin) => origin,
		Err(failure) => return store(out, stack, RuneMark::failed(failure)),
	};
	if values.len() != 8 {
		return store(
			out,
			stack,
			RuneMark::failed(PresentationFailure::new(
				origin,
				"funnel(...) takes stage and value strings",
			)),
		);
	}
	let mut fields = Vec::with_capacity(2);
	for value in &values[..2] {
		match rune::from_value::<Ref<str>>(value.clone()) {
			Ok(field) => fields.push(field.to_owned()),
			Err(_) => {
				return store(
					out,
					stack,
					RuneMark::failed(PresentationFailure::new(
						origin,
						"funnel(...) field arguments must evaluate to strings",
					)),
				);
			}
		}
	}
	let decode_span = |index: usize| -> Result<Range<u32>, PresentationFailure> {
		let start = rune::from_value::<i64>(values[index].clone()).map_err(|_| {
			PresentationFailure::new(
				origin,
				"funnel(...) has an invalid injected argument origin",
			)
		})?;
		let end = rune::from_value::<i64>(values[index + 1].clone()).map_err(|_| {
			PresentationFailure::new(
				origin,
				"funnel(...) has an invalid injected argument origin",
			)
		})?;
		Origin::decode(start, end).map(Origin::range)
	};
	let origins = match (|| {
		Ok(FunnelFieldOrigins {
			stage: decode_span(2)?,
			value: decode_span(4)?,
		})
	})() {
		Ok(origins) => origins,
		Err(failure) => return store(out, stack, RuneMark::failed(failure)),
	};
	let fields = FunnelFields {
		stage: fields[0].clone(),
		value: fields[1].clone(),
	};
	let sink = StyledRunePlotSink {
		sink: DisposableRunePlotSink {
			composition: PlotComposition::single(PlotType::Funnel),
			x: fields.stage.clone(),
			y: fields.value.clone(),
			category: None,
			interval_end: None,
			mark_fields: Some(SpannedMarkFields::Funnel { fields, origins }),
			origin: origin.range(),
		},
		settings: LayerSettings::default(),
		stacked: false,
		percent: false,
	};
	store(
		out,
		stack,
		RuneMark {
			sink: Ok(sink),
			origin,
			modified: false,
			point_size_origin: None,
			stroke_width_origin: None,
			bar_width_origin: None,
			category_gap_origin: None,
			stacked_origin: None,
			percent_origin: None,
			continuous_intent: None,
			continuous_intent_origin: None,
		},
	)
}

fn raw_pie(stack: &mut dyn Memory, addr: InstAddress, args: usize, out: Output) -> VmResult<()> {
	let values = rune::vm_try!(stack.slice_at(addr, args));
	let origin = match raw_origin(values) {
		Ok(origin) => origin,
		Err(failure) => return store(out, stack, RuneMark::failed(failure)),
	};
	if values.len() != 8 {
		return store(
			out,
			stack,
			RuneMark::failed(PresentationFailure::new(
				origin,
				"pie(...) takes category and value strings",
			)),
		);
	}
	let mut fields = Vec::with_capacity(2);
	for value in &values[..2] {
		match rune::from_value::<Ref<str>>(value.clone()) {
			Ok(field) => fields.push(field.to_owned()),
			Err(_) => {
				return store(
					out,
					stack,
					RuneMark::failed(PresentationFailure::new(
						origin,
						"pie(...) field arguments must evaluate to strings",
					)),
				);
			}
		}
	}
	let decode_span = |index: usize| -> Result<Range<u32>, PresentationFailure> {
		let start = rune::from_value::<i64>(values[index].clone()).map_err(|_| {
			PresentationFailure::new(origin, "pie(...) has an invalid injected argument origin")
		})?;
		let end = rune::from_value::<i64>(values[index + 1].clone()).map_err(|_| {
			PresentationFailure::new(origin, "pie(...) has an invalid injected argument origin")
		})?;
		Origin::decode(start, end).map(Origin::range)
	};
	let origins = match (|| {
		Ok(PieFieldOrigins {
			category: decode_span(2)?,
			value: decode_span(4)?,
		})
	})() {
		Ok(origins) => origins,
		Err(failure) => return store(out, stack, RuneMark::failed(failure)),
	};
	let fields = PieFields {
		category: fields[0].clone(),
		value: fields[1].clone(),
	};
	let sink = StyledRunePlotSink {
		sink: DisposableRunePlotSink {
			composition: PlotComposition::single(PlotType::Pie),
			x: fields.category.clone(),
			y: fields.value.clone(),
			category: None,
			interval_end: None,
			mark_fields: Some(SpannedMarkFields::Pie { fields, origins }),
			origin: origin.range(),
		},
		settings: LayerSettings::default(),
		stacked: false,
		percent: false,
	};
	store(
		out,
		stack,
		RuneMark {
			sink: Ok(sink),
			origin,
			modified: false,
			point_size_origin: None,
			stroke_width_origin: None,
			bar_width_origin: None,
			category_gap_origin: None,
			stacked_origin: None,
			percent_origin: None,
			continuous_intent: None,
			continuous_intent_origin: None,
		},
	)
}

fn raw_violin(stack: &mut dyn Memory, addr: InstAddress, args: usize, out: Output) -> VmResult<()> {
	let values = rune::vm_try!(stack.slice_at(addr, args));
	let origin = match raw_origin(values) {
		Ok(origin) => origin,
		Err(failure) => return store(out, stack, RuneMark::failed(failure)),
	};
	if values.len() != 14 {
		return store(
			out,
			stack,
			RuneMark::failed(PresentationFailure::new(
				origin,
				"violin(...) takes group, evaluation_value, density, and median strings",
			)),
		);
	}
	let mut fields = Vec::with_capacity(4);
	for value in &values[..4] {
		match rune::from_value::<Ref<str>>(value.clone()) {
			Ok(field) => fields.push(field.to_owned()),
			Err(_) => {
				return store(
					out,
					stack,
					RuneMark::failed(PresentationFailure::new(
						origin,
						"violin(...) field arguments must evaluate to strings",
					)),
				);
			}
		}
	}
	let decode_span = |index: usize| -> Result<Range<u32>, PresentationFailure> {
		let start = rune::from_value::<i64>(values[index].clone()).map_err(|_| {
			PresentationFailure::new(
				origin,
				"violin(...) has an invalid injected argument origin",
			)
		})?;
		let end = rune::from_value::<i64>(values[index + 1].clone()).map_err(|_| {
			PresentationFailure::new(
				origin,
				"violin(...) has an invalid injected argument origin",
			)
		})?;
		Origin::decode(start, end).map(Origin::range)
	};
	let origins = match (|| {
		Ok(ViolinFieldOrigins {
			group: decode_span(4)?,
			evaluation_value: decode_span(6)?,
			density: decode_span(8)?,
			median: decode_span(10)?,
		})
	})() {
		Ok(origins) => origins,
		Err(failure) => return store(out, stack, RuneMark::failed(failure)),
	};
	let fields = ViolinFields {
		group: fields[0].clone(),
		evaluation_value: fields[1].clone(),
		density: fields[2].clone(),
		median: fields[3].clone(),
	};
	let sink = StyledRunePlotSink {
		sink: DisposableRunePlotSink {
			composition: PlotComposition::single(PlotType::Violin),
			x: fields.group.clone(),
			y: fields.evaluation_value.clone(),
			category: None,
			interval_end: None,
			mark_fields: Some(SpannedMarkFields::Violin { fields, origins }),
			origin: origin.range(),
		},
		settings: LayerSettings::default(),
		stacked: false,
		percent: false,
	};
	store(
		out,
		stack,
		RuneMark {
			sink: Ok(sink),
			origin,
			modified: false,
			point_size_origin: None,
			stroke_width_origin: None,
			bar_width_origin: None,
			category_gap_origin: None,
			stacked_origin: None,
			percent_origin: None,
			continuous_intent: None,
			continuous_intent_origin: None,
		},
	)
}

fn raw_radial_dial(
	stack: &mut dyn Memory,
	addr: InstAddress,
	args: usize,
	out: Output,
) -> VmResult<()> {
	let values = rune::vm_try!(stack.slice_at(addr, args));
	let origin = match raw_origin(values) {
		Ok(origin) => origin,
		Err(failure) => return store(out, stack, RuneMark::failed(failure)),
	};
	if !matches!(values.len(), 8 | 11) {
		return store(
			out,
			stack,
			RuneMark::failed(PresentationFailure::new(
				origin,
				"radial_dial(...) takes value and maximum strings with an optional trailing label string",
			)),
		);
	}
	let field_count = (values.len() - 2) / 3;
	let mut fields = Vec::with_capacity(field_count);
	for value in &values[..field_count] {
		match rune::from_value::<Ref<str>>(value.clone()) {
			Ok(field) => fields.push(field.to_owned()),
			Err(_) => {
				return store(
					out,
					stack,
					RuneMark::failed(PresentationFailure::new(
						origin,
						"radial_dial(...) field arguments must evaluate to strings",
					)),
				);
			}
		}
	}
	let decode_span = |index: usize| -> Result<Range<u32>, PresentationFailure> {
		let start = rune::from_value::<i64>(values[index].clone()).map_err(|_| {
			PresentationFailure::new(
				origin,
				"radial_dial(...) has an invalid injected argument origin",
			)
		})?;
		let end = rune::from_value::<i64>(values[index + 1].clone()).map_err(|_| {
			PresentationFailure::new(
				origin,
				"radial_dial(...) has an invalid injected argument origin",
			)
		})?;
		Origin::decode(start, end).map(Origin::range)
	};
	let origin_base = field_count;
	let origins = match (|| {
		Ok(RadialDialFieldOrigins {
			value: decode_span(origin_base)?,
			maximum: decode_span(origin_base + 2)?,
			label: (field_count == 3)
				.then(|| decode_span(origin_base + 4))
				.transpose()?,
		})
	})() {
		Ok(origins) => origins,
		Err(failure) => return store(out, stack, RuneMark::failed(failure)),
	};
	let fields = RadialDialFields {
		value: fields[0].clone(),
		maximum: fields[1].clone(),
		label: (field_count == 3).then(|| fields[2].clone()),
	};
	let sink = StyledRunePlotSink {
		sink: DisposableRunePlotSink {
			composition: PlotComposition::single(PlotType::RadialDial),
			x: fields.value.clone(),
			y: fields.maximum.clone(),
			category: None,
			interval_end: None,
			mark_fields: Some(SpannedMarkFields::RadialDial { fields, origins }),
			origin: origin.range(),
		},
		settings: LayerSettings::default(),
		stacked: false,
		percent: false,
	};
	store(
		out,
		stack,
		RuneMark {
			sink: Ok(sink),
			origin,
			modified: false,
			point_size_origin: None,
			stroke_width_origin: None,
			bar_width_origin: None,
			category_gap_origin: None,
			stacked_origin: None,
			percent_origin: None,
			continuous_intent: None,
			continuous_intent_origin: None,
		},
	)
}

fn raw_heatmap(
	stack: &mut dyn Memory,
	addr: InstAddress,
	args: usize,
	out: Output,
) -> VmResult<()> {
	let values = rune::vm_try!(stack.slice_at(addr, args));
	let origin = match raw_origin(values) {
		Ok(origin) => origin,
		Err(failure) => return store(out, stack, RuneMark::failed(failure)),
	};
	if values.len() != 11 {
		return store(
			out,
			stack,
			RuneMark::failed(PresentationFailure::new(
				origin,
				"heatmap(...) takes x, y, and value strings",
			)),
		);
	}
	let mut fields = Vec::with_capacity(3);
	for value in &values[..3] {
		match rune::from_value::<Ref<str>>(value.clone()) {
			Ok(field) => fields.push(field.to_owned()),
			Err(_) => {
				return store(
					out,
					stack,
					RuneMark::failed(PresentationFailure::new(
						origin,
						"heatmap(...) field arguments must evaluate to strings",
					)),
				);
			}
		}
	}
	let decode_span = |index: usize| -> Result<Range<u32>, PresentationFailure> {
		let start = rune::from_value::<i64>(values[index].clone()).map_err(|_| {
			PresentationFailure::new(
				origin,
				"heatmap(...) has an invalid injected argument origin",
			)
		})?;
		let end = rune::from_value::<i64>(values[index + 1].clone()).map_err(|_| {
			PresentationFailure::new(
				origin,
				"heatmap(...) has an invalid injected argument origin",
			)
		})?;
		Origin::decode(start, end).map(Origin::range)
	};
	let origins = match (|| {
		Ok(HeatmapFieldOrigins {
			x: decode_span(3)?,
			y: decode_span(5)?,
			value: decode_span(7)?,
		})
	})() {
		Ok(origins) => origins,
		Err(failure) => return store(out, stack, RuneMark::failed(failure)),
	};
	let heatmap_fields = HeatmapFields {
		x: fields[0].clone(),
		y: fields[1].clone(),
		value: fields[2].clone(),
	};
	let sink = StyledRunePlotSink {
		sink: DisposableRunePlotSink {
			composition: PlotComposition::single(PlotType::Heatmap),
			x: heatmap_fields.x.clone(),
			y: heatmap_fields.y.clone(),
			category: None,
			interval_end: None,
			mark_fields: Some(SpannedMarkFields::Heatmap {
				fields: heatmap_fields,
				origins,
			}),
			origin: origin.range(),
		},
		settings: LayerSettings::default(),
		stacked: false,
		percent: false,
	};
	store(
		out,
		stack,
		RuneMark {
			sink: Ok(sink),
			origin,
			modified: false,
			point_size_origin: None,
			stroke_width_origin: None,
			bar_width_origin: None,
			category_gap_origin: None,
			stacked_origin: None,
			percent_origin: None,
			continuous_intent: None,
			continuous_intent_origin: None,
		},
	)
}

#[derive(Clone, Copy)]
enum StringConstructor {
	Title,
	Subtitle,
	Facet,
	Palette,
}

fn raw_title(stack: &mut dyn Memory, addr: InstAddress, args: usize, out: Output) -> VmResult<()> {
	raw_string_element(StringConstructor::Title, "title", stack, addr, args, out)
}

fn raw_subtitle(
	stack: &mut dyn Memory,
	addr: InstAddress,
	args: usize,
	out: Output,
) -> VmResult<()> {
	raw_string_element(
		StringConstructor::Subtitle,
		"subtitle",
		stack,
		addr,
		args,
		out,
	)
}

fn raw_x_axis(stack: &mut dyn Memory, addr: InstAddress, args: usize, out: Output) -> VmResult<()> {
	raw_axis(Axis::X, stack, addr, args, out)
}

fn raw_y_axis(stack: &mut dyn Memory, addr: InstAddress, args: usize, out: Output) -> VmResult<()> {
	raw_axis(Axis::Y, stack, addr, args, out)
}

fn raw_axis(
	axis: Axis,
	stack: &mut dyn Memory,
	addr: InstAddress,
	args: usize,
	out: Output,
) -> VmResult<()> {
	let values = rune::vm_try!(stack.slice_at(addr, args));
	let origin = raw_origin(values).unwrap_or(Origin { start: 0, end: 0 });
	let value = if values.len() == 2 {
		Ok(FigureElementKind::Axis(axis))
	} else {
		Err(PresentationFailure::new(
			origin,
			format!("{}() takes no arguments", axis.name()),
		))
	};
	store(out, stack, RuneFigureElement::new(value, origin))
}

fn raw_facet(stack: &mut dyn Memory, addr: InstAddress, args: usize, out: Output) -> VmResult<()> {
	raw_string_element(StringConstructor::Facet, "facet", stack, addr, args, out)
}

fn raw_figure_facet_gutter(
	stack: &mut dyn Memory,
	addr: InstAddress,
	args: usize,
	out: Output,
) -> VmResult<()> {
	let values = rune::vm_try!(stack.slice_at(addr, args));
	let origin = raw_origin(values).unwrap_or(Origin { start: 0, end: 0 });
	let mut element = match values
		.first()
		.cloned()
		.map(rune::from_value::<RuneFigureElement>)
	{
		Some(Ok(element)) => element,
		_ => RuneFigureElement::new(
			Err(PresentationFailure::new(
				origin,
				"gutter(...) requires a facet(...) receiver",
			)),
			origin,
		),
	};
	if element.value.is_err() {
		return store(out, stack, element);
	}
	if !matches!(element.value.as_ref(), Ok(FigureElementKind::Facet(_))) {
		element.value = Err(PresentationFailure::new(
			origin,
			"gutter(...) requires a facet(...) receiver",
		));
		return store(out, stack, element);
	}
	if values.len() != 4 {
		element.value = Err(PresentationFailure::new(
			origin,
			"gutter(...) requires exactly one finite nonnegative number",
		));
		return store(out, stack, element);
	}
	if element.facet_gutter_origin.is_some() {
		element.value = Err(PresentationFailure::new(
			origin,
			"facet(...).gutter(...) may appear at most once",
		));
		return store(out, stack, element);
	}
	match rune_number(&values[1]) {
		Some(value) if value.is_finite() && value >= 0.0 => {
			element.facet_gutter = Some(value as f32);
			element.facet_gutter_origin = Some(origin);
		}
		_ => {
			element.value = Err(PresentationFailure::new(
				origin,
				"gutter(...) requires exactly one finite nonnegative number",
			));
		}
	}
	store(out, stack, element)
}

fn raw_palette(
	stack: &mut dyn Memory,
	addr: InstAddress,
	args: usize,
	out: Output,
) -> VmResult<()> {
	raw_string_element(
		StringConstructor::Palette,
		"palette",
		stack,
		addr,
		args,
		out,
	)
}

fn raw_sequential_palette(
	stack: &mut dyn Memory,
	addr: InstAddress,
	args: usize,
	out: Output,
) -> VmResult<()> {
	raw_ramp_element(true, "sequential_palette", stack, addr, args, out)
}

fn raw_diverging_palette(
	stack: &mut dyn Memory,
	addr: InstAddress,
	args: usize,
	out: Output,
) -> VmResult<()> {
	raw_ramp_element(false, "diverging_palette", stack, addr, args, out)
}

fn raw_ramp_element(
	sequential: bool,
	name: &'static str,
	stack: &mut dyn Memory,
	addr: InstAddress,
	args: usize,
	out: Output,
) -> VmResult<()> {
	let values = rune::vm_try!(stack.slice_at(addr, args));
	let origin = raw_origin(values).unwrap_or(Origin { start: 0, end: 0 });
	let value = if values.len() == 3 {
		match rune::from_value::<Ref<str>>(values[0].clone()) {
			Ok(token) => match ColorTheme::from_ramp_token(&token) {
				Some(ramp) => Ok(if sequential {
					FigureElementKind::SequentialPalette(ramp)
				} else {
					FigureElementKind::DivergingPalette(ramp)
				}),
				None => Err(PresentationFailure::new(
					origin,
					format!("{name}(...) has unknown ramp token `{}`", &*token),
				)),
			},
			Err(_) => Err(PresentationFailure::new(
				origin,
				format!("{name}(...) requires one ramp token string"),
			)),
		}
	} else {
		Err(PresentationFailure::new(
			origin,
			format!("{name}(...) requires one ramp token string"),
		))
	};
	store(out, stack, RuneFigureElement::new(value, origin))
}

fn raw_string_element(
	kind: StringConstructor,
	name: &'static str,
	stack: &mut dyn Memory,
	addr: InstAddress,
	args: usize,
	out: Output,
) -> VmResult<()> {
	let values = rune::vm_try!(stack.slice_at(addr, args));
	let origin = raw_origin(values).unwrap_or(Origin { start: 0, end: 0 });
	let value = if values.len() == 2 {
		match kind {
			StringConstructor::Title => Ok(FigureElementKind::TextHandle(TextHandleKind::Title)),
			StringConstructor::Subtitle => {
				Ok(FigureElementKind::TextHandle(TextHandleKind::Subtitle))
			}
			_ => Err(PresentationFailure::new(
				origin,
				format!("{name}(...) requires one string"),
			)),
		}
	} else if values.len() == 3 {
		match rune::from_value::<Ref<str>>(values[0].clone()) {
			Ok(value) => {
				let value = value.to_owned();
				Ok(match kind {
					StringConstructor::Title => FigureElementKind::Title(value),
					StringConstructor::Subtitle => FigureElementKind::Subtitle(value),
					StringConstructor::Facet => FigureElementKind::Facet(value),
					StringConstructor::Palette => FigureElementKind::Palette(value),
				})
			}
			Err(_) => Err(PresentationFailure::new(
				origin,
				format!("{name}(...) requires one string"),
			)),
		}
	} else {
		Err(PresentationFailure::new(
			origin,
			format!("{name}(...) requires one string"),
		))
	};
	store(out, stack, RuneFigureElement::new(value, origin))
}

fn raw_skin(stack: &mut dyn Memory, addr: InstAddress, args: usize, out: Output) -> VmResult<()> {
	let values = rune::vm_try!(stack.slice_at(addr, args));
	let origin = raw_origin(values).unwrap_or(Origin { start: 0, end: 0 });
	let name = if values.len() == 3 {
		rune::from_value::<Ref<str>>(values[0].clone())
			.map(|value| value.to_owned())
			.map_err(|_| PresentationFailure::new(origin, "skin(...) requires one string"))
	} else {
		Err(PresentationFailure::new(
			origin,
			"skin(...) requires one string",
		))
	};
	store(
		out,
		stack,
		RuneSkin {
			name,
			projection: PlotChromeProjection::Ordered,
			duotone_origin: None,
			origin,
		},
	)
}

fn raw_legend(stack: &mut dyn Memory, addr: InstAddress, args: usize, out: Output) -> VmResult<()> {
	let values = rune::vm_try!(stack.slice_at(addr, args));
	let origin = raw_origin(values).unwrap_or(Origin { start: 0, end: 0 });
	let value = if values.len() == 2 {
		Ok(FigureElementKind::Legend)
	} else {
		Err(PresentationFailure::new(
			origin,
			"legend() takes no arguments",
		))
	};
	store(out, stack, RuneFigureElement::new(value, origin))
}

#[allow(clippy::result_large_err)] // A refused receiver returns the original figure element intact.
fn legend_receiver(
	values: &[Value],
	origin: Origin,
	modifier: &str,
) -> Result<RuneFigureElement, RuneFigureElement> {
	let mut element = match values
		.first()
		.cloned()
		.map(rune::from_value::<RuneFigureElement>)
	{
		Some(Ok(element)) => element,
		_ => RuneFigureElement::new(
			Err(PresentationFailure::new(
				origin,
				format!("{modifier}(...) requires a legend() receiver"),
			)),
			origin,
		),
	};
	if element.value.is_err() {
		return Err(element);
	}
	if !matches!(element.value.as_ref(), Ok(FigureElementKind::Legend)) {
		element.value = Err(PresentationFailure::new(
			origin,
			format!("{modifier}(...) requires a legend() receiver"),
		));
		Err(element)
	} else {
		Ok(element)
	}
}

fn raw_legend_at(
	stack: &mut dyn Memory,
	addr: InstAddress,
	args: usize,
	out: Output,
) -> VmResult<()> {
	let values = rune::vm_try!(stack.slice_at(addr, args));
	let origin = raw_origin(values).unwrap_or(Origin { start: 0, end: 0 });
	let mut element = match legend_receiver(values, origin, "at") {
		Ok(element) => element,
		Err(element) => return store(out, stack, element),
	};
	if values.len() != 5 {
		element.value = Err(PresentationFailure::new(
			origin,
			"at(...) requires exactly two numeric values",
		));
	} else if element.legend_presence_origin.is_some() {
		element.value = Err(PresentationFailure::new(
			origin,
			"legend().at(...) or .hide() may appear at most once",
		));
	} else {
		let position = rune_number(&values[1])
			.zip(rune_number(&values[2]))
			.and_then(|(x, y)| LegendPosition::new(x, y))
			.ok_or_else(|| {
				PresentationFailure::new(origin, "at(...) requires finite positions".to_owned())
			});
		match position {
			Ok(position) => {
				element.legend_presence = Some(Presence::Show(position));
				element.legend_presence_origin = Some(origin);
			}
			Err(error) => element.value = Err(error),
		}
	}
	store(out, stack, element)
}

fn raw_legend_hide(
	stack: &mut dyn Memory,
	addr: InstAddress,
	args: usize,
	out: Output,
) -> VmResult<()> {
	let values = rune::vm_try!(stack.slice_at(addr, args));
	let origin = raw_origin(values).unwrap_or(Origin { start: 0, end: 0 });
	let mut element = match legend_receiver(values, origin, "hide") {
		Ok(element) => element,
		Err(element) => return store(out, stack, element),
	};
	if values.len() != 3 {
		element.value = Err(PresentationFailure::new(
			origin,
			"hide() takes no arguments",
		));
	} else if element.legend_presence_origin.is_some() {
		element.value = Err(PresentationFailure::new(
			origin,
			"legend().at(...) or .hide() may appear at most once",
		));
	} else {
		element.legend_presence = Some(Presence::Hide);
		element.legend_presence_origin = Some(origin);
	}
	store(out, stack, element)
}

#[derive(Clone, Copy)]
enum NumericScalarConstructor {
	HoverSize,
}

fn raw_hover_size(
	stack: &mut dyn Memory,
	addr: InstAddress,
	args: usize,
	out: Output,
) -> VmResult<()> {
	raw_numeric_scalar(
		NumericScalarConstructor::HoverSize,
		"hover_size",
		stack,
		addr,
		args,
		out,
	)
}

fn raw_padding(
	stack: &mut dyn Memory,
	addr: InstAddress,
	args: usize,
	out: Output,
) -> VmResult<()> {
	let values = rune::vm_try!(stack.slice_at(addr, args));
	let origin = raw_origin(values).unwrap_or(Origin { start: 0, end: 0 });
	let element = if values.len() == 2 {
		RuneFigureElement::new(Ok(FigureElementKind::Padding), origin)
	} else {
		RuneFigureElement::new(
			Err(PresentationFailure::new(
				origin,
				"padding() takes no arguments",
			)),
			origin,
		)
	};
	store(out, stack, element)
}

fn raw_figure_padding_top(
	stack: &mut dyn Memory,
	addr: InstAddress,
	args: usize,
	out: Output,
) -> VmResult<()> {
	raw_figure_padding_edge(PaddingEdge::Top, stack, addr, args, out)
}

fn raw_figure_padding_right(
	stack: &mut dyn Memory,
	addr: InstAddress,
	args: usize,
	out: Output,
) -> VmResult<()> {
	raw_figure_padding_edge(PaddingEdge::Right, stack, addr, args, out)
}

fn raw_figure_padding_bottom(
	stack: &mut dyn Memory,
	addr: InstAddress,
	args: usize,
	out: Output,
) -> VmResult<()> {
	raw_figure_padding_edge(PaddingEdge::Bottom, stack, addr, args, out)
}

fn raw_figure_padding_left(
	stack: &mut dyn Memory,
	addr: InstAddress,
	args: usize,
	out: Output,
) -> VmResult<()> {
	raw_figure_padding_edge(PaddingEdge::Left, stack, addr, args, out)
}

fn raw_figure_padding_edge(
	edge: PaddingEdge,
	stack: &mut dyn Memory,
	addr: InstAddress,
	args: usize,
	out: Output,
) -> VmResult<()> {
	let values = rune::vm_try!(stack.slice_at(addr, args));
	let origin = raw_origin(values).unwrap_or(Origin { start: 0, end: 0 });
	let mut element = match values
		.first()
		.cloned()
		.map(rune::from_value::<RuneFigureElement>)
	{
		Some(Ok(element)) => element,
		_ => RuneFigureElement::new(
			Err(PresentationFailure::new(
				origin,
				format!("{}(...) requires padding()", edge.name()),
			)),
			origin,
		),
	};
	if element.value.is_err() {
		return store(out, stack, element);
	}
	if !matches!(element.value.as_ref(), Ok(FigureElementKind::Padding)) {
		element.value = Err(PresentationFailure::new(
			origin,
			format!("{}(...) requires padding()", edge.name()),
		));
		return store(out, stack, element);
	}
	if values.len() != 4 {
		element.value = Err(PresentationFailure::new(
			origin,
			format!("{}(...) takes exactly one numeric value", edge.name()),
		));
		return store(out, stack, element);
	}
	let index = edge.index();
	if element.padding_edge_origins[index].is_some() {
		element.value = Err(PresentationFailure::new(
			origin,
			format!("padding().{}(...) may appear at most once", edge.name()),
		));
		return store(out, stack, element);
	}
	let Some(pixels) = rune_number(&values[1]) else {
		element.value = Err(PresentationFailure::new(
			origin,
			format!("{}(...) requires a numeric value", edge.name()),
		));
		return store(out, stack, element);
	};
	let value = match PlotPaddingPixels::new(pixels as f32) {
		Ok(value) => value,
		Err(error) => {
			element.value = Err(PresentationFailure::new(origin, error.to_string()));
			return store(out, stack, element);
		}
	};
	*edge.slot(&mut element.padding) = Some(value);
	element.padding_edge_origins[index] = Some(origin);
	store(out, stack, element)
}

#[allow(clippy::result_large_err)] // A refused receiver returns the original figure element intact.
fn axis_receiver(
	values: &[Value],
	origin: Origin,
	name: &'static str,
) -> Result<(RuneFigureElement, Axis), RuneFigureElement> {
	let element = match values
		.first()
		.cloned()
		.map(rune::from_value::<RuneFigureElement>)
	{
		Some(Ok(element)) => element,
		_ => {
			return Err(RuneFigureElement::new(
				Err(PresentationFailure::new(
					origin,
					format!("{name}(...) requires an axis receiver"),
				)),
				origin,
			));
		}
	};
	let axis = match &element.value {
		Err(_) => return Err(element),
		Ok(FigureElementKind::Axis(axis)) => *axis,
		Ok(_) => {
			return Err(RuneFigureElement::new(
				Err(PresentationFailure::new(
					origin,
					format!("{name}(...) requires an axis receiver"),
				)),
				origin,
			));
		}
	};
	Ok((element, axis))
}

fn raw_axis_label(
	stack: &mut dyn Memory,
	addr: InstAddress,
	args: usize,
	out: Output,
) -> VmResult<()> {
	let values = rune::vm_try!(stack.slice_at(addr, args));
	let origin = raw_origin(values).unwrap_or(Origin { start: 0, end: 0 });
	let (mut element, axis) = match axis_receiver(values, origin, "label") {
		Ok(receiver) => receiver,
		Err(element) => return store(out, stack, element),
	};
	if values.len() != 4 {
		element.value = Err(PresentationFailure::new(
			origin,
			"label(...) requires one nonempty string",
		));
	} else if element.axis.label.is_some() {
		element.value = Err(PresentationFailure::new(
			origin,
			format!("{}().label(...) may appear at most once", axis.name()),
		));
	} else {
		match rune::from_value::<Ref<str>>(values[1].clone()) {
			Ok(label) if !label.trim().is_empty() => {
				element.axis.label = Some((label.to_owned(), origin));
			}
			_ => {
				element.value = Err(PresentationFailure::new(
					origin,
					"label(...) requires one nonempty string",
				));
			}
		}
	}
	store(out, stack, element)
}

fn raw_figure_label_size(
	stack: &mut dyn Memory,
	addr: InstAddress,
	args: usize,
	out: Output,
) -> VmResult<()> {
	let values = rune::vm_try!(stack.slice_at(addr, args));
	let origin = raw_origin(values).unwrap_or(Origin { start: 0, end: 0 });
	let mut element = match values
		.first()
		.cloned()
		.map(rune::from_value::<RuneFigureElement>)
	{
		Some(Ok(element)) => element,
		_ => RuneFigureElement::new(
			Err(PresentationFailure::new(
				origin,
				"label_size(...) requires an x_axis(), y_axis(), or facet(...) receiver",
			)),
			origin,
		),
	};
	if element.value.is_err() {
		return store(out, stack, element);
	}
	let receiver = match element.value.as_ref() {
		Ok(FigureElementKind::Axis(axis)) => Some(*axis),
		Ok(FigureElementKind::Facet(_)) => None,
		_ => {
			element.value = Err(PresentationFailure::new(
				origin,
				"label_size(...) requires an x_axis(), y_axis(), or facet(...) receiver",
			));
			return store(out, stack, element);
		}
	};
	if values.len() != 4 {
		element.value = Err(PresentationFailure::new(
			origin,
			"label_size(...) takes exactly one numeric value",
		));
	} else {
		match receiver {
			Some(axis) if element.axis.label_size.is_some() => {
				element.value = Err(PresentationFailure::new(
					origin,
					format!("{}().label_size(...) may appear at most once", axis.name()),
				));
			}
			None if element.facet_label_size_origin.is_some() => {
				element.value = Err(PresentationFailure::new(
					origin,
					"facet(...).label_size(...) may appear at most once",
				));
			}
			Some(axis) => {
				let size = rune_number(&values[1])
					.ok_or_else(|| {
						PresentationFailure::new(
							origin,
							"label_size(...) requires a number".to_owned(),
						)
					})
					.and_then(|value| match axis {
						Axis::X => XLabelTextSize::new(value as f32)
							.map(AxisLabelSize::X)
							.map_err(|error| PresentationFailure::new(origin, error.to_string())),
						Axis::Y => YLabelTextSize::new(value as f32)
							.map(AxisLabelSize::Y)
							.map_err(|error| PresentationFailure::new(origin, error.to_string())),
					});
				match size {
					Ok(size) => element.axis.label_size = Some((size, origin)),
					Err(error) => element.value = Err(error),
				}
			}
			None => match rune_number(&values[1])
				.ok_or_else(|| {
					PresentationFailure::new(origin, "label_size(...) requires a number".to_owned())
				})
				.and_then(|value| {
					FacetLabelTextSize::new(value as f32)
						.map_err(|error| PresentationFailure::new(origin, error.to_string()))
				}) {
				Ok(size) => {
					element.facet_label_size = Some(size);
					element.facet_label_size_origin = Some(origin);
				}
				Err(error) => element.value = Err(error),
			},
		}
	}
	store(out, stack, element)
}

fn raw_axis_bounds(
	stack: &mut dyn Memory,
	addr: InstAddress,
	args: usize,
	out: Output,
) -> VmResult<()> {
	let values = rune::vm_try!(stack.slice_at(addr, args));
	let origin = raw_origin(values).unwrap_or(Origin { start: 0, end: 0 });
	let (mut element, axis) = match axis_receiver(values, origin, "bounds") {
		Ok(receiver) => receiver,
		Err(element) => return store(out, stack, element),
	};
	if values.len() != 5 {
		element.value = Err(PresentationFailure::new(
			origin,
			"bounds(...) requires exactly two numeric values",
		));
	} else if element.axis.bounds.is_some() {
		element.value = Err(PresentationFailure::new(
			origin,
			format!("{}().bounds(...) may appear at most once", axis.name()),
		));
	} else {
		let bounds = (|| {
			let minimum = rune_number(&values[1]).ok_or_else(|| {
				PresentationFailure::new(origin, "bounds(...) requires numeric values")
			})?;
			let maximum = rune_number(&values[2]).ok_or_else(|| {
				PresentationFailure::new(origin, "bounds(...) requires numeric values")
			})?;
			if !minimum.is_finite() || !maximum.is_finite() {
				Err(PresentationFailure::new(
					origin,
					"bounds(...) requires finite bounds",
				))
			} else if minimum == maximum {
				Err(PresentationFailure::new(
					origin,
					"bounds(...) cannot form a collapsed axis; min must be less than max",
				))
			} else if minimum > maximum {
				Err(PresentationFailure::new(
					origin,
					"bounds(...) cannot reverse an axis; min must be less than max",
				))
			} else {
				Ok((minimum, maximum))
			}
		})();
		match bounds {
			Ok((minimum, maximum)) => element.axis.bounds = Some((minimum, maximum, origin)),
			Err(error) => element.value = Err(error),
		}
	}
	store(out, stack, element)
}

fn raw_axis_tick_step(
	stack: &mut dyn Memory,
	addr: InstAddress,
	args: usize,
	out: Output,
) -> VmResult<()> {
	let values = rune::vm_try!(stack.slice_at(addr, args));
	let origin = raw_origin(values).unwrap_or(Origin { start: 0, end: 0 });
	let (mut element, axis) = match axis_receiver(values, origin, "tick_step") {
		Ok(receiver) => receiver,
		Err(element) => return store(out, stack, element),
	};
	if values.len() != 4 {
		element.value = Err(PresentationFailure::new(
			origin,
			"tick_step(...) requires exactly one numeric value",
		));
	} else if element.axis.tick_step.is_some() || element.axis.tick_every.is_some() {
		element.value = Err(PresentationFailure::new(
			origin,
			format!(
				"{}().tick_step(...) cannot combine with another tick placement",
				axis.name()
			),
		));
	} else {
		let value = rune_number(&values[1])
			.ok_or_else(|| PresentationFailure::new(origin, "tick_step(...) requires a number"))
			.and_then(|value| {
				if !value.is_finite() {
					Err(PresentationFailure::new(
						origin,
						"tick_step(...) requires a finite step",
					))
				} else if value <= 0.0 {
					Err(PresentationFailure::new(
						origin,
						"tick_step(...) step must be strictly positive",
					))
				} else {
					Ok(value)
				}
			});
		match value {
			Ok(value) => element.axis.tick_step = Some((value, origin)),
			Err(error) => element.value = Err(error),
		}
	}
	store(out, stack, element)
}

fn raw_axis_tick_every(
	stack: &mut dyn Memory,
	addr: InstAddress,
	args: usize,
	out: Output,
) -> VmResult<()> {
	let values = rune::vm_try!(stack.slice_at(addr, args));
	let origin = raw_origin(values).unwrap_or(Origin { start: 0, end: 0 });
	let (mut element, axis) = match axis_receiver(values, origin, "tick_every") {
		Ok(receiver) => receiver,
		Err(element) => return store(out, stack, element),
	};
	if values.len() != 4 {
		element.value = Err(PresentationFailure::new(
			origin,
			"tick_every(...) requires exactly one interval string",
		));
	} else if element.axis.tick_every.is_some() || element.axis.tick_step.is_some() {
		element.value = Err(PresentationFailure::new(
			origin,
			format!(
				"{}().tick_every(...) cannot combine with another tick placement",
				axis.name()
			),
		));
	} else {
		let interval = rune::from_value::<Ref<str>>(values[1].clone())
			.map_err(|_| {
				PresentationFailure::new(origin, "tick_every(...) requires an interval string")
			})
			.and_then(|value| {
				TemporalInterval::parse(&value)
					.map_err(|error| PresentationFailure::new(origin, error.to_string()))
			})
			.and_then(|interval| {
				PositiveTemporalInterval::try_from_interval(interval).map_err(|_| {
					PresentationFailure::new(
						origin,
						"tick_every(...) requires a strictly positive interval",
					)
				})
			});
		match interval {
			Ok(interval) => element.axis.tick_every = Some((interval, origin)),
			Err(error) => element.value = Err(error),
		}
	}
	store(out, stack, element)
}

fn raw_axis_minor_ticks(
	stack: &mut dyn Memory,
	addr: InstAddress,
	args: usize,
	out: Output,
) -> VmResult<()> {
	let values = rune::vm_try!(stack.slice_at(addr, args));
	let origin = raw_origin(values).unwrap_or(Origin { start: 0, end: 0 });
	let (mut element, axis) = match axis_receiver(values, origin, "minor_ticks") {
		Ok(receiver) => receiver,
		Err(element) => return store(out, stack, element),
	};
	if values.len() != 4 {
		element.value = Err(PresentationFailure::new(
			origin,
			"minor_ticks(...) requires exactly one numeric value",
		));
	} else if element.axis.minor_ticks.is_some() || element.axis.tick_every.is_some() {
		element.value = Err(PresentationFailure::new(
			origin,
			format!(
				"{}().minor_ticks(...) cannot combine with .tick_every(...)",
				axis.name()
			),
		));
	} else {
		let count = rune_number(&values[1])
			.ok_or_else(|| PresentationFailure::new(origin, "minor_ticks(...) requires a number"))
			.and_then(|value| {
				if !value.is_finite() {
					Err(PresentationFailure::new(
						origin,
						"minor_ticks(...) requires a finite count",
					))
				} else if value < 0.0 || value.fract() != 0.0 || value > u32::MAX as f64 {
					Err(PresentationFailure::new(
						origin,
						"minor_ticks(...) count must be a non-negative integer",
					))
				} else {
					Ok(value as u32)
				}
			});
		match count {
			Ok(count) => element.axis.minor_ticks = Some((count, origin)),
			Err(error) => element.value = Err(error),
		}
	}
	store(out, stack, element)
}

fn raw_axis_tick_size(
	stack: &mut dyn Memory,
	addr: InstAddress,
	args: usize,
	out: Output,
) -> VmResult<()> {
	let values = rune::vm_try!(stack.slice_at(addr, args));
	let origin = raw_origin(values).unwrap_or(Origin { start: 0, end: 0 });
	let (mut element, axis) = match axis_receiver(values, origin, "tick_size") {
		Ok(receiver) => receiver,
		Err(element) => return store(out, stack, element),
	};
	if values.len() != 4 {
		element.value = Err(PresentationFailure::new(
			origin,
			"tick_size(...) takes exactly one numeric value",
		));
	} else if element.axis.tick_size.is_some() {
		element.value = Err(PresentationFailure::new(
			origin,
			format!("{}().tick_size(...) may appear at most once", axis.name()),
		));
	} else {
		let size = rune_number(&values[1])
			.ok_or_else(|| {
				PresentationFailure::new(origin, "tick_size(...) requires a number".to_owned())
			})
			.and_then(|value| match axis {
				Axis::X => XTickTextSize::new(value as f32)
					.map(AxisTickSize::X)
					.map_err(|error| PresentationFailure::new(origin, error.to_string())),
				Axis::Y => YTickTextSize::new(value as f32)
					.map(AxisTickSize::Y)
					.map_err(|error| PresentationFailure::new(origin, error.to_string())),
			});
		match size {
			Ok(size) => element.axis.tick_size = Some((size, origin)),
			Err(error) => element.value = Err(error),
		}
	}
	store(out, stack, element)
}

fn raw_axis_tick_rotation(
	stack: &mut dyn Memory,
	addr: InstAddress,
	args: usize,
	out: Output,
) -> VmResult<()> {
	let values = rune::vm_try!(stack.slice_at(addr, args));
	let origin = raw_origin(values).unwrap_or(Origin { start: 0, end: 0 });
	let (mut element, axis) = match axis_receiver(values, origin, "tick_rotation") {
		Ok(receiver) => receiver,
		Err(element) => return store(out, stack, element),
	};
	if axis != Axis::X {
		element.value = Err(PresentationFailure::new(
			origin,
			"tick_rotation(...) is available on x_axis(), not y_axis()",
		));
	} else if values.len() != 4 {
		element.value = Err(PresentationFailure::new(
			origin,
			"tick_rotation(...) takes exactly one numeric value",
		));
	} else if element.axis.tick_rotation.is_some() {
		element.value = Err(PresentationFailure::new(
			origin,
			"x_axis().tick_rotation(...) may appear at most once",
		));
	} else {
		let rotation = rune_number(&values[1])
			.ok_or_else(|| {
				PresentationFailure::new(origin, "tick_rotation(...) requires a number".to_owned())
			})
			.and_then(|value| {
				TickRotation::new(value as f32)
					.map_err(|error| PresentationFailure::new(origin, error.to_string()))
			});
		match rotation {
			Ok(rotation) => element.axis.tick_rotation = Some((rotation, origin)),
			Err(error) => element.value = Err(error),
		}
	}
	store(out, stack, element)
}

fn raw_numeric_scalar(
	kind: NumericScalarConstructor,
	name: &'static str,
	stack: &mut dyn Memory,
	addr: InstAddress,
	args: usize,
	out: Output,
) -> VmResult<()> {
	let values = rune::vm_try!(stack.slice_at(addr, args));
	let origin = raw_origin(values).unwrap_or(Origin { start: 0, end: 0 });
	let element = if values.len() != 3 {
		RuneFigureElement::new(
			Err(PresentationFailure::new(
				origin,
				format!("{name}(...) requires exactly one numeric value"),
			)),
			origin,
		)
	} else {
		match kind {
			NumericScalarConstructor::HoverSize => {
				presentation_hover_size(values[0].clone(), origin.start.into(), origin.end.into())
			}
		}
	};
	store(out, stack, element)
}

fn raw_table(stack: &mut dyn Memory, addr: InstAddress, args: usize, out: Output) -> VmResult<()> {
	let values = rune::vm_try!(stack.slice_at(addr, args));
	let origin = raw_origin(values).unwrap_or(Origin { start: 0, end: 0 });
	let mut table = presentation_table(origin.start.into(), origin.end.into());
	if values.len() != 2 {
		table.failure = Some(PresentationFailure::new(
			origin,
			"table() takes no arguments",
		));
	}
	store(out, stack, table)
}

fn raw_mark(
	plot_type: PlotType,
	name: &'static str,
	stack: &mut dyn Memory,
	addr: InstAddress,
	args: usize,
	out: Output,
) -> VmResult<()> {
	let values = rune::vm_try!(stack.slice_at(addr, args));
	let origin = match raw_origin(values) {
		Ok(origin) => origin,
		Err(failure) => return store(out, stack, RuneMark::failed(failure)),
	};
	let fields = &values[..values.len().saturating_sub(2)];
	let message = match plot_type {
		PlotType::Scatter => "scatter(...) takes x and y strings with an optional category",
		PlotType::Bar => "bar(...) takes x and y strings with an optional series",
		PlotType::Line => "line(...) takes x and y strings with an optional series",
		PlotType::StackedArea => "area(...) takes x and y strings with an optional series",
		PlotType::Histogram => "histogram(...) takes bin_start, bin_end, and count strings",
		_ => unreachable!("Rune exposes only five authored mark families"),
	};
	let arity_ok = if plot_type == PlotType::Histogram {
		fields.len() == 3
	} else {
		matches!(fields.len(), 2 | 3)
	};
	let sink = if arity_ok {
		let mut decoded = Vec::with_capacity(fields.len());
		for field in fields {
			match rune::from_value::<Ref<str>>(field.clone()) {
				Ok(field) => decoded.push(field.to_owned()),
				Err(_) => {
					return store(
						out,
						stack,
						RuneMark::failed(PresentationFailure::new(
							origin,
							format!("{name}(...) field arguments must evaluate to strings"),
						)),
					);
				}
			}
		}
		let (x, y, category, interval_end) = if plot_type == PlotType::Histogram {
			(
				decoded[0].clone(),
				decoded[2].clone(),
				None,
				Some(decoded[1].clone()),
			)
		} else {
			(
				decoded[0].clone(),
				decoded[1].clone(),
				decoded.get(2).cloned(),
				None,
			)
		};
		Ok(StyledRunePlotSink {
			sink: DisposableRunePlotSink {
				composition: PlotComposition::single(plot_type),
				x,
				y,
				category,
				interval_end,
				mark_fields: None,
				origin: origin.range(),
			},
			settings: LayerSettings::default(),
			stacked: false,
			percent: false,
		})
	} else {
		Err(PresentationFailure::new(origin, message))
	};
	store(
		out,
		stack,
		RuneMark {
			sink,
			origin,
			modified: false,
			point_size_origin: None,
			stroke_width_origin: None,
			bar_width_origin: None,
			category_gap_origin: None,
			stacked_origin: None,
			percent_origin: None,
			continuous_intent: None,
			continuous_intent_origin: None,
		},
	)
}

impl RuneMark {
	fn failed(failure: PresentationFailure) -> Self {
		let origin = Origin {
			start: failure.span.start,
			end: failure.span.end,
		};
		Self {
			sink: Err(failure),
			origin,
			modified: false,
			point_size_origin: None,
			stroke_width_origin: None,
			bar_width_origin: None,
			category_gap_origin: None,
			stacked_origin: None,
			percent_origin: None,
			continuous_intent: None,
			continuous_intent_origin: None,
		}
	}

	fn plot_type(&self) -> Option<PlotType> {
		self.sink
			.as_ref()
			.ok()
			.map(|sink| sink.sink.composition.primary_plot_type())
	}
}

fn raw_origin(values: &[Value]) -> Result<Origin, PresentationFailure> {
	let [.., start, end] = values else {
		return Err(PresentationFailure {
			span: 0..0,
			message: "generated presentation call omitted its origin".to_owned(),
		});
	};
	let start = start.as_signed().map_err(|_| PresentationFailure {
		span: 0..0,
		message: "generated presentation origin start is not an integer".to_owned(),
	})?;
	let end = end.as_signed().map_err(|_| PresentationFailure {
		span: 0..0,
		message: "generated presentation origin end is not an integer".to_owned(),
	})?;
	Origin::decode(start, end)
}

fn store<T: rune::ToValue>(out: Output, stack: &mut dyn Memory, value: T) -> VmResult<()> {
	rune::vm_try!(out.store(stack, || rune::to_value(value)));
	VmResult::Ok(())
}

fn presentation_hover_size(value: Value, start: i64, end: i64) -> RuneFigureElement {
	let origin = Origin::decode(start, end).unwrap_or(Origin { start: 0, end: 0 });
	let value = rune_number(&value)
		.ok_or_else(|| {
			PresentationFailure::new(origin, "hover_size(...) requires a number".to_owned())
		})
		.and_then(|value| {
			HoverTextSize::new(value as f32)
				.map(FigureElementKind::HoverSize)
				.map_err(|error| PresentationFailure::new(origin, error.to_string()))
		});
	RuneFigureElement::new(value, origin)
}

fn rune_number(value: &Value) -> Option<f64> {
	value
		.as_float()
		.ok()
		.or_else(|| value.as_signed().ok().map(|value| value as f64))
		.or_else(|| value.as_unsigned().ok().map(|value| value as f64))
}

fn skin_duotone(mut skin: RuneSkin, start: i64, end: i64) -> RuneSkin {
	let origin = Origin::decode(start, end).unwrap_or(Origin { start: 0, end: 0 });
	if skin.duotone_origin.is_some() {
		skin.name = Err(PresentationFailure::new(
			origin,
			"duotone() may appear at most once on a plot skin",
		));
	} else {
		skin.projection = PlotChromeProjection::Duotone;
		skin.duotone_origin = Some(origin);
	}
	skin
}

fn presentation_table(start: i64, end: i64) -> RuneTable {
	let origin = Origin::decode(start, end).unwrap_or(Origin { start: 0, end: 0 });
	RuneTable {
		skin_name: None,
		skin_origin: None,
		duotone: false,
		duotone_origin: None,
		show_types: false,
		show_types_origin: None,
		failure: None,
		origin,
	}
}

fn table_skin(mut table: RuneTable, name: Ref<str>, start: i64, end: i64) -> RuneTable {
	let origin = Origin::decode(start, end).unwrap_or(Origin { start: 0, end: 0 });
	if table.skin_origin.is_some() {
		table.failure = Some(PresentationFailure::new(
			origin,
			"skin(...) may appear at most once on a table sink",
		));
	} else {
		table.skin_name = Some(name.to_owned());
		table.skin_origin = Some(origin);
	}
	table
}

fn table_duotone(mut table: RuneTable, start: i64, end: i64) -> RuneTable {
	let origin = Origin::decode(start, end).unwrap_or(Origin { start: 0, end: 0 });
	if table.duotone_origin.is_some() {
		table.failure = Some(PresentationFailure::new(
			origin,
			"duotone() may appear at most once on a table sink",
		));
	} else {
		table.duotone = true;
		table.duotone_origin = Some(origin);
	}
	table
}

fn table_show_types(mut table: RuneTable, start: i64, end: i64) -> RuneTable {
	let origin = Origin::decode(start, end).unwrap_or(Origin { start: 0, end: 0 });
	if table.show_types_origin.is_some() {
		table.failure = Some(PresentationFailure::new(
			origin,
			"show_types() may appear at most once on a table sink",
		));
	} else {
		table.show_types = true;
		table.show_types_origin = Some(origin);
	}
	table
}

fn raw_skin_duotone(
	stack: &mut dyn Memory,
	addr: InstAddress,
	args: usize,
	out: Output,
) -> VmResult<()> {
	let values = rune::vm_try!(stack.slice_at(addr, args));
	let origin = raw_origin(values).unwrap_or(Origin { start: 0, end: 0 });
	let mut skin = match values.first().cloned().map(rune::from_value::<RuneSkin>) {
		Some(Ok(skin)) => skin,
		_ => RuneSkin {
			name: Err(PresentationFailure::new(
				origin,
				"duotone() requires a skin receiver",
			)),
			projection: PlotChromeProjection::Ordered,
			duotone_origin: None,
			origin,
		},
	};
	if values.len() != 3 {
		skin.name = Err(PresentationFailure::new(
			origin,
			"duotone() takes no arguments",
		));
	} else {
		skin = skin_duotone(skin, origin.start.into(), origin.end.into());
	}
	store(out, stack, skin)
}

#[derive(Clone, Copy)]
enum TableModifier {
	Skin,
	Duotone,
	ShowTypes,
}

fn raw_table_skin(
	stack: &mut dyn Memory,
	addr: InstAddress,
	args: usize,
	out: Output,
) -> VmResult<()> {
	raw_table_modifier(TableModifier::Skin, "skin", true, stack, addr, args, out)
}

fn raw_table_duotone(
	stack: &mut dyn Memory,
	addr: InstAddress,
	args: usize,
	out: Output,
) -> VmResult<()> {
	raw_table_modifier(
		TableModifier::Duotone,
		"duotone",
		false,
		stack,
		addr,
		args,
		out,
	)
}

fn raw_table_show_types(
	stack: &mut dyn Memory,
	addr: InstAddress,
	args: usize,
	out: Output,
) -> VmResult<()> {
	raw_table_modifier(
		TableModifier::ShowTypes,
		"show_types",
		false,
		stack,
		addr,
		args,
		out,
	)
}

fn raw_table_modifier(
	kind: TableModifier,
	name: &'static str,
	takes_value: bool,
	stack: &mut dyn Memory,
	addr: InstAddress,
	args: usize,
	out: Output,
) -> VmResult<()> {
	let values = rune::vm_try!(stack.slice_at(addr, args));
	let origin = raw_origin(values).unwrap_or(Origin { start: 0, end: 0 });
	let mut table = match values.first().cloned().map(rune::from_value::<RuneTable>) {
		Some(Ok(table)) => table,
		_ => {
			let mut table = presentation_table(origin.start.into(), origin.end.into());
			table.failure = Some(PresentationFailure::new(
				origin,
				format!("{name}(...) requires a table receiver"),
			));
			table
		}
	};
	if table.failure.is_some() {
		return store(out, stack, table);
	}
	let expected = if takes_value { 4 } else { 3 };
	if values.len() != expected {
		table.failure = Some(PresentationFailure::new(
			origin,
			if takes_value {
				format!("{name}(...) takes exactly one argument")
			} else {
				format!("{name}() takes no arguments")
			},
		));
		return store(out, stack, table);
	}
	table = match kind {
		TableModifier::Skin => match rune::from_value::<Ref<str>>(values[1].clone()) {
			Ok(value) => table_skin(table, value, origin.start.into(), origin.end.into()),
			Err(_) => {
				table.failure = Some(PresentationFailure::new(
					origin,
					"skin(...) requires a theme string",
				));
				table
			}
		},
		TableModifier::Duotone => table_duotone(table, origin.start.into(), origin.end.into()),
		TableModifier::ShowTypes => table_show_types(table, origin.start.into(), origin.end.into()),
	};
	store(out, stack, table)
}

fn mark_point_size(mut mark: RuneMark, value: Value, start: i64, end: i64) -> RuneMark {
	let origin = Origin::decode(start, end).unwrap_or(Origin { start: 0, end: 0 });
	mark.modified = true;
	if mark.point_size_origin.is_some() {
		mark.sink = Err(PresentationFailure::new(
			origin,
			"point_size(...) may appear at most once on a mark",
		));
		return mark;
	}
	mark.point_size_origin = Some(origin);
	if mark.plot_type() != Some(PlotType::Scatter) {
		mark.sink = Err(PresentationFailure::new(
			origin,
			"point_size(...) is available on scatter marks, not line, bar, or area marks",
		));
		return mark;
	}
	let Some(value) = rune_number(&value) else {
		mark.sink = Err(PresentationFailure::new(
			origin,
			"point_size(...) requires a numeric value",
		));
		return mark;
	};
	match PointSize::new(value as f32) {
		Ok(value) => {
			if let Ok(sink) = &mut mark.sink {
				sink.settings.point_size = Override::Set(value);
			}
		}
		Err(error) => mark.sink = Err(PresentationFailure::new(origin, error.to_string())),
	}
	mark
}

fn mark_stroke_width(mut mark: RuneMark, value: Value, start: i64, end: i64) -> RuneMark {
	let origin = Origin::decode(start, end).unwrap_or(Origin { start: 0, end: 0 });
	mark.modified = true;
	if mark.stroke_width_origin.is_some() {
		mark.sink = Err(PresentationFailure::new(
			origin,
			"stroke_width(...) may appear at most once on a mark",
		));
		return mark;
	}
	mark.stroke_width_origin = Some(origin);
	if mark.plot_type() != Some(PlotType::Line) {
		mark.sink = Err(PresentationFailure::new(
			origin,
			"stroke_width(...) is available on line marks, not scatter, bar, or area marks",
		));
		return mark;
	}
	let Some(value) = rune_number(&value) else {
		mark.sink = Err(PresentationFailure::new(
			origin,
			"stroke_width(...) requires a numeric value",
		));
		return mark;
	};
	match StrokeWidth::new(value as f32) {
		Ok(value) => {
			if let Ok(sink) = &mut mark.sink {
				sink.settings.stroke_width = Override::Set(value);
			}
		}
		Err(error) => mark.sink = Err(PresentationFailure::new(origin, error.to_string())),
	}
	mark
}

fn mark_bar_width(mut mark: RuneMark, value: Value, start: i64, end: i64) -> RuneMark {
	let origin = Origin::decode(start, end).unwrap_or(Origin { start: 0, end: 0 });
	mark.modified = true;
	if mark.bar_width_origin.is_some() {
		mark.sink = Err(PresentationFailure::new(
			origin,
			"bar_width(...) may appear at most once on a mark",
		));
		return mark;
	}
	mark.bar_width_origin = Some(origin);
	if mark.plot_type() != Some(PlotType::Bar) {
		mark.sink = Err(PresentationFailure::new(
			origin,
			"bar_width(...) is available on bar marks, not line, scatter, or area marks",
		));
		return mark;
	}
	let Some(value) = rune_number(&value) else {
		mark.sink = Err(PresentationFailure::new(
			origin,
			"bar_width(...) requires a numeric value",
		));
		return mark;
	};
	match BarWidth::new(value as f32) {
		Ok(value) => {
			if let Ok(sink) = &mut mark.sink {
				sink.settings.bar_width = Override::Set(value);
			}
		}
		Err(error) => mark.sink = Err(PresentationFailure::new(origin, error.to_string())),
	}
	mark
}

fn mark_category_gap(mut mark: RuneMark, value: Value, start: i64, end: i64) -> RuneMark {
	let origin = Origin::decode(start, end).unwrap_or(Origin { start: 0, end: 0 });
	mark.modified = true;
	if mark.category_gap_origin.is_some() {
		mark.sink = Err(PresentationFailure::new(
			origin,
			"category_gap(...) may appear at most once on a mark",
		));
		return mark;
	}
	mark.category_gap_origin = Some(origin);
	if mark.plot_type() != Some(PlotType::Bar) {
		mark.sink = Err(PresentationFailure::new(
			origin,
			"category_gap(...) is available on bar marks, not line, scatter, or area marks",
		));
		return mark;
	}
	let Some(value) = rune_number(&value) else {
		mark.sink = Err(PresentationFailure::new(
			origin,
			"category_gap(...) requires a numeric value",
		));
		return mark;
	};
	match CategoryGap::new(value as f32) {
		Ok(value) => {
			if let Ok(sink) = &mut mark.sink {
				sink.settings.category_gap = Override::Set(value);
			}
		}
		Err(error) => mark.sink = Err(PresentationFailure::new(origin, error.to_string())),
	}
	mark
}

fn mark_stacked(mut mark: RuneMark, start: i64, end: i64) -> RuneMark {
	let origin = Origin::decode(start, end).unwrap_or(Origin { start: 0, end: 0 });
	mark.modified = true;
	if mark.stacked_origin.is_some() {
		mark.sink = Err(PresentationFailure::new(
			origin,
			"stacked() may appear at most once on a bar mark",
		));
		return mark;
	}
	mark.stacked_origin = Some(origin);
	if mark.plot_type() != Some(PlotType::Bar) {
		mark.sink = Err(PresentationFailure::new(
			origin,
			"stacked() is available on bar marks, not line, scatter, or area marks",
		));
		return mark;
	}
	if let Ok(sink) = &mut mark.sink {
		sink.stacked = true;
	}
	mark
}

fn mark_percent(mut mark: RuneMark, start: i64, end: i64) -> RuneMark {
	let origin = Origin::decode(start, end).unwrap_or(Origin { start: 0, end: 0 });
	mark.modified = true;
	if mark.percent_origin.is_some() {
		mark.sink = Err(PresentationFailure::new(
			origin,
			"percent() may appear at most once on a stacked mark",
		));
		return mark;
	}
	mark.percent_origin = Some(origin);
	if !matches!(
		mark.plot_type(),
		Some(PlotType::Bar | PlotType::StackedArea)
	) {
		mark.sink = Err(PresentationFailure::new(
			origin,
			"percent() is available on stacked bar and area marks, not line or scatter marks",
		));
		return mark;
	}
	if let Ok(sink) = &mut mark.sink {
		sink.percent = true;
	}
	mark
}

fn mark_diverging(mut mark: RuneMark, anchor: ContinuousAnchor, start: i64, end: i64) -> RuneMark {
	let origin = Origin::decode(start, end).unwrap_or(Origin { start: 0, end: 0 });
	if mark.continuous_intent_origin.is_some() {
		mark.sink = Err(PresentationFailure::new(
			origin,
			"diverging() and diverging_at(...) form one modifier family and may appear at most once",
		));
		return mark;
	}
	mark.continuous_intent_origin = Some(origin);
	if !matches!(mark.plot_type(), Some(PlotType::Heatmap | PlotType::Bubble)) {
		mark.sink = Err(PresentationFailure::new(
			origin,
			"diverging colour is available on heatmap and bubble marks",
		));
		return mark;
	}
	mark.continuous_intent = Some(ContinuousScaleIntent {
		job: crate::plot::spec::ColorJob::Diverging,
		anchor,
	});
	mark
}

fn mark_category(mut mark: RuneMark, category: String, start: i64, end: i64) -> RuneMark {
	let origin = Origin::decode(start, end).unwrap_or(Origin { start: 0, end: 0 });
	if !matches!(mark.plot_type(), Some(PlotType::Radar | PlotType::Parallel)) {
		mark.sink = Err(PresentationFailure::new(
			origin,
			"category(...) is available on radar and parallel marks",
		));
		return mark;
	}
	let Ok(styled) = &mut mark.sink else {
		return mark;
	};
	let Some(spanned) = &mut styled.sink.mark_fields else {
		unreachable!("authored multivariate mark carries its fields")
	};
	let (fields, origins) = match spanned {
		SpannedMarkFields::Radar { fields, origins }
		| SpannedMarkFields::Parallel { fields, origins } => (fields, origins),
		_ => unreachable!("multivariate mark carries matching field transport"),
	};
	if fields.category().is_some() {
		mark.sink = Err(PresentationFailure::new(
			origin,
			"category(...) may appear at most once on a radar or parallel mark",
		));
		return mark;
	}
	fields.set_category(category);
	origins.category = Some(origin.range());
	mark
}

#[derive(Clone, Copy)]
enum MarkModifier {
	PointSize,
	StrokeWidth,
	BarWidth,
	CategoryGap,
	Stacked,
	Percent,
	Diverging,
	DivergingAt,
	Category,
}

fn raw_mark_point_size(
	stack: &mut dyn Memory,
	addr: InstAddress,
	args: usize,
	out: Output,
) -> VmResult<()> {
	raw_mark_modifier(
		MarkModifier::PointSize,
		"point_size",
		true,
		stack,
		addr,
		args,
		out,
	)
}

fn raw_mark_stroke_width(
	stack: &mut dyn Memory,
	addr: InstAddress,
	args: usize,
	out: Output,
) -> VmResult<()> {
	raw_mark_modifier(
		MarkModifier::StrokeWidth,
		"stroke_width",
		true,
		stack,
		addr,
		args,
		out,
	)
}

fn raw_mark_bar_width(
	stack: &mut dyn Memory,
	addr: InstAddress,
	args: usize,
	out: Output,
) -> VmResult<()> {
	raw_mark_modifier(
		MarkModifier::BarWidth,
		"bar_width",
		true,
		stack,
		addr,
		args,
		out,
	)
}

fn raw_mark_category_gap(
	stack: &mut dyn Memory,
	addr: InstAddress,
	args: usize,
	out: Output,
) -> VmResult<()> {
	raw_mark_modifier(
		MarkModifier::CategoryGap,
		"category_gap",
		true,
		stack,
		addr,
		args,
		out,
	)
}

fn raw_mark_category(
	stack: &mut dyn Memory,
	addr: InstAddress,
	args: usize,
	out: Output,
) -> VmResult<()> {
	raw_mark_modifier(
		MarkModifier::Category,
		"category",
		true,
		stack,
		addr,
		args,
		out,
	)
}

fn raw_mark_stacked(
	stack: &mut dyn Memory,
	addr: InstAddress,
	args: usize,
	out: Output,
) -> VmResult<()> {
	raw_mark_modifier(
		MarkModifier::Stacked,
		"stacked",
		false,
		stack,
		addr,
		args,
		out,
	)
}

fn raw_mark_percent(
	stack: &mut dyn Memory,
	addr: InstAddress,
	args: usize,
	out: Output,
) -> VmResult<()> {
	raw_mark_modifier(
		MarkModifier::Percent,
		"percent",
		false,
		stack,
		addr,
		args,
		out,
	)
}

fn raw_mark_diverging(
	stack: &mut dyn Memory,
	addr: InstAddress,
	args: usize,
	out: Output,
) -> VmResult<()> {
	raw_mark_modifier(
		MarkModifier::Diverging,
		"diverging",
		false,
		stack,
		addr,
		args,
		out,
	)
}

fn raw_mark_diverging_at(
	stack: &mut dyn Memory,
	addr: InstAddress,
	args: usize,
	out: Output,
) -> VmResult<()> {
	raw_mark_modifier(
		MarkModifier::DivergingAt,
		"diverging_at",
		true,
		stack,
		addr,
		args,
		out,
	)
}

fn raw_figure_font_size(
	stack: &mut dyn Memory,
	addr: InstAddress,
	args: usize,
	out: Output,
) -> VmResult<()> {
	let values = rune::vm_try!(stack.slice_at(addr, args));
	let origin = raw_origin(values).unwrap_or(Origin { start: 0, end: 0 });
	let mut element = match values
		.first()
		.cloned()
		.map(rune::from_value::<RuneFigureElement>)
	{
		Some(Ok(element)) => element,
		_ => RuneFigureElement::new(
			Err(PresentationFailure::new(
				origin,
				"font_size(...) requires a figure text member",
			)),
			origin,
		),
	};
	if element.value.is_err() {
		return store(out, stack, element);
	}
	if values.len() != 4 {
		element.value = Err(PresentationFailure::new(
			origin,
			"font_size(...) takes exactly one numeric value",
		));
		return store(out, stack, element);
	}
	if element.font_size_origin.is_some() {
		element.value = Err(PresentationFailure::new(
			origin,
			"font_size(...) may appear at most once on a figure text member",
		));
		return store(out, stack, element);
	}
	element.font_size_origin = Some(origin);
	let Some(points) = rune_number(&values[1]) else {
		element.value = Err(PresentationFailure::new(
			origin,
			"font_size(...) requires a numeric value",
		));
		return store(out, stack, element);
	};
	let text_size = match element.value.as_ref().expect("checked above") {
		FigureElementKind::Title(_) | FigureElementKind::TextHandle(TextHandleKind::Title) => {
			TitleTextSize::new(points as f32)
				.map(FigureTextSize::Title)
				.map_err(|error| error.to_string())
		}
		FigureElementKind::Subtitle(_)
		| FigureElementKind::TextHandle(TextHandleKind::Subtitle) => SubtitleTextSize::new(points as f32)
			.map(FigureTextSize::Subtitle)
			.map_err(|error| error.to_string()),
		FigureElementKind::Legend => LegendTextSize::new(points as f32)
			.map(FigureTextSize::Legend)
			.map_err(|error| error.to_string()),
		_ => Err("font_size(...) is available on title, subtitle, and legend members".to_owned()),
	};
	match text_size {
		Ok(text_size) => element.font_size = Some(text_size),
		Err(message) => element.value = Err(PresentationFailure::new(origin, message)),
	}
	store(out, stack, element)
}

fn raw_mark_modifier(
	kind: MarkModifier,
	name: &'static str,
	takes_value: bool,
	stack: &mut dyn Memory,
	addr: InstAddress,
	args: usize,
	out: Output,
) -> VmResult<()> {
	let values = rune::vm_try!(stack.slice_at(addr, args));
	let origin = raw_origin(values).unwrap_or(Origin { start: 0, end: 0 });
	let mut mark = match values.first().cloned().map(rune::from_value::<RuneMark>) {
		Some(Ok(mark)) => mark,
		_ => RuneMark::failed(PresentationFailure::new(
			origin,
			format!("{name}(...) requires a mark receiver"),
		)),
	};
	if mark.sink.is_err() {
		return store(out, stack, mark);
	}
	let expected = if takes_value { 4 } else { 3 };
	if values.len() != expected {
		mark.sink = Err(PresentationFailure::new(
			origin,
			if takes_value {
				format!("{name}(...) takes exactly one argument")
			} else {
				format!("{name}() takes no arguments")
			},
		));
		return store(out, stack, mark);
	}
	let start = i64::from(origin.start);
	let end = i64::from(origin.end);
	mark = match kind {
		MarkModifier::PointSize => mark_point_size(mark, values[1].clone(), start, end),
		MarkModifier::StrokeWidth => mark_stroke_width(mark, values[1].clone(), start, end),
		MarkModifier::BarWidth => mark_bar_width(mark, values[1].clone(), start, end),
		MarkModifier::CategoryGap => mark_category_gap(mark, values[1].clone(), start, end),
		MarkModifier::Stacked => mark_stacked(mark, start, end),
		MarkModifier::Percent => mark_percent(mark, start, end),
		MarkModifier::Diverging => mark_diverging(mark, ContinuousAnchor::DefaultZero, start, end),
		MarkModifier::DivergingAt => match rune_number(&values[1]) {
			Some(anchor) => mark_diverging(mark, ContinuousAnchor::Authored(anchor), start, end),
			None => RuneMark::failed(PresentationFailure::new(
				origin,
				"diverging_at(...) requires a numeric anchor",
			)),
		},
		MarkModifier::Category => match rune::from_value::<Ref<str>>(values[1].clone()) {
			Ok(category) => mark_category(mark, category.to_owned(), start, end),
			Err(_) => RuneMark::failed(PresentationFailure::new(
				origin,
				"category(...) requires a field string",
			)),
		},
	};
	store(out, stack, mark)
}

fn raw_layers(stack: &mut dyn Memory, addr: InstAddress, args: usize, out: Output) -> VmResult<()> {
	let values = rune::vm_try!(stack.slice_at(addr, args));
	let origin = match raw_origin(values) {
		Ok(origin) => origin,
		Err(failure) => {
			return store(
				out,
				stack,
				RuneLayers {
					value: Err(failure),
					origin: Origin { start: 0, end: 0 },
				},
			);
		}
	};
	let value = if values.len() != 3 {
		Err(PresentationFailure::new(
			origin,
			"layers(...) takes exactly one layer list",
		))
	} else {
		decode_layers(&values[0], origin)
	};
	store(out, stack, RuneLayers { value, origin })
}

fn decode_layers(value: &Value, origin: Origin) -> Result<LayeredPlotSpec, PresentationFailure> {
	let values = value
		.borrow_ref::<rune::runtime::Vec>()
		.map_err(|_| PresentationFailure::new(origin, "layers(...) requires a layer list"))?;
	let mut layers = Vec::with_capacity(values.len());
	for value in values.iter() {
		let mark = value.borrow_ref::<RuneMark>().map_err(|_| {
			PresentationFailure::new(
				presentation_value_origin(value).unwrap_or(origin),
				"a layer must be line(x, y) or scatter(x, y)",
			)
		})?;
		let styled = mark.sink.clone()?;
		let layer = styled.into_layer().map_err(|styled| {
			PresentationFailure::new(
				mark.origin,
				format!(
					"{}(...) is a view sink, not a shared-Cartesian layer",
					match styled.sink.composition.primary_plot_type() {
						PlotType::Bar => "bar",
						PlotType::StackedArea => "area",
						PlotType::Line => "line",
						PlotType::Scatter => "scatter",
						PlotType::Hexbin => "hexbin",
						PlotType::FillBetween => "fill_between",
						PlotType::Candlestick => "candlestick",
						_ => "mark",
					}
				),
			)
		})?;
		layers.push(layer);
	}
	let mut layers = layers.into_iter();
	let Some(first) = layers.next() else {
		return Err(PresentationFailure::new(
			origin,
			"layers([]) cannot form a non-empty plot",
		));
	};
	let mut specification = LayeredPlotSpec::new(first);
	for layer in layers {
		specification.push(layer);
	}
	Ok(specification)
}

fn raw_figure(stack: &mut dyn Memory, addr: InstAddress, args: usize, out: Output) -> VmResult<()> {
	let values = rune::vm_try!(stack.slice_at(addr, args));
	let origin = match raw_origin(values) {
		Ok(origin) => origin,
		Err(failure) => {
			return store(
				out,
				stack,
				RunePresentation {
					value: Err(failure),
					origin: Origin { start: 0, end: 0 },
				},
			);
		}
	};
	let value = if values.len() != 3 {
		Err(PresentationFailure::new(
			origin,
			"figure(...) takes exactly one element list",
		))
	} else {
		decode_figure(&values[0], origin).map(RuneViewIntent::Plot)
	};
	store(out, stack, RunePresentation { value, origin })
}

#[derive(Default)]
struct FigureParts {
	title: Option<(String, Origin)>,
	subtitle: Option<(String, Origin)>,
	x_label: Option<(String, Origin)>,
	y_label: Option<(String, Origin)>,
	title_size: Option<(TitleTextSize, Origin)>,
	subtitle_size: Option<(SubtitleTextSize, Origin)>,
	x_label_size: Option<(XLabelTextSize, Origin)>,
	y_label_size: Option<(YLabelTextSize, Origin)>,
	facet: Option<(String, Origin)>,
	facet_gutter: Option<(f32, Origin)>,
	facet_label_size: Option<(FacetLabelTextSize, Origin)>,
	palette: Option<(String, Origin)>,
	sequential_palette: Option<(ColorTheme, Origin)>,
	diverging_palette: Option<(ColorTheme, Origin)>,
	continuous_intent: Option<ContinuousScaleIntent>,
	continuous_intent_origin: Option<Origin>,
	legend: Option<(Presence<LegendPosition>, Origin)>,
	legend_size: Option<(LegendTextSize, Origin)>,
	skin: Option<(PlotSkinSelection, Origin)>,
	x_bounds: Option<(f64, f64, Origin)>,
	y_bounds: Option<(f64, f64, Origin)>,
	x_tick_step: Option<(f64, Origin)>,
	y_tick_step: Option<(f64, Origin)>,
	x_tick_every: Option<(PositiveTemporalInterval, Origin)>,
	y_tick_every: Option<(PositiveTemporalInterval, Origin)>,
	x_minor_ticks: Option<(u32, Origin)>,
	y_minor_ticks: Option<(u32, Origin)>,
	hover_size: Option<(HoverTextSize, Origin)>,
	x_tick_rotation: Option<(TickRotation, Origin)>,
	x_tick_size: Option<(XTickTextSize, Origin)>,
	y_tick_size: Option<(YTickTextSize, Origin)>,
	padding: Option<(FigurePadding, Origin)>,
	body: Option<(RunePlotBody, Origin)>,
}

fn decode_figure(value: &Value, origin: Origin) -> Result<RunePlotIntent, PresentationFailure> {
	let values = value
		.borrow_ref::<rune::runtime::Vec>()
		.map_err(|_| PresentationFailure::new(origin, "figure(...) requires an element list"))?;
	let mut parts = FigureParts::default();
	for value in values.iter() {
		if let Ok(element) = value.borrow_ref::<RuneFigureElement>() {
			let kind = element.value.clone()?;
			merge_element(
				&mut parts,
				kind,
				element.font_size,
				element.font_size_origin,
				element.legend_presence.clone(),
				element.legend_presence_origin,
				element.facet_gutter,
				element.facet_label_size,
				element.padding,
				element.axis.clone(),
				element.origin,
			)?;
			continue;
		}
		if let Ok(skin) = value.borrow_ref::<RuneSkin>() {
			let name = skin.name.clone()?;
			set_once(
				&mut parts.skin,
				(PlotSkinSelection::new(name, skin.projection), skin.origin),
				skin.origin,
				"a figure has at most one plot skin",
			)?;
			continue;
		}
		if let Ok(mark) = value.borrow_ref::<RuneMark>() {
			let body = finalize_mark_body(&mark)?;
			parts.continuous_intent = mark.continuous_intent;
			parts.continuous_intent_origin = mark.continuous_intent_origin;
			set_once(
				&mut parts.body,
				(body, mark.origin),
				mark.origin,
				"a figure contains exactly one plot body",
			)?;
			continue;
		}
		if let Ok(layers) = value.borrow_ref::<RuneLayers>() {
			let body = RunePlotBody::Layered(layers.value.clone()?);
			set_once(
				&mut parts.body,
				(body, layers.origin),
				layers.origin,
				"a figure contains exactly one plot body",
			)?;
			continue;
		}
		return Err(PresentationFailure::new(
			presentation_value_origin(value).unwrap_or(origin),
			"a figure child must be presentation furniture or one plot body",
		));
	}
	let Some((body, _)) = parts.body.take() else {
		return Err(PresentationFailure::new(
			origin,
			"a figure requires exactly one plot body",
		));
	};
	parts.finish(body)
}

fn presentation_value_origin(value: &Value) -> Option<Origin> {
	if let Ok(value) = value.borrow_ref::<RuneFigureElement>() {
		return Some(value.origin);
	}
	if let Ok(value) = value.borrow_ref::<RuneSkin>() {
		return Some(value.origin);
	}
	if let Ok(value) = value.borrow_ref::<RuneMark>() {
		return Some(value.origin);
	}
	if let Ok(value) = value.borrow_ref::<RuneLayers>() {
		return Some(value.origin);
	}
	if let Ok(value) = value.borrow_ref::<RuneTable>() {
		return Some(value.origin);
	}
	if let Ok(value) = value.borrow_ref::<RunePresentation>() {
		return Some(value.origin);
	}
	None
}

fn set_once<T>(
	slot: &mut Option<T>,
	value: T,
	origin: Origin,
	message: &'static str,
) -> Result<(), PresentationFailure> {
	if slot.is_some() {
		Err(PresentationFailure::new(origin, message))
	} else {
		*slot = Some(value);
		Ok(())
	}
}

fn merge_element(
	parts: &mut FigureParts,
	element: FigureElementKind,
	font_size: Option<FigureTextSize>,
	font_size_origin: Option<Origin>,
	legend_presence: Option<Presence<LegendPosition>>,
	legend_presence_origin: Option<Origin>,
	facet_gutter: Option<f32>,
	facet_label_size: Option<FacetLabelTextSize>,
	padding: FigurePadding,
	axis_properties: AxisProperties,
	origin: Origin,
) -> Result<(), PresentationFailure> {
	match element {
		FigureElementKind::Title(value) => {
			set_once(
				&mut parts.title,
				(value, origin),
				origin,
				"a figure has at most one title",
			)?;
			if let Some(size) = font_size {
				let FigureTextSize::Title(size) = size else {
					unreachable!("title size matches title")
				};
				set_once(
					&mut parts.title_size,
					(size, origin),
					origin,
					"a figure has at most one title font size",
				)?;
			}
			Ok(())
		}
		FigureElementKind::Subtitle(value) => {
			set_once(
				&mut parts.subtitle,
				(value, origin),
				origin,
				"a figure has at most one subtitle",
			)?;
			if let Some(size) = font_size {
				let FigureTextSize::Subtitle(size) = size else {
					unreachable!("subtitle size matches subtitle")
				};
				set_once(
					&mut parts.subtitle_size,
					(size, origin),
					origin,
					"a figure has at most one subtitle font size",
				)?;
			}
			Ok(())
		}
		FigureElementKind::TextHandle(handle) => match (handle, font_size) {
			(TextHandleKind::Title, Some(FigureTextSize::Title(size))) => set_once(
				&mut parts.title_size,
				(size, origin),
				origin,
				"a figure has at most one title font size",
			),
			(TextHandleKind::Subtitle, Some(FigureTextSize::Subtitle(size))) => set_once(
				&mut parts.subtitle_size,
				(size, origin),
				origin,
				"a figure has at most one subtitle font size",
			),
			(_, None) => Err(PresentationFailure::new(
				origin,
				"a bare text member requires text or .font_size(...)",
			)),
			_ => unreachable!("text handle and font-size variant match"),
		},
		FigureElementKind::Axis(axis) => merge_axis(parts, axis, axis_properties, origin),
		FigureElementKind::Facet(value) => {
			set_once(
				&mut parts.facet,
				(value, origin),
				origin,
				"a figure has at most one facet element",
			)?;
			if let Some(value) = facet_gutter {
				set_once(
					&mut parts.facet_gutter,
					(value, origin),
					origin,
					"facet(...).gutter(...) may appear at most once",
				)?;
			}
			if let Some(value) = facet_label_size {
				set_once(
					&mut parts.facet_label_size,
					(value, origin),
					origin,
					"facet(...).label_size(...) may appear at most once",
				)?;
			}
			Ok(())
		}
		FigureElementKind::Palette(value) => set_once(
			&mut parts.palette,
			(value, origin),
			origin,
			"a figure has at most one categorical palette",
		),
		FigureElementKind::SequentialPalette(value) => set_once(
			&mut parts.sequential_palette,
			(value, origin),
			origin,
			"a figure has at most one sequential_palette",
		),
		FigureElementKind::DivergingPalette(value) => set_once(
			&mut parts.diverging_palette,
			(value, origin),
			origin,
			"a figure has at most one diverging_palette",
		),
		FigureElementKind::Legend => {
			if legend_presence.is_none() && font_size.is_none() {
				return Err(PresentationFailure::new(
					origin,
					"legend() requires .at(...), .hide(), or .font_size(...)",
				));
			}
			if let Some(value) = legend_presence {
				let property_origin = legend_presence_origin.unwrap_or(origin);
				set_once(
					&mut parts.legend,
					(value, property_origin),
					property_origin,
					"legend().at(...) or .hide() may appear at most once in a figure",
				)?;
			}
			if let Some(size) = font_size {
				let property_origin = font_size_origin.unwrap_or(origin);
				let FigureTextSize::Legend(size) = size else {
					unreachable!("legend size matches legend")
				};
				set_once(
					&mut parts.legend_size,
					(size, property_origin),
					property_origin,
					"legend().font_size(...) may appear at most once in a figure",
				)?;
			}
			Ok(())
		}
		FigureElementKind::HoverSize(value) => set_once(
			&mut parts.hover_size,
			(value, origin),
			origin,
			"a figure has at most one hover_size element",
		),
		FigureElementKind::Padding => {
			if padding.top.is_none()
				&& padding.right.is_none()
				&& padding.bottom.is_none()
				&& padding.left.is_none()
			{
				return Err(PresentationFailure::new(
					origin,
					"padding() requires at least one edge modifier",
				));
			}
			set_once(
				&mut parts.padding,
				(padding, origin),
				origin,
				"a figure has at most one padding element",
			)
		}
	}
}

fn merge_axis(
	parts: &mut FigureParts,
	axis: Axis,
	properties: AxisProperties,
	origin: Origin,
) -> Result<(), PresentationFailure> {
	if properties.is_empty() {
		return Err(PresentationFailure::new(
			origin,
			format!("{}() requires at least one modifier", axis.name()),
		));
	}
	match axis {
		Axis::X => {
			if let Some((label, property_origin)) = properties.label {
				set_once(
					&mut parts.x_label,
					(label, property_origin),
					property_origin,
					"x_axis().label(...) may appear at most once in a figure",
				)?;
			}
			if let Some((AxisLabelSize::X(size), property_origin)) = properties.label_size {
				set_once(
					&mut parts.x_label_size,
					(size, property_origin),
					property_origin,
					"x_axis().label_size(...) may appear at most once in a figure",
				)?;
			}
			if let Some((minimum, maximum, property_origin)) = properties.bounds {
				set_once(
					&mut parts.x_bounds,
					(minimum, maximum, property_origin),
					property_origin,
					"x_axis().bounds(...) may appear at most once in a figure",
				)?;
			}
			if let Some((step, property_origin)) = properties.tick_step {
				if parts.x_tick_every.is_some() {
					return Err(PresentationFailure::new(
						property_origin,
						"x_axis().tick_step(...) and .tick_every(...) cannot be combined",
					));
				}
				set_once(
					&mut parts.x_tick_step,
					(step, property_origin),
					property_origin,
					"x_axis().tick_step(...) may appear at most once in a figure",
				)?;
			}
			if let Some((interval, property_origin)) = properties.tick_every {
				if parts.x_tick_step.is_some() || parts.x_minor_ticks.is_some() {
					return Err(PresentationFailure::new(
						property_origin,
						"x_axis().tick_every(...) cannot be combined with .tick_step(...) or .minor_ticks(...)",
					));
				}
				set_once(
					&mut parts.x_tick_every,
					(interval, property_origin),
					property_origin,
					"x_axis().tick_every(...) may appear at most once in a figure",
				)?;
			}
			if let Some((count, property_origin)) = properties.minor_ticks {
				if parts.x_tick_every.is_some() {
					return Err(PresentationFailure::new(
						property_origin,
						"x_axis().minor_ticks(...) cannot be combined with .tick_every(...)",
					));
				}
				set_once(
					&mut parts.x_minor_ticks,
					(count, property_origin),
					property_origin,
					"x_axis().minor_ticks(...) may appear at most once in a figure",
				)?;
			}
			if let Some((AxisTickSize::X(size), property_origin)) = properties.tick_size {
				set_once(
					&mut parts.x_tick_size,
					(size, property_origin),
					property_origin,
					"x_axis().tick_size(...) may appear at most once in a figure",
				)?;
			}
			if let Some((rotation, property_origin)) = properties.tick_rotation {
				set_once(
					&mut parts.x_tick_rotation,
					(rotation, property_origin),
					property_origin,
					"x_axis().tick_rotation(...) may appear at most once in a figure",
				)?;
			}
		}
		Axis::Y => {
			if let Some((label, property_origin)) = properties.label {
				set_once(
					&mut parts.y_label,
					(label, property_origin),
					property_origin,
					"y_axis().label(...) may appear at most once in a figure",
				)?;
			}
			if let Some((AxisLabelSize::Y(size), property_origin)) = properties.label_size {
				set_once(
					&mut parts.y_label_size,
					(size, property_origin),
					property_origin,
					"y_axis().label_size(...) may appear at most once in a figure",
				)?;
			}
			if let Some((minimum, maximum, property_origin)) = properties.bounds {
				set_once(
					&mut parts.y_bounds,
					(minimum, maximum, property_origin),
					property_origin,
					"y_axis().bounds(...) may appear at most once in a figure",
				)?;
			}
			if let Some((step, property_origin)) = properties.tick_step {
				if parts.y_tick_every.is_some() {
					return Err(PresentationFailure::new(
						property_origin,
						"y_axis().tick_step(...) and .tick_every(...) cannot be combined",
					));
				}
				set_once(
					&mut parts.y_tick_step,
					(step, property_origin),
					property_origin,
					"y_axis().tick_step(...) may appear at most once in a figure",
				)?;
			}
			if let Some((interval, property_origin)) = properties.tick_every {
				if parts.y_tick_step.is_some() || parts.y_minor_ticks.is_some() {
					return Err(PresentationFailure::new(
						property_origin,
						"y_axis().tick_every(...) cannot be combined with .tick_step(...) or .minor_ticks(...)",
					));
				}
				set_once(
					&mut parts.y_tick_every,
					(interval, property_origin),
					property_origin,
					"y_axis().tick_every(...) may appear at most once in a figure",
				)?;
			}
			if let Some((count, property_origin)) = properties.minor_ticks {
				if parts.y_tick_every.is_some() {
					return Err(PresentationFailure::new(
						property_origin,
						"y_axis().minor_ticks(...) cannot be combined with .tick_every(...)",
					));
				}
				set_once(
					&mut parts.y_minor_ticks,
					(count, property_origin),
					property_origin,
					"y_axis().minor_ticks(...) may appear at most once in a figure",
				)?;
			}
			if let Some((AxisTickSize::Y(size), property_origin)) = properties.tick_size {
				set_once(
					&mut parts.y_tick_size,
					(size, property_origin),
					property_origin,
					"y_axis().tick_size(...) may appear at most once in a figure",
				)?;
			}
			debug_assert!(properties.tick_rotation.is_none());
		}
	}
	Ok(())
}

impl FigureParts {
	fn finish(self, body: RunePlotBody) -> Result<RunePlotIntent, PresentationFailure> {
		let mut authored_settings = PlotSettingsLayer::default();
		if let Some((title, _)) = &self.title {
			authored_settings.title = Presence::Show(Arc::new(title.clone()));
		}
		if let Some((subtitle, _)) = &self.subtitle {
			authored_settings.subtitle = Presence::Show(Arc::new(subtitle.clone()));
		}
		if let Some((x_label, _)) = &self.x_label {
			authored_settings.x_label = Presence::Show(Arc::new(x_label.clone()));
		}
		if let Some((y_label, _)) = &self.y_label {
			authored_settings.y_label = Presence::Show(Arc::new(y_label.clone()));
		}
		if let Some((size, _)) = self.title_size {
			authored_settings.title_size = Override::Set(size);
		}
		if let Some((size, _)) = self.subtitle_size {
			authored_settings.subtitle_size = Override::Set(size);
		}
		if let Some((size, _)) = self.x_label_size {
			authored_settings.x_label_size = Override::Set(size);
		}
		if let Some((size, _)) = self.y_label_size {
			authored_settings.y_label_size = Override::Set(size);
		}
		if let Some((size, _)) = self.x_tick_size {
			authored_settings.x_tick_size = Override::Set(size);
		}
		if let Some((size, _)) = self.y_tick_size {
			authored_settings.y_tick_size = Override::Set(size);
		}
		if let Some((size, _)) = self.facet_label_size {
			authored_settings.facet_label_size = Override::Set(size);
		}
		if let Some((legend, _)) = self.legend {
			authored_settings.legend = legend;
		}
		if let Some((size, _)) = self.legend_size {
			authored_settings.legend_size = Override::Set(size);
		}
		if let Some((hover_size, _)) = self.hover_size {
			authored_settings.hover_size = Override::Set(hover_size);
		}
		if let Some((rotation, _)) = self.x_tick_rotation {
			authored_settings.x_tick_rotation = Override::Set(rotation);
		}
		if let Some((padding, _)) = self.padding {
			if let Some(value) = padding.top {
				authored_settings.plot_padding_top = Override::Set(value);
			}
			if let Some(value) = padding.right {
				authored_settings.plot_padding_right = Override::Set(value);
			}
			if let Some(value) = padding.bottom {
				authored_settings.plot_padding_bottom = Override::Set(value);
			}
			if let Some(value) = padding.left {
				authored_settings.plot_padding_left = Override::Set(value);
			}
		}
		if let Some((skin, _)) = &self.skin {
			authored_settings.plot_skin = Override::Set(skin.clone());
		}
		if let Some((palette, _)) = &self.palette {
			authored_settings.categorical_palette = Override::Set(Arc::new(palette.clone()));
		}
		if let Some((ramp, _)) = self.sequential_palette {
			authored_settings.sequential_ramp = Override::Set(ramp);
		}
		if let Some((ramp, _)) = self.diverging_palette {
			authored_settings.diverging_ramp = Override::Set(ramp);
		}
		if self.x_bounds.is_some() || self.y_bounds.is_some() {
			authored_settings.domains = Override::Set(PlotDomainOverrides {
				x_min: self.x_bounds.map(|(minimum, _, _)| minimum),
				x_max: self.x_bounds.map(|(_, maximum, _)| maximum),
				y_min: self.y_bounds.map(|(minimum, _, _)| minimum),
				y_max: self.y_bounds.map(|(_, maximum, _)| maximum),
			});
		}
		if self.x_tick_step.is_some()
			|| self.y_tick_step.is_some()
			|| self.x_tick_every.is_some()
			|| self.y_tick_every.is_some()
			|| self.x_minor_ticks.is_some()
			|| self.y_minor_ticks.is_some()
		{
			let mut guides = PlotGuideSettings {
				x_tick_placement: self
					.x_tick_step
					.map(|(step, _)| TickPlacement::step(step).expect("validated tick step"))
					.or_else(|| {
						self.x_tick_every
							.map(|(interval, _)| TickPlacement::Temporal(interval))
					})
					.unwrap_or(TickPlacement::DEFAULT),
				y_tick_placement: self
					.y_tick_step
					.map(|(step, _)| TickPlacement::step(step).expect("validated tick step"))
					.or_else(|| {
						self.y_tick_every
							.map(|(interval, _)| TickPlacement::Temporal(interval))
					})
					.unwrap_or(TickPlacement::DEFAULT),
				..Default::default()
			};
			if let Some((count, _)) = self.x_minor_ticks {
				guides.x_minor_ticks = count;
			}
			if let Some((count, _)) = self.y_minor_ticks {
				guides.y_minor_ticks = count;
			}
			authored_settings.guides = Override::Set(guides);
		}
		Ok(RunePlotIntent {
			body,
			continuous_intent: self.continuous_intent,
			continuous_intent_span: self.continuous_intent_origin.map(Origin::range),
			facet: self.facet.as_ref().map(|(facet, _)| facet.clone()),
			facet_gutter: self.facet_gutter.map(|(value, _)| value),
			authored_settings,
			plot_skin_span: self.skin.map(|(_, origin)| origin.range()),
			facet_span: self.facet.map(|(_, origin)| origin.range()),
			domain_spans: RunePlotDomainSpans {
				x: self.x_bounds.map(|(_, _, origin)| origin.range()),
				y: self.y_bounds.map(|(_, _, origin)| origin.range()),
			},
			tick_step_spans: RunePlotTickStepSpans {
				x: self.x_tick_step.map(|(_, origin)| origin.range()),
				y: self.y_tick_step.map(|(_, origin)| origin.range()),
			},
			tick_every_spans: RunePlotTickEverySpans {
				x: self.x_tick_every.map(|(_, origin)| origin.range()),
				y: self.y_tick_every.map(|(_, origin)| origin.range()),
			},
			minor_tick_spans: RunePlotMinorTickSpans {
				x: self.x_minor_ticks.map(|(_, origin)| origin.range()),
				y: self.y_minor_ticks.map(|(_, origin)| origin.range()),
			},
		})
	}
}

fn finalize_mark_body(mark: &RuneMark) -> Result<RunePlotBody, PresentationFailure> {
	let sink = mark.sink.clone()?;
	if sink.percent && sink.sink.composition.primary_plot_type() == PlotType::Bar && !sink.stacked {
		return Err(PresentationFailure::new(
			mark.percent_origin.unwrap_or(mark.origin),
			"percent() requires stacked() on bar marks",
		));
	}
	if mark.modified {
		Ok(RunePlotBody::StyledSingle(sink))
	} else {
		Ok(RunePlotBody::Single(sink.sink))
	}
}

fn finalize_table(table: &RuneTable) -> Result<RuneTableSink, PresentationFailure> {
	if let Some(failure) = &table.failure {
		return Err(failure.clone());
	}
	if table.duotone && table.skin_name.is_none() {
		return Err(PresentationFailure::new(
			table.duotone_origin.unwrap_or(table.origin),
			"duotone() requires skin(string)",
		));
	}
	let skin = match &table.skin_name {
		Some(name) => RuneTableSkinIntent::NamedTheme {
			name: name.clone(),
			projection: if table.duotone {
				RuneTableSkinProjection::Duotone
			} else {
				RuneTableSkinProjection::PaletteForward
			},
		},
		None => RuneTableSkinIntent::Default,
	};
	Ok(RuneTableSink {
		skin,
		show_column_types: if table.show_types {
			Override::Set(true)
		} else {
			Override::Inherit
		},
	})
}

fn decode_slot(
	value: &Value,
	slot_span: Range<u32>,
) -> Result<RuneViewIntent, PresentationFailure> {
	if let Ok(presentation) = value.borrow_ref::<RunePresentation>() {
		return presentation.value.clone();
	}
	if let Ok(mark) = value.borrow_ref::<RuneMark>() {
		let mut plot = RunePlotIntent::bare(finalize_mark_body(&mark)?);
		plot.continuous_intent = mark.continuous_intent;
		plot.continuous_intent_span = mark.continuous_intent_origin.map(Origin::range);
		return Ok(RuneViewIntent::Plot(plot));
	}
	if let Ok(layers) = value.borrow_ref::<RuneLayers>() {
		return Ok(RuneViewIntent::Plot(RunePlotIntent::bare(
			RunePlotBody::Layered(layers.value.clone()?),
		)));
	}
	if let Ok(table) = value.borrow_ref::<RuneTable>() {
		return Ok(RuneViewIntent::Table(finalize_table(&table)?));
	}
	Err(PresentationFailure {
		span: slot_span,
		message: "a view slot must evaluate to table, bar, line, scatter, area, layers, or figure"
			.to_owned(),
	})
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn the_method_guard_recognises_every_presentation_value_family() {
		// One constructor from each family: element, skin, mark, layers,
		// table and figure. Their values are what the guard must accept.
		let source = "pub fn main() { [\
			polariton_presentation::__x_axis(0, 1), \
			polariton_presentation::__skin(\"greys_rs2\", 0, 1), \
			polariton_presentation::__line(\"x\", \"y\", 0, 1), \
			polariton_presentation::__layers([polariton_presentation::__line(\"x\", \"y\", 0, 1)], 0, 1), \
			polariton_presentation::__table(0, 1), \
			polariton_presentation::__figure([], 0, 1), \
			7, \"text\", \
		] }";
		let context = build_context().unwrap();
		let mut sources = Sources::new();
		sources.insert(Source::new("guard", source).unwrap()).unwrap();
		let unit = rune::prepare(&mut sources).with_context(&context).build().unwrap();
		let mut vm = Vm::new(Arc::new(context.runtime().unwrap()), Arc::new(unit));
		let values = vm.call(["main"], ()).unwrap();
		let values = values.borrow_ref::<rune::runtime::Vec>().unwrap();
		let recognised: Vec<bool> = values.iter().map(is_presentation_value).collect();
		assert_eq!(recognised, [true, true, true, true, true, true, false, false]);
		// Every family seen above is a distinct registered type.
		let families: HashSet<rune::Hash> =
			values.iter().take(6).map(|value| value.type_hash()).collect();
		assert_eq!(families.len(), presentation_type_hashes().len());
	}

	fn execute(
		source: &str,
		slots: &[Range<u32>],
	) -> Result<Vec<RuneViewIntent>, PresentationFailure> {
		let fallback = slots.first().cloned().unwrap_or(0..0);
		execute_generated(
			source,
			slots,
			&PresentationSourceMap::identity(source, fallback),
			PresentationLimits::default(),
		)
	}

	#[test]
	fn product_0076_typed_values_execute_helpers_arrays_and_real_modifiers() {
		let source = r#"
fn bounds() { polariton_presentation::__y_axis(13, 28).__bounds(0, 160, 13, 28) }
fn elements() {
    [
        polariton_presentation::__title("Computed", 31, 48),
        bounds(),
        polariton_presentation::__bar("region", "revenue", "product", 52, 96)
            .__stacked(97, 107)
            .__percent(108, 118),
    ]
}
pub fn main() { [polariton_presentation::__figure(elements(), 0, 120)] }
"#;
		let views =
			execute(source, std::slice::from_ref(&(0..120))).expect("typed presentation executes");
		let RuneViewIntent::Plot(plot) = &views[0] else {
			panic!("expected plot");
		};
		assert_eq!(
			plot.authored_settings.title,
			Presence::Show(Arc::new("Computed".to_owned()))
		);
		assert_eq!(
			plot.authored_settings.domains,
			Override::Set(PlotDomainOverrides {
				x_min: None,
				x_max: None,
				y_min: Some(0.0),
				y_max: Some(160.0),
			})
		);
		let RunePlotBody::StyledSingle(mark) = &plot.body else {
			panic!("expected a real modified mark");
		};
		assert!(mark.stacked);
		assert!(mark.percent);
	}

	#[test]
	fn product_0076_duplicate_and_applicability_fail_at_injected_origins() {
		let duplicate = r#"pub fn main() { [polariton_presentation::__figure([
polariton_presentation::__title("first", 10, 20),
polariton_presentation::__title("second", 30, 41),
polariton_presentation::__line("x", "y", 50, 62)
], 0, 80)] }"#;
		assert_eq!(
			execute(duplicate, std::slice::from_ref(&(0..80))),
			Err(PresentationFailure {
				span: 30..41,
				message: "a figure has at most one title".to_owned(),
			})
		);

		let wrong_register = r#"pub fn main() { [polariton_presentation::__line("x", "y", 0, 14).__stacked(15, 25)] }"#;
		assert_eq!(
			execute(wrong_register, std::slice::from_ref(&(0..25))),
			Err(PresentationFailure {
				span: 15..25,
				message: "stacked() is available on bar marks, not line, scatter, or area marks"
					.to_owned(),
			})
		);
	}

	#[test]
	fn product_0121_facet_gutter_is_a_facet_member_with_local_refusals() {
		let valid = r#"pub fn main() { [polariton_presentation::__figure([
polariton_presentation::__facet("region", 10, 25).__gutter(24, 26, 41),
polariton_presentation::__bar("quarter", "sales", 42, 66)
], 0, 67)] }"#;
		let views = execute(valid, std::slice::from_ref(&(0..67))).expect("faceted gutter figure");
		let RuneViewIntent::Plot(plot) = &views[0] else {
			panic!("expected plot");
		};
		assert_eq!(plot.facet.as_deref(), Some("region"));
		assert_eq!(plot.facet_gutter, Some(24.0));
		assert_eq!(plot.authored_settings, PlotSettingsLayer::default());

		for wrong_receiver in [
			r#"pub fn main() { [polariton_presentation::__figure([
polariton_presentation::__title("title", 10, 25).__gutter(4, 26, 41),
polariton_presentation::__bar("quarter", "sales", 42, 66)
], 0, 67)] }"#,
			r#"pub fn main() { [polariton_presentation::__figure([
polariton_presentation::__skin("greys_rs2", 10, 25).__gutter(4, 26, 41),
polariton_presentation::__bar("quarter", "sales", 42, 66)
], 0, 67)] }"#,
		] {
			assert_eq!(
				execute(wrong_receiver, std::slice::from_ref(&(0..67))),
				Err(PresentationFailure {
					span: 26..41,
					message: "gutter(...) requires a facet(...) receiver".to_owned(),
				})
			);
		}

		let duplicate = r#"pub fn main() { [polariton_presentation::__figure([
polariton_presentation::__facet("region", 10, 25).__gutter(12, 26, 41).__gutter(24, 42, 57),
polariton_presentation::__bar("quarter", "sales", 58, 82)
], 0, 83)] }"#;
		assert_eq!(
			execute(duplicate, std::slice::from_ref(&(0..83))),
			Err(PresentationFailure {
				span: 42..57,
				message: "facet(...).gutter(...) may appear at most once".to_owned(),
			})
		);
	}

	#[test]
	fn product_0124_facet_label_size_is_typed_and_owned_by_the_facet_member() {
		let valid = r#"pub fn main() { [polariton_presentation::__figure([
polariton_presentation::__facet("region", 10, 25).__gutter(24, 26, 41).__label_size(18, 42, 57),
polariton_presentation::__bar("quarter", "sales", 58, 82)
], 0, 83)] }"#;
		let views =
			execute(valid, std::slice::from_ref(&(0..83))).expect("faceted label-size figure");
		let RuneViewIntent::Plot(plot) = &views[0] else {
			panic!("expected plot");
		};
		assert_eq!(plot.facet.as_deref(), Some("region"));
		assert_eq!(plot.facet_gutter, Some(24.0));
		assert_eq!(
			plot.authored_settings.facet_label_size,
			Override::Set(FacetLabelTextSize::new(18.0).unwrap())
		);

		for source in [
			r#"pub fn main() { [polariton_presentation::__figure([
polariton_presentation::__title("title", 10, 25).__label_size(16, 26, 41),
polariton_presentation::__bar("quarter", "sales", 42, 66)
], 0, 67)] }"#,
			r#"pub fn main() { [polariton_presentation::__figure([
polariton_presentation::__skin("greys_rs2", 10, 25).__label_size(16, 26, 41),
polariton_presentation::__bar("quarter", "sales", 42, 66)
], 0, 67)] }"#,
		] {
			assert_eq!(
				execute(source, std::slice::from_ref(&(0..67))),
				Err(PresentationFailure {
					span: 26..41,
					message:
						"label_size(...) requires an x_axis(), y_axis(), or facet(...) receiver"
							.to_owned(),
				})
			);
		}

		let duplicate = r#"pub fn main() { [polariton_presentation::__figure([
polariton_presentation::__facet("region", 10, 25).__label_size(12, 26, 41).__label_size(18, 42, 57),
polariton_presentation::__bar("quarter", "sales", 58, 82)
], 0, 83)] }"#;
		assert_eq!(
			execute(duplicate, std::slice::from_ref(&(0..83))),
			Err(PresentationFailure {
				span: 42..57,
				message: "facet(...).label_size(...) may appear at most once".to_owned(),
			})
		);
	}

	#[test]
	fn product_0076_instruction_budget_stops_a_presentation_loop() {
		let source = "pub fn main() { loop {} }";
		let error = execute_generated(
			source,
			std::slice::from_ref(&(0..25)),
			&PresentationSourceMap::identity(source, 0..25),
			PresentationLimits {
				instructions: 100,
				..PresentationLimits::default()
			},
		)
		.expect_err("instruction budget must stop the loop");
		assert_eq!(error.span, 0..25);
		assert!(error.message.contains("budget"), "{}", error.message);
		assert_eq!(PRESENTATION_COMPILE_ALLOCATION_CAP, 256 * 1024 * 1024);
		assert_eq!(PRESENTATION_RUNTIME_ALLOCATION_CAP, 256 * 1024 * 1024);
		assert_eq!(PRESENTATION_INSTRUCTION_BUDGET, 50_000_000);
	}

	#[test]
	fn product_0076_allocation_budget_and_wrong_slot_are_bounded_and_attributed() {
		let source = "pub fn main() { [42] }";
		let slot = 18..22;
		assert_eq!(
			execute_generated(
				source,
				std::slice::from_ref(&slot),
				&PresentationSourceMap::identity(source, slot.clone()),
				PresentationLimits::default(),
			),
			Err(PresentationFailure {
				span: slot,
				message: "a view slot must evaluate to table, bar, line, scatter, area, layers, or figure"
					.to_owned(),
			})
		);

		let allocation_error = execute_generated(
			"pub fn main() { [] }",
			&[],
			&PresentationSourceMap::identity("pub fn main() { [] }", 0..20),
			PresentationLimits {
				compile_allocation: 1024,
				..PresentationLimits::default()
			},
		)
		.expect_err("the parameterized compile allocation cap must stop construction");
		assert!(
			allocation_error
				.message
				.to_ascii_lowercase()
				.contains("memory")
				|| allocation_error
					.message
					.to_ascii_lowercase()
					.contains("alloc"),
			"{}",
			allocation_error.message
		);
	}

	#[test]
	fn product_0076_source_map_distinguishes_copied_and_synthetic_text() {
		let map = PresentationSourceMap {
			segments: vec![
				PresentationMapSegment {
					generated: 0..5,
					kind: PresentationMapKind::Copied { original_start: 40 },
				},
				PresentationMapSegment {
					generated: 5..12,
					kind: PresentationMapKind::Synthetic { owner: 80..96 },
				},
			],
			fallback: 100..120,
		};
		assert_eq!(map.map_span(1..4), 41..44);
		assert_eq!(map.map_span(7..9), 80..96);
		assert_eq!(map.map_span(3..8), 43..96);
		assert_eq!(map.map_span(30..31), 100..120);
	}
}
