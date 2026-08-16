//! PRODUCT-0076's bounded, typed Rune presentation evaluator.
//!
//! Relation planning remains in Toron. This module owns only values which can
//! be produced before a relation schema exists, and every value carries an
//! origin in the author's original buffer.

use crate::plot::chrome::PlotChromeProjection;
use crate::plot::colors::ColorTheme;
use crate::plot::common::TickPlacement;
use crate::plot::composition::PlotComposition;
use crate::plot::continuous_color::{ContinuousAnchor, ContinuousScaleIntent};
use crate::plot::core::PlotType;
use crate::plot::spec::{
	BarWidth, BoxPlotFields, BubbleFieldNames, BubbleFields, CategoryGap, FunnelFields,
	HeatmapFields, LayerSettings, LayeredPlotSpec, LegendPosition, MultivariateFields, Override,
	PieFields, PointSize, Presence, RadialDialFields, StrokeWidth, ViolinFields,
};
use crate::plot::theme::{
	PlotDomainOverrides, PlotGuideSettings, PlotSettingsLayer, PlotSkinSelection,
};
use crate::rune_relation_envelope::{
	BoxPlotFieldOrigins, BubbleFieldOrigins, DisposableRunePlotSink, FunnelFieldOrigins,
	HeatmapFieldOrigins, MultivariateFieldOrigins, PieFieldOrigins, RadialDialFieldOrigins,
	RunePlotBody, RunePlotDomainSpans, RunePlotIntent, RunePlotMinorTickSpans,
	RunePlotTickStepSpans, RuneTableSink, RuneTableSkinIntent, RuneTableSkinProjection,
	RuneViewIntent, SpannedMarkFields, StyledRunePlotSink, ViolinFieldOrigins,
};
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
	XLabel(String),
	YLabel(String),
	Facet(String),
	FacetGutter(f32),
	Palette(String),
	SequentialPalette(ColorTheme),
	DivergingPalette(ColorTheme),
	Legend(Presence<LegendPosition>),
	XBounds(f64, f64),
	YBounds(f64, f64),
	XTickStep(f64),
	YTickStep(f64),
	XMinorTicks(u32),
	YMinorTicks(u32),
}

#[derive(Any, Clone, Debug)]
#[rune(item = ::polariton_presentation, name = FigureElement)]
struct RuneFigureElement {
	value: Result<FigureElementKind, PresentationFailure>,
	origin: Origin,
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
	module.ty::<RuneFigureElement>().map_err(module_error)?;
	module.ty::<RuneSkin>().map_err(module_error)?;
	module.ty::<RuneMark>().map_err(module_error)?;
	module.ty::<RuneLayers>().map_err(module_error)?;
	module.ty::<RuneTable>().map_err(module_error)?;
	module.ty::<RunePresentation>().map_err(module_error)?;

	for (name, function) in [
		("__bar", raw_bar as RawFunction),
		("__line", raw_line as RawFunction),
		("__scatter", raw_scatter as RawFunction),
		("__area", raw_area as RawFunction),
		("__histogram", raw_histogram as RawFunction),
		("__box", raw_box as RawFunction),
		("__bubble", raw_bubble as RawFunction),
		("__heatmap", raw_heatmap as RawFunction),
		("__funnel", raw_funnel as RawFunction),
		("__radial_dial", raw_radial_dial as RawFunction),
		("__pie", raw_pie as RawFunction),
		("__violin", raw_violin as RawFunction),
		("__radar", raw_radar as RawFunction),
		("__parallel", raw_parallel as RawFunction),
		("__title", raw_title as RawFunction),
		("__subtitle", raw_subtitle as RawFunction),
		("__x_label", raw_x_label as RawFunction),
		("__y_label", raw_y_label as RawFunction),
		("__facet", raw_facet as RawFunction),
		("__facet_gutter", raw_facet_gutter as RawFunction),
		("__palette", raw_palette as RawFunction),
		(
			"__sequential_palette",
			raw_sequential_palette as RawFunction,
		),
		("__diverging_palette", raw_diverging_palette as RawFunction),
		("__skin", raw_skin as RawFunction),
		("__legend_at", raw_legend_at as RawFunction),
		("__hide_legend", raw_hide_legend as RawFunction),
		("__x_bounds", raw_x_bounds as RawFunction),
		("__y_bounds", raw_y_bounds as RawFunction),
		("__x_tick_step", raw_x_tick_step as RawFunction),
		("__y_tick_step", raw_y_tick_step as RawFunction),
		("__x_minor_ticks", raw_x_minor_ticks as RawFunction),
		("__y_minor_ticks", raw_y_minor_ticks as RawFunction),
		("__table", raw_table as RawFunction),
		("__layers", raw_layers as RawFunction),
		("__figure", raw_figure as RawFunction),
	] {
		module
			.raw_function(name, function)
			.build()
			.map_err(module_error)?;
	}
	for (name, function) in [
		("__point_size", raw_mark_point_size as RawFunction),
		("__stroke_width", raw_mark_stroke_width as RawFunction),
		("__bar_width", raw_mark_bar_width as RawFunction),
		("__category_gap", raw_mark_category_gap as RawFunction),
		("__category", raw_mark_category as RawFunction),
		("__stacked", raw_mark_stacked as RawFunction),
		("__percent", raw_mark_percent as RawFunction),
		("__diverging", raw_mark_diverging as RawFunction),
		("__diverging_at", raw_mark_diverging_at as RawFunction),
	] {
		module
			.raw_function(name, function)
			.build_associated::<RuneMark>()
			.map_err(module_error)?;
	}
	module
		.raw_function("__duotone", raw_skin_duotone as RawFunction)
		.build_associated::<RuneSkin>()
		.map_err(module_error)?;
	for (name, function) in [
		("__skin", raw_table_skin as RawFunction),
		("__duotone", raw_table_duotone as RawFunction),
		("__show_types", raw_table_show_types as RawFunction),
	] {
		module
			.raw_function(name, function)
			.build_associated::<RuneTable>()
			.map_err(module_error)?;
	}

	context.install(module).map_err(module_error)?;
	Ok(context)
}

fn module_error(error: impl std::fmt::Display) -> String {
	format!("Rune module error: {error}")
}

type RawFunction = fn(&mut dyn Memory, InstAddress, usize, Output) -> VmResult<()>;

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
	XLabel,
	YLabel,
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

fn raw_x_label(
	stack: &mut dyn Memory,
	addr: InstAddress,
	args: usize,
	out: Output,
) -> VmResult<()> {
	raw_string_element(StringConstructor::XLabel, "x_label", stack, addr, args, out)
}

fn raw_y_label(
	stack: &mut dyn Memory,
	addr: InstAddress,
	args: usize,
	out: Output,
) -> VmResult<()> {
	raw_string_element(StringConstructor::YLabel, "y_label", stack, addr, args, out)
}

fn raw_facet(stack: &mut dyn Memory, addr: InstAddress, args: usize, out: Output) -> VmResult<()> {
	raw_string_element(StringConstructor::Facet, "facet", stack, addr, args, out)
}

fn raw_facet_gutter(
	stack: &mut dyn Memory,
	addr: InstAddress,
	args: usize,
	out: Output,
) -> VmResult<()> {
	let values = rune::vm_try!(stack.slice_at(addr, args));
	let origin = raw_origin(values).unwrap_or(Origin { start: 0, end: 0 });
	let value = if values.len() != 3 {
		Err(PresentationFailure::new(
			origin,
			"facet_gutter(...) requires exactly one finite nonnegative number",
		))
	} else {
		match rune_number(&values[0]) {
			Some(value) if value.is_finite() && value >= 0.0 => {
				Ok(FigureElementKind::FacetGutter(value as f32))
			}
			_ => Err(PresentationFailure::new(
				origin,
				"facet_gutter(...) requires exactly one finite nonnegative number",
			)),
		}
	};
	store(out, stack, RuneFigureElement { value, origin })
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
	store(out, stack, RuneFigureElement { value, origin })
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
	let value = if values.len() == 3 {
		match rune::from_value::<Ref<str>>(values[0].clone()) {
			Ok(value) => {
				let value = value.to_owned();
				if matches!(kind, StringConstructor::XLabel | StringConstructor::YLabel)
					&& value.trim().is_empty()
				{
					Err(PresentationFailure::new(
						origin,
						format!("{name}(...) requires a nonempty string"),
					))
				} else {
					Ok(match kind {
						StringConstructor::Title => FigureElementKind::Title(value),
						StringConstructor::Subtitle => FigureElementKind::Subtitle(value),
						StringConstructor::XLabel => FigureElementKind::XLabel(value),
						StringConstructor::YLabel => FigureElementKind::YLabel(value),
						StringConstructor::Facet => FigureElementKind::Facet(value),
						StringConstructor::Palette => FigureElementKind::Palette(value),
					})
				}
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
	store(out, stack, RuneFigureElement { value, origin })
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

#[derive(Clone, Copy)]
enum NumericPairConstructor {
	Legend,
	XBounds,
	YBounds,
}

fn raw_legend_at(
	stack: &mut dyn Memory,
	addr: InstAddress,
	args: usize,
	out: Output,
) -> VmResult<()> {
	raw_numeric_pair(
		NumericPairConstructor::Legend,
		"legend_at",
		stack,
		addr,
		args,
		out,
	)
}

fn raw_x_bounds(
	stack: &mut dyn Memory,
	addr: InstAddress,
	args: usize,
	out: Output,
) -> VmResult<()> {
	raw_numeric_pair(
		NumericPairConstructor::XBounds,
		"x_bounds",
		stack,
		addr,
		args,
		out,
	)
}

fn raw_y_bounds(
	stack: &mut dyn Memory,
	addr: InstAddress,
	args: usize,
	out: Output,
) -> VmResult<()> {
	raw_numeric_pair(
		NumericPairConstructor::YBounds,
		"y_bounds",
		stack,
		addr,
		args,
		out,
	)
}

fn raw_numeric_pair(
	kind: NumericPairConstructor,
	name: &'static str,
	stack: &mut dyn Memory,
	addr: InstAddress,
	args: usize,
	out: Output,
) -> VmResult<()> {
	let values = rune::vm_try!(stack.slice_at(addr, args));
	let origin = raw_origin(values).unwrap_or(Origin { start: 0, end: 0 });
	let element = if values.len() != 4 {
		RuneFigureElement {
			value: Err(PresentationFailure::new(
				origin,
				format!("{name}(...) requires exactly two numeric values"),
			)),
			origin,
		}
	} else {
		match kind {
			NumericPairConstructor::Legend => presentation_legend_at(
				values[0].clone(),
				values[1].clone(),
				origin.start.into(),
				origin.end.into(),
			),
			NumericPairConstructor::XBounds => presentation_x_bounds(
				values[0].clone(),
				values[1].clone(),
				origin.start.into(),
				origin.end.into(),
			),
			NumericPairConstructor::YBounds => presentation_y_bounds(
				values[0].clone(),
				values[1].clone(),
				origin.start.into(),
				origin.end.into(),
			),
		}
	};
	store(out, stack, element)
}

fn raw_hide_legend(
	stack: &mut dyn Memory,
	addr: InstAddress,
	args: usize,
	out: Output,
) -> VmResult<()> {
	let values = rune::vm_try!(stack.slice_at(addr, args));
	let origin = raw_origin(values).unwrap_or(Origin { start: 0, end: 0 });
	let element = if values.len() == 2 {
		presentation_hide_legend(origin.start.into(), origin.end.into())
	} else {
		RuneFigureElement {
			value: Err(PresentationFailure::new(
				origin,
				"hide_legend() takes no arguments",
			)),
			origin,
		}
	};
	store(out, stack, element)
}

#[derive(Clone, Copy)]
enum NumericScalarConstructor {
	XTickStep,
	YTickStep,
	XMinorTicks,
	YMinorTicks,
}

fn raw_x_tick_step(
	stack: &mut dyn Memory,
	addr: InstAddress,
	args: usize,
	out: Output,
) -> VmResult<()> {
	raw_numeric_scalar(
		NumericScalarConstructor::XTickStep,
		"x_tick_step",
		stack,
		addr,
		args,
		out,
	)
}

fn raw_y_tick_step(
	stack: &mut dyn Memory,
	addr: InstAddress,
	args: usize,
	out: Output,
) -> VmResult<()> {
	raw_numeric_scalar(
		NumericScalarConstructor::YTickStep,
		"y_tick_step",
		stack,
		addr,
		args,
		out,
	)
}

fn raw_x_minor_ticks(
	stack: &mut dyn Memory,
	addr: InstAddress,
	args: usize,
	out: Output,
) -> VmResult<()> {
	raw_numeric_scalar(
		NumericScalarConstructor::XMinorTicks,
		"x_minor_ticks",
		stack,
		addr,
		args,
		out,
	)
}

fn raw_y_minor_ticks(
	stack: &mut dyn Memory,
	addr: InstAddress,
	args: usize,
	out: Output,
) -> VmResult<()> {
	raw_numeric_scalar(
		NumericScalarConstructor::YMinorTicks,
		"y_minor_ticks",
		stack,
		addr,
		args,
		out,
	)
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
		RuneFigureElement {
			value: Err(PresentationFailure::new(
				origin,
				format!("{name}(...) requires exactly one numeric value"),
			)),
			origin,
		}
	} else {
		match kind {
			NumericScalarConstructor::XTickStep => {
				presentation_x_tick_step(values[0].clone(), origin.start.into(), origin.end.into())
			}
			NumericScalarConstructor::YTickStep => {
				presentation_y_tick_step(values[0].clone(), origin.start.into(), origin.end.into())
			}
			NumericScalarConstructor::XMinorTicks => presentation_x_minor_ticks(
				values[0].clone(),
				origin.start.into(),
				origin.end.into(),
			),
			NumericScalarConstructor::YMinorTicks => presentation_y_minor_ticks(
				values[0].clone(),
				origin.start.into(),
				origin.end.into(),
			),
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

fn presentation_legend_at(x: Value, y: Value, start: i64, end: i64) -> RuneFigureElement {
	numeric_pair_element(x, y, start, end, "legend_at", |x, y| {
		LegendPosition::new(x, y)
			.map(|position| FigureElementKind::Legend(Presence::Show(position)))
			.ok_or_else(|| "legend_at(...) requires finite positions".to_owned())
	})
}

fn presentation_hide_legend(start: i64, end: i64) -> RuneFigureElement {
	let origin = Origin::decode(start, end).unwrap_or(Origin { start: 0, end: 0 });
	RuneFigureElement {
		value: Ok(FigureElementKind::Legend(Presence::Hide)),
		origin,
	}
}

fn presentation_x_bounds(
	minimum: Value,
	maximum: Value,
	start: i64,
	end: i64,
) -> RuneFigureElement {
	bounds_element(minimum, maximum, start, end, "x_bounds", true)
}

fn presentation_y_bounds(
	minimum: Value,
	maximum: Value,
	start: i64,
	end: i64,
) -> RuneFigureElement {
	bounds_element(minimum, maximum, start, end, "y_bounds", false)
}

fn bounds_element(
	minimum: Value,
	maximum: Value,
	start: i64,
	end: i64,
	name: &'static str,
	x_axis: bool,
) -> RuneFigureElement {
	numeric_pair_element(
		minimum,
		maximum,
		start,
		end,
		name,
		move |minimum, maximum| {
			if !minimum.is_finite() || !maximum.is_finite() {
				return Err(format!("{name}(...) requires finite bounds"));
			}
			if minimum == maximum {
				return Err(format!(
					"{name}(...) cannot form a collapsed axis; min must be less than max"
				));
			}
			if minimum > maximum {
				return Err(format!(
					"{name}(...) cannot reverse an axis; min must be less than max"
				));
			}
			Ok(if x_axis {
				FigureElementKind::XBounds(minimum, maximum)
			} else {
				FigureElementKind::YBounds(minimum, maximum)
			})
		},
	)
}

fn numeric_pair_element(
	first: Value,
	second: Value,
	start: i64,
	end: i64,
	name: &'static str,
	make: impl FnOnce(f64, f64) -> Result<FigureElementKind, String>,
) -> RuneFigureElement {
	let origin = Origin::decode(start, end).unwrap_or(Origin { start: 0, end: 0 });
	let value = (|| {
		let first = rune_number(&first).ok_or_else(|| {
			PresentationFailure::new(origin, format!("{name}(...) requires numeric values"))
		})?;
		let second = rune_number(&second).ok_or_else(|| {
			PresentationFailure::new(origin, format!("{name}(...) requires numeric values"))
		})?;
		make(first, second).map_err(|message| PresentationFailure::new(origin, message))
	})();
	RuneFigureElement { value, origin }
}

fn presentation_x_tick_step(value: Value, start: i64, end: i64) -> RuneFigureElement {
	numeric_scalar_element(value, start, end, "x_tick_step", true)
}

fn presentation_y_tick_step(value: Value, start: i64, end: i64) -> RuneFigureElement {
	numeric_scalar_element(value, start, end, "y_tick_step", false)
}

fn numeric_scalar_element(
	value: Value,
	start: i64,
	end: i64,
	name: &'static str,
	x_axis: bool,
) -> RuneFigureElement {
	let origin = Origin::decode(start, end).unwrap_or(Origin { start: 0, end: 0 });
	let value = rune_number(&value)
		.ok_or_else(|| PresentationFailure::new(origin, format!("{name}(...) requires a number")))
		.and_then(|value| {
			if !value.is_finite() {
				Err(PresentationFailure::new(
					origin,
					format!("{name}(...) requires a finite step"),
				))
			} else if value <= 0.0 {
				Err(PresentationFailure::new(
					origin,
					format!("{name}(...) step must be strictly positive"),
				))
			} else {
				Ok(if x_axis {
					FigureElementKind::XTickStep(value)
				} else {
					FigureElementKind::YTickStep(value)
				})
			}
		});
	RuneFigureElement { value, origin }
}

fn presentation_x_minor_ticks(value: Value, start: i64, end: i64) -> RuneFigureElement {
	minor_ticks_element(value, start, end, "x_minor_ticks", true)
}

fn presentation_y_minor_ticks(value: Value, start: i64, end: i64) -> RuneFigureElement {
	minor_ticks_element(value, start, end, "y_minor_ticks", false)
}

fn minor_ticks_element(
	value: Value,
	start: i64,
	end: i64,
	name: &'static str,
	x_axis: bool,
) -> RuneFigureElement {
	let origin = Origin::decode(start, end).unwrap_or(Origin { start: 0, end: 0 });
	let value = rune_number(&value)
		.ok_or_else(|| PresentationFailure::new(origin, format!("{name}(...) requires a number")))
		.and_then(|value| {
			if !value.is_finite() {
				Err(PresentationFailure::new(
					origin,
					format!("{name}(...) requires a finite count"),
				))
			} else if value < 0.0 || value.fract() != 0.0 || value > u32::MAX as f64 {
				Err(PresentationFailure::new(
					origin,
					format!("{name}(...) count must be a non-negative integer"),
				))
			} else {
				Ok(if x_axis {
					FigureElementKind::XMinorTicks(value as u32)
				} else {
					FigureElementKind::YMinorTicks(value as u32)
				})
			}
		});
	RuneFigureElement { value, origin }
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
	facet: Option<(String, Origin)>,
	facet_gutter: Option<(f32, Origin)>,
	palette: Option<(String, Origin)>,
	sequential_palette: Option<(ColorTheme, Origin)>,
	diverging_palette: Option<(ColorTheme, Origin)>,
	continuous_intent: Option<ContinuousScaleIntent>,
	continuous_intent_origin: Option<Origin>,
	legend: Option<(Presence<LegendPosition>, Origin)>,
	skin: Option<(PlotSkinSelection, Origin)>,
	x_bounds: Option<(f64, f64, Origin)>,
	y_bounds: Option<(f64, f64, Origin)>,
	x_tick_step: Option<(f64, Origin)>,
	y_tick_step: Option<(f64, Origin)>,
	x_minor_ticks: Option<(u32, Origin)>,
	y_minor_ticks: Option<(u32, Origin)>,
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
			merge_element(&mut parts, kind, element.origin)?;
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
	origin: Origin,
) -> Result<(), PresentationFailure> {
	match element {
		FigureElementKind::Title(value) => set_once(
			&mut parts.title,
			(value, origin),
			origin,
			"a figure has at most one title",
		),
		FigureElementKind::Subtitle(value) => set_once(
			&mut parts.subtitle,
			(value, origin),
			origin,
			"a figure has at most one subtitle",
		),
		FigureElementKind::XLabel(value) => set_once(
			&mut parts.x_label,
			(value, origin),
			origin,
			"a figure has at most one x_label element",
		),
		FigureElementKind::YLabel(value) => set_once(
			&mut parts.y_label,
			(value, origin),
			origin,
			"a figure has at most one y_label element",
		),
		FigureElementKind::Facet(value) => set_once(
			&mut parts.facet,
			(value, origin),
			origin,
			"a figure has at most one facet element",
		),
		FigureElementKind::FacetGutter(value) => set_once(
			&mut parts.facet_gutter,
			(value, origin),
			origin,
			"a figure has at most one facet_gutter element",
		),
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
		FigureElementKind::Legend(value) => set_once(
			&mut parts.legend,
			(value, origin),
			origin,
			"a figure has at most one legend placement or visibility element",
		),
		FigureElementKind::XBounds(minimum, maximum) => set_once(
			&mut parts.x_bounds,
			(minimum, maximum, origin),
			origin,
			"a figure has at most one x_bounds element",
		),
		FigureElementKind::YBounds(minimum, maximum) => set_once(
			&mut parts.y_bounds,
			(minimum, maximum, origin),
			origin,
			"a figure has at most one y_bounds element",
		),
		FigureElementKind::XTickStep(value) => set_once(
			&mut parts.x_tick_step,
			(value, origin),
			origin,
			"a figure has at most one x_tick_step element",
		),
		FigureElementKind::YTickStep(value) => set_once(
			&mut parts.y_tick_step,
			(value, origin),
			origin,
			"a figure has at most one y_tick_step element",
		),
		FigureElementKind::XMinorTicks(value) => set_once(
			&mut parts.x_minor_ticks,
			(value, origin),
			origin,
			"a figure has at most one x_minor_ticks element",
		),
		FigureElementKind::YMinorTicks(value) => set_once(
			&mut parts.y_minor_ticks,
			(value, origin),
			origin,
			"a figure has at most one y_minor_ticks element",
		),
	}
}

impl FigureParts {
	fn finish(self, body: RunePlotBody) -> Result<RunePlotIntent, PresentationFailure> {
		if let Some((_, origin)) = self.facet_gutter {
			if self.facet.is_none() {
				return Err(PresentationFailure::new(
					origin,
					"facet_gutter(...) requires facet(...)",
				));
			}
		}
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
		if let Some((legend, _)) = self.legend {
			authored_settings.legend = legend;
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
			|| self.x_minor_ticks.is_some()
			|| self.y_minor_ticks.is_some()
		{
			let mut guides = PlotGuideSettings::default();
			guides.x_tick_placement = self
				.x_tick_step
				.map(|(step, _)| TickPlacement::step(step).expect("validated tick step"))
				.unwrap_or(TickPlacement::DEFAULT);
			guides.y_tick_placement = self
				.y_tick_step
				.map(|(step, _)| TickPlacement::step(step).expect("validated tick step"))
				.unwrap_or(TickPlacement::DEFAULT);
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
fn bounds() { polariton_presentation::__y_bounds(0, 160, 13, 28) }
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
		let views = execute(source, &[0..120]).expect("typed presentation executes");
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
			execute(duplicate, &[0..80]),
			Err(PresentationFailure {
				span: 30..41,
				message: "a figure has at most one title".to_owned(),
			})
		);

		let wrong_register = r#"pub fn main() { [polariton_presentation::__line("x", "y", 0, 14).__stacked(15, 25)] }"#;
		assert_eq!(
			execute(wrong_register, &[0..25]),
			Err(PresentationFailure {
				span: 15..25,
				message: "stacked() is available on bar marks, not line, scatter, or area marks"
					.to_owned(),
			})
		);
	}

	#[test]
	fn product_0110_facet_gutter_is_faceted_figure_furniture() {
		let valid = r#"pub fn main() { [polariton_presentation::__figure([
polariton_presentation::__facet("region", 10, 25),
polariton_presentation::__facet_gutter(24, 26, 41),
polariton_presentation::__bar("quarter", "sales", 42, 66)
], 0, 67)] }"#;
		let views = execute(valid, &[0..67]).expect("faceted gutter figure");
		let RuneViewIntent::Plot(plot) = &views[0] else {
			panic!("expected plot");
		};
		assert_eq!(plot.facet.as_deref(), Some("region"));
		assert_eq!(plot.facet_gutter, Some(24.0));
		assert_eq!(plot.authored_settings, PlotSettingsLayer::default());

		let nonfaceted = r#"pub fn main() { [polariton_presentation::__figure([
polariton_presentation::__facet_gutter(24, 10, 25),
polariton_presentation::__bar("quarter", "sales", 26, 50)
], 0, 51)] }"#;
		assert_eq!(
			execute(nonfaceted, &[0..51]),
			Err(PresentationFailure {
				span: 10..25,
				message: "facet_gutter(...) requires facet(...)".to_owned(),
			})
		);

		let duplicate = r#"pub fn main() { [polariton_presentation::__figure([
polariton_presentation::__facet("region", 10, 25),
polariton_presentation::__facet_gutter(12, 26, 41),
polariton_presentation::__facet_gutter(24, 42, 57),
polariton_presentation::__bar("quarter", "sales", 58, 82)
], 0, 83)] }"#;
		assert_eq!(
			execute(duplicate, &[0..83]),
			Err(PresentationFailure {
				span: 42..57,
				message: "a figure has at most one facet_gutter element".to_owned(),
			})
		);
	}

	#[test]
	fn product_0076_instruction_budget_stops_a_presentation_loop() {
		let source = "pub fn main() { loop {} }";
		let error = execute_generated(
			source,
			&[0..25],
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
				&[slot.clone()],
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
