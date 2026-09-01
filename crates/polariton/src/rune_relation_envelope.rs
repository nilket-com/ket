//! Static Rune view transport and revision scheduling.
//!
//! PRODUCT-0076 moved presentation meaning into a bounded typed Rune VM. This
//! module owns the transported authored intent, relation source maps, and the
//! two-stage revision scheduler; the static shell and source partition live in
//! `rune_view_compiler`, while relation semantics remain Toron's.

#[cfg(test)]
use crate::plot::chrome::PlotChromeProjection;
#[cfg(test)]
use crate::plot::common::TickPlacement;
use crate::plot::composition::PlotComposition;
use crate::plot::core::PlotType;
#[cfg(test)]
use crate::plot::spec::{BarWidth, CategoryGap, PointSize, StrokeWidth};
use crate::plot::spec::{
	BoxPlotFields, BubbleFields, BubblePreparationChannel, CandlestickFields, FillBetweenFields,
	HeatmapFields, HexbinFields, LayerMark, LayerSettings, LayerSpec, LayeredPlotSpec, MarkFields,
	MultivariateFields, Override, PieFields, Presence, ViolinFields,
};
use crate::plot::theme::PlotSettingsLayer;
#[cfg(test)]
use crate::plot::theme::{PlotDomainOverrides, PlotSkinSelection};
use crate::presentation_registry::PresentationSourceModel;
pub(crate) use crate::rune_view::{LocalViewIdentity, ViewContentKind, ViewPresentation};
use libaryon::core::BufferId;
#[cfg(test)]
use rune::ast;
use std::collections::{HashMap, HashSet};
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(test)]
const WORKSPACE_ENTRYPOINT_PREFIX: &str = "pub fn main(workspace) {\n";
#[cfg(test)]
const KET_ENTRYPOINT_PREFIX: &str = "pub fn main(ket) {\n";
#[cfg(test)]
const ENTRYPOINT_SUFFIX: &str = "\n}\n";

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum RuneViewIntent {
	Table(RuneTableSink),
	Plot(RunePlotIntent),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RuneTableSkinProjection {
	PaletteForward,
	Duotone,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) enum RuneTableSkinIntent {
	#[default]
	Default,
	NamedTheme {
		name: String,
		projection: RuneTableSkinProjection,
	},
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RuneTableSink {
	pub(crate) skin: RuneTableSkinIntent,
	pub(crate) show_column_types: Override<bool>,
}

impl Default for RuneTableSink {
	fn default() -> Self {
		Self {
			skin: RuneTableSkinIntent::Default,
			show_column_types: Override::Inherit,
		}
	}
}

#[derive(Clone, Debug)]
pub(crate) struct RunePlotIntent {
	pub(crate) body: RunePlotBody,
	/// PRODUCT-0091: mark-level continuous mapping semantics, kept outside
	/// `MarkFields` because it is not field identity.
	pub(crate) continuous_intent: Option<crate::plot::continuous_color::ContinuousScaleIntent>,
	/// Diagnostic provenance only; deliberately excluded from equality.
	pub(crate) continuous_intent_span: Option<Range<u32>>,
	pub(crate) facet: Option<String>,
	/// PRODUCT-0110: figure-grid layout is intentionally outside the plot
	/// settings cascade because it reserves space between facets.
	pub(crate) facet_gutter: Option<f32>,
	pub(crate) authored_settings: PlotSettingsLayer,
	pub(crate) plot_skin_span: Option<Range<u32>>,
	pub(crate) facet_span: Option<Range<u32>>,
	pub(crate) domain_spans: RunePlotDomainSpans,
	pub(crate) tick_step_spans: RunePlotTickStepSpans,
	pub(crate) tick_every_spans: RunePlotTickEverySpans,
	pub(crate) minor_tick_spans: RunePlotMinorTickSpans,
}

impl PartialEq for RunePlotIntent {
	fn eq(&self, other: &Self) -> bool {
		self.body == other.body
			&& self.continuous_intent == other.continuous_intent
			&& self.facet == other.facet
			&& self.facet_gutter == other.facet_gutter
			&& self.authored_settings == other.authored_settings
	}
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct RunePlotDomainSpans {
	pub(crate) x: Option<Range<u32>>,
	pub(crate) y: Option<Range<u32>>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct RunePlotTickStepSpans {
	pub(crate) x: Option<Range<u32>>,
	pub(crate) y: Option<Range<u32>>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct RunePlotTickEverySpans {
	pub(crate) x: Option<Range<u32>>,
	pub(crate) y: Option<Range<u32>>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct RunePlotMinorTickSpans {
	pub(crate) x: Option<Range<u32>>,
	pub(crate) y: Option<Range<u32>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum RunePlotBody {
	Single(DisposableRunePlotSink),
	StyledSingle(StyledRunePlotSink),
	Layered(LayeredPlotSpec),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StyledRunePlotSink {
	pub(crate) sink: DisposableRunePlotSink,
	pub(crate) settings: LayerSettings,
	/// PRODUCT-0064: structural Bar-family member selection, kept separate
	/// from the Layer-stage geometry settings and from PlotType identity.
	pub(crate) stacked: bool,
	/// PRODUCT-0072: normalized stack-scale intent. This stays outside
	/// LayerSettings because themes and panes cannot override data semantics.
	pub(crate) percent: bool,
}

impl StyledRunePlotSink {
	pub(crate) fn into_layer(self) -> Result<LayerSpec, Self> {
		if self.sink.category.is_some() {
			return Err(self);
		}
		let mark = match self.sink.composition.primary_plot_type() {
			PlotType::Line => LayerMark::Line,
			PlotType::Scatter => LayerMark::Scatter,
			PlotType::Bar
			| PlotType::StackedArea
			| PlotType::Histogram
			| PlotType::BoxPlot
			| PlotType::Heatmap
			| PlotType::Hexbin
			| PlotType::FillBetween
			| PlotType::Candlestick
			| PlotType::Bubble
			| PlotType::Funnel
			| PlotType::RadialDial
			| PlotType::Pie
			| PlotType::Radar
			| PlotType::Parallel => {
				return Err(self);
			}
			other => unreachable!("styled Rune sink admitted unexpected plot type {other:?}"),
		};
		let mut layer = LayerSpec::xy(mark, self.sink.x, self.sink.y);
		layer.settings = self.settings;
		Ok(layer)
	}
}

impl RunePlotIntent {
	pub(crate) fn bare(body: RunePlotBody) -> Self {
		Self {
			body,
			continuous_intent: None,
			continuous_intent_span: None,
			facet: None,
			facet_gutter: None,
			authored_settings: PlotSettingsLayer::default(),
			plot_skin_span: None,
			facet_span: None,
			domain_spans: RunePlotDomainSpans::default(),
			tick_step_spans: RunePlotTickStepSpans::default(),
			tick_every_spans: RunePlotTickEverySpans::default(),
			minor_tick_spans: RunePlotMinorTickSpans::default(),
		}
	}
}

// PRODUCT-0014 replaces this production normal form. Keep the old vocabulary
// test-only while the existing behavioral corpus migrates its type-oriented
// assertions to `ViewPresentation`.
#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum RuneViewForm {
	Table,
	Bar,
	Line,
	Scatter,
	Area,
	Histogram,
	Box,
	Bubble,
	Funnel,
	RadialDial,
}

#[cfg(test)]
impl From<RuneViewForm> for ViewPresentation {
	fn from(form: RuneViewForm) -> Self {
		let (content_kind, display_label) = match form {
			RuneViewForm::Table => (ViewContentKind::Table, "table"),
			RuneViewForm::Bar => (ViewContentKind::Plot, "plot.bar"),
			RuneViewForm::Line => (ViewContentKind::Plot, "plot.line"),
			RuneViewForm::Scatter => (ViewContentKind::Plot, "plot.scatter"),
			RuneViewForm::Area => (ViewContentKind::Plot, "plot.area"),
			RuneViewForm::Histogram => (ViewContentKind::Plot, "plot.histogram"),
			RuneViewForm::Box => (ViewContentKind::Plot, "plot.box"),
			RuneViewForm::Bubble => (ViewContentKind::Plot, "plot.bubble"),
			RuneViewForm::Funnel => (ViewContentKind::Plot, "plot.funnel"),
			RuneViewForm::RadialDial => (ViewContentKind::Plot, "plot.radial_dial"),
		};
		Self {
			content_kind,
			display_label: display_label.to_owned(),
			authored_title: Presence::Inherit,
		}
	}
}

impl RuneViewIntent {
	pub(crate) fn presentation(&self) -> ViewPresentation {
		match self {
			Self::Table(_) => ViewPresentation {
				content_kind: ViewContentKind::Table,
				display_label: "table".to_owned(),
				authored_title: Presence::Inherit,
			},
			Self::Plot(plot) => {
				let display_label = match &plot.body {
					RunePlotBody::Single(plot) => match plot.composition.primary_plot_type() {
						PlotType::Bar => "plot.bar".to_owned(),
						PlotType::Line => "plot.line".to_owned(),
						PlotType::Scatter => "plot.scatter".to_owned(),
						PlotType::StackedArea => "plot.area".to_owned(),
						PlotType::Histogram => "plot.histogram".to_owned(),
						PlotType::BoxPlot => "plot.box".to_owned(),
						PlotType::Heatmap => "plot.heatmap".to_owned(),
						PlotType::Hexbin => "plot.hexbin".to_owned(),
						PlotType::FillBetween => "plot.fill_between".to_owned(),
						PlotType::Candlestick => "plot.candlestick".to_owned(),
						PlotType::Bubble => "plot.bubble".to_owned(),
						PlotType::Funnel => "plot.funnel".to_owned(),
						PlotType::RadialDial => "plot.radial_dial".to_owned(),
						PlotType::Pie => "plot.pie".to_owned(),
						PlotType::Violin => "plot.violin".to_owned(),
						PlotType::Radar => "plot.radar".to_owned(),
						PlotType::Parallel => "plot.parallel".to_owned(),
						other => unreachable!(
							"first-contact sink admitted unexpected plot type {other:?}"
						),
					},
					RunePlotBody::StyledSingle(styled) => {
						match styled.sink.composition.primary_plot_type() {
							PlotType::Bar => "plot.bar".to_owned(),
							PlotType::Line => "plot.line".to_owned(),
							PlotType::Scatter => "plot.scatter".to_owned(),
							PlotType::StackedArea => "plot.area".to_owned(),
							PlotType::Histogram => "plot.histogram".to_owned(),
							PlotType::BoxPlot => "plot.box".to_owned(),
							PlotType::Heatmap => "plot.heatmap".to_owned(),
							PlotType::Hexbin => "plot.hexbin".to_owned(),
							PlotType::FillBetween => "plot.fill_between".to_owned(),
							PlotType::Candlestick => "plot.candlestick".to_owned(),
							PlotType::Bubble => "plot.bubble".to_owned(),
							PlotType::Pie => "plot.pie".to_owned(),
							other => {
								unreachable!("styled sink admitted unexpected plot type {other:?}")
							}
						}
					}
					RunePlotBody::Layered(plot) => plot.title(),
				};
				ViewPresentation {
					content_kind: ViewContentKind::Plot,
					display_label,
					authored_title: plot.authored_settings.title.clone(),
				}
			}
		}
	}

	#[cfg(test)]
	pub(crate) fn form(&self) -> RuneViewForm {
		let presentation = self.presentation();
		match (
			presentation.content_kind,
			presentation.display_label.as_str(),
		) {
			(ViewContentKind::Table, "table") => RuneViewForm::Table,
			(ViewContentKind::Plot, "plot.bar") => RuneViewForm::Bar,
			(ViewContentKind::Plot, "plot.line") => RuneViewForm::Line,
			(ViewContentKind::Plot, "plot.scatter") => RuneViewForm::Scatter,
			(ViewContentKind::Plot, "plot.area") => RuneViewForm::Area,
			(ViewContentKind::Plot, "plot.histogram") => RuneViewForm::Histogram,
			(ViewContentKind::Plot, "plot.box") => RuneViewForm::Box,
			(ViewContentKind::Plot, "plot.bubble") => RuneViewForm::Bubble,
			(ViewContentKind::Plot, "plot.funnel") => RuneViewForm::Funnel,
			(ViewContentKind::Plot, "plot.radial_dial") => RuneViewForm::RadialDial,
			_ => panic!("a composite presentation has no legacy RuneViewForm"),
		}
	}
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SpannedRuneViewIntent {
	pub(crate) intent: RuneViewIntent,
	pub(crate) span: Range<u32>,
	pub(crate) identity: LocalViewIdentity,
	pub(crate) output_order: usize,
}

/// First-contact presentation data only. This is not `PlotSpec` v0 and dies
/// when the plotting contract lands.
#[derive(Clone, Debug)]
pub(crate) struct DisposableRunePlotSink {
	pub(crate) composition: PlotComposition,
	pub(crate) x: String,
	pub(crate) y: String,
	pub(crate) category: Option<String>,
	/// PRODUCT-0079: the second edge of an explicit histogram interval.
	pub(crate) interval_end: Option<String>,
	/// PRODUCT-0094: mark-specific fields coupled to their diagnostic origins.
	pub(crate) mark_fields: Option<SpannedMarkFields>,
	/// Original authored mark call, retained for post-query diagnostics.
	pub(crate) origin: Range<u32>,
}

// Authored location is diagnostic provenance, not plot semantics. Keeping it
// out of equality preserves the canonical-spelling equivalences established
// by PRODUCT-0008 while still carrying it to PRODUCT-0079's post-query errors.
impl PartialEq for DisposableRunePlotSink {
	fn eq(&self, other: &Self) -> bool {
		self.composition == other.composition
			&& self.x == other.x
			&& self.y == other.y
			&& self.category == other.category
			&& self.interval_end == other.interval_end
			&& match (&self.mark_fields, &other.mark_fields) {
				(None, None) => true,
				(Some(left), Some(right)) => left.semantic_eq(right),
				_ => false,
			}
	}
}

impl Eq for DisposableRunePlotSink {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct HeatmapFieldOrigins {
	pub(crate) x: Range<u32>,
	pub(crate) y: Range<u32>,
	pub(crate) value: Range<u32>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct HexbinFieldOrigins {
	pub(crate) x: Range<u32>,
	pub(crate) y: Range<u32>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct FillBetweenFieldOrigins {
	pub(crate) x: Range<u32>,
	pub(crate) lower: Range<u32>,
	pub(crate) upper: Range<u32>,
	pub(crate) middle: Option<Range<u32>>,
}

impl FillBetweenFieldOrigins {
	pub(crate) fn for_role(
		&self,
		role: crate::plot::kernels::fill_between::FillBetweenRole,
	) -> Option<Range<u32>> {
		use crate::plot::kernels::fill_between::FillBetweenRole;
		match role {
			FillBetweenRole::X => Some(self.x.clone()),
			FillBetweenRole::Lower => Some(self.lower.clone()),
			FillBetweenRole::Upper => Some(self.upper.clone()),
			FillBetweenRole::Middle => self.middle.clone(),
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CandlestickFieldOrigins {
	pub(crate) x: Range<u32>,
	pub(crate) open: Range<u32>,
	pub(crate) high: Range<u32>,
	pub(crate) low: Range<u32>,
	pub(crate) close: Range<u32>,
}

impl CandlestickFieldOrigins {
	pub(crate) fn for_role(
		&self,
		role: crate::plot::kernels::candlestick::CandlestickRole,
	) -> Range<u32> {
		use crate::plot::kernels::candlestick::CandlestickRole;
		match role {
			CandlestickRole::X => self.x.clone(),
			CandlestickRole::Open => self.open.clone(),
			CandlestickRole::High => self.high.clone(),
			CandlestickRole::Low => self.low.clone(),
			CandlestickRole::Close => self.close.clone(),
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct FunnelFieldOrigins {
	pub(crate) stage: Range<u32>,
	pub(crate) value: Range<u32>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RadialDialFieldOrigins {
	pub(crate) value: Range<u32>,
	pub(crate) maximum: Range<u32>,
	pub(crate) label: Option<Range<u32>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PieFieldOrigins {
	pub(crate) category: Range<u32>,
	pub(crate) value: Range<u32>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ViolinFieldOrigins {
	pub(crate) group: Range<u32>,
	pub(crate) evaluation_value: Range<u32>,
	pub(crate) density: Range<u32>,
	pub(crate) median: Range<u32>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MultivariateFieldOrigins {
	pub(crate) dimensions: Vec<Range<u32>>,
	pub(crate) category: Option<Range<u32>>,
}

impl MultivariateFieldOrigins {
	pub(crate) fn dimension(&self, fields: &MultivariateFields, field: &str) -> Option<Range<u32>> {
		fields
			.dimensions()
			.iter()
			.position(|candidate| candidate == field)
			.and_then(|index| self.dimensions.get(index).cloned())
	}
}

impl HeatmapFieldOrigins {
	pub(crate) fn keys(&self) -> Range<u32> {
		self.x.start.min(self.y.start)..self.x.end.max(self.y.end)
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BoxPlotFieldOrigins {
	pub(crate) category: Range<u32>,
	pub(crate) whisker_low: Range<u32>,
	pub(crate) q1: Range<u32>,
	pub(crate) median: Range<u32>,
	pub(crate) q3: Range<u32>,
	pub(crate) whisker_high: Range<u32>,
	pub(crate) outlier: Range<u32>,
}

impl BoxPlotFieldOrigins {
	pub(crate) fn for_role(
		&self,
		role: crate::plot::kernels::boxplot::BoxPlotFieldRole,
	) -> Range<u32> {
		use crate::plot::kernels::boxplot::BoxPlotFieldRole;
		match role {
			BoxPlotFieldRole::Category => self.category.clone(),
			BoxPlotFieldRole::WhiskerLow => self.whisker_low.clone(),
			BoxPlotFieldRole::Q1 => self.q1.clone(),
			BoxPlotFieldRole::Median => self.median.clone(),
			BoxPlotFieldRole::Q3 => self.q3.clone(),
			BoxPlotFieldRole::WhiskerHigh => self.whisker_high.clone(),
			BoxPlotFieldRole::Outlier => self.outlier.clone(),
		}
	}
}

#[derive(Clone, Debug)]
pub(crate) struct BubbleFieldOrigins {
	pub(crate) x: Range<u32>,
	pub(crate) y: Range<u32>,
	pub(crate) size: Range<u32>,
	pub(crate) color: Range<u32>,
	pub(crate) label: Option<Range<u32>>,
}

impl BubbleFieldOrigins {
	pub(crate) fn for_role(&self, role: BubblePreparationChannel) -> Option<Range<u32>> {
		match role {
			BubblePreparationChannel::X => Some(self.x.clone()),
			BubblePreparationChannel::Y => Some(self.y.clone()),
			BubblePreparationChannel::Size => Some(self.size.clone()),
			BubblePreparationChannel::Color => Some(self.color.clone()),
			BubblePreparationChannel::Label => self.label.clone(),
		}
	}
}

/// Mark-specific fields at the Rune boundary. The variant couples semantic
/// fields to matching diagnostic provenance; provenance deliberately does not
/// participate in `DisposableRunePlotSink` equality.
#[derive(Clone, Debug)]
pub(crate) enum SpannedMarkFields {
	BoxPlot {
		fields: BoxPlotFields,
		origins: BoxPlotFieldOrigins,
	},
	Heatmap {
		fields: HeatmapFields,
		origins: HeatmapFieldOrigins,
	},
	Hexbin {
		fields: HexbinFields,
		origins: HexbinFieldOrigins,
	},
	FillBetween {
		fields: FillBetweenFields,
		origins: FillBetweenFieldOrigins,
	},
	Candlestick {
		fields: CandlestickFields,
		origins: CandlestickFieldOrigins,
	},
	Bubble {
		fields: BubbleFields,
		origins: BubbleFieldOrigins,
	},
	Funnel {
		fields: crate::plot::spec::FunnelFields,
		origins: FunnelFieldOrigins,
	},
	RadialDial {
		fields: crate::plot::spec::RadialDialFields,
		origins: RadialDialFieldOrigins,
	},
	Pie {
		fields: PieFields,
		origins: PieFieldOrigins,
	},
	Violin {
		fields: ViolinFields,
		origins: ViolinFieldOrigins,
	},
	Radar {
		fields: MultivariateFields,
		origins: MultivariateFieldOrigins,
	},
	Parallel {
		fields: MultivariateFields,
		origins: MultivariateFieldOrigins,
	},
}

impl SpannedMarkFields {
	pub(crate) fn to_mark_fields(&self) -> MarkFields {
		match self {
			Self::BoxPlot { fields, .. } => MarkFields::BoxPlot(fields.clone()),
			Self::Heatmap { fields, .. } => MarkFields::Heatmap(fields.clone()),
			Self::Hexbin { fields, .. } => MarkFields::Hexbin(fields.clone()),
			Self::FillBetween { fields, .. } => MarkFields::FillBetween(fields.clone()),
			Self::Candlestick { fields, .. } => MarkFields::Candlestick(fields.clone()),
			Self::Bubble { fields, .. } => MarkFields::Bubble(fields.clone()),
			Self::Funnel { fields, .. } => MarkFields::Funnel(fields.clone()),
			Self::RadialDial { fields, .. } => MarkFields::RadialDial(fields.clone()),
			Self::Pie { fields, .. } => MarkFields::Pie(fields.clone()),
			Self::Violin { fields, .. } => MarkFields::Violin(fields.clone()),
			Self::Radar { fields, .. } => MarkFields::Radar(fields.clone()),
			Self::Parallel { fields, .. } => MarkFields::Parallel(fields.clone()),
		}
	}

	pub(crate) fn box_plot(&self) -> Option<(&BoxPlotFields, &BoxPlotFieldOrigins)> {
		match self {
			Self::BoxPlot { fields, origins } => Some((fields, origins)),
			_ => None,
		}
	}

	pub(crate) fn heatmap(&self) -> Option<(&HeatmapFields, &HeatmapFieldOrigins)> {
		match self {
			Self::Heatmap { fields, origins } => Some((fields, origins)),
			_ => None,
		}
	}

	pub(crate) fn hexbin(&self) -> Option<(&HexbinFields, &HexbinFieldOrigins)> {
		match self {
			Self::Hexbin { fields, origins } => Some((fields, origins)),
			_ => None,
		}
	}

	pub(crate) fn fill_between(&self) -> Option<(&FillBetweenFields, &FillBetweenFieldOrigins)> {
		match self {
			Self::FillBetween { fields, origins } => Some((fields, origins)),
			_ => None,
		}
	}

	pub(crate) fn candlestick(&self) -> Option<(&CandlestickFields, &CandlestickFieldOrigins)> {
		match self {
			Self::Candlestick { fields, origins } => Some((fields, origins)),
			_ => None,
		}
	}

	pub(crate) fn bubble(&self) -> Option<(&BubbleFields, &BubbleFieldOrigins)> {
		match self {
			Self::Bubble { fields, origins } => Some((fields, origins)),
			_ => None,
		}
	}

	pub(crate) fn funnel(&self) -> Option<(&crate::plot::spec::FunnelFields, &FunnelFieldOrigins)> {
		match self {
			Self::Funnel { fields, origins } => Some((fields, origins)),
			_ => None,
		}
	}

	pub(crate) fn radial_dial(
		&self,
	) -> Option<(
		&crate::plot::spec::RadialDialFields,
		&RadialDialFieldOrigins,
	)> {
		match self {
			Self::RadialDial { fields, origins } => Some((fields, origins)),
			_ => None,
		}
	}

	pub(crate) fn pie(&self) -> Option<(&PieFields, &PieFieldOrigins)> {
		match self {
			Self::Pie { fields, origins } => Some((fields, origins)),
			_ => None,
		}
	}

	pub(crate) fn violin(&self) -> Option<(&ViolinFields, &ViolinFieldOrigins)> {
		match self {
			Self::Violin { fields, origins } => Some((fields, origins)),
			_ => None,
		}
	}

	pub(crate) fn multivariate(
		&self,
	) -> Option<(PlotType, &MultivariateFields, &MultivariateFieldOrigins)> {
		match self {
			Self::Radar { fields, origins } => Some((PlotType::Radar, fields, origins)),
			Self::Parallel { fields, origins } => Some((PlotType::Parallel, fields, origins)),
			_ => None,
		}
	}

	fn semantic_eq(&self, other: &Self) -> bool {
		match (self, other) {
			(Self::BoxPlot { fields: left, .. }, Self::BoxPlot { fields: right, .. }) => {
				left == right
			}
			(Self::Heatmap { fields: left, .. }, Self::Heatmap { fields: right, .. }) => {
				left == right
			}
			(Self::Hexbin { fields: left, .. }, Self::Hexbin { fields: right, .. }) => {
				left == right
			}
			(Self::FillBetween { fields: left, .. }, Self::FillBetween { fields: right, .. }) => {
				left == right
			}
			(Self::Candlestick { fields: left, .. }, Self::Candlestick { fields: right, .. }) => {
				left == right
			}
			(Self::Bubble { fields: left, .. }, Self::Bubble { fields: right, .. }) => {
				left == right
			}
			(Self::Funnel { fields: left, .. }, Self::Funnel { fields: right, .. }) => {
				left == right
			}
			(Self::RadialDial { fields: left, .. }, Self::RadialDial { fields: right, .. }) => {
				left == right
			}
			(Self::Pie { fields: left, .. }, Self::Pie { fields: right, .. }) => left == right,
			(Self::Violin { fields: left, .. }, Self::Violin { fields: right, .. }) => {
				left == right
			}
			(Self::Radar { fields: left, .. }, Self::Radar { fields: right, .. })
			| (Self::Parallel { fields: left, .. }, Self::Parallel { fields: right, .. }) => left == right,
			_ => false,
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SourceMapSegment {
	pub(crate) transformed: Range<u32>,
	pub(crate) original_start: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RuneEnvelopeSourceMap {
	pub(crate) segments: Vec<SourceMapSegment>,
	pub(crate) diagnostic_fallback: Range<u32>,
}

impl RuneEnvelopeSourceMap {
	pub(crate) fn map_span(&self, span: toron_client::RuneSourceSpan) -> Option<Range<u32>> {
		self.segments.iter().find_map(|segment| {
			if span.start < segment.transformed.start || span.end > segment.transformed.end {
				return None;
			}
			let start = segment
				.original_start
				.checked_add(span.start.checked_sub(segment.transformed.start)?)?;
			let end = segment
				.original_start
				.checked_add(span.end.checked_sub(segment.transformed.start)?)?;
			Some(start..end)
		})
	}

	pub(crate) fn diagnostic_fallback(&self) -> Range<u32> {
		self.diagnostic_fallback.clone()
	}
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct TransformedRuneView {
	pub(crate) executions: Vec<TransformedRuneRelation>,
	pub(crate) presentation_order: Vec<LocalViewIdentity>,
	pub(crate) presentation_source_model: PresentationSourceModel,
}

/// The relation VM entrypoint chosen by static compilation. Kept in the
/// transformed envelope because the generated program text alone must not be
/// used to infer which root object receives it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum RelationRootIdentity {
	Workspace,
	Ket,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct TransformedRuneRelation {
	pub(crate) origin: String,
	pub(crate) root: RelationRootIdentity,
	pub(crate) source: String,
	pub(crate) views: Vec<SpannedRuneViewIntent>,
	pub(crate) source_map: RuneEnvelopeSourceMap,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RelationExecutionKey {
	pub(crate) origin: String,
	pub(crate) root: RelationRootIdentity,
	pub(crate) program: String,
	pub(crate) views: Vec<LocalViewIdentity>,
}

/// Compiler-derived semantic equality for deciding whether a retained
/// relation result may answer a candidate presentation compile. Diagnostic
/// spans and source-map offsets are intentionally absent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RelationPlanKey {
	pub(crate) executions: Vec<RelationExecutionKey>,
}

impl RelationPlanKey {
	pub(crate) fn from_view(view: &TransformedRuneView) -> Self {
		Self {
			executions: view
				.executions
				.iter()
				.map(|execution| RelationExecutionKey {
					origin: execution.origin.clone(),
					root: execution.root,
					program: execution.source.clone(),
					views: execution
						.views
						.iter()
						.map(|view| view.identity.clone())
						.collect(),
				})
				.collect(),
		}
	}
}

/// Runtime half of the reuse proof. The compiler key proves sameness of work;
/// this witness proves the retained answer is still the authoritative
/// publication for that work.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ArtifactReuseWitness {
	pub(crate) baseline: RelationPlanKey,
	pub(crate) artifact_generation: u64,
}

impl ArtifactReuseWitness {
	pub(crate) fn proves(
		&self,
		candidate: &RelationPlanKey,
		authoritative_generation: u64,
	) -> bool {
		self.baseline == *candidate && self.artifact_generation == authoritative_generation
	}
}

impl TransformedRuneView {
	pub(crate) fn relation_count(&self) -> usize {
		self.executions.len()
	}

	pub(crate) fn into_relations(self) -> impl Iterator<Item = TransformedRuneRelation> {
		self.executions.into_iter()
	}

	#[cfg(test)]
	pub(crate) fn presentation_order(&self) -> &[LocalViewIdentity] {
		&self.presentation_order
	}

	pub(crate) fn presentation_views(&self) -> Vec<&SpannedRuneViewIntent> {
		let mut views = self
			.executions
			.iter()
			.flat_map(|execution| execution.views.iter())
			.collect::<Vec<_>>();
		views.sort_by_key(|view| view.output_order);
		views
	}
}

impl std::ops::Deref for TransformedRuneView {
	type Target = TransformedRuneRelation;

	fn deref(&self) -> &Self::Target {
		self.executions
			.first()
			.expect("a transformed view program has an execution")
	}
}

#[derive(Clone, Debug)]
pub(crate) struct PendingRuneView {
	pub(crate) source_buffer: BufferId,
	pub(crate) revision: u64,
	pub(crate) view: TransformedRuneView,
}

#[derive(Clone, Debug)]
pub(crate) struct PendingRunePresentation {
	pub(crate) source_buffer: BufferId,
	pub(crate) revision: u64,
	pub(crate) source: String,
	freshness: Option<Arc<AtomicU64>>,
}

impl PendingRunePresentation {
	pub(crate) fn new(source_buffer: BufferId, revision: u64, source: String) -> Self {
		Self {
			source_buffer,
			revision,
			source,
			freshness: None,
		}
	}

	pub(crate) fn is_current(&self) -> bool {
		self.freshness
			.as_ref()
			.is_some_and(|latest| latest.load(Ordering::Acquire) == self.revision)
	}
}

pub(crate) struct PresentationCompletion {
	pub(crate) accepted: bool,
	pub(crate) next: Option<PendingRunePresentation>,
}

#[derive(Default)]
pub(crate) struct RuneViewScheduler {
	presentation_in_flight: HashMap<BufferId, u64>,
	presentation_pending: HashMap<BufferId, PendingRunePresentation>,
	presentation_latest: HashMap<BufferId, Arc<AtomicU64>>,
	in_flight: HashMap<BufferId, HashSet<String>>,
	pending: HashMap<BufferId, PendingRuneView>,
	pub(crate) submitted: u64,
	pub(crate) executed: u64,
	pub(crate) superseded_before_execution: u64,
	pub(crate) presentation_submitted: u64,
	pub(crate) presentation_executed: u64,
	pub(crate) presentation_superseded_before_execution: u64,
}

impl RuneViewScheduler {
	pub(crate) fn submit_presentation(
		&mut self,
		mut work: PendingRunePresentation,
	) -> Option<PendingRunePresentation> {
		self.presentation_submitted = self.presentation_submitted.saturating_add(1);
		let latest = self
			.presentation_latest
			.entry(work.source_buffer)
			.or_insert_with(|| Arc::new(AtomicU64::new(work.revision)));
		latest.store(work.revision, Ordering::Release);
		work.freshness = Some(Arc::clone(latest));
		if !self
			.presentation_in_flight
			.contains_key(&work.source_buffer)
		{
			self.presentation_in_flight
				.insert(work.source_buffer, work.revision);
			self.presentation_executed = self.presentation_executed.saturating_add(1);
			return Some(work);
		}
		if self
			.presentation_pending
			.insert(work.source_buffer, work)
			.is_some()
		{
			self.presentation_superseded_before_execution = self
				.presentation_superseded_before_execution
				.saturating_add(1);
		}
		None
	}

	pub(crate) fn complete_presentation(
		&mut self,
		source_buffer: BufferId,
		revision: u64,
	) -> PresentationCompletion {
		let active = self
			.presentation_in_flight
			.remove(&source_buffer)
			.expect("presentation completion requires an in-flight buffer");
		assert_eq!(
			active, revision,
			"presentation completion requires the active revision"
		);
		let next = self.presentation_pending.remove(&source_buffer);
		if let Some(next) = &next {
			assert!(
				self.presentation_in_flight
					.insert(source_buffer, next.revision)
					.is_none(),
				"a pending presentation starts only after completion"
			);
			self.presentation_executed = self.presentation_executed.saturating_add(1);
		} else {
			self.presentation_latest.remove(&source_buffer);
		}
		PresentationCompletion {
			accepted: next.is_none(),
			next,
		}
	}

	pub(crate) fn submit(&mut self, work: PendingRuneView) -> Option<PendingRuneView> {
		self.submitted = self.submitted.saturating_add(1);
		if !self.in_flight.contains_key(&work.source_buffer) {
			self.start(&work);
			self.executed = self.executed.saturating_add(1);
			return Some(work);
		}
		if self.pending.insert(work.source_buffer, work).is_some() {
			self.superseded_before_execution = self.superseded_before_execution.saturating_add(1);
		}
		None
	}

	pub(crate) fn complete(
		&mut self,
		source_buffer: BufferId,
		origin: &str,
	) -> Option<PendingRuneView> {
		let origins = self
			.in_flight
			.get_mut(&source_buffer)
			.expect("Rune view completion requires an in-flight buffer");
		assert!(
			origins.remove(origin),
			"Rune view completion requires an in-flight origin"
		);
		if !origins.is_empty() {
			return None;
		}
		self.in_flight.remove(&source_buffer);
		let pending = self.pending.remove(&source_buffer)?;
		self.start(&pending);
		self.executed = self.executed.saturating_add(1);
		Some(pending)
	}

	pub(crate) fn cancel(&mut self, source_buffer: BufferId) {
		self.presentation_in_flight.remove(&source_buffer);
		self.presentation_pending.remove(&source_buffer);
		self.presentation_latest.remove(&source_buffer);
		self.in_flight.remove(&source_buffer);
		self.pending.remove(&source_buffer);
	}

	fn start(&mut self, work: &PendingRuneView) {
		let origins = work
			.view
			.executions
			.iter()
			.map(|execution| execution.origin.clone())
			.collect::<HashSet<_>>();
		assert!(
			!origins.is_empty(),
			"a scheduled view program has an origin"
		);
		assert!(
			self.in_flight.insert(work.source_buffer, origins).is_none(),
			"a buffer cannot start while another program is in flight"
		);
	}

	#[cfg(test)]
	fn is_in_flight(&self, source_buffer: BufferId) -> bool {
		self.in_flight.contains_key(&source_buffer)
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum RuneEnvelopeError {
	NoTerminalView,
	RecognizedNoOutput,
	Parse(String),
	Invalid(String),
	InvalidView {
		span: Range<u32>,
		message: String,
	},
	DuplicateName {
		name: String,
		first: Range<u32>,
		duplicate: Range<u32>,
	},
}

impl std::fmt::Display for RuneEnvelopeError {
	fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::NoTerminalView => formatter.write_str(
				"Rune relation views use top-level table(relation), plot.bar(relation, \"x\", \"y\"), plot.line(relation, \"x\", \"y\"), plot.scatter(relation, \"x\", \"y\"), or plot.area(relation, \"x\", \"y\")",
			),
			Self::RecognizedNoOutput => {
				formatter.write_str("Rune view set is bound but has no output root")
			}
			Self::Parse(message) => write!(formatter, "Rune parse: {message}"),
			Self::Invalid(message) => write!(formatter, "Rune view syntax: {message}"),
			Self::InvalidView { span, message } => write!(
				formatter,
				"Rune view syntax at bytes {}..{}: {message}",
				span.start, span.end
			),
			Self::DuplicateName {
				name,
				first,
				duplicate,
			} => write!(
				formatter,
				"view name {name:?} appears twice at bytes {}..{} and {}..{}; names are unique per buffer",
				first.start, first.end, duplicate.start, duplicate.end
			),
		}
	}
}

impl std::error::Error for RuneEnvelopeError {}

#[cfg(test)]
pub(crate) fn transform(source: &str) -> Result<TransformedRuneView, RuneEnvelopeError> {
	crate::rune_view_compiler::compile(source)
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::plot::spec::{BubbleFieldNames, BubbleFields};

	#[test]
	fn product_0144_direct_styled_hexbin_is_defensively_labelled_and_refuses_layering() {
		let styled = StyledRunePlotSink {
			sink: DisposableRunePlotSink {
				composition: PlotComposition::single(PlotType::Hexbin),
				x: "x".to_owned(),
				y: "y".to_owned(),
				category: None,
				interval_end: None,
				mark_fields: Some(SpannedMarkFields::Hexbin {
					fields: HexbinFields {
						x: "x".to_owned(),
						y: "y".to_owned(),
					},
					origins: HexbinFieldOrigins {
						x: 7..10,
						y: 12..15,
					},
				}),
				origin: 0..16,
			},
			settings: LayerSettings::default(),
			stacked: false,
			percent: false,
		};
		assert!(styled.clone().into_layer().is_err());
		let intent = RuneViewIntent::Plot(RunePlotIntent::bare(RunePlotBody::StyledSingle(styled)));
		assert_eq!(intent.presentation().display_label, "plot.hexbin");
	}

	#[test]
	fn product_0146_direct_styled_fill_between_is_defensively_labelled_and_refuses_layering() {
		let styled = StyledRunePlotSink {
			sink: DisposableRunePlotSink {
				composition: PlotComposition::single(PlotType::FillBetween),
				x: "x".to_owned(),
				y: "low".to_owned(),
				category: None,
				interval_end: None,
				mark_fields: Some(SpannedMarkFields::FillBetween {
					fields: FillBetweenFields {
						x: "x".to_owned(),
						lower: "low".to_owned(),
						upper: "high".to_owned(),
						middle: None,
					},
					origins: FillBetweenFieldOrigins {
						x: 13..16,
						lower: 18..23,
						upper: 25..31,
						middle: None,
					},
				}),
				origin: 0..32,
			},
			settings: LayerSettings::default(),
			stacked: false,
			percent: false,
		};
		assert!(styled.clone().into_layer().is_err());
		let intent = RuneViewIntent::Plot(RunePlotIntent::bare(RunePlotBody::StyledSingle(styled)));
		assert_eq!(intent.presentation().display_label, "plot.fill_between");
	}

	#[test]
	fn product_0148_direct_styled_candlestick_is_defensively_labelled_and_refuses_layering() {
		let styled = StyledRunePlotSink {
			sink: DisposableRunePlotSink {
				composition: PlotComposition::single(PlotType::Candlestick),
				x: "stamp".to_owned(),
				y: "open".to_owned(),
				category: None,
				interval_end: None,
				mark_fields: Some(SpannedMarkFields::Candlestick {
					fields: CandlestickFields {
						x: "stamp".to_owned(),
						open: "open".to_owned(),
						high: "high".to_owned(),
						low: "low".to_owned(),
						close: "close".to_owned(),
					},
					origins: CandlestickFieldOrigins {
						x: 12..19,
						open: 21..27,
						high: 29..35,
						low: 37..42,
						close: 44..51,
					},
				}),
				origin: 0..52,
			},
			settings: LayerSettings::default(),
			stacked: false,
			percent: false,
		};
		assert!(styled.clone().into_layer().is_err());
		let intent = RuneViewIntent::Plot(RunePlotIntent::bare(RunePlotBody::StyledSingle(styled)));
		assert_eq!(intent.presentation().display_label, "plot.candlestick");
	}

	#[derive(Clone, Debug, PartialEq)]
	struct TestSemantics {
		executions: Vec<String>,
		presentation_order: Vec<RuneViewIntent>,
		execution_count: usize,
		sinks: Vec<RuneViewIntent>,
	}

	trait TestEnvelopeSemantics {
		fn semantics(&self) -> TestSemantics;
	}

	impl TestEnvelopeSemantics for TransformedRuneView {
		fn semantics(&self) -> TestSemantics {
			let sinks = self
				.presentation_views()
				.into_iter()
				.map(|view| view.intent.clone())
				.collect::<Vec<_>>();
			TestSemantics {
				executions: self
					.executions
					.iter()
					.map(|execution| execution.origin.clone())
					.collect(),
				presentation_order: sinks.clone(),
				execution_count: self.executions.len(),
				sinks,
			}
		}
	}

	fn envelope(source: &str) -> TransformedRuneView {
		transform(source).expect("execution envelope")
	}

	fn product_0094_heatmap_sink(value: &str, start: u32) -> DisposableRunePlotSink {
		DisposableRunePlotSink {
			composition: PlotComposition::single(PlotType::Heatmap),
			x: "x".to_owned(),
			y: "y".to_owned(),
			category: None,
			interval_end: None,
			mark_fields: Some(SpannedMarkFields::Heatmap {
				fields: HeatmapFields {
					x: "x".to_owned(),
					y: "y".to_owned(),
					value: value.to_owned(),
				},
				origins: HeatmapFieldOrigins {
					x: start..start + 1,
					y: start + 2..start + 3,
					value: start + 4..start + 5,
				},
			}),
			origin: start..start + 6,
		}
	}

	#[test]
	fn product_0094_sink_equality_ignores_origins_but_not_semantic_fields() {
		let first = product_0094_heatmap_sink("value", 10);
		let relocated = product_0094_heatmap_sink("value", 100);
		let changed = product_0094_heatmap_sink("other_value", 10);
		assert_eq!(first, relocated);
		assert_ne!(first, changed);
	}

	#[test]
	fn product_0094_variants_keep_accessors_and_variable_arity() {
		let heatmap = product_0094_heatmap_sink("value", 10);
		let (_, origins) = heatmap
			.mark_fields
			.as_ref()
			.and_then(SpannedMarkFields::heatmap)
			.expect("heatmap fields and origins");
		assert_eq!(origins.keys(), 10..13);

		for (label, expected) in [
			(None, vec!["x", "y", "size", "color"]),
			(
				Some("label".to_owned()),
				vec!["x", "y", "size", "color", "label"],
			),
		] {
			let staged = SpannedMarkFields::Bubble {
				fields: BubbleFields::new(BubbleFieldNames::new("x", "y", "size", "color"), label),
				origins: BubbleFieldOrigins {
					x: 1..2,
					y: 3..4,
					size: 5..6,
					color: 7..8,
					label: None,
				},
			};
			assert_eq!(staged.to_mark_fields().all().collect::<Vec<_>>(), expected);
		}
	}

	#[test]
	fn terminal_table_is_substituted_and_mapped() {
		let source = "let r = workspace.table(\"test2\").sort([col(\"cat\").asc()]);\ntable(r)";
		let transformed = transform(source).expect("table envelope");
		assert_eq!(transformed.views.len(), 1);
		assert_eq!(
			transformed.views[0].intent,
			RuneViewIntent::Table(RuneTableSink::default())
		);
		assert!(transformed.source.starts_with(WORKSPACE_ENTRYPOINT_PREFIX));
		assert!(transformed.source.ends_with(ENTRYPOINT_SUFFIX));
		assert!(transformed.source.contains("let r = workspace.table"));
		assert!(transformed.source.contains("\nr\n}"));
		let server_r = transformed
			.source
			.rfind("\nr\n")
			.expect("returned relation")
			+ 1;
		let mapped = transformed
			.source_map
			.map_span(toron_client::RuneSourceSpan {
				start: server_r as u32,
				end: server_r as u32 + 1,
			})
			.expect("mapped relation");
		assert_eq!(&source[mapped.start as usize..mapped.end as usize], "r");
		assert_eq!(
			transformed
				.source_map
				.map_span(toron_client::RuneSourceSpan { start: 0, end: 3 }),
			None,
			"wrapper-only diagnostics require the relation fallback"
		);
		let fallback = transformed.source_map.diagnostic_fallback();
		assert_eq!(
			&source[fallback.start as usize..fallback.end as usize],
			"r",
			"Toron has seen the relation, not the presentation sink"
		);
	}

	#[test]
	fn inline_and_bound_relations_have_equal_semantics_not_equal_envelopes() {
		let inline = envelope(
			"table(workspace.table(\"t\").sort([asc(col(\"c\"))]));\nplot.bar(workspace.table(\"t\").sort([asc(col(\"c\"))]), \"c\", \"v\")",
		);
		let bound = envelope(
			"let r = workspace.table(\"t\").sort([asc(col(\"c\"))]);\ntable(r);\nplot.bar(r, \"c\", \"v\")",
		);
		assert_ne!(inline, bound, "rendering structure and spans really differ");
		assert_eq!(inline.semantics(), bound.semantics());
	}

	#[test]
	fn semicolon_terminated_view_still_returns_the_relation() {
		let transformed = transform("let r = workspace.table(\"test2\");\ntable(r);")
			.expect("semicolon-terminated table envelope");
		assert!(transformed.source.contains("\nr\n}"));
		assert!(!transformed.source.contains("\nr;\n}"));
	}

	#[test]
	fn interleaved_roots_are_removed_and_retained_segments_stay_mapped() {
		let source = "let r = workspace.table(\"test2\");\ntable(r);\nlet marker = 1;\nplot.bar(r, \"cat\", \"val\");\nlet tail = marker + 1;";
		let transformed = transform(source).expect("interleaved envelope");
		assert_eq!(
			transformed
				.views
				.iter()
				.map(|view| view.intent.form())
				.collect::<Vec<_>>(),
			[RuneViewForm::Table, RuneViewForm::Bar]
		);
		assert!(!transformed.source.contains("table(r)"));
		assert!(!transformed.source.contains("plot.bar"));
		assert!(transformed.source.contains("let marker = 1"));
		assert!(transformed.source.contains("let tail = marker + 1"));
		let transformed_marker = transformed.source.find("marker").expect("marker retained");
		let mapped = transformed
			.source_map
			.map_span(toron_client::RuneSourceSpan {
				start: transformed_marker as u32,
				end: transformed_marker as u32 + 6,
			})
			.expect("retained segment maps");
		assert_eq!(
			&source[mapped.start as usize..mapped.end as usize],
			"marker"
		);
	}

	#[test]
	fn retained_trailing_expression_is_terminated_before_the_relation_return() {
		let transformed =
			transform("let r = workspace.table(\"test2\");\ntable(r);\nlet marker = 1;\nmarker")
				.expect("view before trailing expression");
		assert!(transformed.source.contains("marker\n;\nr\n}"));
		rune::parse::parse_all::<ast::ItemFn>(&transformed.source, rune::SourceId::EMPTY, false)
			.expect("rendered entrypoint remains valid Rune");
	}

	#[test]
	fn plot_forms_retain_only_view_intent() {
		let bar = transform("let r = workspace.table(\"test2\"); plot.bar(r, \"cat\", \"val\")")
			.expect("bar envelope");
		assert_eq!(
			bar.views[0].intent.presentation().authored_title,
			Presence::Inherit,
			"bare sinks carry no source-authored chrome title"
		);
		let RuneViewIntent::Plot(bar) = &bar.views[0].intent else {
			panic!("expected plot");
		};
		assert_eq!(bar.authored_settings, PlotSettingsLayer::default());
		let RunePlotBody::Single(bar) = &bar.body else {
			panic!("expected a single bar mark");
		};
		assert_eq!(bar.composition.primary_plot_type(), PlotType::Bar);
		assert_eq!(
			(&bar.x, &bar.y, &bar.category),
			(&"cat".to_owned(), &"val".to_owned(), &None)
		);

		let scatter =
			transform("let r = workspace.table(\"test2\"); plot.scatter(r, \"x\", \"y\", \"cat\")")
				.expect("scatter envelope");
		let RuneViewIntent::Plot(scatter) = &scatter.views[0].intent else {
			panic!("expected plot");
		};
		let RunePlotBody::Single(scatter) = &scatter.body else {
			panic!("expected a single scatter mark");
		};
		assert_eq!(scatter.composition.primary_plot_type(), PlotType::Scatter);
		assert_eq!(scatter.category.as_deref(), Some("cat"));

		let line = transform("let r = workspace.table(\"test2\"); plot.line(r, \"x\", \"y\")")
			.expect("line envelope");
		let RuneViewIntent::Plot(line) = &line.views[0].intent else {
			panic!("expected plot");
		};
		let RunePlotBody::Single(line) = &line.body else {
			panic!("expected a single line mark");
		};
		assert_eq!(line.composition.primary_plot_type(), PlotType::Line);
		assert_eq!(
			(&line.x, &line.y, &line.category),
			(&"x".to_owned(), &"y".to_owned(), &None)
		);
	}

	#[test]
	fn trailing_three_view_block_strips_once_and_retains_each_span() {
		let source = "let r = workspace.table(\"test2\");\ntable(r);\nplot.bar(r, \"cat\", \"val\");\nplot.scatter(r, \"cat\", \"val\")";
		let transformed = transform(source).expect("three-view envelope");
		assert_eq!(
			transformed
				.views
				.iter()
				.map(|view| view.intent.form())
				.collect::<Vec<_>>(),
			[
				RuneViewForm::Table,
				RuneViewForm::Bar,
				RuneViewForm::Scatter
			]
		);
		assert!(transformed.source.contains("\nr\n}"));
		assert!(!transformed.source.contains("plot.bar"));
		for view in &transformed.views {
			assert!(
				source[view.span.start as usize..view.span.end as usize].contains('('),
				"each sink retains its original call span"
			);
		}

		let buffer = BufferId::new(41);
		let mut scheduler = RuneViewScheduler::default();
		let dispatched = scheduler
			.submit(PendingRuneView {
				source_buffer: buffer,
				revision: 1,
				view: transformed,
			})
			.expect("one view-set execution");
		assert_eq!(dispatched.view.views.len(), 3);
		assert_eq!(
			(
				scheduler.submitted,
				scheduler.executed,
				scheduler.superseded_before_execution
			),
			(1, 1, 0),
			"three sinks are one scheduler execution"
		);
	}

	#[test]
	fn origin_resolved_identity_groups_multiple_origins_and_repeated_inline_text() {
		let mixed_source = "let a = workspace.table(\"a\"); let b = workspace.table(\"b\"); table(a); let marker = 1; plot.bar(b, \"x\", \"y\")";
		let semantics = transform(mixed_source)
			.expect("analyze multiple origins")
			.semantics();
		assert_eq!(semantics.execution_count, 2);
		assert_eq!(
			semantics
				.presentation_order
				.iter()
				.map(RuneViewIntent::form)
				.collect::<Vec<_>>(),
			[RuneViewForm::Table, RuneViewForm::Bar]
		);
		let mixed = transform(mixed_source).expect("multiple origins normalize");
		assert_eq!(mixed.relation_count(), 2);
		assert_eq!(
			mixed.presentation_order(),
			&[
				LocalViewIdentity::Anonymous(0),
				LocalViewIdentity::Anonymous(1)
			]
		);

		let duplicate =
			transform("let r = workspace.table(\"t\"); table(r); let marker = 1; table(r)")
				.expect("duplicate anonymous forms are legal");
		assert_eq!(duplicate.relation_count(), 1);
		assert_eq!(duplicate.views.len(), 2);

		let repeated_inline = transform(
			"table(workspace.table(\"t\")); plot.bar(workspace.table(\"t\"), \"x\", \"y\")",
		)
		.expect("identical inline origins at different spans");
		assert_eq!(repeated_inline.relation_count(), 1);
		assert_eq!(repeated_inline.views.len(), 2);
	}

	#[test]
	fn named_views_are_optional_unique_and_do_not_change_sink_form() {
		let source = "workspace.table(\"t\").views([view(\"points\", scatter(\"x\", \"y\")), table(), view(\"bars\", bar(\"x\", \"y\"))])";
		let transformed = transform(source).expect("named and anonymous views normalize");
		assert_eq!(
			transformed.presentation_order(),
			&[
				LocalViewIdentity::Named("points".to_owned()),
				LocalViewIdentity::Anonymous(1),
				LocalViewIdentity::Named("bars".to_owned()),
			]
		);
		assert_eq!(transformed.views[0].intent.form(), RuneViewForm::Scatter);
		assert_eq!(transformed.views[2].intent.form(), RuneViewForm::Bar);

		let duplicate = "workspace.table(\"t\").views([view(\"same\", table())]); workspace.table(\"u\").views([view(\"same\", bar(\"x\", \"y\"))])";
		let error = transform(duplicate).expect_err("duplicate names refuse");
		let RuneEnvelopeError::DuplicateName {
			name,
			first,
			duplicate: second,
		} = error
		else {
			panic!("expected duplicate name refusal");
		};
		assert_eq!(name, "same");
		assert_eq!(
			&duplicate[first.start as usize..first.end as usize],
			"\"same\""
		);
		assert_eq!(
			&duplicate[second.start as usize..second.end as usize],
			"\"same\""
		);
	}

	#[test]
	fn a_view_before_a_non_view_statement_is_now_an_output_root() {
		// plans/product/impl/0003_typed_output_root_discovery.md deliberately
		// reverses the former trailing-contiguity assertion.
		let transformed = transform(
			"let r = workspace.table(\"test2\"); table(r); let marker = 1; plot.bar(r, \"cat\", \"val\")",
		)
		.expect("position-independent roots");
		assert_eq!(transformed.views.len(), 2);
		assert_eq!(transformed.views[0].intent.form(), RuneViewForm::Table);
		assert_eq!(transformed.views[1].intent.form(), RuneViewForm::Bar);
		assert!(!transformed.source.contains("table(r)"));
		assert!(transformed.source.contains("let marker = 1"));
	}

	#[test]
	fn shadowed_binding_and_direct_call_collisions_remain_ordinary_rune() {
		let binding_collision = "fn table(x) { x }\nlet n = table(1);\nlet r = workspace.table(\"t\");\nplot.bar(r, \"cat\", \"val\");";
		let transformed = transform(binding_collision).expect("plot establishes envelope");
		assert_eq!(transformed.views.len(), 1);
		assert_eq!(transformed.views[0].intent.form(), RuneViewForm::Bar);
		assert!(transformed.source.contains("fn table(x) { x }"));
		assert!(transformed.source.contains("let n = table(1);"));

		let direct_collision = "fn table(x) { x }\n\ntable(1);\nlet answer = 42;\nanswer";
		assert_eq!(
			transform(direct_collision),
			Err(RuneEnvelopeError::NoTerminalView),
			"a user-defined table call must still reach the local Rune host"
		);
	}

	#[test]
	fn shadowing_is_per_form_and_collected_before_root_classification() {
		let plot_shadowed = transform(
			"let plot = 1; let r = workspace.table(\"t\"); plot.bar(r, \"x\", \"y\"); table(r)",
		)
		.expect("table remains unshadowed");
		assert_eq!(plot_shadowed.views.len(), 1);
		assert_eq!(plot_shadowed.views[0].intent.form(), RuneViewForm::Table);
		assert!(plot_shadowed.source.contains("plot.bar"));

		let table_shadowed = transform(
			"fn table(x) { x } let r = workspace.table(\"t\"); table(r); plot.bar(r, \"x\", \"y\")",
		)
		.expect("plot remains unshadowed");
		assert_eq!(table_shadowed.views.len(), 1);
		assert_eq!(table_shadowed.views[0].intent.form(), RuneViewForm::Bar);
		assert!(table_shadowed.source.contains("table(r)"));

		let later_shadow = transform(
			"let r = workspace.table(\"t\"); table(r); let table = 1; plot.bar(r, \"x\", \"y\")",
		)
		.expect("later table binding suppresses earlier candidate");
		assert_eq!(later_shadow.views.len(), 1);
		assert_eq!(later_shadow.views[0].intent.form(), RuneViewForm::Bar);
		assert!(later_shadow.source.contains("table(r)"));

		let positive =
			transform("let r = workspace.table(\"t\"); table(r); plot.bar(r, \"x\", \"y\")")
				.expect("both unshadowed forms");
		assert_eq!(positive.views.len(), 2);
	}

	#[test]
	fn non_root_presentation_is_partitioned_and_wrong_arity_refuses() {
		let source = "fn helper(r) { table(r); }\nlet r = workspace.table(\"t\");\ntable(r)";
		let transformed = transform(source).expect("only the final table is a root");
		assert_eq!(transformed.views.len(), 1);
		assert_eq!(transformed.views[0].intent.form(), RuneViewForm::Table);
		assert!(!transformed.source.contains("fn helper"));
		assert!(matches!(
			transform("plot.bar(workspace.table(\"t\"), \"x\")"),
			Err(RuneEnvelopeError::InvalidView { .. })
		));
		assert!(matches!(
			transform("let r = workspace.table(\"t\"); let v = table(r); table(r)"),
			Err(RuneEnvelopeError::InvalidView { .. })
		));
	}

	#[test]
	fn semantic_equality_ignores_ordinary_order_but_preserves_root_order() {
		let ordinary_first = envelope(
			"let marker = 1; let r = workspace.table(\"t\"); table(r); plot.bar(r, \"x\", \"y\")",
		);
		let relation_first = envelope(
			"let r = workspace.table(\"t\"); let marker = 1; table(r); plot.bar(r, \"x\", \"y\")",
		);
		assert_eq!(ordinary_first.semantics(), relation_first.semantics());

		let roots_reordered =
			envelope("let r = workspace.table(\"t\"); plot.bar(r, \"x\", \"y\"); table(r)");
		assert_ne!(ordinary_first.semantics(), roots_reordered.semantics());
	}

	#[test]
	fn product_0008_direct_factored_and_legacy_forms_share_one_semantics() {
		let direct = envelope(
			"workspace.table(\"t\").sort([asc(col(\"c\"))]).views([scatter(\"c\",\"v\"), bar(\"c\",\"v\")])",
		);
		let relation_bound = envelope(
			"let r = workspace.table(\"t\").sort([asc(col(\"c\"))]); r.views([scatter(\"c\",\"v\"), bar(\"c\",\"v\")])",
		);
		let view_bound = envelope(
			"let v = workspace.table(\"t\").sort([asc(col(\"c\"))]).views([scatter(\"c\",\"v\")]); v.views([bar(\"c\",\"v\")])",
		);
		let legacy = envelope(
			"let r = workspace.table(\"t\").sort([asc(col(\"c\"))]); plot.scatter(r, \"c\", \"v\"); plot.bar(r, \"c\", \"v\")",
		);

		assert_ne!(direct, relation_bound);
		assert_ne!(relation_bound, view_bound);
		assert_eq!(direct.semantics(), relation_bound.semantics());
		assert_eq!(direct.semantics(), view_bound.semantics());
		assert_eq!(direct.semantics(), legacy.semantics());

		let factored_source = "let v = workspace.table(\"t\").sort([asc(col(\"c\"))]).views([scatter(\"c\",\"v\")]); v.views([bar(\"c\",\"v\")])";
		let rendered = transform(factored_source).expect("factored view set lowers");
		assert!(!rendered.source.contains(".views"));
		assert!(!rendered.source.contains("scatter("));
		assert!(!rendered.source.contains("bar("));
		assert_eq!(rendered.views.len(), 2);
		let fallback = rendered.source_map.diagnostic_fallback();
		assert_eq!(
			&factored_source[fallback.start as usize..fallback.end as usize],
			"workspace.table(\"t\").sort([asc(col(\"c\"))])",
			"fallback stays on the relation receiver inside the peeled binding"
		);
		assert_eq!(
			&[
				&factored_source
					[rendered.views[0].span.start as usize..rendered.views[0].span.end as usize],
				&factored_source
					[rendered.views[1].span.start as usize..rendered.views[1].span.end as usize],
			],
			&["scatter(\"c\",\"v\")", "bar(\"c\",\"v\")"],
			"each sink keeps its constructor span even when its binding is peeled"
		);
	}

	#[test]
	fn ket_is_an_exact_workspace_alias_for_relation_provenance() {
		let workspace = envelope(
			"let r = workspace.table(\"t\").sort([asc(col(\"c\"))]); r.views([scatter(\"c\",\"v\"), bar(\"c\",\"v\")])",
		);
		let ket = envelope(
			"let r = ket.table(\"t\").sort([asc(col(\"c\"))]); r.views([scatter(\"c\",\"v\"), bar(\"c\",\"v\")])",
		);
		let legacy = envelope(
			"let r = ket.table(\"t\").sort([asc(col(\"c\"))]); plot.scatter(r, \"c\", \"v\"); plot.bar(r, \"c\", \"v\")",
		);

		assert_eq!(ket.semantics(), workspace.semantics());
		assert_eq!(legacy.semantics(), workspace.semantics());
		let rendered =
			transform("ket.table(\"t\").views([scatter(\"c\", \"v\"), bar(\"c\", \"v\")])")
				.expect("ket relation root lowers");
		assert!(rendered.source.starts_with(KET_ENTRYPOINT_PREFIX));
		assert!(rendered.source.contains("\nket.table(\"t\")\n}"));
	}

	#[test]
	fn a_shadowed_ket_is_ordinary_rune_without_shadowing_workspace() {
		let shadowed = "let ket = local; ket.table(\"t\").views([table()])";
		assert_eq!(transform(shadowed), Err(RuneEnvelopeError::NoTerminalView));

		let workspace_remains =
			envelope("let ket = local; workspace.table(\"t\").views([table()])");
		assert_eq!(
			workspace_remains.semantics().sinks,
			vec![RuneViewIntent::Table(RuneTableSink::default())]
		);
	}

	#[test]
	fn product_0008_single_view_sugars_are_exact_canonical_projections() {
		for (sugar, canonical, legacy) in [
			(
				"workspace.table(\"t\").scatter(\"c\", \"v\")",
				"workspace.table(\"t\").views([scatter(\"c\", \"v\")])",
				"plot.scatter(workspace.table(\"t\"), \"c\", \"v\")",
			),
			(
				"workspace.table(\"t\").bar(\"c\", \"v\")",
				"workspace.table(\"t\").views([bar(\"c\", \"v\")])",
				"plot.bar(workspace.table(\"t\"), \"c\", \"v\")",
			),
			(
				"workspace.table(\"t\").table()",
				"workspace.table(\"t\").views([table()])",
				"table(workspace.table(\"t\"))",
			),
		] {
			assert_eq!(envelope(sugar).semantics(), envelope(canonical).semantics());
			assert_eq!(envelope(sugar).semantics(), envelope(legacy).semantics());
		}
	}

	#[test]
	fn product_0012_line_spellings_are_exact_canonical_projections() {
		let sugar = envelope("workspace.table(\"t\").line(\"x\", \"y\")");
		let canonical = envelope("workspace.table(\"t\").views([line(\"x\", \"y\")])");
		let legacy = envelope("plot.line(workspace.table(\"t\"), \"x\", \"y\")");

		assert_eq!(sugar.semantics(), canonical.semantics());
		assert_eq!(sugar.semantics(), legacy.semantics());
		assert_eq!(sugar.semantics().sinks[0].form(), RuneViewForm::Line);
	}

	#[test]
	fn product_0012_line_recognition_is_contextual_shadowed_and_atomic() {
		let ordinary = "fn line(x, y) { x + y } let value = line(1, 2); value";
		assert_eq!(transform(ordinary), Err(RuneEnvelopeError::NoTerminalView));

		let plot_shadowed = "let plot = local; plot.line(workspace.table(\"t\"), \"x\", \"y\")";
		assert_eq!(
			transform(plot_shadowed),
			Err(RuneEnvelopeError::NoTerminalView)
		);

		for source in [
			"workspace.table(\"t\").views([line(\"x\")])",
			"workspace.table(\"t\").views([line(\"x\", \"y\", \"series\", \"extra\")])",
			"workspace.table(\"t\").views([line(x, \"y\")])",
			"workspace.table(\"t\").line(\"x\")",
			"plot.line(workspace.table(\"t\"), \"x\")",
			"plot.line(workspace.table(\"t\"), x, \"y\")",
		] {
			assert!(
				matches!(
					transform(source),
					Err(RuneEnvelopeError::InvalidView { .. })
				),
				"malformed line must refuse at its sink: {source}"
			);
		}

		let partial = "workspace.table(\"t\").views([table(), line(\"x\")])";
		assert!(matches!(
			transform(partial),
			Err(RuneEnvelopeError::InvalidView { .. })
		));
	}

	#[test]
	fn product_0054_series_line_spellings_project_one_named_color_channel() {
		let sources = [
			"workspace.table(\"t\").line(\"x\", \"y\", \"series\")",
			"workspace.table(\"t\").views([line(\"x\", \"y\", \"series\")])",
			"plot.line(workspace.table(\"t\"), \"x\", \"y\", \"series\")",
		];
		let semantics = sources.map(|source| envelope(source).semantics());
		assert_eq!(semantics[0], semantics[1]);
		assert_eq!(semantics[0], semantics[2]);
		for semantics in semantics {
			let RuneViewIntent::Plot(plot) = &semantics.sinks[0] else {
				panic!("series line must remain a plot sink")
			};
			let RunePlotBody::Single(line) = &plot.body else {
				panic!("unmodified series line must remain a single mark")
			};
			assert_eq!(line.composition.primary_plot_type(), PlotType::Line);
			assert_eq!((&line.x, &line.y), (&"x".to_owned(), &"y".to_owned()));
			assert_eq!(line.category.as_deref(), Some("series"));
		}
	}

	#[test]
	fn product_0054_series_line_reaches_named_figure_and_native_modifier_positions() {
		for source in [
			"workspace.table(\"t\").views([view(\"trend\", line(\"x\", \"y\", \"series\"))])",
			"workspace.table(\"t\").views([figure([line(\"x\", \"y\", \"series\")])])",
		] {
			let transformed = transform(source).expect("series line position");
			let RuneViewIntent::Plot(plot) = &transformed.views[0].intent else {
				panic!("series line must remain a plot")
			};
			let RunePlotBody::Single(line) = &plot.body else {
				panic!("unmodified series line must remain a single mark")
			};
			assert_eq!(line.category.as_deref(), Some("series"));
		}

		let styled = transform(
			"workspace.table(\"t\").views([figure([line(\"x\", \"y\", \"series\").stroke_width(2.0)])])",
		)
		.expect("styled series line");
		let RuneViewIntent::Plot(plot) = &styled.views[0].intent else {
			panic!("styled series line must remain a plot")
		};
		let RunePlotBody::StyledSingle(styled) = &plot.body else {
			panic!("series modifier must be retained")
		};
		assert_eq!(styled.sink.category.as_deref(), Some("series"));
		assert_eq!(
			styled.settings.stroke_width,
			Override::Set(StrokeWidth::new(2.0).unwrap())
		);
	}

	#[test]
	fn product_0054_series_line_refusals_keep_layers_closed_and_rune_contextual() {
		for source in [
			"workspace.table(\"t\").views([line(\"x\", \"y\", \"series\", \"extra\")])",
			"workspace.table(\"t\").views([line(\"x\", \"y\", series)])",
			"workspace.table(\"t\").views([layers([line(\"x\", \"y\", \"series\")])])",
		] {
			assert!(
				matches!(
					transform(source),
					Err(RuneEnvelopeError::InvalidView { .. })
				),
				"malformed series line must refuse: {source}"
			);
		}

		let source = r#"
let held = line("x", "y", "series");
consume(line("a", "b", "group"));
workspace.table("t").views([table()])
"#;
		let error = transform(source).expect_err("presentation calls cannot leak through consume");
		assert!(error.to_string().contains("consume"), "{error}");
	}

	#[test]
	fn product_0014_layers_lower_in_authored_order() {
		fn lowered(source: &str) -> LayeredPlotSpec {
			let transformed = transform(source).expect("layers envelope");
			let RuneViewIntent::Plot(plot) = &transformed.views[0].intent else {
				panic!("expected layered plot intent")
			};
			let RunePlotBody::Layered(specification) = &plot.body else {
				panic!("expected layered plot intent")
			};
			specification.clone()
		}

		let line_scatter = lowered(
			"workspace.table(\"t\").views([layers([line(\"x\", \"y\"), scatter(\"x\", \"y\")])])",
		);
		assert_eq!(
			line_scatter
				.layers
				.iter()
				.map(|layer| layer.mark)
				.collect::<Vec<_>>(),
			[LayerMark::Line, LayerMark::Scatter]
		);
		assert_eq!(line_scatter.title(), "Line + Scatter");

		let scatter_line = lowered(
			"workspace.table(\"t\").views([layers([scatter(\"x\", \"y\"), line(\"x\", \"y\")])])",
		);
		assert_eq!(
			scatter_line
				.layers
				.iter()
				.map(|layer| layer.mark)
				.collect::<Vec<_>>(),
			[LayerMark::Scatter, LayerMark::Line]
		);
	}

	#[test]
	fn product_0014_layer_depth_and_admissible_marks_refuse_at_the_inner_span() {
		for (source, rejected) in [
			("workspace.table(\"t\").views([layers([])])", "layers([])"),
			(
				"workspace.table(\"t\").views([layers([layers([line(\"x\", \"y\")])])])",
				"layers([line(\"x\", \"y\")])",
			),
			(
				"workspace.table(\"t\").views([layers([table()])])",
				"table()",
			),
			(
				"workspace.table(\"t\").views([layers([bar(\"c\", \"v\")])])",
				"bar(\"c\", \"v\")",
			),
		] {
			let RuneEnvelopeError::InvalidView { span, .. } =
				transform(source).expect_err("invalid layer must refuse")
			else {
				panic!("expected an attributed view refusal")
			};
			assert_eq!(&source[span.start as usize..span.end as usize], rejected);
		}
	}

	#[test]
	fn product_0014_scatter_arity_is_contextual() {
		let source = "workspace.table(\"t\").views([layers([scatter(\"x\", \"y\", \"cat\")])])";
		let RuneEnvelopeError::InvalidView { span, .. } =
			transform(source).expect_err("layer color syntax must refuse")
		else {
			panic!("expected an attributed view refusal")
		};
		assert_eq!(
			&source[span.start as usize..span.end as usize],
			"scatter(\"x\", \"y\", \"cat\")"
		);

		let control = transform("workspace.table(\"t\").views([scatter(\"x\", \"y\", \"cat\")])")
			.expect("the established scatter view arity remains legal");
		let RuneViewIntent::Plot(scatter) = &control.views[0].intent else {
			panic!("expected ordinary scatter sink")
		};
		let RunePlotBody::Single(scatter) = &scatter.body else {
			panic!("expected ordinary scatter sink")
		};
		assert_eq!(scatter.category.as_deref(), Some("cat"));
	}

	#[test]
	fn product_0076_layers_outside_a_slot_cannot_leak_to_toron() {
		let source = r#"
layers([line("x", "y")]);
let held = layers([scatter("x", "y")]);
consume(layers([line("x", "y")]));
workspace.table("t").views([table()])
"#;
		let error = transform(source).expect_err("unknown local consumer refuses before Toron");
		assert!(error.to_string().contains("consume"), "{error}");
	}

	#[test]
	fn product_0016_figure_lowers_body_and_headings_independently() {
		let source = r#"workspace.table("t").views([figure([
			title("Revenue"),
			subtitle("Trailing twelve months"),
			layers([line("month", "revenue"), scatter("month", "revenue")]),
		])])"#;
		let transformed = transform(source).expect("figure envelope");
		let [view] = transformed.views.as_slice() else {
			panic!("one figure remains one view");
		};
		let RuneViewIntent::Plot(plot) = &view.intent else {
			panic!("figure must route as a plot");
		};
		let RunePlotBody::Layered(specification) = &plot.body else {
			panic!("layers body stays layered");
		};
		assert_eq!(
			specification
				.layers
				.iter()
				.map(|layer| layer.mark)
				.collect::<Vec<_>>(),
			[LayerMark::Line, LayerMark::Scatter]
		);
		let mut expected = PlotSettingsLayer::default();
		expected.title = Presence::Show(Arc::new("Revenue".to_owned()));
		expected.subtitle = Presence::Show(Arc::new("Trailing twelve months".to_owned()));
		assert_eq!(plot.authored_settings, expected);
		assert_eq!(view.intent.presentation().display_label, "Line + Scatter");
		assert_eq!(view.intent.presentation().authored_title, expected.title);
	}

	#[test]
	fn product_0125_legend_member_preserves_presence_and_composes_with_size() {
		let source = r#"
let legend_x = 1.5;
let legend_y = -0.5;
workspace.table("t").views([figure([
	legend().at(legend_x, legend_y).font_size(24),
	line("x", "y"),
])])
"#;
		let transformed = transform(source).expect("authored legend position");
		let RuneViewIntent::Plot(plot) = &transformed.views[0].intent else {
			panic!("expected plot");
		};
		let Presence::Show(position) = plot.authored_settings.legend else {
			panic!("legend().at must lower to Show");
		};
		assert_eq!((position.x(), position.y()), (1.5, -0.5));
		assert_eq!(
			plot.authored_settings.legend_size,
			Override::Set(crate::plot::spec::LegendTextSize::new(24.0).unwrap())
		);

		let hidden = transform(
			"workspace.table(\"t\").views([figure([legend().hide(), line(\"x\", \"y\")])])",
		)
		.expect("hidden legend");
		let RuneViewIntent::Plot(hidden) = &hidden.views[0].intent else {
			panic!("expected plot");
		};
		assert!(matches!(hidden.authored_settings.legend, Presence::Hide));
	}

	#[test]
	fn product_0125_legend_members_refuse_ambiguity_and_undefined_values_at_source() {
		for (source, rejected) in [
			(
				"workspace.table(\"t\").views([figure([legend().at(0.1, 0.2), legend().hide(), line(\"x\", \"y\")])])",
				".hide()",
			),
			(
				"workspace.table(\"t\").views([figure([legend().hide(), legend().at(0.1, 0.2), line(\"x\", \"y\")])])",
				".at(0.1, 0.2)",
			),
			(
				"workspace.table(\"t\").views([figure([legend().at(0.1, 0.2), legend().at(0.3, 0.4), line(\"x\", \"y\")])])",
				".at(0.3, 0.4)",
			),
			(
				"workspace.table(\"t\").views([figure([legend().hide(), legend().hide(), line(\"x\", \"y\")])])",
				".hide()",
			),
			(
				"workspace.table(\"t\").views([figure([legend().font_size(12), legend().font_size(18), line(\"x\", \"y\")])])",
				".font_size(18)",
			),
		] {
			let RuneEnvelopeError::InvalidView { span, message } =
				transform(source).expect_err("ambiguous legend property must refuse")
			else {
				panic!("expected attributed legend refusal");
			};
			assert_eq!(&source[span.start as usize..span.end as usize], rejected);
			assert!(message.contains("may appear at most once"));
		}

		for (member, expected) in [
			("legend()", "requires .at(...), .hide(), or .font_size(...)"),
			("legend().hide(1)", "takes no arguments"),
			("legend().at(0.5)", "requires exactly two numeric values"),
			("legend().at(1e999, 0.5)", "requires finite positions"),
			("legend().at(value, 0.5)", "No local variable"),
			("title(\"T\").at(0.5, 0.2)", "requires a legend() receiver"),
		] {
			let source =
				format!("workspace.table(\"t\").views([figure([{member}, line(\"x\", \"y\")])])");
			let RuneEnvelopeError::InvalidView { message, .. } =
				transform(&source).expect_err("undefined legend property must refuse")
			else {
				panic!("expected attributed legend refusal");
			};
			assert!(message.contains(expected), "{message}");
		}
	}

	#[test]
	fn product_0125_legend_member_is_ordinary_rune_outside_a_figure_member() {
		let source = r#"
legend().at(0.98, 0.02);
let hidden = legend().hide();
consume(legend().at(1.5, -0.5));
workspace.table("t").views([table()])
"#;
		let error = transform(source).expect_err("presentation calls cannot leak through consume");
		assert!(error.to_string().contains("consume"), "{error}");
	}

	#[test]
	fn product_0042_facet_is_structural_figure_metadata_and_resolves_bindings() {
		let source = r#"
let field = "region";
workspace.table("t").views([figure([
		facet(field),
		title("Revenue"),
		bar("region", "revenue"),
	])])"#;
		let transformed = transform(source).expect("facet figure envelope");
		let RuneViewIntent::Plot(plot) = &transformed.views[0].intent else {
			panic!("facet must route as a plot");
		};
		assert_eq!(plot.facet.as_deref(), Some("region"));
		assert!(plot.facet_span.is_some());

		for source in [
			"workspace.table(\"t\").views([figure([facet(\"region\")])])",
			"workspace.table(\"t\").views([figure([facet(\"region\"), facet(\"other\"), bar(\"region\", \"revenue\")])])",
			"workspace.table(\"t\").views([figure([layers([facet(\"region\"), line(\"x\", \"y\")])])])",
			"workspace.table(\"t\").views([figure([facet(\"region\")])])",
		] {
			assert!(
				transform(source).is_err(),
				"accepted malformed facet: {source}"
			);
		}
	}

	#[test]
	fn product_0036_figure_bounds_lower_once_as_one_domain_override() {
		let source = r#"workspace.table("t").views([figure([
			y_axis().bounds(-1.0, 10.0),
			title("Bounded"),
			x_axis().bounds(0.0, 20.0),
			layers([line("x", "y"), scatter("x", "y")]),
		])])"#;
		let transformed = transform(source).expect("bounded figure envelope");
		let RuneViewIntent::Plot(plot) = &transformed.views[0].intent else {
			panic!("figure must route as a plot");
		};
		assert_eq!(
			plot.authored_settings.domains,
			Override::Set(PlotDomainOverrides {
				x_min: Some(0.0),
				x_max: Some(20.0),
				y_min: Some(-1.0),
				y_max: Some(10.0),
			})
		);
		assert_eq!(
			&source[plot.domain_spans.x.clone().unwrap().start as usize
				..plot.domain_spans.x.clone().unwrap().end as usize],
			".bounds(0.0, 20.0)"
		);
		assert_eq!(
			&source[plot.domain_spans.y.clone().unwrap().start as usize
				..plot.domain_spans.y.clone().unwrap().end as usize],
			".bounds(-1.0, 10.0)"
		);
		let RunePlotBody::Layered(specification) = &plot.body else {
			panic!("bounds must not change the layered body");
		};
		assert_eq!(specification.layers.len(), 2);
		assert!(
			specification
				.layers
				.iter()
				.all(|layer| layer.settings == LayerSettings::default())
		);
	}

	#[test]
	fn product_0036_negative_bounds_and_omission_are_exact() {
		let negative = transform(
			"workspace.table(\"t\").views([figure([y_axis().bounds(-1.0, 1.0), bar(\"cat\", \"val\")])])",
		)
		.expect("unary-negated bound");
		let RuneViewIntent::Plot(negative) = &negative.views[0].intent else {
			panic!("bounded bar");
		};
		assert_eq!(
			negative.authored_settings.domains,
			Override::Set(PlotDomainOverrides {
				x_min: None,
				x_max: None,
				y_min: Some(-1.0),
				y_max: Some(1.0),
			})
		);

		let omitted = transform("workspace.table(\"t\").views([figure([line(\"x\", \"y\")])])")
			.expect("ordinary unbounded figure");
		let RuneViewIntent::Plot(omitted) = &omitted.views[0].intent else {
			panic!("ordinary figure");
		};
		assert_eq!(omitted.authored_settings.domains, Override::Inherit);
		assert_eq!(omitted.domain_spans, RunePlotDomainSpans::default());
	}

	#[test]
	fn product_0036_undefined_bounds_refuse_at_the_exact_constructor() {
		for (bounds, expected) in [
			(".bounds(1e999, 2.0)", "requires finite bounds"),
			(".bounds(10.0, 0.0)", "cannot reverse an axis"),
			(".bounds(5.0, 5.0)", "cannot form a collapsed axis"),
			(".bounds(value, 2.0)", "No local variable"),
			(".bounds(0.0)", "requires exactly two numeric values"),
		] {
			let source = format!(
				"workspace.table(\"t\").views([figure([x_axis(){bounds}, line(\"x\", \"y\")])])"
			);
			let RuneEnvelopeError::InvalidView { span, message } =
				transform(&source).expect_err("undefined bounds must refuse")
			else {
				panic!("expected attributed bounds refusal");
			};
			assert_eq!(
				&source[span.start as usize..span.end as usize],
				if bounds.contains("value") {
					"value"
				} else {
					bounds
				}
			);
			assert!(message.contains(expected), "{message}");
		}

		let duplicate = "workspace.table(\"t\").views([figure([y_axis().bounds(0.0, 1.0), y_axis().bounds(2.0, 3.0), line(\"x\", \"y\")])])";
		let RuneEnvelopeError::InvalidView { span, message } =
			transform(duplicate).expect_err("duplicate bounds must refuse")
		else {
			panic!("expected duplicate refusal");
		};
		assert_eq!(
			&duplicate[span.start as usize..span.end as usize],
			".bounds(2.0, 3.0)"
		);
		assert!(message.contains("y_axis().bounds(...) may appear at most once"));
	}

	#[test]
	fn product_0038_tick_steps_lower_once_and_keep_axes_independent() {
		for (literal, expected) in [("1", 1.0), ("1.0", 1.0)] {
			let source = format!(
				"workspace.table(\"t\").views([figure([y_axis().bounds(0, 7), y_axis().tick_step({literal}), bar(\"cat\", \"val\")])])"
			);
			let transformed = transform(&source).expect("tick step figure");
			let RuneViewIntent::Plot(plot) = &transformed.views[0].intent else {
				panic!("expected plot");
			};
			let Override::Set(guides) = &plot.authored_settings.guides else {
				panic!("expected guide override");
			};
			assert_eq!(
				guides.y_tick_placement,
				TickPlacement::step(expected).unwrap()
			);
			assert_eq!(guides.x_tick_placement, TickPlacement::DEFAULT);
			assert_eq!(
				&source[plot.tick_step_spans.y.clone().unwrap().start as usize
					..plot.tick_step_spans.y.clone().unwrap().end as usize],
				format!(".tick_step({literal})")
			);
		}

		let no_bounds = transform(
			"workspace.table(\"t\").views([figure([y_axis().tick_step(1), line(\"x\", \"y\")])])",
		)
		.expect("data-derived step figure");
		let RuneViewIntent::Plot(plot) = &no_bounds.views[0].intent else {
			panic!("expected plot");
		};
		assert!(plot.domain_spans.x.is_none() && plot.domain_spans.y.is_none());
		assert_eq!(plot.tick_step_spans.x, None);
		assert!(plot.tick_step_spans.y.is_some());
	}

	#[test]
	fn product_0038_tick_step_refusals_are_value_and_span_attributed() {
		for (constructor, expected) in [
			(".tick_step(0)", "step must be strictly positive"),
			(".tick_step(-1)", "step must be strictly positive"),
			(".tick_step(1e999)", "requires a finite step"),
			(".tick_step(value)", "No local variable"),
			(".tick_step()", "requires exactly one numeric value"),
		] {
			let source = format!(
				"workspace.table(\"t\").views([figure([y_axis(){constructor}, line(\"x\", \"y\")])])"
			);
			let RuneEnvelopeError::InvalidView { span, message } =
				transform(&source).expect_err("invalid tick step")
			else {
				panic!("expected attributed tick-step refusal");
			};
			assert_eq!(
				&source[span.start as usize..span.end as usize],
				if constructor.contains("value") {
					"value"
				} else {
					constructor
				}
			);
			assert!(message.contains(expected), "{message}");
		}
		let duplicate = "workspace.table(\"t\").views([figure([y_axis().tick_step(1), y_axis().tick_step(2), line(\"x\", \"y\")])])";
		let RuneEnvelopeError::InvalidView { span, message } =
			transform(duplicate).expect_err("duplicate tick step")
		else {
			panic!("expected duplicate tick-step refusal");
		};
		assert_eq!(
			&duplicate[span.start as usize..span.end as usize],
			".tick_step(2)"
		);
		assert!(message.contains("y_axis().tick_step(...) may appear at most once"));
	}

	#[test]
	fn product_0039_minor_ticks_lower_as_independent_counts() {
		for (literal, expected) in [("0", 0), ("1", 1), ("1.0", 1), ("4294967295", u32::MAX)] {
			let source = format!(
				"workspace.table(\"t\").views([figure([x_axis().minor_ticks({literal}), y_axis().minor_ticks({literal}), bar(\"cat\", \"val\")])])"
			);
			let transformed = transform(&source).expect("minor tick figure");
			let RuneViewIntent::Plot(plot) = &transformed.views[0].intent else {
				panic!("expected plot");
			};
			let Override::Set(guides) = &plot.authored_settings.guides else {
				panic!("expected guide override");
			};
			assert_eq!(guides.x_minor_ticks, expected);
			assert_eq!(guides.y_minor_ticks, expected);
		}

		let source = "workspace.table(\"t\").views([figure([y_axis().minor_ticks(1), bar(\"cat\", \"val\")])])";
		let transformed = transform(source).expect("single minor axis");
		let RuneViewIntent::Plot(plot) = &transformed.views[0].intent else {
			panic!("expected plot");
		};
		assert!(plot.minor_tick_spans.x.is_none());
		assert_eq!(
			&source[plot.minor_tick_spans.y.clone().unwrap().start as usize
				..plot.minor_tick_spans.y.clone().unwrap().end as usize],
			".minor_ticks(1)"
		);
	}

	#[test]
	fn product_0039_minor_tick_value_refusals_are_attributed() {
		for (constructor, expected) in [
			(".minor_ticks(-1)", "count must be a non-negative integer"),
			(".minor_ticks(2.5)", "count must be a non-negative integer"),
			(".minor_ticks(1e999)", "requires a finite count"),
			(".minor_ticks(value)", "No local variable"),
			(".minor_ticks()", "requires exactly one numeric value"),
		] {
			let source = format!(
				"workspace.table(\"t\").views([figure([y_axis(){constructor}, line(\"x\", \"y\")])])"
			);
			let RuneEnvelopeError::InvalidView { span, message } =
				transform(&source).expect_err("invalid minor count must refuse")
			else {
				panic!("expected invalid-view refusal");
			};
			assert_eq!(
				&source[span.start as usize..span.end as usize],
				if constructor.contains("value") {
					"value"
				} else {
					constructor
				}
			);
			assert!(message.contains(expected), "{message}");
		}

		let duplicate = "workspace.table(\"t\").views([figure([y_axis().minor_ticks(1), y_axis().minor_ticks(2), line(\"x\", \"y\")])])";
		let RuneEnvelopeError::InvalidView { span, message } =
			transform(duplicate).expect_err("duplicate minor count must refuse")
		else {
			panic!("expected duplicate refusal");
		};
		assert_eq!(
			&duplicate[span.start as usize..span.end as usize],
			".minor_ticks(2)"
		);
		assert!(message.contains("y_axis().minor_ticks(...) may appear at most once"));
	}

	#[test]
	fn product_0040_static_literal_bindings_feed_every_figure_element() {
		let source = r#"
let title_text = "Bound title";
let title_alias = title_text;
let subtitle_text = "Bound subtitle";
let skin_name = "magma_r";
let palette_name = "terrain";
let minimum = 0;
let maximum = 7;
let major_step = 1;
let minor_count = 2;
workspace.table("t").views([figure([
    title(title_alias),
    subtitle(subtitle_text),
    skin(skin_name),
    palette(palette_name),
    y_axis().bounds(minimum, maximum),
    y_axis().tick_step(major_step),
    y_axis().minor_ticks(minor_count),
    bar("cat", "val"),
])])
"#;
		let transformed = transform(source).expect("static literal bindings");
		let RuneViewIntent::Plot(plot) = &transformed.views[0].intent else {
			panic!("expected plot");
		};
		assert_eq!(
			plot.authored_settings.title,
			Presence::Show(Arc::new("Bound title".to_owned()))
		);
		assert_eq!(
			plot.authored_settings.subtitle,
			Presence::Show(Arc::new("Bound subtitle".to_owned()))
		);
		assert_eq!(
			plot.authored_settings.plot_skin,
			Override::Set(PlotSkinSelection::new(
				"magma_r".to_owned(),
				PlotChromeProjection::Ordered,
			))
		);
		assert_eq!(
			plot.authored_settings.categorical_palette,
			Override::Set(Arc::new("terrain".to_owned()))
		);
		assert_eq!(
			plot.authored_settings.domains,
			Override::Set(PlotDomainOverrides {
				x_min: None,
				x_max: None,
				y_min: Some(0.0),
				y_max: Some(7.0),
			})
		);
		let Override::Set(guides) = &plot.authored_settings.guides else {
			panic!("expected guide override");
		};
		assert_eq!(guides.y_tick_placement, TickPlacement::step(1.0).unwrap());
		assert_eq!(guides.y_minor_ticks, 2);

		for (source, expected) in [
			(
				"let title_text = make_title(); workspace.table(\"t\").views([figure([title(title_text), line(\"x\", \"y\")])])",
				"Missing item make_title",
			),
			(
				"let step = -1; workspace.table(\"t\").views([figure([y_axis().tick_step(step), line(\"x\", \"y\")])])",
				"step must be strictly positive",
			),
			(
				"let maximum = 0; workspace.table(\"t\").views([figure([y_axis().bounds(1, maximum), line(\"x\", \"y\")])])",
				"cannot reverse an axis",
			),
		] {
			let RuneEnvelopeError::InvalidView { message, .. } =
				transform(source).expect_err("static binding refusal")
			else {
				panic!("expected attributed static-binding refusal");
			};
			assert!(message.contains(expected), "{message}");
		}
	}

	#[test]
	fn product_0076_stage0_computed_figure_element_is_the_banked_red_boundary() {
		let source = r#"let yb = y_axis().bounds(0, 160);
workspace.table("demo_sales").views([figure([
    title("Grouped bars"),
    yb,
    bar("region", "revenue", "product"),
])])"#;
		let transformed = transform(source).expect("PRODUCT-0076 inverts the banked refusal");
		let RuneViewIntent::Plot(plot) = &transformed.views[0].intent else {
			panic!("computed figure remains a plot")
		};
		assert_eq!(
			plot.authored_settings.domains,
			Override::Set(PlotDomainOverrides {
				x_min: None,
				x_max: None,
				y_min: Some(0.0),
				y_max: Some(160.0),
			})
		);
	}

	#[test]
	fn product_0016_one_body_figure_is_the_same_view_as_its_bare_mark() {
		fn only_view(source: &str) -> SpannedRuneViewIntent {
			transform(source)
				.expect("one-view envelope")
				.into_relations()
				.next()
				.expect("one relation")
				.views
				.into_iter()
				.next()
				.expect("one view")
		}
		let bare = only_view("workspace.table(\"t\").views([view(\"trend\", line(\"x\", \"y\"))])");
		let figure = only_view(
			"workspace.table(\"t\").views([view(\"trend\", figure([line(\"x\", \"y\")]))])",
		);
		assert_eq!(bare.identity, figure.identity);
		assert_eq!(bare.intent, figure.intent);
		assert_eq!(bare.intent.presentation(), figure.intent.presentation());
	}

	#[test]
	fn product_0016_figure_refusals_are_inner_attributed_and_atomic() {
		for (source, rejected) in [
			("workspace.table(\"t\").views([figure()])", "figure()"),
			(
				"workspace.table(\"t\").views([figure([], [])])",
				"figure([], [])",
			),
			(
				"workspace.table(\"t\").views([figure([title(\"a\"), title(\"b\"), line(\"x\", \"y\")])])",
				"title(\"b\")",
			),
			(
				"workspace.table(\"t\").views([figure([subtitle(\"a\"), subtitle(\"b\"), line(\"x\", \"y\")])])",
				"subtitle(\"b\")",
			),
			(
				"workspace.table(\"t\").views([figure([title(\"only metadata\")])])",
				"figure([title(\"only metadata\")])",
			),
			(
				"workspace.table(\"t\").views([figure([line(\"x\", \"y\"), scatter(\"x\", \"y\")])])",
				"scatter(\"x\", \"y\")",
			),
			(
				"workspace.table(\"t\").views([figure([table()])])",
				"table()",
			),
			(
				"workspace.table(\"t\").views([figure([figure([line(\"x\", \"y\")])])])",
				"figure([line(\"x\", \"y\")])",
			),
			(
				"workspace.table(\"t\").views([figure([view(\"inner\", line(\"x\", \"y\"))])])",
				"view",
			),
			(
				"workspace.table(\"t\").views([figure([layers([title(\"wrong level\"), line(\"x\", \"y\")])])])",
				"title(\"wrong level\")",
			),
			(
				"let heading = title_text(); workspace.table(\"t\").views([figure([title(heading), line(\"x\", \"y\")])])",
				"title_text",
			),
			(
				"workspace.table(\"t\").views([figure([subtitle(), line(\"x\", \"y\")])])",
				"subtitle()",
			),
		] {
			let RuneEnvelopeError::InvalidView { span, .. } =
				transform(source).expect_err("malformed figure must refuse atomically")
			else {
				panic!("expected an attributed figure refusal")
			};
			assert_eq!(&source[span.start as usize..span.end as usize], rejected);
		}
		let computed = transform(
			"let parts = [line(\"x\", \"y\")]; workspace.table(\"t\").views([figure(parts)])",
		)
		.expect("PRODUCT-0076 executes a bound figure array");
		assert!(matches!(computed.views[0].intent, RuneViewIntent::Plot(_)));
	}

	#[test]
	fn product_0016_subtitle_only_is_legal_and_figure_is_contextual() {
		let source =
			"workspace.table(\"t\").views([figure([subtitle(\"Detail\"), line(\"x\", \"y\")])])";
		let transformed = transform(source).expect("subtitle-only figure");
		let RuneViewIntent::Plot(plot) = &transformed.views[0].intent else {
			panic!("figure routes as plot");
		};
		assert_eq!(plot.authored_settings.title, Presence::Inherit);
		assert_eq!(
			plot.authored_settings.subtitle,
			Presence::Show(Arc::new("Detail".to_owned()))
		);

		let ordinary = r#"
			figure([title("ordinary"), line("x", "y")]);
			let held = figure([subtitle("ordinary"), line("x", "y")]);
			workspace.table("t").views([table()])
		"#;
		let retained = transform(ordinary).expect("unclaimed presentation runs locally");
		assert!(!retained.source.contains("figure([title(\"ordinary\")"));
		assert_eq!(retained.views.len(), 1);
	}

	#[test]
	fn product_0030_palette_is_unordered_figure_metadata_and_identity_stays_structural() {
		fn only_plot(source: &str) -> SpannedRuneViewIntent {
			transform(source)
				.expect("categorical palette figure")
				.into_relations()
				.next()
				.expect("one relation")
				.views
				.into_iter()
				.next()
				.expect("one view")
		}

		let first = only_plot(
			r#"workspace.table("t").views([view("bars", figure([
				palette("tab20c"), title("Categories"), subtitle("Six"), bar("cat", "val")
			]))])"#,
		);
		let reordered = only_plot(
			r#"workspace.table("t").views([view("bars", figure([
				bar("cat", "val"), subtitle("Six"), palette("tab20c"), title("Categories")
			]))])"#,
		);
		assert_eq!(first.identity, reordered.identity);
		assert_eq!(first.intent, reordered.intent);
		let presentation = first.intent.presentation();
		let RuneViewIntent::Plot(plot) = first.intent else {
			panic!("figure must remain a plot intent")
		};
		assert_eq!(
			plot.authored_settings.categorical_palette,
			Override::Set(Arc::new("tab20c".to_owned()))
		);
		assert_eq!(presentation.display_label, "plot.bar");
	}

	#[test]
	fn product_0030_palette_refusals_are_contextual_attributed_and_atomic() {
		for (source, rejected) in [
			(
				"workspace.table(\"t\").views([figure([palette(\"a\"), palette(\"b\"), bar(\"cat\", \"val\")])])",
				"palette(\"b\")",
			),
			(
				"workspace.table(\"t\").views([figure([palette(), bar(\"cat\", \"val\")])])",
				"palette()",
			),
			(
				"workspace.table(\"t\").views([figure([palette(\"a\", \"b\"), bar(\"cat\", \"val\")])])",
				"palette(\"a\", \"b\")",
			),
			(
				"let p = make_palette(); workspace.table(\"t\").views([figure([palette(p), bar(\"cat\", \"val\")])])",
				"make_palette",
			),
			(
				"workspace.table(\"t\").views([figure([palette(\"tab20c\")])])",
				"figure([palette(\"tab20c\")])",
			),
			(
				"workspace.table(\"t\").views([figure([layers([palette(\"tab20c\"), line(\"x\", \"y\")])])])",
				"palette(\"tab20c\")",
			),
			(
				"workspace.table(\"t\").views([bar(\"cat\", \"val\").palette(\"tab20c\")])",
				"bar(\"cat\", \"val\").palette(\"tab20c\")",
			),
			(
				"workspace.table(\"t\").views([bar(\"cat\", \"val\").skin(\"tab20c\")])",
				"bar(\"cat\", \"val\").skin(\"tab20c\")",
			),
		] {
			let RuneEnvelopeError::InvalidView { span, .. } =
				transform(source).expect_err("malformed palette surface must refuse atomically")
			else {
				panic!("expected an attributed palette refusal")
			};
			assert_eq!(&source[span.start as usize..span.end as usize], rejected);
		}

		let ordinary = r#"
			palette("tab20c");
			let held = palette("tab20c_s1");
			figure([palette("ordinary"), bar("cat", "val")]);
			workspace.table("t").views([table()])
		"#;
		let retained = transform(ordinary).expect("unclaimed presentation runs locally");
		assert!(!retained.source.contains("palette(\"tab20c\");"));
		assert!(!retained.source.contains("palette(\"ordinary\")"));
		assert_eq!(retained.views.len(), 1);
	}

	#[test]
	fn product_0032_skin_is_unordered_direct_figure_metadata() {
		let only_plot = |source: &str| {
			transform(source)
				.expect("plot skin figure")
				.into_relations()
				.next()
				.unwrap()
				.views
				.into_iter()
				.next()
				.unwrap()
		};
		let first = only_plot(
			r#"workspace.table("t").views([view("bars", figure([
				skin("nord"), palette("tab20c"), title("Categories"), bar("cat", "val")
			]))])"#,
		);
		let reordered = only_plot(
			r#"workspace.table("t").views([view("bars", figure([
				bar("cat", "val"), title("Categories"), palette("tab20c"), skin("nord")
			]))])"#,
		);
		assert_eq!(first.identity, reordered.identity);
		assert_eq!(first.intent, reordered.intent);
		let RuneViewIntent::Plot(plot) = first.intent else {
			panic!("figure plot")
		};
		assert_eq!(
			plot.authored_settings.plot_skin,
			Override::Set(PlotSkinSelection::new(
				"nord".to_owned(),
				PlotChromeProjection::Ordered,
			))
		);
		assert_eq!(plot.authored_settings.resolved_skin, Override::Inherit);

		let explicit = only_plot(
			"workspace.table(\"t\").views([figure([skin(\"nord\").duotone(), bar(\"cat\", \"val\")])])",
		);
		let RuneViewIntent::Plot(plot) = explicit.intent else {
			panic!("explicit Duotone figure")
		};
		assert_eq!(
			plot.authored_settings.plot_skin,
			Override::Set(PlotSkinSelection::new(
				"nord".to_owned(),
				PlotChromeProjection::Duotone,
			))
		);
	}

	#[test]
	fn product_0032_skin_refusals_are_contextual_and_attributed() {
		for (source, rejected) in [
			(
				"workspace.table(\"t\").views([figure([skin(\"nord\"), skin(\"viridis\"), bar(\"cat\", \"val\")])])",
				"skin(\"viridis\")",
			),
			(
				"workspace.table(\"t\").views([figure([skin(), bar(\"cat\", \"val\")])])",
				"skin()",
			),
			(
				"workspace.table(\"t\").views([figure([skin(\"a\", \"b\"), bar(\"cat\", \"val\")])])",
				"skin(\"a\", \"b\")",
			),
			(
				"let s = make_skin(); workspace.table(\"t\").views([figure([skin(s), bar(\"cat\", \"val\")])])",
				"make_skin",
			),
			(
				"workspace.table(\"t\").views([figure([layers([skin(\"nord\"), line(\"x\", \"y\")])])])",
				"skin(\"nord\")",
			),
			(
				"workspace.table(\"t\").views([bar(\"cat\", \"val\").skin(\"nord\")])",
				"bar(\"cat\", \"val\").skin(\"nord\")",
			),
			(
				"workspace.table(\"t\").views([figure([skin(\"nord\").duotone(1), bar(\"cat\", \"val\")])])",
				"skin(\"nord\").duotone(1)",
			),
			(
				"workspace.table(\"t\").views([figure([skin(\"nord\").duotone().duotone(), bar(\"cat\", \"val\")])])",
				"skin(\"nord\").duotone().duotone()",
			),
			(
				"workspace.table(\"t\").views([figure([skin(\"nord\").frobnicate(), bar(\"cat\", \"val\")])])",
				"skin(\"nord\").frobnicate()",
			),
		] {
			let RuneEnvelopeError::InvalidView { span, .. } =
				transform(source).expect_err("invalid plot skin position")
			else {
				panic!("attributed refusal")
			};
			let actual = &source[span.start as usize..span.end as usize];
			if rejected.contains(".duotone") {
				assert_eq!(actual, &rejected[rejected.rfind('.').expect("modifier")..]);
			} else {
				assert_eq!(actual, rejected);
			}
		}

		let ordinary = r#"
			skin("nord");
			let held = skin("viridis");
			let quiet = skin("tab20c_s8").duotone();
			figure([skin("ordinary"), bar("cat", "val")]);
			workspace.table("t").views([table().skin("nord")])
		"#;
		let retained = transform(ordinary).expect("unclaimed presentation runs locally");
		assert!(!retained.source.contains("skin(\"nord\");"));
		assert_eq!(retained.views.len(), 1);
	}

	fn product_0018_only_layer(source: &str) -> LayerSpec {
		let transformed = transform(source).expect("point-sized scatter envelope");
		let RuneViewIntent::Plot(plot) = &transformed.views[0].intent else {
			panic!("point-sized scatter must be a plot")
		};
		match &plot.body {
			RunePlotBody::StyledSingle(styled) => styled
				.clone()
				.into_layer()
				.expect("point-sized scatter remains layer-composable"),
			RunePlotBody::Layered(specification) => specification
				.layers
				.iter()
				.find(|layer| layer.settings.point_size != Override::Inherit)
				.expect("layered plot contains the styled scatter")
				.clone(),
			RunePlotBody::Single(_) => panic!("the modifier must not be discarded"),
		}
	}

	#[test]
	fn product_0018_point_size_flattens_in_every_proven_mark_position() {
		let layered = product_0018_only_layer(
			"workspace.table(\"t\").views([layers([line(\"x\", \"y\"), scatter(\"x\", \"y\").point_size(2.0)])])",
		);
		let direct = product_0018_only_layer(
			"workspace.table(\"t\").views([scatter(\"x\", \"y\").point_size(2.0)])",
		);
		let figure = product_0018_only_layer(
			"workspace.table(\"t\").views([figure([title(\"Sized\"), scatter(\"x\", \"y\").point_size(2.0)])])",
		);
		for layer in [layered, direct, figure] {
			assert_eq!(layer.mark, LayerMark::Scatter);
			assert_eq!(
				layer.encoding,
				LayerSpec::xy(LayerMark::Scatter, "x", "y").encoding
			);
			assert_eq!(
				layer.settings.point_size,
				Override::Set(PointSize::new(2.0).expect("accepted factor"))
			);
		}
	}

	fn product_0047_single_sink(source: &str) -> DisposableRunePlotSink {
		let transformed = transform(source).expect("series bar envelope");
		let RuneViewIntent::Plot(plot) = &transformed.views[0].intent else {
			panic!("plot intent");
		};
		match &plot.body {
			RunePlotBody::Single(sink) => sink.clone(),
			RunePlotBody::StyledSingle(styled) => styled.sink.clone(),
			other => panic!("standalone bar body, got {other:?}"),
		}
	}

	fn product_0058_only_intent(source: &str) -> RuneViewIntent {
		transform(source)
			.expect("static mark-field binding")
			.into_relations()
			.next()
			.expect("one relation")
			.views
			.into_iter()
			.next()
			.expect("one view")
			.intent
	}

	#[test]
	fn product_0058_bound_fields_equal_inline_in_every_standalone_position() {
		let bindings = r#"
let first_field = "x";
let second_field = "y";
let color_field = "series";
"#;
		for mark in ["bar", "line", "scatter"] {
			for field_count in [2, 3] {
				let inline_args = if field_count == 2 {
					"\"x\", \"y\""
				} else {
					"\"x\", \"y\", \"series\""
				};
				let bound_args = if field_count == 2 {
					"first_field, second_field"
				} else {
					"first_field, second_field, color_field"
				};
				let inline_mark = format!("{mark}({inline_args})");
				let bound_mark = format!("{mark}({bound_args})");
				for (position, inline, bound) in [
					(
						"legacy",
						format!("plot.{mark}(workspace.table(\"t\"), {inline_args})"),
						format!("plot.{mark}(workspace.table(\"t\"), {bound_args})"),
					),
					(
						"relation sugar",
						format!("workspace.table(\"t\").{mark}({inline_args})"),
						format!("workspace.table(\"t\").{mark}({bound_args})"),
					),
					(
						"canonical",
						format!("workspace.table(\"t\").views([{inline_mark}])"),
						format!("workspace.table(\"t\").views([{bound_mark}])"),
					),
					(
						"named",
						format!("workspace.table(\"t\").views([view(\"named\", {inline_mark})])"),
						format!("workspace.table(\"t\").views([view(\"named\", {bound_mark})])"),
					),
					(
						"figure",
						format!("workspace.table(\"t\").views([figure([{inline_mark}])])"),
						format!("workspace.table(\"t\").views([figure([{bound_mark}])])"),
					),
				] {
					assert_eq!(
						product_0058_only_intent(&inline),
						product_0058_only_intent(&format!("{bindings}\n{bound}")),
						"{mark}/{field_count} fields diverged at {position}"
					);
				}
			}
		}

		for (mark, modifier) in [
			("bar", "bar_width(0.5).category_gap(0.2)"),
			("line", "stroke_width(2.0)"),
			("scatter", "point_size(2.0)"),
		] {
			let inline = format!(
				"workspace.table(\"t\").views([{mark}(\"x\", \"y\", \"series\").{modifier}])"
			);
			let bound = format!(
				"{bindings}\nworkspace.table(\"t\").views([{mark}(first_field, second_field, color_field).{modifier}])"
			);
			assert_eq!(
				product_0058_only_intent(&inline),
				product_0058_only_intent(&bound),
				"bound fields changed {mark}'s modifier register"
			);
		}
	}

	#[test]
	fn product_0146_bound_fill_between_fields_equal_inline_in_both_arities() {
		let bindings = r#"
let x = "x";
let low = "low";
let high = "high";
let mid = "mid";
"#;
		for (inline, bound) in [
			(
				"fill_between(\"x\", \"low\", \"high\")",
				"fill_between(x, low, high)",
			),
			(
				"fill_between(\"x\", \"low\", \"high\", \"mid\")",
				"fill_between(x, low, high, mid)",
			),
		] {
			let inline = format!("workspace.table(\"t\").views([{inline}])");
			let bound = format!("{bindings}\nworkspace.table(\"t\").views([{bound}])");
			assert_eq!(
				product_0058_only_intent(&inline),
				product_0058_only_intent(&bound)
			);
		}
	}

	#[test]
	fn product_0148_bound_candlestick_fields_equal_inline() {
		let inline = "workspace.table(\"t\").views([candlestick(\"stamp\", \"open\", \"high\", \"low\", \"close\")])";
		let bound = r#"
let x = "stamp";
let open = "open";
let high = "high";
let low = "low";
let close = "close";
workspace.table("t").views([candlestick(x, open, high, low, close)])
"#;
		assert_eq!(
			product_0058_only_intent(inline),
			product_0058_only_intent(bound)
		);
	}

	#[test]
	fn product_0058_bound_layer_fields_preserve_the_closed_layer_surface() {
		let inline = r#"workspace.table("t").views([layers([
			line("x", "y").stroke_width(2.0),
			scatter("x", "y").point_size(2.0),
		])])"#;
		let bound = r#"
let x_field = "x";
let y_field = "y";
workspace.table("t").views([layers([
	line(x_field, y_field).stroke_width(2.0),
	 scatter(x_field, y_field).point_size(2.0),
])])
"#;
		assert_eq!(
			product_0058_only_intent(inline),
			product_0058_only_intent(bound)
		);

		for source in [
			r#"let x = "x"; let y = "y"; let series = "series";
			workspace.table("t").views([layers([line(x, y, series)])])"#,
			r#"let x = "x"; let y = "y";
			workspace.table("t").views([layers([bar(x, y)])])"#,
		] {
			assert!(
				transform(source).is_err(),
				"binding coverage widened the layer/register surface: {source}"
			);
		}
		transform(
			r#"let x = "x"; let y = "y"; let width = 2.0;
			workspace.table("t").views([layers([line(x, y).stroke_width(width)])])"#,
		)
		.expect("PRODUCT-0076 executes a bound layer modifier argument");

		for (source, rejected) in [
			(
				r#"workspace.table("t").views([layers([bar(missing, "y").bar_width(0.5)])])"#,
				"missing",
			),
			(
				r#"workspace.table("t").views([layers([line(missing, "y", "series").stroke_width(2.0)])])"#,
				"missing",
			),
		] {
			let RuneEnvelopeError::InvalidView { span, message } =
				transform(source).expect_err("layer context refuses before field resolution")
			else {
				panic!("expected layer-context refusal")
			};
			assert_eq!(&source[span.start as usize..span.end as usize], rejected);
			assert!(message.contains("No local variable"), "{message}");
		}
	}

	#[test]
	fn product_0058_lookup_and_argument_refusals_survive_product_0076_partitioning() {
		let aliases = r#"
let original = "x";
let alias = original;
let y_field = "y";
workspace.table("t").views([line(alias, y_field)])
"#;
		let transformed = transform(aliases).expect("transitive mark-field alias");
		assert!(
			!transformed.source.contains("let original = \"x\";")
				&& !transformed.source.contains("let alias = original;"),
			"PRODUCT-0076 executes presentation-only aliases locally instead of retaining them for Toron"
		);
		let RuneViewIntent::Plot(plot) = &transformed.views[0].intent else {
			panic!("bound line must remain a plot")
		};
		let RunePlotBody::Single(sink) = &plot.body else {
			panic!("bound line must remain a single mark")
		};
		assert_eq!((sink.x.as_str(), sink.y.as_str()), ("x", "y"));

		let shadowed = transform(
			r#"
let field = "first";
workspace.table("t").views([line(field, "y")]);
let field = "second";
workspace.table("t").views([line(field, "y")])
"#,
		)
		.expect("most recent preceding binding");
		let xs = shadowed
			.views
			.iter()
			.map(|view| match &view.intent {
				RuneViewIntent::Plot(RunePlotIntent {
					body: RunePlotBody::Single(sink),
					..
				}) => sink.x.as_str(),
				_ => panic!("expected single line"),
			})
			.collect::<Vec<_>>();
		assert_eq!(xs, ["first", "second"]);

		let collision = product_0058_only_intent(
			r#"let bar = "region";
			workspace.table("t").views([bar(bar, "revenue")])"#,
		);
		let RuneViewIntent::Plot(RunePlotIntent {
			body: RunePlotBody::Single(sink),
			..
		}) = collision
		else {
			panic!("constructor-position bar")
		};
		assert_eq!((sink.x.as_str(), sink.y.as_str()), ("region", "revenue"));

		for (source, rejected, expected) in [
			(
				"workspace.table(\"t\").views([bar(missing, \"y\")])",
				"missing",
				"No local variable",
			),
			(
				"let field = make_field(); workspace.table(\"t\").views([bar(field, \"y\")])",
				"make_field",
				"Missing item make_field",
			),
			(
				"let field = field; workspace.table(\"t\").views([bar(field, \"y\")])",
				"field",
				"No local variable",
			),
			(
				"let field = 7; workspace.table(\"t\").views([bar(field, \"y\")])",
				"bar(field, \"y\")",
				"field arguments must evaluate to strings",
			),
		] {
			let RuneEnvelopeError::InvalidView { span, message } =
				transform(source).expect_err("invalid mark-field binding")
			else {
				panic!("expected attributed mark-field refusal")
			};
			assert_eq!(&source[span.start as usize..span.end as usize], rejected);
			assert!(message.contains(expected), "{message}");
		}
		let concatenated =
			product_0058_only_intent("workspace.table(\"t\").views([bar(\"x\" + \"z\", \"y\")])");
		let RuneViewIntent::Plot(RunePlotIntent {
			body: RunePlotBody::Single(sink),
			..
		}) = concatenated
		else {
			panic!("computed string field remains a bar")
		};
		assert_eq!(sink.x, "xz");
	}

	#[test]
	fn product_0047_three_field_bar_normalizes_across_all_three_spellings() {
		// PRODUCT-0008 §2: the spellings are exact projections of one normal form.
		let free = product_0047_single_sink(
			"plot.bar(workspace.table(\"t\"), \"cat\", \"val\", \"series\")",
		);
		let sugar =
			product_0047_single_sink("workspace.table(\"t\").bar(\"cat\", \"val\", \"series\")");
		let canonical = product_0047_single_sink(
			"workspace.table(\"t\").views([bar(\"cat\", \"val\", \"series\")])",
		);
		let figure = product_0047_single_sink(
			"workspace.table(\"t\").views([figure([bar(\"cat\", \"val\", \"series\")])])",
		);
		assert_eq!(free, sugar);
		assert_eq!(free, canonical);
		assert_eq!(free, figure);
		assert_eq!(
			(free.x.as_str(), free.y.as_str(), free.category.as_deref()),
			("cat", "val", Some("series"))
		);
		assert_eq!(free.composition.primary_plot_type(), PlotType::Bar);
	}

	#[test]
	fn product_0047_two_field_bar_is_unchanged_at_every_spelling() {
		for source in [
			"plot.bar(workspace.table(\"t\"), \"cat\", \"val\")",
			"workspace.table(\"t\").bar(\"cat\", \"val\")",
			"workspace.table(\"t\").views([bar(\"cat\", \"val\")])",
			"workspace.table(\"t\").views([figure([bar(\"cat\", \"val\")])])",
		] {
			let sink = product_0047_single_sink(source);
			assert_eq!(sink.category, None, "two-field bar names no color channel");
			assert_eq!((sink.x.as_str(), sink.y.as_str()), ("cat", "val"));
		}
	}

	#[test]
	fn product_0047_series_bar_inherits_the_bar_width_register() {
		// PRODUCT-0046 §1's first new-form application: the register belongs to
		// the mark, so the new encoding form admits it without a new record.
		let transformed = transform(
			"workspace.table(\"t\").views([figure([bar(\"cat\", \"val\", \"series\").bar_width(0.5)])])",
		)
		.expect("styled series bar envelope");
		let RuneViewIntent::Plot(plot) = &transformed.views[0].intent else {
			panic!("plot intent");
		};
		let RunePlotBody::StyledSingle(styled) = &plot.body else {
			panic!("styled standalone bar");
		};
		assert_eq!(styled.sink.category.as_deref(), Some("series"));
		assert_eq!(
			styled.settings.bar_width,
			Override::Set(BarWidth::new(0.5).expect("accepted fraction"))
		);
		assert!(styled.clone().into_layer().is_err());
	}

	#[test]
	fn product_0047_series_bar_refusals_are_inherited_unchanged() {
		// Wrong arity, non-literal fields, and the layers admission all refuse
		// exactly as they did at two fields.
		for source in [
			"workspace.table(\"t\").views([bar(\"cat\", \"val\", \"series\", \"extra\")])",
			"workspace.table(\"t\").views([figure([bar(\"cat\", \"val\", \"series\", \"extra\")])])",
			"workspace.table(\"t\").views([bar(\"cat\", \"val\", series)])",
			"workspace.table(\"t\").views([layers([bar(\"cat\", \"val\", \"series\")])])",
			"workspace.table(\"t\").views([layers([bar(\"cat\", \"val\")])])",
			"workspace.table(\"t\").views([figure([bar(\"cat\", \"val\", \"series\").point_size(2.0)])])",
		] {
			let error = transform(source).expect_err("inherited refusal");
			let RuneEnvelopeError::InvalidView { span, .. } = error else {
				panic!("span-attributed refusal for {source}: {error:?}");
			};
			assert!(span.start < span.end, "refusal names its span");
		}
	}

	#[test]
	fn product_0046_category_scatter_keeps_its_encoding_and_point_size() {
		let transformed = transform(
			"workspace.table(\"t\").views([figure([scatter(\"units\", \"revenue\", \"product\").point_size(2.0)])])",
		)
		.expect("styled category scatter envelope");
		let RuneViewIntent::Plot(plot) = &transformed.views[0].intent else {
			panic!("plot intent");
		};
		let RunePlotBody::StyledSingle(styled) = &plot.body else {
			panic!("styled standalone scatter");
		};
		assert_eq!(styled.sink.category.as_deref(), Some("product"));
		assert_eq!(
			styled.settings.point_size,
			Override::Set(PointSize::new(2.0).unwrap())
		);
		assert!(styled.clone().into_layer().is_err());
	}

	#[test]
	fn product_0018_point_size_uses_the_substrate_domain_without_clamping() {
		for factor in ["0.25", "1", "2.0", "8.0"] {
			let source = format!(
				"workspace.table(\"t\").views([layers([scatter(\"x\", \"y\").point_size({factor})])])"
			);
			let layer = product_0018_only_layer(&source);
			let Override::Set(size) = layer.settings.point_size else {
				panic!("accepted source must set point size")
			};
			assert_eq!(size.factor(), factor.parse::<f32>().unwrap());
		}

		for rejected in [
			"point_size()",
			"point_size(1.0, 2.0)",
			"point_size(size)",
			"point_size(-1.0)",
			"point_size(0.0)",
			"point_size(0.24)",
			"point_size(8.01)",
		] {
			let source = format!(
				"workspace.table(\"t\").views([layers([scatter(\"x\", \"y\").{rejected}])])"
			);
			let RuneEnvelopeError::InvalidView { span, .. } =
				transform(&source).expect_err("invalid point size must refuse")
			else {
				panic!("expected an attributed point-size refusal")
			};
			assert_eq!(
				&source[span.start as usize..span.end as usize],
				if rejected == "point_size(size)" {
					"size".to_owned()
				} else {
					format!(".{rejected}")
				}
			);
		}
		transform(
			"workspace.table(\"t\").views([layers([scatter(\"x\", \"y\").point_size(1.0 + 1.0)])])",
		)
		.expect("PRODUCT-0076 executes computed point size");
	}

	#[test]
	fn product_0018_modifier_register_is_closed_duplicate_free_and_atomic() {
		for (modified, rejected) in [
			(
				"scatter(\"x\", \"y\").opacity(0.5)",
				"scatter(\"x\", \"y\").opacity(0.5)",
			),
			(
				"scatter(\"x\", \"y\").title(\"wrong level\")",
				"scatter(\"x\", \"y\").title(\"wrong level\")",
			),
			(
				"scatter(\"x\", \"y\").point_size(2.0).point_size(3.0)",
				".point_size(3.0)",
			),
			("line(\"x\", \"y\").point_size(2.0)", ".point_size(2.0)"),
		] {
			let source = format!("workspace.table(\"t\").views([table(), layers([{modified}])])");
			let RuneEnvelopeError::InvalidView { span, .. } =
				transform(&source).expect_err("invalid modifier must refuse the whole view set")
			else {
				panic!("expected an attributed modifier refusal")
			};
			assert_eq!(&source[span.start as usize..span.end as usize], rejected);
		}

		let category = transform("workspace.table(\"t\").views([scatter(\"x\", \"y\", \"cat\")])")
			.expect("the established unmodified category scatter remains legal");
		let RuneViewIntent::Plot(plot) = &category.views[0].intent else {
			panic!("category scatter remains a plot")
		};
		let RunePlotBody::Single(scatter) = &plot.body else {
			panic!("unmodified category scatter keeps its established body")
		};
		assert_eq!(scatter.category.as_deref(), Some("cat"));
	}

	#[test]
	fn product_0018_modifier_is_ordinary_rune_outside_proven_positions() {
		let source = r#"
			let styled = scatter("x", "y").point_size(2.0);
			consume(scatter("a", "b").point_size(4.0));
			workspace.table("t").views([table()])
		"#;
		let error = transform(source).expect_err("presentation calls cannot leak through consume");
		assert!(error.to_string().contains("consume"), "{error}");
	}

	#[test]
	fn product_0018_styled_single_keeps_structural_presentation() {
		for source in [
			"workspace.table(\"t\").views([scatter(\"x\", \"y\").point_size(1.0)])",
			"workspace.table(\"t\").views([figure([scatter(\"x\", \"y\").point_size(2.0)])])",
		] {
			let transformed = transform(source).expect("styled single scatter");
			assert_eq!(
				transformed.views[0].intent.presentation().display_label,
				"plot.scatter"
			);
		}
	}

	fn product_0020_only_line(source: &str) -> LayerSpec {
		let transformed = transform(source).expect("stroke-width line envelope");
		let RuneViewIntent::Plot(plot) = &transformed.views[0].intent else {
			panic!("stroke-width line must be a plot")
		};
		match &plot.body {
			RunePlotBody::StyledSingle(styled) => styled
				.clone()
				.into_layer()
				.expect("stroke-width line remains layer-composable"),
			RunePlotBody::Layered(specification) => specification
				.layers
				.iter()
				.find(|layer| layer.settings.stroke_width != Override::Inherit)
				.expect("layered plot contains the styled line")
				.clone(),
			RunePlotBody::Single(_) => panic!("the modifier must not be discarded"),
		}
	}

	#[test]
	fn product_0020_stroke_width_flattens_in_every_proven_mark_position() {
		let layered = product_0020_only_line(
			"workspace.table(\"t\").views([layers([line(\"x\", \"y\").stroke_width(2.0), scatter(\"x\", \"y\")])])",
		);
		let direct = product_0020_only_line(
			"workspace.table(\"t\").views([line(\"x\", \"y\").stroke_width(2.0)])",
		);
		let figure = product_0020_only_line(
			"workspace.table(\"t\").views([figure([title(\"Wide\"), line(\"x\", \"y\").stroke_width(2.0)])])",
		);
		for layer in [layered, direct, figure] {
			assert_eq!(layer.mark, LayerMark::Line);
			assert_eq!(
				layer.encoding,
				LayerSpec::xy(LayerMark::Line, "x", "y").encoding
			);
			assert_eq!(layer.settings.point_size, Override::Inherit);
			assert_eq!(
				layer.settings.stroke_width,
				Override::Set(StrokeWidth::new(2.0).expect("accepted factor"))
			);
		}
	}

	#[test]
	fn product_0020_stroke_width_inherits_bounds_and_refuses_atomically() {
		for factor in ["0.25", "1", "2.0", "8.0"] {
			let source = format!(
				"workspace.table(\"t\").views([layers([line(\"x\", \"y\").stroke_width({factor})])])"
			);
			let layer = product_0020_only_line(&source);
			let Override::Set(width) = layer.settings.stroke_width else {
				panic!("accepted source must set stroke width")
			};
			assert_eq!(width.factor(), factor.parse::<f32>().unwrap());
		}

		for (rejected, attributed) in [
			("stroke_width()", ".stroke_width()"),
			("stroke_width(1.0, 2.0)", ".stroke_width(1.0, 2.0)"),
			("stroke_width(width)", "width"),
			("stroke_width(-1.0)", ".stroke_width(-1.0)"),
			("stroke_width(0.0)", ".stroke_width(0.0)"),
			("stroke_width(0.24)", ".stroke_width(0.24)"),
			("stroke_width(8.01)", ".stroke_width(8.01)"),
			("stroke_width(2.0).stroke_width(3.0)", ".stroke_width(3.0)"),
		] {
			let source = format!(
				"workspace.table(\"t\").views([table(), layers([line(\"x\", \"y\").{rejected}])])"
			);
			let RuneEnvelopeError::InvalidView { span, .. } =
				transform(&source).expect_err("invalid stroke width must refuse")
			else {
				panic!("expected attributed stroke-width refusal")
			};
			assert_eq!(&source[span.start as usize..span.end as usize], attributed);
		}
		transform(
			"workspace.table(\"t\").views([layers([line(\"x\", \"y\").stroke_width(1.0 + 1.0)])])",
		)
		.expect("PRODUCT-0076 executes computed stroke width");
	}

	#[test]
	fn product_0020_modifier_compatibility_is_closed_and_order_independent() {
		for (modified, rejected) in [
			(
				"scatter(\"x\", \"y\").stroke_width(2.0)",
				".stroke_width(2.0)",
			),
			("line(\"x\", \"y\").point_size(2.0)", ".point_size(2.0)"),
			(
				"line(\"x\", \"y\").stroke_width(2.0).point_size(2.0)",
				".point_size(2.0)",
			),
			(
				"line(\"x\", \"y\").point_size(2.0).stroke_width(2.0)",
				".point_size(2.0)",
			),
		] {
			let source = format!("workspace.table(\"t\").views([layers([{modified}])])");
			let RuneEnvelopeError::InvalidView { span, .. } =
				transform(&source).expect_err("wrong receiver must refuse complete chain")
			else {
				panic!("expected attributed compatibility refusal")
			};
			assert_eq!(&source[span.start as usize..span.end as usize], rejected);
		}

		for control in [
			"workspace.table(\"t\").views([line(\"x\", \"y\")])",
			"workspace.table(\"t\").views([scatter(\"x\", \"y\").point_size(2.0)])",
		] {
			transform(control).expect("established control remains legal");
		}
	}

	#[test]
	fn product_0020_stroke_width_is_ordinary_rune_outside_proven_positions() {
		let source = r#"
			let styled = line("x", "y").stroke_width(2.0);
			consume(line("a", "b").stroke_width(4.0));
			workspace.table("t").views([table()])
		"#;
		let error = transform(source).expect_err("presentation calls cannot leak through consume");
		assert!(error.to_string().contains("consume"), "{error}");
	}

	#[test]
	fn product_0020_styled_single_line_keeps_structural_presentation() {
		for source in [
			"workspace.table(\"t\").views([line(\"x\", \"y\").stroke_width(1.0)])",
			"workspace.table(\"t\").views([figure([line(\"x\", \"y\").stroke_width(2.0)])])",
		] {
			let transformed = transform(source).expect("styled single line");
			assert_eq!(
				transformed.views[0].intent.presentation().display_label,
				"plot.line"
			);
		}
	}

	fn product_0022_only_bar(source: &str) -> StyledRunePlotSink {
		let transformed = transform(source).expect("bar-width envelope");
		let RuneViewIntent::Plot(plot) = &transformed.views[0].intent else {
			panic!("bar-width sink must be a plot")
		};
		let RunePlotBody::StyledSingle(styled) = &plot.body else {
			panic!("bar width must use the styled-single carrier")
		};
		styled.clone()
	}

	#[test]
	fn product_0022_bar_width_flattens_in_direct_and_figure_positions() {
		for source in [
			"workspace.table(\"t\").views([bar(\"cat\", \"val\").bar_width(0.5)])",
			"workspace.table(\"t\").views([figure([title(\"Narrow\"), bar(\"cat\", \"val\").bar_width(0.5)])])",
		] {
			let styled = product_0022_only_bar(source);
			assert_eq!(styled.sink.composition.primary_plot_type(), PlotType::Bar);
			assert_eq!(styled.sink.x, "cat");
			assert_eq!(styled.sink.y, "val");
			assert_eq!(styled.sink.category, None);
			assert_eq!(styled.settings.point_size, Override::Inherit);
			assert_eq!(styled.settings.stroke_width, Override::Inherit);
			assert_eq!(styled.settings.category_gap, Override::Inherit);
			assert_eq!(
				styled.settings.bar_width,
				Override::Set(BarWidth::new(0.5).unwrap())
			);
		}
	}

	#[test]
	fn product_0022_bar_width_uses_substrate_bounds_and_atomic_refusals() {
		for fraction in ["0.000001", "0.5", "0.9", "1.0"] {
			let source = format!(
				"workspace.table(\"t\").views([bar(\"cat\", \"val\").bar_width({fraction})])"
			);
			let styled = product_0022_only_bar(&source);
			let Override::Set(width) = styled.settings.bar_width else {
				panic!("accepted bar width must be set")
			};
			assert_eq!(width.fraction(), fraction.parse::<f32>().unwrap());
		}

		for rejected in [
			"bar_width()",
			"bar_width(0.5, 0.6)",
			"bar_width(width)",
			"bar_width(-0.1)",
			"bar_width(0.0)",
			"bar_width(1.01)",
			"bar_width(0.5).bar_width(0.6)",
			"opacity(0.5)",
		] {
			let source =
				format!("workspace.table(\"t\").views([bar(\"cat\", \"val\").{rejected}])");
			assert!(transform(&source).is_err(), "accepted {rejected}");
		}
		transform("workspace.table(\"t\").views([bar(\"cat\", \"val\").bar_width(0.25 + 0.25)])")
			.expect("PRODUCT-0076 executes computed bar width");
	}

	#[test]
	fn product_0022_receiver_rules_keep_bar_out_of_ordered_layers() {
		for rejected in [
			"line(\"x\", \"y\").bar_width(0.5)",
			"scatter(\"x\", \"y\").bar_width(0.5)",
			"bar(\"cat\", \"val\").point_size(2.0)",
			"bar(\"cat\", \"val\").stroke_width(2.0)",
			"bar(\"cat\", \"val\").bar_width(0.5).point_size(2.0)",
			"bar(\"cat\", \"val\").stroke_width(2.0).bar_width(0.5)",
		] {
			let source = format!("workspace.table(\"t\").views([{rejected}])");
			assert!(transform(&source).is_err(), "accepted {rejected}");
		}

		let layered =
			"workspace.table(\"t\").views([layers([bar(\"cat\", \"val\").bar_width(0.5)])])";
		let error = transform(layered).expect_err("bar remains outside LayerMark");
		assert!(error.to_string().contains("not a shared-Cartesian layer"));

		for control in [
			"workspace.table(\"t\").views([bar(\"cat\", \"val\")])",
			"workspace.table(\"t\").views([layers([line(\"x\", \"y\").stroke_width(2.0), scatter(\"x\", \"y\").point_size(2.0)])])",
		] {
			assert!(transform(control).is_ok(), "rejected control {control}");
		}
	}

	#[test]
	fn product_0022_bar_width_is_ordinary_rune_outside_proven_positions() {
		let source = r#"
let styled = bar("cat", "val").bar_width(0.5);
consume(bar("a", "b").bar_width(0.7));
workspace.table("t").views([table()])
"#;
		let error = transform(source).expect_err("presentation calls cannot leak through consume");
		assert!(error.to_string().contains("consume"), "{error}");
	}

	#[test]
	fn product_0053_category_gap_flattens_across_bar_forms_and_modifier_order() {
		for source in [
			"workspace.table(\"t\").views([bar(\"cat\", \"val\").category_gap(0.2)])",
			"workspace.table(\"t\").views([figure([bar(\"cat\", \"val\", \"series\").category_gap(0.2)])])",
		] {
			let styled = product_0022_only_bar(source);
			assert_eq!(styled.sink.composition.primary_plot_type(), PlotType::Bar);
			assert_eq!(
				styled.settings.category_gap,
				Override::Set(CategoryGap::new(0.2).unwrap())
			);
			assert_eq!(styled.settings.bar_width, Override::Inherit);
			assert!(styled.clone().into_layer().is_err());
		}

		let width_then_gap = product_0022_only_bar(
			"workspace.table(\"t\").views([bar(\"cat\", \"val\", \"series\").bar_width(0.5).category_gap(0.2)])",
		);
		let gap_then_width = product_0022_only_bar(
			"workspace.table(\"t\").views([bar(\"cat\", \"val\", \"series\").category_gap(0.2).bar_width(0.5)])",
		);
		assert_eq!(width_then_gap, gap_then_width);
		assert_eq!(
			width_then_gap.settings.bar_width,
			Override::Set(BarWidth::new(0.5).unwrap())
		);
		assert_eq!(
			width_then_gap.settings.category_gap,
			Override::Set(CategoryGap::new(0.2).unwrap())
		);
	}

	#[test]
	fn product_0053_category_gap_bounds_receivers_and_context_are_closed() {
		for fraction in ["0.0", "0.000001", "0.2", "0.999999"] {
			let source = format!(
				"workspace.table(\"t\").views([bar(\"cat\", \"val\").category_gap({fraction})])"
			);
			let styled = product_0022_only_bar(&source);
			let Override::Set(gap) = styled.settings.category_gap else {
				panic!("accepted category gap must be set")
			};
			assert_eq!(gap.fraction(), fraction.parse::<f32>().unwrap());
		}

		for rejected in [
			"category_gap()",
			"category_gap(0.2, 0.3)",
			"category_gap(gap)",
			"category_gap(-0.1)",
			"category_gap(1.0)",
			"category_gap(1.01)",
			"category_gap(0.2).category_gap(0.3)",
		] {
			let source =
				format!("workspace.table(\"t\").views([bar(\"cat\", \"val\").{rejected}])");
			assert!(transform(&source).is_err(), "accepted {rejected}");
		}
		transform("workspace.table(\"t\").views([bar(\"cat\", \"val\").category_gap(0.1 + 0.1)])")
			.expect("PRODUCT-0076 executes computed category gap");

		for rejected in [
			"line(\"x\", \"y\").category_gap(0.2)",
			"scatter(\"x\", \"y\").category_gap(0.2)",
		] {
			let source = format!("workspace.table(\"t\").views([{rejected}])");
			assert!(transform(&source).is_err(), "accepted {rejected}");
		}
		let layered =
			"workspace.table(\"t\").views([layers([bar(\"cat\", \"val\").category_gap(0.2)])])";
		assert!(
			transform(layered)
				.expect_err("bar remains outside LayerMark")
				.to_string()
				.contains("not a shared-Cartesian layer")
		);

		let source = r#"
let styled = bar("cat", "val").category_gap(0.2);
consume(bar("a", "b").category_gap(0.4));
workspace.table("t").views([table()])
"#;
		let error = transform(source).expect_err("presentation calls cannot leak through consume");
		assert!(error.to_string().contains("consume"), "{error}");
	}

	#[test]
	fn product_0064_stacked_is_a_zero_argument_order_independent_bar_member() {
		for source in [
			"workspace.table(\"t\").views([bar(\"cat\", \"val\").stacked()])",
			"workspace.table(\"t\").views([figure([bar(\"cat\", \"val\", \"series\").stacked()])])",
		] {
			let styled = product_0022_only_bar(source);
			assert!(styled.stacked);
			assert_eq!(styled.sink.composition.primary_plot_type(), PlotType::Bar);
			assert_eq!(styled.settings, LayerSettings::default());
			assert!(styled.clone().into_layer().is_err());
			assert_eq!(
				RuneViewIntent::Plot(RunePlotIntent::bare(RunePlotBody::StyledSingle(styled)))
					.presentation()
					.display_label,
				"plot.bar",
				"the label names authored Bar intent before member resolution"
			);
		}

		let first = product_0022_only_bar(
			"workspace.table(\"t\").views([bar(\"cat\", \"val\", \"series\").stacked().bar_width(0.5).category_gap(0.2)])",
		);
		let second = product_0022_only_bar(
			"workspace.table(\"t\").views([bar(\"cat\", \"val\", \"series\").category_gap(0.2).bar_width(0.5).stacked()])",
		);
		assert_eq!(first, second, "the bar register is a set, not a pipeline");
	}

	#[test]
	fn product_0064_stacked_refusals_are_atomic_and_receiver_scoped() {
		for rejected in [
			"bar(\"cat\", \"val\").stacked().stacked()",
			"bar(\"cat\", \"val\").stacked(true)",
			"bar(\"cat\", \"val\").stacked(1)",
			"line(\"x\", \"y\").stacked()",
			"scatter(\"x\", \"y\").stacked()",
		] {
			let source = format!("workspace.table(\"t\").views([{rejected}])");
			let RuneEnvelopeError::InvalidView { span, .. } =
				transform(&source).expect_err("invalid stacked modifier must refuse")
			else {
				panic!("expected attributed stacked refusal")
			};
			assert_eq!(
				&source[span.start as usize..span.end as usize],
				&rejected[rejected.rfind('.').expect("modifier")..],
				"the offending modifier owns the refusal"
			);
		}

		let layered = "workspace.table(\"t\").views([layers([bar(\"cat\", \"val\").stacked()])])";
		let error = transform(layered).expect_err("stacking does not admit bar layering");
		assert!(error.to_string().contains("not a shared-Cartesian layer"));
	}

	#[test]
	fn product_0072_percent_is_an_order_independent_stacked_register_member() {
		let first = product_0022_only_bar(
			"workspace.table(\"t\").views([bar(\"cat\", \"val\", \"series\").stacked().percent()])",
		);
		let second = product_0022_only_bar(
			"workspace.table(\"t\").views([bar(\"cat\", \"val\", \"series\").percent().stacked()])",
		);
		assert_eq!(first, second, "the stack register is a set, not a pipeline");
		assert!(first.stacked && first.percent);
		let bindings = r#"
let category_field = "cat";
let value_field = "val";
let series_field = "series";
let x_field = "x";
"#;
		assert_eq!(
			product_0022_only_bar(&format!(
				"{bindings}\nworkspace.table(\"t\").views([bar(category_field, value_field, series_field).stacked().percent()])"
			)),
			first,
			"static bindings resolve before the vertical percent-bar register"
		);
		let horizontal = product_0022_only_bar(
			"workspace.table(\"t\").views([bar(\"val\", \"cat\", \"series\").stacked().percent()])",
		);
		assert_eq!(
			product_0022_only_bar(&format!(
				"{bindings}\nworkspace.table(\"t\").views([bar(value_field, category_field, series_field).percent().stacked()])"
			)),
			horizontal,
			"static bindings resolve before horizontal pair selection"
		);

		let RuneViewIntent::Plot(RunePlotIntent {
			body: RunePlotBody::StyledSingle(area),
			..
		}) = product_0058_only_intent(
			"workspace.table(\"t\").views([area(\"x\", \"y\", \"series\").percent()])",
		)
		else {
			panic!("percent area uses the styled standalone carrier")
		};
		assert!(area.percent);
		assert!(!area.stacked, "area is inherently stacked");
		let bound_area = product_0058_only_intent(&format!(
			"{bindings}\nworkspace.table(\"t\").views([area(x_field, value_field, series_field).percent()])"
		));
		let RuneViewIntent::Plot(RunePlotIntent {
			body: RunePlotBody::StyledSingle(bound_area),
			..
		}) = bound_area
		else {
			panic!("bound percent area uses the styled standalone carrier")
		};
		assert!(bound_area.percent);
		assert_eq!(
			(bound_area.sink.x.as_str(), bound_area.sink.y.as_str()),
			("x", "val")
		);
		assert_eq!(bound_area.sink.category.as_deref(), Some("series"));

		let two_field_bar = product_0022_only_bar(
			"workspace.table(\"t\").views([bar(\"cat\", \"val\").percent().stacked()])",
		);
		assert!(two_field_bar.percent && two_field_bar.stacked);
		let RuneViewIntent::Plot(RunePlotIntent {
			body: RunePlotBody::StyledSingle(two_field_area),
			..
		}) = product_0058_only_intent("workspace.table(\"t\").views([area(\"x\", \"val\").percent()])")
		else {
			panic!("two-field percent area is accepted")
		};
		assert!(two_field_area.percent);
		assert_eq!(two_field_area.sink.category, None);

		for (source, reason) in [
			(
				"workspace.table(\"t\").views([bar(\"cat\", \"val\").percent()])",
				"requires stacked()",
			),
			(
				"workspace.table(\"t\").views([line(\"x\", \"y\").percent()])",
				"stacked bar and area",
			),
			(
				"workspace.table(\"t\").views([scatter(\"x\", \"y\").percent()])",
				"stacked bar and area",
			),
			(
				"workspace.table(\"t\").views([area(\"x\", \"y\").percent().percent()])",
				"at most once",
			),
			(
				"workspace.table(\"t\").views([area(\"x\", \"y\").percent(true)])",
				"takes no arguments",
			),
		] {
			let error = transform(source).expect_err("invalid percent spelling must refuse");
			assert!(error.to_string().contains(reason), "{error}");
		}
	}

	#[test]
	fn product_0070_area_spellings_and_static_bindings_share_one_standalone_intent() {
		let inline = [
			"plot.area(workspace.table(\"t\"), \"x\", \"y\", \"series\")".to_owned(),
			"workspace.table(\"t\").area(\"x\", \"y\", \"series\")".to_owned(),
			"workspace.table(\"t\").views([area(\"x\", \"y\", \"series\")])".to_owned(),
			"workspace.table(\"t\").views([view(\"market\", area(\"x\", \"y\", \"series\"))])"
				.to_owned(),
			"workspace.table(\"t\").views([figure([area(\"x\", \"y\", \"series\")])])".to_owned(),
		];
		let expected = product_0058_only_intent(&inline[0]);
		for (index, source) in inline.iter().enumerate() {
			assert_eq!(
				product_0058_only_intent(source),
				expected,
				"area position {index} must lower to the same mark intent"
			);
		}

		let bindings = r#"
let x_field = "x";
let y_field = "y";
let series_field = "series";
"#;
		for bound in [
			"plot.area(workspace.table(\"t\"), x_field, y_field, series_field)",
			"workspace.table(\"t\").area(x_field, y_field, series_field)",
			"workspace.table(\"t\").views([area(x_field, y_field, series_field)])",
			"workspace.table(\"t\").views([view(\"market\", area(x_field, y_field, series_field))])",
			"workspace.table(\"t\").views([figure([area(x_field, y_field, series_field)])])",
		] {
			assert_eq!(
				product_0058_only_intent(&format!("{bindings}\n{bound}")),
				expected,
				"static bindings are source factoring in every area field position"
			);
		}

		let two = product_0058_only_intent("workspace.table(\"t\").views([area(\"x\", \"y\")])");
		let RuneViewIntent::Plot(RunePlotIntent {
			body: RunePlotBody::Single(two),
			..
		}) = two
		else {
			panic!("two-field area remains a standalone plot")
		};
		assert_eq!(two.composition.primary_plot_type(), PlotType::StackedArea);
		assert_eq!((two.x.as_str(), two.y.as_str()), ("x", "y"));
		assert_eq!(two.category, None);
		assert_eq!(
			RuneViewIntent::Plot(RunePlotIntent::bare(RunePlotBody::Single(two)))
				.presentation()
				.display_label,
			"plot.area"
		);
	}

	#[test]
	fn product_0070_area_refusals_keep_layers_and_registers_closed() {
		for source in [
			"workspace.table(\"t\").views([area(\"x\")])",
			"workspace.table(\"t\").views([area(\"x\", \"y\", \"series\", \"extra\")])",
			"workspace.table(\"t\").views([area(missing, \"y\")])",
			"workspace.table(\"t\").views([layers([area(\"x\", \"y\")])])",
		] {
			assert!(
				matches!(
					transform(source),
					Err(RuneEnvelopeError::InvalidView { .. })
				),
				"malformed or layered area must refuse: {source}"
			);
		}

		for (chain, reason) in [
			("area(\"x\", \"y\").stacked()", "bar marks"),
			("area(\"x\", \"y\").bar_width(0.5)", "bar marks"),
			("area(\"x\", \"y\").category_gap(0.2)", "bar marks"),
		] {
			let source = format!("workspace.table(\"t\").views([{chain}])");
			let RuneEnvelopeError::InvalidView { span, message } =
				transform(&source).expect_err("cross-register area modifier refuses")
			else {
				panic!("expected attributed area refusal")
			};
			let modifier = chain.find(").").expect("receiver modifier") + 1;
			assert_eq!(
				&source[span.start as usize..span.end as usize],
				&chain[modifier..]
			);
			assert!(message.contains(reason), "{message}");
		}

		let ordinary = r#"
let held = area("x", "y", "series");
consume(area("a", "b"));
workspace.table("t").views([table()])
"#;
		let error =
			transform(ordinary).expect_err("presentation calls cannot leak through consume");
		assert!(error.to_string().contains("consume"), "{error}");
	}

	#[test]
	fn product_0014_layers_recognition_is_outer_attributed_and_atomic() {
		for (source, rejected) in [
			("workspace.table(\"t\").views([layers()])", "layers()"),
			(
				"workspace.table(\"t\").views([layers([], [])])",
				"layers([], [])",
			),
			(
				"workspace.table(\"t\").views([layers([line(\"x\")])])",
				"line(\"x\")",
			),
			(
				"workspace.table(\"t\").views([layers([scatter(x, \"y\")])])",
				"x",
			),
			(
				"workspace.table(\"t\").views([layers([unknown(\"x\", \"y\")])])",
				"unknown",
			),
			(
				"workspace.table(\"t\").views([layers([line(\"x\", \"y\"), unknown()])])",
				"unknown",
			),
		] {
			let RuneEnvelopeError::InvalidView { span, .. } =
				transform(source).expect_err("malformed layers sink must refuse atomically")
			else {
				panic!("expected an attributed view refusal")
			};
			assert_eq!(&source[span.start as usize..span.end as usize], rejected);
		}
		let computed = transform(
			"let marks = [line(\"x\", \"y\")]; workspace.table(\"t\").views([layers(marks)])",
		)
		.expect("PRODUCT-0076 executes a bound layer array");
		assert!(matches!(
			computed.views[0].intent,
			RuneViewIntent::Plot(RunePlotIntent {
				body: RunePlotBody::Layered(_),
				..
			})
		));
	}

	#[test]
	fn product_0008_orders_within_sets_then_by_emission_position() {
		let table_before_emit = envelope(
			"let r = workspace.table(\"t\"); let v = r.views([scatter(\"c\",\"v\")]); table(r); v",
		);
		assert_eq!(
			table_before_emit
				.semantics()
				.sinks
				.iter()
				.map(RuneViewIntent::form)
				.collect::<Vec<_>>(),
			[RuneViewForm::Table, RuneViewForm::Scatter]
		);

		let emit_before_table = envelope(
			"let r = workspace.table(\"t\"); let v = r.views([scatter(\"c\",\"v\")]); v; table(r)",
		);
		assert_eq!(
			emit_before_table
				.semantics()
				.sinks
				.iter()
				.map(RuneViewIntent::form)
				.collect::<Vec<_>>(),
			[RuneViewForm::Scatter, RuneViewForm::Table]
		);

		let reverse_definition_order = envelope(
			"let r = workspace.table(\"t\"); let scatter_view = r.views([scatter(\"c\",\"v\")]); let bar_view = r.views([bar(\"c\",\"v\")]); bar_view; scatter_view",
		);
		assert_eq!(
			reverse_definition_order
				.semantics()
				.sinks
				.iter()
				.map(RuneViewIntent::form)
				.collect::<Vec<_>>(),
			[RuneViewForm::Bar, RuneViewForm::Scatter]
		);

		let list_then_chain = envelope(
			"workspace.table(\"t\").views([table(), scatter(\"c\",\"v\")]).views([bar(\"c\",\"v\")])",
		);
		assert_eq!(
			list_then_chain
				.semantics()
				.sinks
				.iter()
				.map(RuneViewIntent::form)
				.collect::<Vec<_>>(),
			[
				RuneViewForm::Table,
				RuneViewForm::Scatter,
				RuneViewForm::Bar
			]
		);
	}

	#[test]
	fn product_0008_context_is_total_atomic_and_span_attributed() {
		let ordinary = "fn scatter(a, b) { a + b } let s = scatter(1, 2); helper(scatter(3, 4)); let workspace = local; workspace.scatter(\"c\", \"v\")";
		assert_eq!(transform(ordinary), Err(RuneEnvelopeError::NoTerminalView));

		for source in [
			"workspace.table(\"t\").views([])",
			"workspace.table(\"t\").views([]).views([table()])",
			"workspace.table(\"t\").views([scatter(\"c\")])",
			"workspace.table(\"t\").views([scatter(c, \"v\")])",
			"workspace.table(\"t\").views([unknown()])",
			"workspace.table(\"t\").views([view(\"n\")])",
			"workspace.table(\"t\").views([view(name, table())])",
			"workspace.table(\"t\").views([view(\"outer\", view(\"inner\", table()))])",
		] {
			let error = transform(source).expect_err("proven view syntax must refuse atomically");
			let RuneEnvelopeError::InvalidView { span, .. } = error else {
				panic!("expected attributed view refusal for {source}: {error:?}");
			};
			assert!(span.start < span.end, "refusal span must name source");
		}

		let partial = "workspace.table(\"t\").views([table(), unknown()])";
		assert!(matches!(
			transform(partial),
			Err(RuneEnvelopeError::InvalidView { .. })
		));
		assert_eq!(
			transform("workspace.table(\"t\").views([table()])")
				.expect("positive contextual control")
				.views
				.len(),
			1
		);
		let computed =
			transform("let s = scatter(\"c\", \"v\"); workspace.table(\"t\").views([s])")
				.expect("PRODUCT-0076 executes a bound static slot");
		assert_eq!(computed.views[0].intent.form(), RuneViewForm::Scatter);
		assert_eq!(
			transform("let v = workspace.table(\"t\").views([table()]); consume(v)"),
			Err(RuneEnvelopeError::RecognizedNoOutput),
			"a bound view set remains distinct from malformed presentation"
		);
	}

	#[test]
	fn product_0008_bound_but_unused_view_set_is_a_distinct_no_output_outcome() {
		assert_eq!(
			transform("let v = workspace.table(\"t\").views([table()]);"),
			Err(RuneEnvelopeError::RecognizedNoOutput)
		);
		assert_eq!(
			transform("let answer = 42; answer"),
			Err(RuneEnvelopeError::NoTerminalView)
		);
	}

	#[test]
	fn zero_roots_produce_no_envelope_and_no_scheduled_execution() {
		assert_eq!(
			transform("let r = 1; r"),
			Err(RuneEnvelopeError::NoTerminalView)
		);

		let scheduler = RuneViewScheduler::default();
		assert_eq!((scheduler.submitted, scheduler.executed), (0, 0));

		let view = transform("let r = workspace.table(\"t\"); table(r)")
			.expect("one-root positive control");
		let mut scheduler = RuneViewScheduler::default();
		assert!(
			scheduler
				.submit(PendingRuneView {
					source_buffer: BufferId::new(42),
					revision: 1,
					view,
				})
				.is_some()
		);
		assert_eq!((scheduler.submitted, scheduler.executed), (1, 1));
	}

	#[test]
	fn product_0027_table_skin_projection_flattens_in_every_order_and_identity_position() {
		let transformed = transform(
			"workspace.table(\"t\").views([table().skin(\"terrain_rs10\"), view(\"named\", table().skin(\"nord_r\").duotone()), table().duotone().skin(\"nord_r\"), table()])",
		)
		.expect("table skin forms");
		assert_eq!(transformed.views.len(), 4);

		let RuneViewIntent::Table(first) = &transformed.views[0].intent else {
			panic!("first sink must remain a table");
		};
		assert_eq!(
			first.skin,
			RuneTableSkinIntent::NamedTheme {
				name: "terrain_rs10".to_owned(),
				projection: RuneTableSkinProjection::PaletteForward,
			}
		);
		assert_eq!(
			transformed.views[0].identity,
			LocalViewIdentity::Anonymous(0)
		);

		let RuneViewIntent::Table(named) = &transformed.views[1].intent else {
			panic!("named sink must remain a table");
		};
		assert_eq!(
			named.skin,
			RuneTableSkinIntent::NamedTheme {
				name: "nord_r".to_owned(),
				projection: RuneTableSkinProjection::Duotone,
			}
		);
		assert_eq!(
			transformed.views[1].identity,
			LocalViewIdentity::Named("named".to_owned())
		);

		assert_eq!(
			transformed.views[2].intent, transformed.views[1].intent,
			"modifier order must not survive flattening"
		);
		assert_eq!(
			transformed.views[3].intent,
			RuneViewIntent::Table(RuneTableSink::default())
		);
	}

	#[test]
	fn product_0027_table_modifier_register_is_closed_bounded_and_atomic() {
		for (sink, rejected) in [
			("table().skin()", ".skin()"),
			(
				"table().skin(\"nord\", \"terrain\")",
				".skin(\"nord\", \"terrain\")",
			),
			("table().skin(theme_name)", "theme_name"),
			(
				"table().skin(\"nord\").skin(\"terrain\")",
				".skin(\"terrain\")",
			),
			("table().duotone()", ".duotone()"),
			("table().skin(\"nord\").duotone(1)", ".duotone(1)"),
			("table().skin(\"nord\").duotone().duotone()", ".duotone()"),
			("table().point_size(2.0)", "table().point_size(2.0)"),
			("table().frobnicate(1)", "table().frobnicate(1)"),
			("table(\"wrong\").skin(\"nord\")", "table(\"wrong\")"),
			(
				"scatter(\"x\", \"y\").skin(\"nord\")",
				"scatter(\"x\", \"y\").skin(\"nord\")",
			),
		] {
			let source =
				format!("workspace.table(\"t\").views([table(), {sink}, line(\"x\", \"y\")])");
			let error = transform(&source).expect_err("invalid modifier must refuse the view list");
			let RuneEnvelopeError::InvalidView { span, .. } = error else {
				panic!("expected an attributed table modifier refusal");
			};
			assert_eq!(
				&source[span.start as usize..span.end as usize],
				rejected,
				"{sink}"
			);
		}
	}

	#[test]
	fn product_0052_show_types_has_no_skin_dependency_and_the_register_remains_a_set() {
		let transformed = transform(
			"workspace.table(\"t\").views([table().show_types(), view(\"named\", table().skin(\"nord\").show_types()), table().show_types().skin(\"nord\").duotone(), table().duotone().show_types().skin(\"nord\"), table()])",
		)
		.expect("authored table type visibility forms");
		assert_eq!(transformed.views.len(), 5);

		let RuneViewIntent::Table(types_only) = &transformed.views[0].intent else {
			panic!("first sink must remain a table");
		};
		assert_eq!(types_only.skin, RuneTableSkinIntent::Default);
		assert_eq!(types_only.show_column_types, Override::Set(true));

		let RuneViewIntent::Table(named) = &transformed.views[1].intent else {
			panic!("named sink must remain a table");
		};
		assert_eq!(named.show_column_types, Override::Set(true));
		assert_eq!(
			named.skin,
			RuneTableSkinIntent::NamedTheme {
				name: "nord".to_owned(),
				projection: RuneTableSkinProjection::PaletteForward,
			}
		);
		assert_eq!(
			transformed.views[2].intent, transformed.views[3].intent,
			"equal modifier sets must flatten identically in either order"
		);
		let RuneViewIntent::Table(duotone) = &transformed.views[2].intent else {
			panic!("duotone sink must remain a table");
		};
		assert_eq!(duotone.show_column_types, Override::Set(true));
		assert_eq!(
			duotone.skin,
			RuneTableSkinIntent::NamedTheme {
				name: "nord".to_owned(),
				projection: RuneTableSkinProjection::Duotone,
			}
		);
		assert_eq!(
			transformed.views[4].intent,
			RuneViewIntent::Table(RuneTableSink::default())
		);
	}

	#[test]
	fn product_0040_table_skin_register_resolves_bindings_in_every_member_order() {
		let source = r#"
let theme = "vapor";
let alias = theme;
workspace.table("t").views([
    table().skin(theme),
    table().show_types().skin(alias),
    table().skin(alias).show_types(),
    table().skin(theme).duotone(),
    table().duotone().skin(alias),
])
"#;
		let transformed = transform(source).expect("bound table skin forms");
		assert_eq!(transformed.views.len(), 5);

		let named = RuneTableSkinIntent::NamedTheme {
			name: "vapor".to_owned(),
			projection: RuneTableSkinProjection::PaletteForward,
		};
		let duotone = RuneTableSkinIntent::NamedTheme {
			name: "vapor".to_owned(),
			projection: RuneTableSkinProjection::Duotone,
		};
		for index in [0, 1, 2] {
			let RuneViewIntent::Table(table) = &transformed.views[index].intent else {
				panic!("bound table skin must remain a table")
			};
			assert_eq!(table.skin, named);
			assert_eq!(
				table.show_column_types,
				if index == 0 {
					Override::Inherit
				} else {
					Override::Set(true)
				}
			);
		}
		for index in [3, 4] {
			let RuneViewIntent::Table(table) = &transformed.views[index].intent else {
				panic!("bound duotone skin must remain a table")
			};
			assert_eq!(table.skin, duotone);
		}
	}

	#[test]
	fn product_0040_table_skin_register_refuses_bad_bindings_atomically() {
		for (source, chain, expected) in [
			(
				"let theme = make_theme(); workspace.table(\"t\").views([table().show_types().skin(theme)])",
				"make_theme",
				"Missing item make_theme",
			),
			(
				"let theme = 7; workspace.table(\"t\").views([table().skin(theme).duotone()])",
				".skin(theme)",
				"requires a theme string",
			),
			(
				"workspace.table(\"t\").views([table().skin(theme)]); let theme = \"vapor\";",
				"theme",
				"No local variable",
			),
			(
				"workspace.table(\"t\").views([table().skin(make_theme())])",
				"make_theme",
				"Missing item make_theme",
			),
		] {
			let RuneEnvelopeError::InvalidView { span, message } =
				transform(source).expect_err("invalid bound table skin must refuse")
			else {
				panic!("expected attributed table-skin refusal")
			};
			assert_eq!(&source[span.start as usize..span.end as usize], chain);
			assert!(message.contains(expected), "{message}");
		}
	}

	#[test]
	fn product_0052_show_types_refusals_are_atomic_and_duotone_owns_its_dependency() {
		for (sink, rejected, message) in [
			(
				"table().show_types(1)",
				".show_types(1)",
				"show_types() takes no arguments",
			),
			(
				"table().show_types().show_types()",
				".show_types()",
				"show_types() may appear at most once on a table sink",
			),
			(
				"table().duotone().show_types()",
				".duotone()",
				"duotone() requires skin(string)",
			),
		] {
			let source =
				format!("workspace.table(\"t\").views([table(), {sink}, line(\"x\", \"y\")])");
			let error = transform(&source).expect_err("invalid modifier must refuse the view list");
			let RuneEnvelopeError::InvalidView {
				span,
				message: actual,
			} = error
			else {
				panic!("expected an attributed table modifier refusal");
			};
			assert_eq!(&source[span.start as usize..span.end as usize], rejected);
			assert_eq!(actual, message);
		}

		let source = r#"
let held = table().show_types();
consume(table().show_types());
workspace.table("t").views([table()])
"#;
		let error = transform(source).expect_err("presentation calls cannot leak through consume");
		assert!(error.to_string().contains("consume"), "{error}");
	}

	#[test]
	fn product_0027_table_projection_calls_are_ordinary_rune_outside_proven_positions() {
		let source = r#"
let styled = table().skin("terrain_rs10");
consume(table().skin("nord"));
consume(table().skin("yellows").duotone());
workspace.table("t").views([table()])
"#;
		let error = transform(source).expect_err("presentation calls cannot leak through consume");
		assert!(error.to_string().contains("consume"), "{error}");

		let sugar = transform("workspace.table(\"t\").table()")
			.expect("accepted relation table sugar remains default");
		assert_eq!(
			sugar.views[0].intent,
			RuneViewIntent::Table(RuneTableSink::default())
		);
	}

	fn relation_verb_findings(source: &str) -> Vec<&'static str> {
		let production = source
			.rsplit_once("\n#[cfg(test)]\nmod tests")
			.map(|(production, _)| production)
			.unwrap_or(source);
		let identifiers = production
			.split(|character: char| !(character == '_' || character.is_alphanumeric()))
			.collect::<HashSet<_>>();
		[
			"project",
			"filter",
			"aggregate",
			"sort",
			"limit",
			"join",
			"window",
		]
		.into_iter()
		.filter(|name| identifiers.contains(name))
		.collect()
	}

	#[test]
	fn product_0003_analysis_never_interprets_relation_verbs() {
		let source = include_str!("rune_relation_envelope.rs");
		assert!(
			relation_verb_findings(source).is_empty(),
			"PRODUCT-0003 keeps relation semantics in Toron"
		);
		assert_eq!(
			relation_verb_findings("fn analyze() { relation.sort(keys); }"),
			vec!["sort"],
			"positive control: the assay must observe a synthetic relation verb"
		);
	}

	#[test]
	fn scheduler_executes_in_flight_then_only_the_newest_pending_view() {
		fn work(buffer: BufferId, revision: u64) -> PendingRuneView {
			PendingRuneView {
				source_buffer: buffer,
				revision,
				view: transform(
					"workspace.table(\"a\").views([table()]); workspace.table(\"b\").views([bar(\"cat\", \"val\")])",
				)
				.expect("two-origin view program"),
			}
		}
		fn origins(work: &PendingRuneView) -> Vec<String> {
			work.view
				.executions
				.iter()
				.map(|execution| execution.origin.clone())
				.collect()
		}
		let buffer = BufferId::new(7);
		let mut scheduler = RuneViewScheduler::default();
		let first = work(buffer, 1);
		let first_origins = origins(&first);
		assert_eq!(scheduler.submit(first).map(|work| work.revision), Some(1));
		assert!(scheduler.submit(work(buffer, 2)).is_none());
		assert!(scheduler.submit(work(buffer, 3)).is_none());
		assert!(scheduler.complete(buffer, &first_origins[1]).is_none());
		assert_eq!(
			scheduler
				.complete(buffer, &first_origins[0])
				.map(|work| work.revision),
			Some(3)
		);
		let current = work(buffer, 3);
		let current_origins = origins(&current);
		assert!(scheduler.complete(buffer, &current_origins[0]).is_none());
		assert!(scheduler.complete(buffer, &current_origins[1]).is_none());
		assert_eq!(
			(
				scheduler.submitted,
				scheduler.executed,
				scheduler.superseded_before_execution
			),
			(3, 2, 1)
		);

		scheduler.submit(work(buffer, 4));
		scheduler.submit(work(buffer, 5));
		scheduler.cancel(buffer);
		assert!(!scheduler.is_in_flight(buffer));
		assert_eq!(
			scheduler.submit(work(buffer, 6)).map(|work| work.revision),
			Some(6),
			"an unavailable workspace must not leave the buffer permanently in flight"
		);
	}

	#[test]
	fn product_0076_presentation_scheduler_coalesces_before_invocation() {
		fn work(buffer: BufferId, revision: u64) -> PendingRunePresentation {
			PendingRunePresentation::new(buffer, revision, format!("revision {revision}"))
		}
		let buffer = BufferId::new(76);
		let mut scheduler = RuneViewScheduler::default();
		let first = scheduler
			.submit_presentation(work(buffer, 1))
			.expect("first revision is offered");
		assert!(first.is_current());
		assert!(scheduler.submit_presentation(work(buffer, 2)).is_none());
		assert!(scheduler.submit_presentation(work(buffer, 3)).is_none());
		assert!(
			!first.is_current(),
			"the blocking closure can reject the old revision before invoking Rune"
		);
		let completion = scheduler.complete_presentation(buffer, 1);
		assert!(!completion.accepted);
		let newest = completion.next.expect("newest retained revision starts");
		assert_eq!(newest.revision, 3);
		assert!(newest.is_current());
		let completion = scheduler.complete_presentation(buffer, 3);
		assert!(completion.accepted);
		assert!(completion.next.is_none());
		assert_eq!(
			(
				scheduler.presentation_submitted,
				scheduler.presentation_executed,
				scheduler.presentation_superseded_before_execution,
			),
			(3, 2, 1)
		);
	}

	#[test]
	fn product_0076_in_flight_presentation_completion_cannot_publish_after_supersession() {
		let buffer = BufferId::new(77);
		let mut scheduler = RuneViewScheduler::default();
		let first = scheduler
			.submit_presentation(PendingRunePresentation::new(buffer, 1, "old".to_owned()))
			.expect("first starts");
		assert!(first.is_current());
		assert!(
			scheduler
				.submit_presentation(PendingRunePresentation::new(buffer, 2, "new".to_owned(),))
				.is_none()
		);
		let old = scheduler.complete_presentation(buffer, 1);
		assert!(
			!old.accepted,
			"an already-computed old result cannot publish"
		);
		assert_eq!(old.next.as_ref().map(|work| work.revision), Some(2));
		let current = scheduler.complete_presentation(buffer, 2);
		assert!(current.accepted);
	}

	#[test]
	fn product_0142_relation_plan_key_ignores_presentation_and_source_offsets() {
		let baseline =
			transform("workspace.table(\"t\").views([figure([title(\"A\"), line(\"x\", \"y\")])])")
				.expect("baseline compile");
		let candidate = transform(
			"workspace.table(\"t\").views([figure([title(\"A much longer title\"), line(\"x\", \"y\")])])",
		)
		.expect("candidate compile");
		assert_ne!(
			baseline, candidate,
			"presentation intent and diagnostic offsets really did change"
		);
		let baseline_key = RelationPlanKey::from_view(&baseline);
		let candidate_key = RelationPlanKey::from_view(&candidate);
		assert_eq!(baseline_key, candidate_key);
		assert!(
			ArtifactReuseWitness {
				baseline: baseline_key,
				artifact_generation: 12,
			}
			.proves(&candidate_key, 12)
		);
	}

	#[test]
	fn product_0142_relation_plan_key_refuses_changed_relation_work_or_stale_artifact() {
		let baseline = transform("workspace.table(\"t\").views([line(\"x\", \"y\")])")
			.expect("baseline compile");
		let candidate = transform(
			"workspace.table(\"t\").filter(col(\"x\").gt(0)).views([line(\"x\", \"y\")])",
		)
		.expect("changed relation compile");
		let baseline_key = RelationPlanKey::from_view(&baseline);
		let candidate_key = RelationPlanKey::from_view(&candidate);
		assert_ne!(baseline_key, candidate_key);
		let witness = ArtifactReuseWitness {
			baseline: baseline_key.clone(),
			artifact_generation: 12,
		};
		assert!(!witness.proves(&candidate_key, 12));
		assert!(!witness.proves(&baseline_key, 13));
	}

	#[test]
	#[should_panic(expected = "Rune view completion requires an in-flight buffer")]
	fn scheduler_completion_without_in_flight_work_is_a_hard_invariant() {
		let mut scheduler = RuneViewScheduler::default();
		let _ = scheduler.complete(BufferId::new(99), "workspace.table(\"t\")");
	}
}
