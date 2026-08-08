//! PRODUCT-0003's deliberately tiny terminal-view envelope.
//!
//! This module recognizes presentation calls and rewrites their relation
//! argument into Toron's entrypoint, naming its single capability parameter
//! `workspace` or the trial alias `ket` to match the proven relation root. It
//! owns no relation verbs, typing, planning, or lowering; those belong
//! exclusively to Toron's native Rune frontend.

use crate::plot::chrome::PlotChromeProjection;
use crate::plot::common::TickPlacement;
use crate::plot::composition::PlotComposition;
use crate::plot::core::PlotType;
use crate::plot::spec::{
	BarWidth, CategoryGap, LayerMark, LayerSettings, LayerSpec, LayeredPlotSpec, LegendPosition,
	Override, PointSize, Presence, StrokeWidth,
};
use crate::plot::theme::{
	PlotDomainOverrides, PlotGuideSettings, PlotSettingsLayer, PlotSkinSelection,
};
use libaryon::core::BufferId;
use rune::ast::{self, Spanned};
use std::collections::{HashMap, HashSet};
use std::ops::Range;
use std::sync::Arc;

const WORKSPACE_ENTRYPOINT_PREFIX: &str = "pub fn main(workspace) {\n";
const KET_ENTRYPOINT_PREFIX: &str = "pub fn main(ket) {\n";
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
	pub(crate) facet: Option<String>,
	pub(crate) authored_settings: PlotSettingsLayer,
	pub(crate) plot_skin_span: Option<Range<u32>>,
	pub(crate) facet_span: Option<Range<u32>>,
	pub(crate) domain_spans: RunePlotDomainSpans,
	pub(crate) tick_step_spans: RunePlotTickStepSpans,
	pub(crate) minor_tick_spans: RunePlotMinorTickSpans,
}

impl PartialEq for RunePlotIntent {
	fn eq(&self, other: &Self) -> bool {
		self.body == other.body
			&& self.facet == other.facet
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
}

impl StyledRunePlotSink {
	pub(crate) fn into_layer(self) -> Result<LayerSpec, Self> {
		if self.sink.category.is_some() {
			return Err(self);
		}
		let mark = match self.sink.composition.primary_plot_type() {
			PlotType::Line => LayerMark::Line,
			PlotType::Scatter => LayerMark::Scatter,
			PlotType::Bar => return Err(self),
			other => unreachable!("styled Rune sink admitted unexpected plot type {other:?}"),
		};
		let mut layer = LayerSpec::xy(mark, self.sink.x, self.sink.y);
		layer.settings = self.settings;
		Ok(layer)
	}
}

impl RunePlotIntent {
	fn bare(body: RunePlotBody) -> Self {
		Self {
			body,
			facet: None,
			authored_settings: PlotSettingsLayer::default(),
			plot_skin_span: None,
			facet_span: None,
			domain_spans: RunePlotDomainSpans::default(),
			tick_step_spans: RunePlotTickStepSpans::default(),
			minor_tick_spans: RunePlotMinorTickSpans::default(),
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum ViewContentKind {
	Table,
	Plot,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ViewPresentation {
	pub(crate) content_kind: ViewContentKind,
	pub(crate) display_label: String,
	pub(crate) authored_title: Presence<Arc<String>>,
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
}

#[cfg(test)]
impl From<RuneViewForm> for ViewPresentation {
	fn from(form: RuneViewForm) -> Self {
		let (content_kind, display_label) = match form {
			RuneViewForm::Table => (ViewContentKind::Table, "table"),
			RuneViewForm::Bar => (ViewContentKind::Plot, "plot.bar"),
			RuneViewForm::Line => (ViewContentKind::Plot, "plot.line"),
			RuneViewForm::Scatter => (ViewContentKind::Plot, "plot.scatter"),
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
						other => unreachable!(
							"first-contact sink admitted unexpected plot type {other:?}"
						),
					},
					RunePlotBody::StyledSingle(styled) => {
						match styled.sink.composition.primary_plot_type() {
							PlotType::Bar => "plot.bar".to_owned(),
							PlotType::Line => "plot.line".to_owned(),
							PlotType::Scatter => "plot.scatter".to_owned(),
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

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum LocalViewIdentity {
	Named(String),
	Anonymous(usize),
}

/// First-contact presentation data only. This is not `PlotSpec` v0 and dies
/// when the plotting contract lands.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DisposableRunePlotSink {
	pub(crate) composition: PlotComposition,
	pub(crate) x: String,
	pub(crate) y: String,
	pub(crate) category: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct SourceMapSegment {
	transformed: Range<u32>,
	original_start: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RuneEnvelopeSourceMap {
	segments: Vec<SourceMapSegment>,
	diagnostic_fallback: Range<u32>,
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
	executions: Vec<TransformedRuneRelation>,
	presentation_order: Vec<LocalViewIdentity>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct TransformedRuneRelation {
	pub(crate) origin: String,
	pub(crate) source: String,
	pub(crate) views: Vec<SpannedRuneViewIntent>,
	pub(crate) source_map: RuneEnvelopeSourceMap,
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

#[derive(Default)]
pub(crate) struct RuneViewScheduler {
	in_flight: HashMap<BufferId, HashSet<String>>,
	pending: HashMap<BufferId, PendingRuneView>,
	pub(crate) submitted: u64,
	pub(crate) executed: u64,
	pub(crate) superseded_before_execution: u64,
}

impl RuneViewScheduler {
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
				"Rune relation views use top-level table(relation), plot.bar(relation, \"x\", \"y\"), plot.line(relation, \"x\", \"y\"), or plot.scatter(relation, \"x\", \"y\")",
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

pub(crate) fn transform(source: &str) -> Result<TransformedRuneView, RuneEnvelopeError> {
	let analysis = analyze(source)?;
	let recognized_presentation = analysis.recognized_presentation;
	let envelopes = analysis.into_envelopes();
	if envelopes.is_empty() {
		return Err(if recognized_presentation {
			RuneEnvelopeError::RecognizedNoOutput
		} else {
			RuneEnvelopeError::NoTerminalView
		});
	}
	let executions = envelopes
		.iter()
		.map(|envelope| render(source, envelope))
		.collect::<Result<Vec<_>, _>>()?;
	let mut ordered = executions
		.iter()
		.flat_map(|execution| execution.views.iter())
		.collect::<Vec<_>>();
	ordered.sort_by_key(|view| view.output_order);
	Ok(TransformedRuneView {
		presentation_order: ordered
			.into_iter()
			.map(|view| view.identity.clone())
			.collect(),
		executions,
	})
}

#[derive(Clone, Debug, PartialEq)]
struct OutputRootAnalysis {
	envelopes: Vec<ExecutionEnvelope>,
	recognized_presentation: bool,
}

impl OutputRootAnalysis {
	#[cfg(test)]
	fn into_envelope(self) -> Option<ExecutionEnvelope> {
		let mut envelopes = self.envelopes;
		(envelopes.len() == 1).then(|| envelopes.remove(0))
	}

	fn into_envelopes(self) -> Vec<ExecutionEnvelope> {
		self.envelopes
	}

	#[cfg(test)]
	fn semantics(&self) -> BufferViewSemantics {
		let mut views = self
			.envelopes
			.iter()
			.flat_map(|envelope| envelope.sinks.iter())
			.collect::<Vec<_>>();
		views.sort_by_key(|sink| sink.output_order);
		BufferViewSemantics {
			executions: self
				.envelopes
				.iter()
				.map(|envelope| envelope.relation_origin.clone())
				.collect(),
			presentation_order: views.into_iter().map(|sink| sink.intent.clone()).collect(),
			execution_count: self.envelopes.len(),
		}
	}
}

#[derive(Clone, Debug, PartialEq)]
struct ExecutionEnvelope {
	retained: Vec<Range<usize>>,
	relation: Range<usize>,
	relation_root: RelationRoot,
	relation_origin: String,
	sinks: NonEmptySinks,
	terminate_retained_tail: bool,
}

#[derive(Clone, Debug, PartialEq)]
struct NonEmptySinks {
	first: ParsedSink,
	rest: Vec<ParsedSink>,
}

impl NonEmptySinks {
	fn iter(&self) -> impl Iterator<Item = &ParsedSink> {
		std::iter::once(&self.first).chain(&self.rest)
	}
}

#[cfg(test)]
#[derive(Clone, Debug, PartialEq)]
struct EnvelopeSemantics {
	relation_origin: String,
	sinks: Vec<RuneViewIntent>,
}

#[cfg(test)]
#[derive(Clone, Debug, PartialEq)]
struct BufferViewSemantics {
	executions: Vec<String>,
	presentation_order: Vec<RuneViewIntent>,
	execution_count: usize,
}

impl ExecutionEnvelope {
	#[cfg(test)]
	fn semantics(&self) -> EnvelopeSemantics {
		EnvelopeSemantics {
			relation_origin: self.relation_origin.clone(),
			sinks: self.sinks.iter().map(|sink| sink.intent.clone()).collect(),
		}
	}
}

#[derive(Clone, Debug, PartialEq)]
struct ParsedSink {
	intent: RuneViewIntent,
	span: Range<usize>,
	name: Option<ParsedViewName>,
	output_order: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ParsedViewName {
	value: String,
	span: Range<usize>,
}

#[derive(Clone, Debug)]
struct ParsedOutputRoot {
	emission: Range<usize>,
	relation: Range<usize>,
	relation_root: RelationRoot,
	relation_origin: String,
	sinks: Vec<ParsedSink>,
}

#[derive(Clone, Debug)]
struct LocalBinding {
	name: String,
	initializer: Range<usize>,
	statement: Range<usize>,
	statement_start: usize,
}

#[derive(Clone, Debug, PartialEq)]
enum StaticLiteralValue {
	String(String),
	Number(f64),
}

#[derive(Clone, Debug, PartialEq)]
struct ResolvedStaticLiteral {
	value: StaticLiteralValue,
	binding: Option<String>,
}

#[derive(Clone, Copy, Debug, Default)]
struct ViewShadows {
	table: bool,
	plot: bool,
	workspace: bool,
	ket: bool,
}

impl ViewShadows {
	fn insert(&mut self, name: &str) {
		match name {
			"table" => self.table = true,
			"plot" => self.plot = true,
			"workspace" => self.workspace = true,
			"ket" => self.ket = true,
			_ => {}
		}
	}
}

#[derive(Clone, Debug)]
enum ExpressionProvenance {
	RelationCandidate {
		span: Range<usize>,
		root: RelationRoot,
		origin: String,
	},
	ViewSet {
		relation: Range<usize>,
		root: RelationRoot,
		origin: String,
		sinks: Vec<ParsedSink>,
	},
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RelationRoot {
	Workspace,
	Ket,
}

fn analyze(source: &str) -> Result<OutputRootAnalysis, RuneEnvelopeError> {
	let block = rune::parse::parse_all::<ast::EmptyBlock>(source, rune::SourceId::EMPTY, false)
		.map_err(|error| RuneEnvelopeError::Parse(error.to_string()))?;

	// Whole-buffer shadowing is deliberately conservative. Complete this pass
	// before classifying any root so a later binding can suppress an earlier call.
	let mut shadows = ViewShadows::default();
	let mut bindings = Vec::new();
	for statement in &block.statements {
		collect_top_level_bindings(statement, source, &mut shadows, &mut bindings)?;
	}

	let mut kinds = HashMap::<String, ExpressionProvenance>::new();
	let mut roots = Vec::<ParsedOutputRoot>::new();
	let mut peeled = Vec::<Range<usize>>::new();
	let mut recognized_presentation = false;
	let mut last_retained_is_expression = false;
	for statement in &block.statements {
		if let Some(root) = recognize_view_root(statement, source, shadows)? {
			let (relation_origin, relation_root) =
				resolve_relation_origin(source, &root, &bindings, shadows)?;
			peeled.push(root.statement.clone());
			roots.push(ParsedOutputRoot {
				emission: root.statement.clone(),
				relation: root.relation,
				relation_root,
				relation_origin,
				sinks: vec![ParsedSink {
					intent: root.intent,
					span: root.statement,
					name: None,
					output_order: 0,
				}],
			});
			continue;
		}

		if let ast::Stmt::Local(local) = statement {
			let statement_span = byte_range(statement.span(), source.len())?;
			let provenance =
				expression_provenance(&local.expr, source, &kinds, shadows, &bindings)?;
			if let Some(binding) = bindings
				.iter()
				.find(|binding| binding.statement == statement_span)
				&& let Some(provenance) = provenance
			{
				if matches!(provenance, ExpressionProvenance::ViewSet { .. }) {
					recognized_presentation = true;
					peeled.push(statement_span);
				}
				last_retained_is_expression = false;
				kinds.insert(binding.name.clone(), provenance);
				continue;
			}
			last_retained_is_expression = false;
			continue;
		}

		let expression = match statement {
			ast::Stmt::Expr(expression) => Some(expression),
			ast::Stmt::Semi(semi) => Some(&semi.expr),
			_ => None,
		};
		if let Some(expression) = expression
			&& let Some(ExpressionProvenance::ViewSet {
				relation,
				root,
				origin,
				sinks,
			}) = expression_provenance(expression, source, &kinds, shadows, &bindings)?
		{
			let emission = byte_range(statement.span(), source.len())?;
			recognized_presentation = true;
			peeled.push(emission.clone());
			roots.push(ParsedOutputRoot {
				emission,
				relation,
				relation_root: root,
				relation_origin: origin,
				sinks,
			});
			last_retained_is_expression = false;
			continue;
		}
		last_retained_is_expression = matches!(statement, ast::Stmt::Expr(_));
	}
	roots.sort_by_key(|root| root.emission.start);
	peeled.sort_by_key(|region| region.start);

	if roots.is_empty() {
		return Ok(OutputRootAnalysis {
			envelopes: Vec::new(),
			recognized_presentation,
		});
	}

	let retained = retained_regions(source.len(), &peeled)?;
	let mut names = HashMap::<String, Range<u32>>::new();
	let mut next_output_order = 0;
	for root in &mut roots {
		for sink in &mut root.sinks {
			sink.output_order = next_output_order;
			next_output_order += 1;
			if let Some(name) = &sink.name {
				let span = u32_range(&name.span, "view name")?;
				if let Some(first) = names.insert(name.value.clone(), span.clone()) {
					return Err(RuneEnvelopeError::DuplicateName {
						name: name.value.clone(),
						first,
						duplicate: span,
					});
				}
			}
		}
	}

	let mut grouped = Vec::<ParsedOutputRoot>::new();
	let mut group_by_origin = HashMap::<String, usize>::new();
	for root in roots {
		if let Some(index) = group_by_origin.get(&root.relation_origin).copied() {
			grouped[index].sinks.extend(root.sinks);
		} else {
			group_by_origin.insert(root.relation_origin.clone(), grouped.len());
			grouped.push(root);
		}
	}

	let envelopes = grouped
		.into_iter()
		.map(|root| {
			let mut sinks = root.sinks.into_iter();
			let first = sinks.next().expect("an output root has at least one sink");
			ExecutionEnvelope {
				retained: retained.clone(),
				relation: root.relation,
				relation_root: root.relation_root,
				relation_origin: root.relation_origin,
				sinks: NonEmptySinks {
					first,
					rest: sinks.collect(),
				},
				terminate_retained_tail: last_retained_is_expression,
			}
		})
		.collect();
	Ok(OutputRootAnalysis {
		envelopes,
		recognized_presentation,
	})
}

fn expression_provenance(
	expression: &ast::Expr,
	source: &str,
	bindings: &HashMap<String, ExpressionProvenance>,
	shadows: ViewShadows,
	literal_bindings: &[LocalBinding],
) -> Result<Option<ExpressionProvenance>, RuneEnvelopeError> {
	match expression {
		ast::Expr::Path(path) => {
			let raw = source_slice(source, path.span())?.trim();
			let root = match raw {
				"workspace" if !shadows.workspace => Some(RelationRoot::Workspace),
				"ket" if !shadows.ket => Some(RelationRoot::Ket),
				_ => None,
			};
			if let Some(root) = root {
				return Ok(Some(ExpressionProvenance::RelationCandidate {
					span: byte_range(path.span(), source.len())?,
					root,
					// Both spellings name one capability, so semantic identity is
					// canonical even though synthesis preserves the authored root.
					origin: "workspace".to_owned(),
				}));
			}
			Ok(bindings.get(raw).cloned())
		}
		ast::Expr::Group(group) => {
			expression_provenance(&group.expr, source, bindings, shadows, literal_bindings)
		}
		ast::Expr::Call(call) => {
			let args = call.args.iter().map(|(arg, _)| arg).collect::<Vec<_>>();
			if let ast::Expr::FieldAccess(field) = call.expr.as_ref() {
				let method = source_slice(source, field.expr_field.span())?.trim();
				let receiver = expression_provenance(
					&field.expr,
					source,
					bindings,
					shadows,
					literal_bindings,
				)?;
				return match receiver {
					Some(ExpressionProvenance::RelationCandidate { span, root, origin }) => {
						provenance_method_on_relation(
							call,
							method,
							&field.expr,
							span,
							root,
							origin,
							&args,
							source,
							literal_bindings,
						)
					}
					Some(ExpressionProvenance::ViewSet {
						relation,
						root,
						origin,
						sinks,
					}) => provenance_method_on_view_set(
						call,
						method,
						relation,
						root,
						origin,
						sinks,
						&args,
						source,
						literal_bindings,
					),
					None => Ok(None),
				};
			}

			if matches!(
				expression_provenance(&call.expr, source, bindings, shadows, literal_bindings)?,
				Some(ExpressionProvenance::ViewSet { .. })
			) {
				return invalid_view(
					call.expr.span(),
					source.len(),
					"a ViewSet may only be emitted or extended with .views([...])",
				);
			}
			for arg in &args {
				if matches!(
					expression_provenance(arg, source, bindings, shadows, literal_bindings)?,
					Some(ExpressionProvenance::ViewSet { .. })
				) {
					return invalid_view(
						arg.span(),
						source.len(),
						"a ViewSet binding may only be emitted or extended with .views([...])",
					);
				}
			}
			Ok(None)
		}
		ast::Expr::Vec(vector) => {
			for (item, _) in vector.items.iter() {
				if matches!(
					expression_provenance(item, source, bindings, shadows, literal_bindings)?,
					Some(ExpressionProvenance::ViewSet { .. })
				) {
					return invalid_view(
						item.span(),
						source.len(),
						"a ViewSet binding may only be emitted or extended with .views([...])",
					);
				}
			}
			Ok(None)
		}
		_ => Ok(None),
	}
}

fn provenance_method_on_relation(
	call: &ast::ExprCall,
	method: &str,
	receiver: &ast::Expr,
	relation: Range<usize>,
	root: RelationRoot,
	mut origin: String,
	args: &[&ast::Expr],
	source: &str,
	literal_bindings: &[LocalBinding],
) -> Result<Option<ExpressionProvenance>, RuneEnvelopeError> {
	match method {
		"views" => {
			let sinks = parse_view_list(call, args, source, literal_bindings)?;
			Ok(Some(ExpressionProvenance::ViewSet {
				relation,
				root,
				origin,
				sinks,
			}))
		}
		"scatter" if matches!(args.len(), 2 | 3) => Ok(Some(ExpressionProvenance::ViewSet {
			relation,
			root,
			origin,
			sinks: vec![parse_sugar_sink(PlotType::Scatter, call, args, source)?],
		})),
		"bar" if matches!(args.len(), 2 | 3) => Ok(Some(ExpressionProvenance::ViewSet {
			relation,
			root,
			origin,
			sinks: vec![parse_sugar_sink(PlotType::Bar, call, args, source)?],
		})),
		"line" if args.len() == 2 => Ok(Some(ExpressionProvenance::ViewSet {
			relation,
			root,
			origin,
			sinks: vec![parse_sugar_sink(PlotType::Line, call, args, source)?],
		})),
		"line" => invalid_view(
			call.span(),
			source.len(),
			"line view sugar requires x and y string fields",
		),
		"table" if args.is_empty() => Ok(Some(ExpressionProvenance::ViewSet {
			relation,
			root,
			origin,
			sinks: vec![ParsedSink {
				intent: RuneViewIntent::Table(RuneTableSink::default()),
				span: byte_range(call.span(), source.len())?,
				name: None,
				output_order: 0,
			}],
		})),
		_ => {
			let receiver = byte_range(receiver.span(), source.len())?;
			let call_span = byte_range(call.span(), source.len())?;
			origin.push_str(source.get(receiver.end..call_span.end).ok_or_else(|| {
				RuneEnvelopeError::Invalid("relation method span is not UTF-8".into())
			})?);
			Ok(Some(ExpressionProvenance::RelationCandidate {
				span: call_span,
				root,
				origin,
			}))
		}
	}
}

fn provenance_method_on_view_set(
	call: &ast::ExprCall,
	method: &str,
	relation: Range<usize>,
	root: RelationRoot,
	origin: String,
	mut sinks: Vec<ParsedSink>,
	args: &[&ast::Expr],
	source: &str,
	literal_bindings: &[LocalBinding],
) -> Result<Option<ExpressionProvenance>, RuneEnvelopeError> {
	if method != "views" {
		return invalid_view(
			call.span(),
			source.len(),
			"a ViewSet may only be emitted or extended with .views([...])",
		);
	}
	sinks.extend(parse_view_list(call, args, source, literal_bindings)?);
	Ok(Some(ExpressionProvenance::ViewSet {
		relation,
		root,
		origin,
		sinks,
	}))
}

fn parse_view_list(
	call: &ast::ExprCall,
	args: &[&ast::Expr],
	source: &str,
	literal_bindings: &[LocalBinding],
) -> Result<Vec<ParsedSink>, RuneEnvelopeError> {
	let [argument] = args else {
		return invalid_view(
			call.span(),
			source.len(),
			"views(...) takes exactly one sink list",
		);
	};
	let ast::Expr::Vec(list) = argument else {
		return invalid_view(
			argument.span(),
			source.len(),
			"views(...) requires a literal sink list",
		);
	};
	if list.items.is_empty() {
		return invalid_view(list.span(), source.len(), "views([]) is empty");
	}
	list.items
		.iter()
		.map(|(item, _)| parse_contextual_sink(item, source, literal_bindings))
		.collect()
}

fn parse_contextual_sink(
	expression: &ast::Expr,
	source: &str,
	literal_bindings: &[LocalBinding],
) -> Result<ParsedSink, RuneEnvelopeError> {
	parse_contextual_sink_inner(expression, source, true, literal_bindings)
}

fn parse_contextual_sink_inner(
	expression: &ast::Expr,
	source: &str,
	allow_identity_wrapper: bool,
	literal_bindings: &[LocalBinding],
) -> Result<ParsedSink, RuneEnvelopeError> {
	if let Some(table) = parse_modified_table(expression, source)? {
		return Ok(ParsedSink {
			intent: RuneViewIntent::Table(table),
			span: byte_range(expression.span(), source.len())?,
			name: None,
			output_order: 0,
		});
	}
	if let Some(layer) = parse_modified_mark(expression, source)? {
		return Ok(ParsedSink {
			intent: RuneViewIntent::Plot(RunePlotIntent::bare(RunePlotBody::StyledSingle(layer))),
			span: byte_range(expression.span(), source.len())?,
			name: None,
			output_order: 0,
		});
	}
	let ast::Expr::Call(call) = expression else {
		return invalid_view(
			expression.span(),
			source.len(),
			"a views list element must be scatter(...), bar(...), line(...), or table()",
		);
	};
	let callee = source_slice(source, call.expr.span())?.trim();
	let args = call.args.iter().map(|(arg, _)| arg).collect::<Vec<_>>();
	if callee == "view" {
		if !allow_identity_wrapper {
			return invalid_view(
				expression.span(),
				source.len(),
				"view(...) wrappers cannot be nested",
			);
		}
		let [name, sink] = args.as_slice() else {
			return invalid_view(
				expression.span(),
				source.len(),
				"view(...) takes a name string and one sink",
			);
		};
		let raw_name = source_slice(source, name.span())?;
		let Some(value) = decode_string_literal(raw_name) else {
			return invalid_view(
				name.span(),
				source.len(),
				"view name must be a string literal",
			);
		};
		let mut parsed = parse_contextual_sink_inner(sink, source, false, literal_bindings)?;
		parsed.name = Some(ParsedViewName {
			value,
			span: byte_range(name.span(), source.len())?,
		});
		return Ok(parsed);
	}
	let intent = match callee {
		"table" if args.is_empty() => RuneViewIntent::Table(RuneTableSink::default()),
		"figure" => {
			RuneViewIntent::Plot(parse_figure(expression, &args, source, literal_bindings)?)
		}
		"bar" if matches!(args.len(), 2 | 3) => {
			let Some(sink) = plot_sink_fields(PlotType::Bar, source, &args, 0)? else {
				return invalid_view(
					expression.span(),
					source.len(),
					"bar fields must be strings",
				);
			};
			RuneViewIntent::Plot(RunePlotIntent::bare(RunePlotBody::Single(sink)))
		}
		"scatter" if matches!(args.len(), 2 | 3) => {
			let Some(sink) = plot_sink_fields(PlotType::Scatter, source, &args, 0)? else {
				return invalid_view(
					expression.span(),
					source.len(),
					"scatter fields must be strings",
				);
			};
			RuneViewIntent::Plot(RunePlotIntent::bare(RunePlotBody::Single(sink)))
		}
		"line" if args.len() == 2 => {
			let Some(sink) = plot_sink_fields(PlotType::Line, source, &args, 0)? else {
				return invalid_view(
					expression.span(),
					source.len(),
					"line fields must be strings",
				);
			};
			RuneViewIntent::Plot(RunePlotIntent::bare(RunePlotBody::Single(sink)))
		}
		"layers" => {
			let specification = parse_layer_list(expression, &args, source)?;
			RuneViewIntent::Plot(RunePlotIntent::bare(RunePlotBody::Layered(specification)))
		}
		"table" => {
			return invalid_view(
				expression.span(),
				source.len(),
				"table() takes no arguments",
			);
		}
		"bar" => {
			return invalid_view(
				expression.span(),
				source.len(),
				"bar(...) takes x and y strings with an optional series",
			);
		}
		"line" => {
			return invalid_view(
				expression.span(),
				source.len(),
				"line(...) takes x and y string fields",
			);
		}
		"scatter" => {
			return invalid_view(
				expression.span(),
				source.len(),
				"scatter(...) takes x and y strings with an optional category",
			);
		}
		_ => {
			return invalid_view(
				expression.span(),
				source.len(),
				"unknown view sink; expected scatter(...), bar(...), line(...), table(), layers([...]), or figure([...])",
			);
		}
	};
	Ok(ParsedSink {
		intent,
		span: byte_range(expression.span(), source.len())?,
		name: None,
		output_order: 0,
	})
}

fn parse_figure(
	outer: &ast::Expr,
	args: &[&ast::Expr],
	source: &str,
	literal_bindings: &[LocalBinding],
) -> Result<RunePlotIntent, RuneEnvelopeError> {
	let [argument] = args else {
		return invalid_view(
			outer.span(),
			source.len(),
			"figure(...) takes exactly one literal figure list",
		);
	};
	let ast::Expr::Vec(list) = argument else {
		return invalid_view(
			argument.span(),
			source.len(),
			"figure(...) requires a literal figure list",
		);
	};
	let mut title = None;
	let mut subtitle = None;
	let mut facet = None;
	let mut skin = None;
	let mut palette = None;
	let mut x_bounds = None;
	let mut y_bounds = None;
	let mut x_tick_step = None;
	let mut y_tick_step = None;
	let mut x_minor_ticks = None;
	let mut y_minor_ticks = None;
	let mut legend = None;
	let mut body = None;
	let mut facet_span = None;
	let mut facet_expr_span = None;
	for (item, _) in list.items.iter() {
		if let Some(selection) = parse_figure_skin(item, source, literal_bindings)? {
			if skin.is_some() {
				return invalid_view(
					item.span(),
					source.len(),
					"a figure has at most one plot skin",
				);
			}
			skin = Some((
				selection,
				u32_range(&byte_range(item.span(), source.len())?, "plot skin")?,
			));
			continue;
		}
		let ast::Expr::Call(call) = item else {
			return invalid_view(
				item.span(),
				source.len(),
				"a figure child must be title(...), subtitle(...), skin(...), palette(...), legend_at(...), hide_legend(), x_bounds(...), y_bounds(...), x_tick_step(...), y_tick_step(...), x_minor_ticks(...), y_minor_ticks(...), or one plot body",
			);
		};
		let callee = source_slice(source, call.expr.span())?.trim();
		let child_args = call.args.iter().map(|(arg, _)| arg).collect::<Vec<_>>();
		match callee {
			"legend_at" => {
				if legend.is_some() {
					return invalid_view(
						item.span(),
						source.len(),
						"a figure has at most one legend placement or visibility element",
					);
				}
				legend = Some(Presence::Show(parse_figure_legend_position(
					item,
					&child_args,
					source,
					literal_bindings,
				)?));
			}
			"hide_legend" => {
				if legend.is_some() {
					return invalid_view(
						item.span(),
						source.len(),
						"a figure has at most one legend placement or visibility element",
					);
				}
				if !child_args.is_empty() {
					return invalid_view(
						item.span(),
						source.len(),
						"hide_legend() takes no arguments",
					);
				}
				legend = Some(Presence::Hide);
			}
			"title" => {
				if title.is_some() {
					return invalid_view(
						item.span(),
						source.len(),
						"a figure has at most one title",
					);
				}
				title = Some(parse_figure_heading(
					item,
					&child_args,
					source,
					"title",
					literal_bindings,
				)?);
			}
			"subtitle" => {
				if subtitle.is_some() {
					return invalid_view(
						item.span(),
						source.len(),
						"a figure has at most one subtitle",
					);
				}
				subtitle = Some(parse_figure_heading(
					item,
					&child_args,
					source,
					"subtitle",
					literal_bindings,
				)?);
			}
			"facet" => {
				if facet.is_some() {
					return invalid_view(
						item.span(),
						source.len(),
						"a figure has at most one facet element",
					);
				}
				facet = Some(parse_figure_heading(
					item,
					&child_args,
					source,
					"facet",
					literal_bindings,
				)?);
				facet_span = Some(u32_range(&byte_range(item.span(), source.len())?, "facet")?);
				facet_expr_span = Some(item.span());
			}
			"palette" => {
				if palette.is_some() {
					return invalid_view(
						item.span(),
						source.len(),
						"a figure has at most one categorical palette",
					);
				}
				palette = Some(parse_figure_heading(
					item,
					&child_args,
					source,
					"palette",
					literal_bindings,
				)?);
			}
			"x_bounds" => {
				if x_bounds.is_some() {
					return invalid_view(
						item.span(),
						source.len(),
						"a figure has at most one x_bounds element",
					);
				}
				x_bounds = Some((
					parse_figure_bounds(item, &child_args, source, "x_bounds", literal_bindings)?,
					u32_range(&byte_range(item.span(), source.len())?, "x bounds")?,
				));
			}
			"y_bounds" => {
				if y_bounds.is_some() {
					return invalid_view(
						item.span(),
						source.len(),
						"a figure has at most one y_bounds element",
					);
				}
				y_bounds = Some((
					parse_figure_bounds(item, &child_args, source, "y_bounds", literal_bindings)?,
					u32_range(&byte_range(item.span(), source.len())?, "y bounds")?,
				));
			}
			"x_tick_step" => {
				if x_tick_step.is_some() {
					return invalid_view(
						item.span(),
						source.len(),
						"a figure has at most one x_tick_step element",
					);
				}
				x_tick_step = Some((
					parse_figure_tick_step(
						item,
						&child_args,
						source,
						"x_tick_step",
						literal_bindings,
					)?,
					u32_range(&byte_range(item.span(), source.len())?, "x tick step")?,
				));
			}
			"y_tick_step" => {
				if y_tick_step.is_some() {
					return invalid_view(
						item.span(),
						source.len(),
						"a figure has at most one y_tick_step element",
					);
				}
				y_tick_step = Some((
					parse_figure_tick_step(
						item,
						&child_args,
						source,
						"y_tick_step",
						literal_bindings,
					)?,
					u32_range(&byte_range(item.span(), source.len())?, "y tick step")?,
				));
			}
			"x_minor_ticks" => {
				if x_minor_ticks.is_some() {
					return invalid_view(
						item.span(),
						source.len(),
						"a figure has at most one x_minor_ticks element",
					);
				}
				x_minor_ticks = Some((
					parse_figure_minor_ticks(
						item,
						&child_args,
						source,
						"x_minor_ticks",
						literal_bindings,
					)?,
					u32_range(&byte_range(item.span(), source.len())?, "x minor ticks")?,
				));
			}
			"y_minor_ticks" => {
				if y_minor_ticks.is_some() {
					return invalid_view(
						item.span(),
						source.len(),
						"a figure has at most one y_minor_ticks element",
					);
				}
				y_minor_ticks = Some((
					parse_figure_minor_ticks(
						item,
						&child_args,
						source,
						"y_minor_ticks",
						literal_bindings,
					)?,
					u32_range(&byte_range(item.span(), source.len())?, "y minor ticks")?,
				));
			}
			_ => {
				if body.is_some() {
					return invalid_view(
						item.span(),
						source.len(),
						"a figure contains exactly one plot body",
					);
				}
				body = Some(parse_figure_body(item, callee, &child_args, source)?);
			}
		}
	}
	let Some(body) = body else {
		return invalid_view(
			facet_expr_span.unwrap_or_else(|| outer.span()),
			source.len(),
			"a figure requires exactly one plot body",
		);
	};
	let mut authored_settings = PlotSettingsLayer::default();
	if let Some(title) = title {
		authored_settings.title = Presence::Show(Arc::new(title));
	}
	if let Some(subtitle) = subtitle {
		authored_settings.subtitle = Presence::Show(Arc::new(subtitle));
	}
	if let Some(legend) = legend {
		authored_settings.legend = legend;
	}
	let plot_skin_span = skin.as_ref().map(|(_, span)| span.clone());
	if let Some((skin, _)) = skin {
		authored_settings.plot_skin = Override::Set(skin);
	}
	if let Some(palette) = palette {
		authored_settings.categorical_palette = Override::Set(Arc::new(palette));
	}
	if x_bounds.is_some() || y_bounds.is_some() {
		authored_settings.domains = Override::Set(PlotDomainOverrides {
			x_min: x_bounds.as_ref().map(|(bounds, _)| bounds.0),
			x_max: x_bounds.as_ref().map(|(bounds, _)| bounds.1),
			y_min: y_bounds.as_ref().map(|(bounds, _)| bounds.0),
			y_max: y_bounds.as_ref().map(|(bounds, _)| bounds.1),
		});
	}
	let domain_spans = RunePlotDomainSpans {
		x: x_bounds.map(|(_, span)| span),
		y: y_bounds.map(|(_, span)| span),
	};
	if x_tick_step.is_some()
		|| y_tick_step.is_some()
		|| x_minor_ticks.is_some()
		|| y_minor_ticks.is_some()
	{
		let mut guides = PlotGuideSettings::default();
		let default_x_minor_ticks = guides.x_minor_ticks;
		let default_y_minor_ticks = guides.y_minor_ticks;
		guides.x_tick_placement = x_tick_step
			.as_ref()
			.map(|(step, _)| TickPlacement::step(*step).expect("validated tick step"))
			.unwrap_or(TickPlacement::DEFAULT);
		guides.y_tick_placement = y_tick_step
			.as_ref()
			.map(|(step, _)| TickPlacement::step(*step).expect("validated tick step"))
			.unwrap_or(TickPlacement::DEFAULT);
		guides.x_minor_ticks = x_minor_ticks
			.as_ref()
			.map(|(count, _)| *count)
			.unwrap_or(default_x_minor_ticks);
		guides.y_minor_ticks = y_minor_ticks
			.as_ref()
			.map(|(count, _)| *count)
			.unwrap_or(default_y_minor_ticks);
		authored_settings.guides = Override::Set(guides);
	}
	let tick_step_spans = RunePlotTickStepSpans {
		x: x_tick_step.map(|(_, span)| span),
		y: y_tick_step.map(|(_, span)| span),
	};
	let minor_tick_spans = RunePlotMinorTickSpans {
		x: x_minor_ticks.map(|(_, span)| span),
		y: y_minor_ticks.map(|(_, span)| span),
	};
	Ok(RunePlotIntent {
		body,
		facet,
		authored_settings,
		plot_skin_span,
		facet_span,
		domain_spans,
		tick_step_spans,
		minor_tick_spans,
	})
}

fn parse_figure_legend_position(
	outer: &ast::Expr,
	args: &[&ast::Expr],
	source: &str,
	literal_bindings: &[LocalBinding],
) -> Result<LegendPosition, RuneEnvelopeError> {
	let [x, y] = args else {
		return invalid_view(
			outer.span(),
			source.len(),
			"legend_at(...) takes exactly two numeric literals",
		);
	};
	let mut values = [0.0; 2];
	for (index, argument) in [x, y].into_iter().enumerate() {
		let Some(resolved) = resolve_static_literal(
			argument,
			source,
			literal_bindings,
			outer.span().start.into_usize(),
		)?
		else {
			return invalid_view(
				argument.span(),
				source.len(),
				static_binding_name(
					argument,
					source,
					literal_bindings,
					outer.span().start.into_usize(),
				)
				.map(|name| {
					format!("legend_at(...) binding {name:?} does not resolve to a literal")
				})
				.unwrap_or_else(|| {
					"legend_at(...) requires unsuffixed numeric literals, optionally negated"
						.to_owned()
				}),
			);
		};
		let StaticLiteralValue::Number(value) = resolved.value else {
			return invalid_view(
				argument.span(),
				source.len(),
				format!(
					"legend_at(...) binding {} resolved to a string literal; expected a numeric literal",
					resolved.binding.as_deref().unwrap_or("argument")
				),
			);
		};
		values[index] = value;
	}
	let Some(position) = LegendPosition::new(values[0], values[1]) else {
		return invalid_view(
			outer.span(),
			source.len(),
			"legend_at(...) requires finite positions",
		);
	};
	Ok(position)
}

fn parse_figure_tick_step(
	outer: &ast::Expr,
	args: &[&ast::Expr],
	source: &str,
	constructor: &str,
	literal_bindings: &[LocalBinding],
) -> Result<f64, RuneEnvelopeError> {
	let [argument] = args else {
		return invalid_view(
			outer.span(),
			source.len(),
			format!("{constructor}(...) takes exactly one numeric literal"),
		);
	};
	let Some(resolved) = resolve_static_literal(
		argument,
		source,
		literal_bindings,
		outer.span().start.into_usize(),
	)?
	else {
		return invalid_view(
			outer.span(),
			source.len(),
			static_binding_name(
				argument,
				source,
				literal_bindings,
				outer.span().start.into_usize(),
			)
			.map(|name| {
				format!("{constructor}(...) binding {name:?} does not resolve to a literal")
			})
			.unwrap_or_else(|| {
				format!(
					"{constructor}(...) requires an unsuffixed numeric literal, optionally negated"
				)
			}),
		);
	};
	let StaticLiteralValue::Number(step) = resolved.value else {
		return invalid_view(
			argument.span(),
			source.len(),
			format!(
				"{constructor}(...) binding {} resolved to a string literal; expected a numeric literal",
				resolved.binding.as_deref().unwrap_or("argument")
			),
		);
	};
	if !step.is_finite() {
		return invalid_view(
			outer.span(),
			source.len(),
			format!("{constructor}(...) requires a finite step"),
		);
	}
	if step <= 0.0 {
		return invalid_view(
			outer.span(),
			source.len(),
			format!("{constructor}(...) step must be strictly positive"),
		);
	}
	Ok(step)
}

fn parse_figure_minor_ticks(
	outer: &ast::Expr,
	args: &[&ast::Expr],
	source: &str,
	constructor: &str,
	literal_bindings: &[LocalBinding],
) -> Result<u32, RuneEnvelopeError> {
	let [argument] = args else {
		return invalid_view(
			outer.span(),
			source.len(),
			format!("{constructor}(...) takes exactly one numeric literal"),
		);
	};
	let Some(resolved) = resolve_static_literal(
		argument,
		source,
		literal_bindings,
		outer.span().start.into_usize(),
	)?
	else {
		return invalid_view(
			outer.span(),
			source.len(),
			static_binding_name(
				argument,
				source,
				literal_bindings,
				outer.span().start.into_usize(),
			)
			.map(|name| {
				format!("{constructor}(...) binding {name:?} does not resolve to a literal")
			})
			.unwrap_or_else(|| {
				format!(
					"{constructor}(...) requires an unsuffixed numeric literal, optionally negated"
				)
			}),
		);
	};
	let StaticLiteralValue::Number(count) = resolved.value else {
		return invalid_view(
			argument.span(),
			source.len(),
			format!(
				"{constructor}(...) binding {} resolved to a string literal; expected a numeric literal",
				resolved.binding.as_deref().unwrap_or("argument")
			),
		);
	};
	if !count.is_finite() {
		return invalid_view(
			outer.span(),
			source.len(),
			format!("{constructor}(...) requires a finite count"),
		);
	}
	if count < 0.0 || count.fract() != 0.0 || count > u32::MAX as f64 {
		return invalid_view(
			outer.span(),
			source.len(),
			format!("{constructor}(...) count must be a non-negative integer"),
		);
	}
	Ok(count as u32)
}

fn parse_figure_bounds(
	outer: &ast::Expr,
	args: &[&ast::Expr],
	source: &str,
	constructor: &str,
	literal_bindings: &[LocalBinding],
) -> Result<(f64, f64), RuneEnvelopeError> {
	let [minimum, maximum] = args else {
		return invalid_view(
			outer.span(),
			source.len(),
			format!("{constructor}(...) takes exactly two numeric literals"),
		);
	};
	let Some(minimum) = resolve_static_literal(
		minimum,
		source,
		literal_bindings,
		outer.span().start.into_usize(),
	)?
	else {
		return invalid_view(
			outer.span(),
			source.len(),
			static_binding_name(
				args[0],
				source,
				literal_bindings,
				outer.span().start.into_usize(),
			)
			.map(|name| {
				format!("{constructor}(...) binding {name:?} does not resolve to a literal")
			})
			.unwrap_or_else(|| {
				format!(
					"{constructor}(...) requires unsuffixed numeric literals, optionally negated"
				)
			}),
		);
	};
	let Some(maximum) = resolve_static_literal(
		maximum,
		source,
		literal_bindings,
		outer.span().start.into_usize(),
	)?
	else {
		return invalid_view(
			outer.span(),
			source.len(),
			static_binding_name(
				args[1],
				source,
				literal_bindings,
				outer.span().start.into_usize(),
			)
			.map(|name| {
				format!("{constructor}(...) binding {name:?} does not resolve to a literal")
			})
			.unwrap_or_else(|| {
				format!(
					"{constructor}(...) requires unsuffixed numeric literals, optionally negated"
				)
			}),
		);
	};
	let StaticLiteralValue::Number(minimum) = minimum.value else {
		return invalid_view(
			args[0].span(),
			source.len(),
			format!(
				"{constructor}(...) binding {} resolved to a string literal; expected a numeric literal",
				minimum.binding.as_deref().unwrap_or("minimum")
			),
		);
	};
	let StaticLiteralValue::Number(maximum) = maximum.value else {
		return invalid_view(
			args[1].span(),
			source.len(),
			format!(
				"{constructor}(...) binding {} resolved to a string literal; expected a numeric literal",
				maximum.binding.as_deref().unwrap_or("maximum")
			),
		);
	};
	if !minimum.is_finite() || !maximum.is_finite() {
		return invalid_view(
			outer.span(),
			source.len(),
			format!("{constructor}(...) requires finite bounds"),
		);
	}
	if minimum == maximum {
		return invalid_view(
			outer.span(),
			source.len(),
			format!("{constructor}(...) cannot form a collapsed axis; min must be less than max"),
		);
	}
	if minimum > maximum {
		return invalid_view(
			outer.span(),
			source.len(),
			format!("{constructor}(...) cannot reverse an axis; min must be less than max"),
		);
	}
	Ok((minimum, maximum))
}

fn parse_signed_numeric_literal(expression: &ast::Expr, source: &str) -> Option<f64> {
	let is_number = match expression {
		ast::Expr::Lit(literal) => matches!(literal.lit, ast::Lit::Number(_)),
		ast::Expr::Unary(unary) => {
			matches!(unary.op, ast::UnOp::Neg(_))
				&& matches!(unary.expr.as_ref(), ast::Expr::Lit(literal) if matches!(literal.lit, ast::Lit::Number(_)))
		}
		_ => false,
	};
	if !is_number {
		return None;
	}
	source_slice(source, expression.span())
		.ok()?
		.replace('_', "")
		.parse::<f64>()
		.ok()
}

fn resolve_static_literal(
	expression: &ast::Expr,
	source: &str,
	bindings: &[LocalBinding],
	use_start: usize,
) -> Result<Option<ResolvedStaticLiteral>, RuneEnvelopeError> {
	let mut visited = Vec::new();
	resolve_static_literal_inner(expression, source, bindings, use_start, &mut visited)
}

fn static_binding_name(
	expression: &ast::Expr,
	source: &str,
	bindings: &[LocalBinding],
	use_start: usize,
) -> Option<String> {
	let raw = source_slice(source, expression.span()).ok()?.trim();
	if !matches!(expression, ast::Expr::Path(_)) || !is_simple_name(raw) {
		return None;
	}
	bindings
		.iter()
		.rev()
		.find(|binding| binding.name == raw && binding.statement_start < use_start)
		.map(|_| raw.to_owned())
}

fn resolve_static_literal_inner(
	expression: &ast::Expr,
	source: &str,
	bindings: &[LocalBinding],
	use_start: usize,
	visited: &mut Vec<String>,
) -> Result<Option<ResolvedStaticLiteral>, RuneEnvelopeError> {
	let raw = source_slice(source, expression.span())?.trim();
	if let Some(value) = decode_string_literal(raw) {
		return Ok(Some(ResolvedStaticLiteral {
			value: StaticLiteralValue::String(value),
			binding: None,
		}));
	}
	if let Some(value) = parse_signed_numeric_literal(expression, source) {
		return Ok(Some(ResolvedStaticLiteral {
			value: StaticLiteralValue::Number(value),
			binding: None,
		}));
	}
	if !matches!(expression, ast::Expr::Path(_)) || !is_simple_name(raw) {
		return Ok(None);
	}
	let Some(binding) = bindings
		.iter()
		.rev()
		.find(|binding| binding.name == raw && binding.statement_start < use_start)
	else {
		return Ok(None);
	};
	if visited.iter().any(|name| name == raw) {
		return Ok(None);
	}
	visited.push(raw.to_owned());
	let initializer = source
		.get(binding.initializer.clone())
		.ok_or_else(|| RuneEnvelopeError::Invalid("binding initializer is not UTF-8".into()))?;
	let resolved = resolve_static_literal_text(
		initializer.trim(),
		source,
		bindings,
		binding.statement_start,
		visited,
	)?;
	visited.pop();
	Ok(resolved.map(|mut resolved| {
		resolved.binding = Some(raw.to_owned());
		resolved
	}))
}

fn resolve_static_literal_text(
	raw: &str,
	source: &str,
	bindings: &[LocalBinding],
	use_start: usize,
	visited: &mut Vec<String>,
) -> Result<Option<ResolvedStaticLiteral>, RuneEnvelopeError> {
	if let Some(value) = decode_string_literal(raw) {
		return Ok(Some(ResolvedStaticLiteral {
			value: StaticLiteralValue::String(value),
			binding: None,
		}));
	}
	if let Ok(value) = raw.replace('_', "").parse::<f64>() {
		return Ok(Some(ResolvedStaticLiteral {
			value: StaticLiteralValue::Number(value),
			binding: None,
		}));
	}
	if !is_simple_name(raw) {
		return Ok(None);
	}
	let Some(binding) = bindings
		.iter()
		.rev()
		.find(|binding| binding.name == raw && binding.statement_start < use_start)
	else {
		return Ok(None);
	};
	if visited.iter().any(|name| name == raw) {
		return Ok(None);
	}
	visited.push(raw.to_owned());
	let initializer = source
		.get(binding.initializer.clone())
		.ok_or_else(|| RuneEnvelopeError::Invalid("binding initializer is not UTF-8".into()))?;
	let resolved = resolve_static_literal_text(
		initializer.trim(),
		source,
		bindings,
		binding.statement_start,
		visited,
	)?;
	visited.pop();
	Ok(resolved.map(|mut resolved| {
		resolved.binding = Some(raw.to_owned());
		resolved
	}))
}

fn parse_figure_skin(
	expression: &ast::Expr,
	source: &str,
	literal_bindings: &[LocalBinding],
) -> Result<Option<PlotSkinSelection>, RuneEnvelopeError> {
	let ast::Expr::Call(_) = expression else {
		return Ok(None);
	};
	let mut current = expression;
	let mut modifiers = Vec::new();
	while let ast::Expr::Call(call) = current
		&& let ast::Expr::FieldAccess(field) = call.expr.as_ref()
	{
		modifiers.push((source_slice(source, field.expr_field.span())?.trim(), call));
		current = &field.expr;
	}
	let ast::Expr::Call(root) = current else {
		return Ok(None);
	};
	if source_slice(source, root.expr.span())?.trim() != "skin" {
		return Ok(None);
	}

	let root_args = root.args.iter().map(|(arg, _)| arg).collect::<Vec<_>>();
	let name = parse_figure_heading(current, &root_args, source, "skin", literal_bindings)?;
	let mut projection = PlotChromeProjection::Ordered;
	for (modifier, call) in modifiers {
		match modifier {
			"duotone" => {
				if projection == PlotChromeProjection::Duotone {
					return invalid_view(
						expression.span(),
						source.len(),
						"duotone() may appear at most once on a plot skin",
					);
				}
				if !call.args.is_empty() {
					return invalid_view(
						expression.span(),
						source.len(),
						"duotone() takes no arguments",
					);
				}
				projection = PlotChromeProjection::Duotone;
			}
			_ => {
				return invalid_view(
					expression.span(),
					source.len(),
					format!("unknown plot-skin modifier {modifier:?}; expected duotone()"),
				);
			}
		}
	}
	Ok(Some(PlotSkinSelection::new(name, projection)))
}

fn parse_figure_heading(
	outer: &ast::Expr,
	args: &[&ast::Expr],
	source: &str,
	kind: &str,
	literal_bindings: &[LocalBinding],
) -> Result<String, RuneEnvelopeError> {
	let [argument] = args else {
		return invalid_view(
			outer.span(),
			source.len(),
			format!("{kind}(...) takes exactly one string literal"),
		);
	};
	let Some(resolved) = resolve_static_literal(
		argument,
		source,
		literal_bindings,
		outer.span().start.into_usize(),
	)?
	else {
		return invalid_view(
			argument.span(),
			source.len(),
			static_binding_name(
				argument,
				source,
				literal_bindings,
				outer.span().start.into_usize(),
			)
			.map(|name| format!("{kind}(...) binding {name:?} does not resolve to a literal"))
			.unwrap_or_else(|| format!("{kind}(...) requires a string literal")),
		);
	};
	let StaticLiteralValue::String(value) = resolved.value else {
		return invalid_view(
			argument.span(),
			source.len(),
			format!(
				"{kind}(...) binding {} resolved to a numeric literal; expected a string literal",
				resolved.binding.as_deref().unwrap_or("argument")
			),
		);
	};
	Ok(value)
}

fn parse_figure_body(
	expression: &ast::Expr,
	callee: &str,
	args: &[&ast::Expr],
	source: &str,
) -> Result<RunePlotBody, RuneEnvelopeError> {
	if let Some(layer) = parse_modified_mark(expression, source)? {
		return Ok(RunePlotBody::StyledSingle(layer));
	}
	let single = |plot_type, message: &str| -> Result<RunePlotBody, RuneEnvelopeError> {
		let Some(sink) = plot_sink_fields(plot_type, source, args, 0)? else {
			return invalid_view(expression.span(), source.len(), message);
		};
		Ok(RunePlotBody::Single(sink))
	};
	match callee {
		"bar" if matches!(args.len(), 2 | 3) => single(PlotType::Bar, "bar fields must be strings"),
		"line" if args.len() == 2 => single(PlotType::Line, "line fields must be strings"),
		"scatter" if matches!(args.len(), 2 | 3) => {
			single(PlotType::Scatter, "scatter fields must be strings")
		}
		"layers" => Ok(RunePlotBody::Layered(parse_layer_list(
			expression, args, source,
		)?)),
		"table" => invalid_view(
			expression.span(),
			source.len(),
			"table() is a view sink, not a figure plot body",
		),
		"figure" => invalid_view(
			expression.span(),
			source.len(),
			"figure([...]) cannot be nested inside a figure",
		),
		"view" => invalid_view(
			expression.span(),
			source.len(),
			"view(...) wraps a figure; it cannot appear inside one",
		),
		"bar" => invalid_view(
			expression.span(),
			source.len(),
			"bar(...) takes x and y strings with an optional series",
		),
		"line" => invalid_view(
			expression.span(),
			source.len(),
			"line(...) takes x and y string fields",
		),
		"scatter" => invalid_view(
			expression.span(),
			source.len(),
			"scatter(...) takes x and y strings with an optional category",
		),
		_ => invalid_view(
			expression.span(),
			source.len(),
			"unknown figure child; expected one bar, line, scatter, or layers plot body",
		),
	}
}

fn parse_layer_list(
	outer: &ast::Expr,
	args: &[&ast::Expr],
	source: &str,
) -> Result<LayeredPlotSpec, RuneEnvelopeError> {
	let [argument] = args else {
		return invalid_view(
			outer.span(),
			source.len(),
			"layers(...) takes exactly one literal layer list",
		);
	};
	let ast::Expr::Vec(list) = argument else {
		return invalid_view(
			outer.span(),
			source.len(),
			"layers(...) requires a literal layer list",
		);
	};
	let mut items = list.items.iter();
	let Some((first, _)) = items.next() else {
		return invalid_view(
			list.span(),
			source.len(),
			"layers([]) cannot form a non-empty plot",
		);
	};
	let mut specification = LayeredPlotSpec::new(parse_layer(first, source)?);
	for (item, _) in items {
		specification.push(parse_layer(item, source)?);
	}
	Ok(specification)
}

fn parse_layer(expression: &ast::Expr, source: &str) -> Result<LayerSpec, RuneEnvelopeError> {
	if let Some(styled) = parse_modified_mark(expression, source)? {
		return match styled.into_layer() {
			Ok(layer) => Ok(layer),
			Err(_) => invalid_view(
				expression.span(),
				source.len(),
				"bar(...) is a view sink, not a shared-Cartesian layer",
			),
		};
	}
	let ast::Expr::Call(call) = expression else {
		return invalid_view(
			expression.span(),
			source.len(),
			"a layer must be line(x, y) or scatter(x, y)",
		);
	};
	let callee = source_slice(source, call.expr.span())?.trim();
	let args = call.args.iter().map(|(arg, _)| arg).collect::<Vec<_>>();
	let mark = match callee {
		"line" if args.len() == 2 => LayerMark::Line,
		"scatter" if args.len() == 2 => LayerMark::Scatter,
		"layers" => {
			return invalid_view(
				expression.span(),
				source.len(),
				"layers([...]) cannot be nested inside a layer list",
			);
		}
		"table" | "bar" => {
			return invalid_view(
				expression.span(),
				source.len(),
				format!("{callee}(...) is a view sink, not a shared-Cartesian layer"),
			);
		}
		"scatter" if args.len() == 3 => {
			return invalid_view(
				expression.span(),
				source.len(),
				"scatter layers take x and y only; category color is not part of the layers surface",
			);
		}
		"line" | "scatter" => {
			return invalid_view(
				expression.span(),
				source.len(),
				format!("{callee}(...) layers take exactly x and y string fields"),
			);
		}
		_ => {
			return invalid_view(
				expression.span(),
				source.len(),
				"unknown layer; expected line(x, y) or scatter(x, y)",
			);
		}
	};
	let mut fields = Vec::with_capacity(2);
	for field in args {
		let raw = source_slice(source, field.span())?;
		let Some(value) = decode_string_literal(raw) else {
			return invalid_view(
				expression.span(),
				source.len(),
				format!("{callee}(...) layer fields must be string literals"),
			);
		};
		fields.push(value);
	}
	Ok(LayerSpec::xy(mark, fields[0].clone(), fields[1].clone()))
}

/// PRODUCT-0027's closed postfix table-sink modifier register. Recognition is
/// contextual: the complete chain is flattened to one palette identity plus
/// projection intent, and no palette or resolved artifact color enters the
/// envelope.
fn parse_modified_table(
	expression: &ast::Expr,
	source: &str,
) -> Result<Option<RuneTableSink>, RuneEnvelopeError> {
	let ast::Expr::Call(outer_call) = expression else {
		return Ok(None);
	};
	if !matches!(outer_call.expr.as_ref(), ast::Expr::FieldAccess(_)) {
		return Ok(None);
	}

	let mut current = expression;
	let mut modifiers = Vec::new();
	loop {
		let ast::Expr::Call(call) = current else {
			return invalid_view(
				expression.span(),
				source.len(),
				"a table modifier must follow table()",
			);
		};
		let ast::Expr::FieldAccess(field) = call.expr.as_ref() else {
			break;
		};
		modifiers.push((source_slice(source, field.expr_field.span())?.trim(), call));
		current = &field.expr;
	}

	let ast::Expr::Call(root) = current else {
		return Ok(None);
	};
	let callee = source_slice(source, root.expr.span())?.trim();
	if callee != "table" {
		return Ok(None);
	}
	if !root.args.is_empty() {
		return invalid_view(
			expression.span(),
			source.len(),
			"a modified table sink starts with table()",
		);
	}

	let mut skin_name = None;
	let mut duotone = false;
	let mut show_column_types = Override::Inherit;
	for (modifier, call) in modifiers {
		match modifier {
			"skin" => {
				if skin_name.is_some() {
					return invalid_view(
						expression.span(),
						source.len(),
						"skin(...) may appear at most once on a table sink",
					);
				}
				let args = call.args.iter().map(|(arg, _)| arg).collect::<Vec<_>>();
				let [argument] = args.as_slice() else {
					return invalid_view(
						expression.span(),
						source.len(),
						"skin(...) takes exactly one theme string literal",
					);
				};
				let raw = source_slice(source, argument.span())?;
				let Some(name) = decode_string_literal(raw) else {
					return invalid_view(
						expression.span(),
						source.len(),
						"skin(...) requires a theme string literal",
					);
				};
				skin_name = Some(name);
			}
			"duotone" => {
				if duotone {
					return invalid_view(
						expression.span(),
						source.len(),
						"duotone() may appear at most once on a table sink",
					);
				}
				if !call.args.is_empty() {
					return invalid_view(
						expression.span(),
						source.len(),
						"duotone() takes no arguments",
					);
				}
				duotone = true;
			}
			"show_types" => {
				if matches!(show_column_types, Override::Set(_)) {
					return invalid_view(
						expression.span(),
						source.len(),
						"show_types() may appear at most once on a table sink",
					);
				}
				if !call.args.is_empty() {
					return invalid_view(
						expression.span(),
						source.len(),
						"show_types() takes no arguments",
					);
				}
				show_column_types = Override::Set(true);
			}
			_ => {
				return invalid_view(
					expression.span(),
					source.len(),
					format!(
						"unknown table modifier {modifier:?}; expected skin(string_literal), duotone(), or show_types()"
					),
				);
			}
		}
	}

	if duotone && skin_name.is_none() {
		return invalid_view(
			expression.span(),
			source.len(),
			"duotone() requires skin(string_literal)",
		);
	}
	let skin = if let Some(name) = skin_name {
		let projection = if duotone {
			RuneTableSkinProjection::Duotone
		} else {
			RuneTableSkinProjection::PaletteForward
		};
		RuneTableSkinIntent::NamedTheme { name, projection }
	} else {
		RuneTableSkinIntent::Default
	};
	Ok(Some(RuneTableSink {
		skin,
		show_column_types,
	}))
}

/// PRODUCT-0018's closed postfix LayerModifier register. A successful parse
/// flattens the complete chain directly into one styled standalone sink; no
/// method node survives into Rune execution or the plot normal form.
fn parse_modified_mark(
	expression: &ast::Expr,
	source: &str,
) -> Result<Option<StyledRunePlotSink>, RuneEnvelopeError> {
	let ast::Expr::Call(outer_call) = expression else {
		return Ok(None);
	};
	if !matches!(outer_call.expr.as_ref(), ast::Expr::FieldAccess(_)) {
		return Ok(None);
	}

	let mut current = expression;
	let mut point_size = None;
	let mut stroke_width = None;
	let mut bar_width = None;
	let mut category_gap = None;
	loop {
		let ast::Expr::Call(call) = current else {
			return invalid_view(
				expression.span(),
				source.len(),
				"a layer modifier must follow a bar(...), line(...), or scatter(...) mark",
			);
		};
		let ast::Expr::FieldAccess(field) = call.expr.as_ref() else {
			break;
		};
		let modifier = source_slice(source, field.expr_field.span())?.trim();
		match modifier {
			"point_size" => {
				if point_size.is_some() {
					return invalid_view(
						expression.span(),
						source.len(),
						"point_size(...) may appear at most once on a mark",
					);
				}
				let factor = parse_layer_modifier_factor(call, expression, source, "point_size")?;
				let modifier_span = u32_range(
					&byte_range(expression.span(), source.len())?,
					"point-size modifier",
				)?;
				point_size = Some(PointSize::new(factor).map_err(|error| {
					RuneEnvelopeError::InvalidView {
						span: modifier_span,
						message: error.to_string(),
					}
				})?);
			}
			"stroke_width" => {
				if stroke_width.is_some() {
					return invalid_view(
						expression.span(),
						source.len(),
						"stroke_width(...) may appear at most once on a mark",
					);
				}
				let factor = parse_layer_modifier_factor(call, expression, source, "stroke_width")?;
				let modifier_span = u32_range(
					&byte_range(expression.span(), source.len())?,
					"stroke-width modifier",
				)?;
				stroke_width = Some(StrokeWidth::new(factor).map_err(|error| {
					RuneEnvelopeError::InvalidView {
						span: modifier_span,
						message: error.to_string(),
					}
				})?);
			}
			"bar_width" => {
				if bar_width.is_some() {
					return invalid_view(
						expression.span(),
						source.len(),
						"bar_width(...) may appear at most once on a mark",
					);
				}
				let fraction = parse_layer_modifier_factor(call, expression, source, "bar_width")?;
				let modifier_span = u32_range(
					&byte_range(expression.span(), source.len())?,
					"bar-width modifier",
				)?;
				bar_width = Some(BarWidth::new(fraction).map_err(|error| {
					RuneEnvelopeError::InvalidView {
						span: modifier_span,
						message: error.to_string(),
					}
				})?);
			}
			"category_gap" => {
				if category_gap.is_some() {
					return invalid_view(
						expression.span(),
						source.len(),
						"category_gap(...) may appear at most once on a mark",
					);
				}
				let fraction =
					parse_layer_modifier_factor(call, expression, source, "category_gap")?;
				let modifier_span = u32_range(
					&byte_range(expression.span(), source.len())?,
					"category-gap modifier",
				)?;
				category_gap = Some(CategoryGap::new(fraction).map_err(|error| {
					RuneEnvelopeError::InvalidView {
						span: modifier_span,
						message: error.to_string(),
					}
				})?);
			}
			_ => {
				return invalid_view(
					expression.span(),
					source.len(),
					format!(
						"unknown layer modifier {modifier:?}; expected point_size(number), stroke_width(number), bar_width(number), or category_gap(number)"
					),
				);
			}
		}
		current = &field.expr;
	}

	let ast::Expr::Call(root) = current else {
		return invalid_view(
			expression.span(),
			source.len(),
			"a layer modifier must follow a bar(...), line(...), or scatter(...) mark",
		);
	};
	let callee = source_slice(source, root.expr.span())?.trim();
	let args = root.args.iter().map(|(arg, _)| arg).collect::<Vec<_>>();
	let plot_type = match callee {
		"bar" => PlotType::Bar,
		"line" => PlotType::Line,
		"scatter" => PlotType::Scatter,
		_ => {
			return invalid_view(
				expression.span(),
				source.len(),
				"a layer modifier must follow a bar(...), line(...), or scatter(...) mark",
			);
		}
	};
	// PRODUCT-0046 §1: a register belongs to the mark, so every encoding form of
	// that mark admits it. Line has one form; bar and scatter each take an
	// optional third color field.
	let arity_ok = match plot_type {
		PlotType::Line => args.len() == 2,
		_ => matches!(args.len(), 2 | 3),
	};
	if !arity_ok {
		return invalid_view(
			expression.span(),
			source.len(),
			match plot_type {
				PlotType::Line => "a modified line takes x and y string fields",
				PlotType::Bar => "a modified bar takes x and y strings with an optional series",
				_ => "a modified scatter takes x and y strings with an optional category",
			},
		);
	}
	match plot_type {
		PlotType::Line if point_size.is_some() => {
			return invalid_view(
				expression.span(),
				source.len(),
				"point_size(...) is available on scatter marks, not line marks",
			);
		}
		PlotType::Scatter if stroke_width.is_some() => {
			return invalid_view(
				expression.span(),
				source.len(),
				"stroke_width(...) is available on line marks, not scatter marks",
			);
		}
		PlotType::Bar if point_size.is_some() || stroke_width.is_some() => {
			return invalid_view(
				expression.span(),
				source.len(),
				"point_size(...) and stroke_width(...) are not available on bar marks",
			);
		}
		PlotType::Line | PlotType::Scatter if bar_width.is_some() => {
			return invalid_view(
				expression.span(),
				source.len(),
				"bar_width(...) is available on bar marks, not line or scatter marks",
			);
		}
		PlotType::Line | PlotType::Scatter if category_gap.is_some() => {
			return invalid_view(
				expression.span(),
				source.len(),
				"category_gap(...) is available on bar marks, not line or scatter marks",
			);
		}
		_ => {}
	}
	let mut fields = Vec::with_capacity(args.len());
	for field in args {
		let raw = source_slice(source, field.span())?;
		let Some(value) = decode_string_literal(raw) else {
			return invalid_view(
				expression.span(),
				source.len(),
				"a modified mark requires string literal fields",
			);
		};
		fields.push(value);
	}
	Ok(Some(StyledRunePlotSink {
		sink: DisposableRunePlotSink {
			composition: PlotComposition::single(plot_type),
			x: fields[0].clone(),
			y: fields[1].clone(),
			category: fields.get(2).cloned(),
		},
		settings: LayerSettings {
			point_size: point_size.map_or(Override::Inherit, Override::Set),
			stroke_width: stroke_width.map_or(Override::Inherit, Override::Set),
			bar_width: bar_width.map_or(Override::Inherit, Override::Set),
			category_gap: category_gap.map_or(Override::Inherit, Override::Set),
		},
	}))
}

fn parse_layer_modifier_factor(
	call: &ast::ExprCall,
	outer: &ast::Expr,
	source: &str,
	modifier: &str,
) -> Result<f32, RuneEnvelopeError> {
	let args = call.args.iter().map(|(arg, _)| arg).collect::<Vec<_>>();
	let [argument] = args.as_slice() else {
		return invalid_view(
			outer.span(),
			source.len(),
			format!("{modifier}(...) takes exactly one numeric literal"),
		);
	};
	if !matches!(argument, ast::Expr::Lit(lit) if matches!(lit.lit, ast::Lit::Number(_))) {
		return invalid_view(
			outer.span(),
			source.len(),
			format!("{modifier}(...) requires a numeric literal"),
		);
	}
	let raw = source_slice(source, argument.span())?.replace('_', "");
	let span = u32_range(&byte_range(outer.span(), source.len())?, "layer modifier")?;
	raw.parse::<f32>()
		.map_err(|_| RuneEnvelopeError::InvalidView {
			span,
			message: format!("{modifier}(...) requires an unsuffixed numeric literal"),
		})
}

fn parse_sugar_sink(
	plot_type: PlotType,
	call: &ast::ExprCall,
	args: &[&ast::Expr],
	source: &str,
) -> Result<ParsedSink, RuneEnvelopeError> {
	let valid_arity = match plot_type {
		PlotType::Line => args.len() == 2,
		PlotType::Bar | PlotType::Scatter => matches!(args.len(), 2 | 3),
		_ => false,
	};
	if !valid_arity {
		let message = match plot_type {
			PlotType::Scatter => {
				"scatter view sugar requires x and y strings with an optional category"
			}
			PlotType::Bar => "bar view sugar requires x and y strings with an optional series",
			PlotType::Line => "line view sugar requires x and y string fields",
			_ => unreachable!("only Rune plot view forms reach sugar parsing"),
		};
		return invalid_view(call.span(), source.len(), message);
	}
	let Some(sink) = plot_sink_fields(plot_type, source, args, 0)? else {
		return invalid_view(
			call.span(),
			source.len(),
			"plot view fields must be string literals",
		);
	};
	Ok(ParsedSink {
		intent: RuneViewIntent::Plot(RunePlotIntent::bare(RunePlotBody::Single(sink))),
		span: byte_range(call.span(), source.len())?,
		name: None,
		output_order: 0,
	})
}

fn invalid_view<T>(
	span: ast::Span,
	source_len: usize,
	message: impl Into<String>,
) -> Result<T, RuneEnvelopeError> {
	let span = byte_range(span, source_len)?;
	Err(RuneEnvelopeError::InvalidView {
		span: u32_range(&span, "view")?,
		message: message.into(),
	})
}

fn render(
	source: &str,
	envelope: &ExecutionEnvelope,
) -> Result<TransformedRuneRelation, RuneEnvelopeError> {
	let entrypoint_prefix = match envelope.relation_root {
		RelationRoot::Workspace => WORKSPACE_ENTRYPOINT_PREFIX,
		RelationRoot::Ket => KET_ENTRYPOINT_PREFIX,
	};
	let retained_len = envelope.retained.iter().map(Range::len).sum::<usize>();
	let mut transformed = String::with_capacity(
		entrypoint_prefix.len()
			+ retained_len
			+ envelope.relation.len()
			+ ENTRYPOINT_SUFFIX.len()
			+ 3,
	);
	transformed.push_str(entrypoint_prefix);
	let mut segments = Vec::new();
	for retained in &envelope.retained {
		let transformed_start = transformed.len();
		transformed.push_str(source.get(retained.clone()).ok_or_else(|| {
			RuneEnvelopeError::Invalid("retained source span is not UTF-8".into())
		})?);
		let transformed_end = transformed.len();
		push_segment(
			&mut segments,
			transformed_start..transformed_end,
			u32::try_from(retained.start)
				.map_err(|_| RuneEnvelopeError::Invalid("source exceeds u32".into()))?,
		)?;
	}
	if envelope.terminate_retained_tail {
		transformed.push_str("\n;");
	}
	transformed.push('\n');
	let relation_start = transformed.len();
	transformed.push_str(
		source
			.get(envelope.relation.clone())
			.ok_or_else(|| RuneEnvelopeError::Invalid("relation span is not UTF-8".into()))?,
	);
	let relation_end = transformed.len();
	transformed.push_str(ENTRYPOINT_SUFFIX);

	push_segment(
		&mut segments,
		relation_start..relation_end,
		u32::try_from(envelope.relation.start)
			.map_err(|_| RuneEnvelopeError::Invalid("source exceeds u32".into()))?,
	)?;

	Ok(TransformedRuneRelation {
		origin: envelope.relation_origin.clone(),
		source: transformed,
		views: envelope
			.sinks
			.iter()
			.map(|sink| {
				Ok(SpannedRuneViewIntent {
					intent: sink.intent.clone(),
					span: u32_range(&sink.span, "view")?,
					identity: match &sink.name {
						Some(name) => LocalViewIdentity::Named(name.value.clone()),
						None => LocalViewIdentity::Anonymous(sink.output_order),
					},
					output_order: sink.output_order,
				})
			})
			.collect::<Result<_, RuneEnvelopeError>>()?,
		source_map: RuneEnvelopeSourceMap {
			segments,
			diagnostic_fallback: u32_range(&envelope.relation, "relation")?,
		},
	})
}

#[derive(Clone, Debug, PartialEq)]
struct ParsedView {
	intent: RuneViewIntent,
	statement: Range<usize>,
	relation: Range<usize>,
}

fn recognize_view_root(
	statement: &ast::Stmt,
	source: &str,
	shadows: ViewShadows,
) -> Result<Option<ParsedView>, RuneEnvelopeError> {
	let (expression, statement_span) = match statement {
		ast::Stmt::Expr(expression) => (expression, expression.span()),
		ast::Stmt::Semi(statement) => (&statement.expr, statement.span()),
		_ => return Ok(None),
	};
	let ast::Expr::Call(call) = expression else {
		return Ok(None);
	};
	let callee = source_slice(source, call.expr.span())?;
	let args = call.args.iter().map(|(arg, _)| arg).collect::<Vec<_>>();
	let (intent, relation) = match callee.trim() {
		"table" if !shadows.table && args.len() == 1 => {
			(RuneViewIntent::Table(RuneTableSink::default()), args[0])
		}
		"plot.bar" if !shadows.plot && matches!(args.len(), 3 | 4) => (
			RuneViewIntent::Plot(RunePlotIntent::bare(RunePlotBody::Single(match plot_sink(
				PlotType::Bar,
				source,
				&args,
			)? {
				Some(sink) => sink,
				None => return Ok(None),
			}))),
			args[0],
		),
		"plot.line" if !shadows.plot && args.len() == 3 => {
			let Some(sink) = plot_sink(PlotType::Line, source, &args)? else {
				return invalid_view(
					call.span(),
					source.len(),
					"plot.line fields must be string literals",
				);
			};
			(
				RuneViewIntent::Plot(RunePlotIntent::bare(RunePlotBody::Single(sink))),
				args[0],
			)
		}
		"plot.line" if !shadows.plot => {
			return invalid_view(
				call.span(),
				source.len(),
				"plot.line(...) takes a relation plus x and y string fields",
			);
		}
		"plot.scatter" if !shadows.plot && matches!(args.len(), 3 | 4) => (
			RuneViewIntent::Plot(RunePlotIntent::bare(RunePlotBody::Single(match plot_sink(
				PlotType::Scatter,
				source,
				&args,
			)? {
				Some(sink) => sink,
				None => return Ok(None),
			}))),
			args[0],
		),
		_ => return Ok(None),
	};
	Ok(Some(ParsedView {
		intent,
		statement: byte_range(statement_span, source.len())?,
		relation: byte_range(relation.span(), source.len())?,
	}))
}

fn retained_regions(
	source_len: usize,
	peeled: &[Range<usize>],
) -> Result<Vec<Range<usize>>, RuneEnvelopeError> {
	let mut retained = Vec::with_capacity(peeled.len() + 1);
	let mut cursor = 0;
	for region in peeled {
		if region.start < cursor || region.end > source_len {
			return Err(RuneEnvelopeError::Invalid(
				"presentation spans overlap or exceed the source".into(),
			));
		}
		if cursor < region.start {
			retained.push(cursor..region.start);
		}
		cursor = region.end;
	}
	if cursor < source_len {
		retained.push(cursor..source_len);
	}
	Ok(retained)
}

fn collect_top_level_bindings(
	statement: &ast::Stmt,
	source: &str,
	shadows: &mut ViewShadows,
	bindings: &mut Vec<LocalBinding>,
) -> Result<(), RuneEnvelopeError> {
	match statement {
		ast::Stmt::Local(local) => {
			let statement = byte_range(statement.span(), source.len())?;
			let mut names = Vec::new();
			collect_pattern_names(&local.pat, source, &mut names)?;
			for name in &names {
				shadows.insert(name);
			}
			if names.len() == 1 {
				bindings.push(LocalBinding {
					name: names.pop().expect("one local binding"),
					initializer: byte_range(local.expr.span(), source.len())?,
					statement_start: statement.start,
					statement,
				});
			}
		}
		ast::Stmt::Item(item, _) => collect_item_shadows(item, source, shadows)?,
		_ => {}
	}
	Ok(())
}

fn collect_pattern_names(
	pattern: &ast::Pat,
	source: &str,
	names: &mut Vec<String>,
) -> Result<(), RuneEnvelopeError> {
	match pattern {
		ast::Pat::Path(path) => {
			let raw = source_slice(source, path.path.span())?.trim();
			if is_simple_name(raw) {
				names.push(raw.to_owned());
			}
		}
		ast::Pat::Vec(vector) => {
			for (pattern, _) in vector.items.iter() {
				collect_pattern_names(pattern, source, names)?;
			}
		}
		ast::Pat::Tuple(tuple) => {
			for (pattern, _) in tuple.items.iter() {
				collect_pattern_names(pattern, source, names)?;
			}
		}
		ast::Pat::Object(object) => {
			for (pattern, _) in object.items.iter() {
				collect_pattern_names(pattern, source, names)?;
			}
		}
		ast::Pat::Binding(binding) => {
			collect_pattern_names(&binding.pat, source, names)?;
		}
		_ => {}
	}
	Ok(())
}

fn collect_item_shadows(
	item: &ast::Item,
	source: &str,
	shadows: &mut ViewShadows,
) -> Result<(), RuneEnvelopeError> {
	let name = match item {
		ast::Item::Fn(item) => Some(item.name.span()),
		ast::Item::Enum(item) => Some(item.name.span()),
		ast::Item::Struct(item) => Some(item.ident.span()),
		ast::Item::Mod(item) => Some(item.name.span()),
		ast::Item::Const(item) => Some(item.name.span()),
		ast::Item::Use(item) => {
			collect_use_shadows(&item.path, source, shadows)?;
			None
		}
		_ => None,
	};
	if let Some(span) = name {
		shadows.insert(source_slice(source, span)?.trim());
	}
	Ok(())
}

fn collect_use_shadows(
	path: &ast::ItemUsePath,
	source: &str,
	shadows: &mut ViewShadows,
) -> Result<(), RuneEnvelopeError> {
	if let Some((_, alias)) = &path.alias {
		shadows.insert(source_slice(source, alias.span())?.trim());
		return Ok(());
	}
	let segment = path
		.segments
		.last()
		.map(|(_, segment)| segment)
		.unwrap_or(&path.first);
	match segment {
		ast::ItemUseSegment::PathSegment(segment) => {
			shadows.insert(source_slice(source, segment.span())?.trim());
		}
		ast::ItemUseSegment::Wildcard(_) => {
			// The imported names are unknowable without semantic resolution. Retain
			// both forms, which is the conservative direction.
			shadows.table = true;
			shadows.plot = true;
		}
		ast::ItemUseSegment::Group(group) => {
			for (path, _) in group.iter() {
				collect_use_shadows(path, source, shadows)?;
			}
		}
		_ => {}
	}
	Ok(())
}

fn resolve_relation_origin(
	source: &str,
	root: &ParsedView,
	bindings: &[LocalBinding],
	shadows: ViewShadows,
) -> Result<(String, RelationRoot), RuneEnvelopeError> {
	let relation = source
		.get(root.relation.clone())
		.ok_or_else(|| RuneEnvelopeError::Invalid("relation span is not UTF-8".into()))?
		.trim();
	if is_simple_name(relation)
		&& let Some(binding) = bindings.iter().rev().find(|binding| {
			binding.name == relation && binding.statement_start < root.statement.start
		}) {
		let origin = source
			.get(binding.initializer.clone())
			.ok_or_else(|| RuneEnvelopeError::Invalid("binding initializer is not UTF-8".into()))?
			.to_owned();
		return Ok(canonical_relation_origin(origin, shadows));
	}
	Ok(canonical_relation_origin(relation.to_owned(), shadows))
}

fn canonical_relation_origin(origin: String, shadows: ViewShadows) -> (String, RelationRoot) {
	if !shadows.ket
		&& let Some(rest) = origin.strip_prefix("ket.")
	{
		return (format!("workspace.{rest}"), RelationRoot::Ket);
	}
	(origin, RelationRoot::Workspace)
}

fn is_simple_name(raw: &str) -> bool {
	let mut chars = raw.chars();
	let Some(first) = chars.next() else {
		return false;
	};
	(first == '_' || first.is_alphabetic())
		&& chars.all(|character| character == '_' || character.is_alphanumeric())
}

fn u32_range(range: &Range<usize>, label: &str) -> Result<Range<u32>, RuneEnvelopeError> {
	Ok(u32::try_from(range.start)
		.map_err(|_| RuneEnvelopeError::Invalid(format!("{label} exceeds u32")))?
		..u32::try_from(range.end)
			.map_err(|_| RuneEnvelopeError::Invalid(format!("{label} exceeds u32")))?)
}

fn plot_sink(
	plot_type: PlotType,
	source: &str,
	args: &[&ast::Expr],
) -> Result<Option<DisposableRunePlotSink>, RuneEnvelopeError> {
	plot_sink_fields(plot_type, source, args, 1)
}

fn plot_sink_fields(
	plot_type: PlotType,
	source: &str,
	args: &[&ast::Expr],
	field_offset: usize,
) -> Result<Option<DisposableRunePlotSink>, RuneEnvelopeError> {
	let field = |index: usize| {
		let raw = source_slice(source, args[field_offset + index].span())?;
		Ok::<_, RuneEnvelopeError>(decode_string_literal(raw))
	};
	let Some(x) = field(0)? else {
		return Ok(None);
	};
	let Some(y) = field(1)? else {
		return Ok(None);
	};
	let category = if args.len() == field_offset + 3 {
		let Some(category) = field(2)? else {
			return Ok(None);
		};
		Some(category)
	} else {
		None
	};
	Ok(Some(DisposableRunePlotSink {
		composition: PlotComposition::single(plot_type),
		x,
		y,
		category,
	}))
}

fn decode_string_literal(raw: &str) -> Option<String> {
	let raw = raw.trim();
	let body = raw.strip_prefix('"')?.strip_suffix('"')?;
	let mut decoded = String::with_capacity(body.len());
	let mut chars = body.chars();
	while let Some(ch) = chars.next() {
		if ch != '\\' {
			decoded.push(ch);
			continue;
		}
		decoded.push(match chars.next()? {
			'"' => '"',
			'\\' => '\\',
			'n' => '\n',
			'r' => '\r',
			't' => '\t',
			_ => return None,
		});
	}
	Some(decoded)
}

fn source_slice(source: &str, span: ast::Span) -> Result<&str, RuneEnvelopeError> {
	let range = byte_range(span, source.len())?;
	source
		.get(range)
		.ok_or_else(|| RuneEnvelopeError::Invalid("Rune span is not a UTF-8 boundary".into()))
}

fn byte_range(span: ast::Span, source_len: usize) -> Result<Range<usize>, RuneEnvelopeError> {
	let start = span.start.into_usize();
	let end = span.end.into_usize();
	if start > end || end > source_len {
		return Err(RuneEnvelopeError::Invalid(
			"Rune span exceeds the source".into(),
		));
	}
	Ok(start..end)
}

fn push_segment(
	segments: &mut Vec<SourceMapSegment>,
	transformed: Range<usize>,
	original_start: u32,
) -> Result<(), RuneEnvelopeError> {
	if transformed.is_empty() {
		return Ok(());
	}
	segments.push(SourceMapSegment {
		transformed: u32::try_from(transformed.start)
			.map_err(|_| RuneEnvelopeError::Invalid("transformed source exceeds u32".into()))?
			..u32::try_from(transformed.end)
				.map_err(|_| RuneEnvelopeError::Invalid("transformed source exceeds u32".into()))?,
		original_start,
	});
	Ok(())
}

#[cfg(test)]
mod tests {
	use super::*;

	fn envelope(source: &str) -> ExecutionEnvelope {
		analyze(source)
			.expect("analyze Rune")
			.into_envelope()
			.expect("execution envelope")
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
		let semantics = analyze(mixed_source)
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
	fn non_root_view_names_and_malformed_calls_are_retained_without_refusal() {
		let source = "fn helper(r) { table(r); }\nlet r = workspace.table(\"t\");\nlet v = table(r);\nif true { table(r); }\n{ table(r); };\nplot.bar(r, \"x\");\nplot.scatter(r, x, \"y\");\ntable(r)";
		let transformed = transform(source).expect("only the final table is a root");
		assert_eq!(transformed.views.len(), 1);
		assert_eq!(transformed.views[0].intent.form(), RuneViewForm::Table);
		for retained in [
			"fn helper(r) { table(r); }",
			"let v = table(r);",
			"if true { table(r); }",
			"{ table(r); };",
			"plot.bar(r, \"x\");",
			"plot.scatter(r, x, \"y\");",
		] {
			assert!(
				transformed.source.contains(retained),
				"retained non-root: {retained}"
			);
		}

		// plans/product/impl/0003_typed_output_root_discovery.md withdraws
		// the old name-only arity refusal until view names are reserved.
		assert_eq!(
			transform("plot.bar(workspace.table(\"t\"), \"x\")"),
			Err(RuneEnvelopeError::NoTerminalView)
		);
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
			"workspace.table(\"t\").views([line(\"x\", \"y\", \"series\")])",
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
			("workspace.table(\"t\").views([layers([])])", "[]"),
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
	fn product_0014_layers_is_ordinary_rune_outside_a_proven_view_list() {
		let source = r#"
layers([line("x", "y")]);
let held = layers([scatter("x", "y")]);
consume(layers([line("x", "y")]));
workspace.table("t").views([table()])
"#;
		let transformed = transform(source).expect("ordinary layers calls are retained");
		assert!(transformed.source.contains("layers([line(\"x\", \"y\")]);"));
		assert!(
			transformed
				.source
				.contains("let held = layers([scatter(\"x\", \"y\")]);")
		);
		assert!(
			transformed
				.source
				.contains("consume(layers([line(\"x\", \"y\")]));")
		);
		assert_eq!(transformed.views.len(), 1);
		assert_eq!(
			transformed.views[0].intent,
			RuneViewIntent::Table(RuneTableSink::default())
		);
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
	fn product_0050_legend_presence_lowers_static_bindings_and_unbounded_fractions() {
		let source = r#"
let legend_x = 1.5;
let legend_y = -0.5;
workspace.table("t").views([figure([
	legend_at(legend_x, legend_y),
	line("x", "y"),
])])
"#;
		let transformed = transform(source).expect("authored legend position");
		let RuneViewIntent::Plot(plot) = &transformed.views[0].intent else {
			panic!("expected plot");
		};
		let Presence::Show(position) = plot.authored_settings.legend else {
			panic!("legend_at must lower to Show");
		};
		assert_eq!((position.x(), position.y()), (1.5, -0.5));

		let hidden = transform(
			"workspace.table(\"t\").views([figure([hide_legend(), line(\"x\", \"y\")])])",
		)
		.expect("hidden legend");
		let RuneViewIntent::Plot(hidden) = &hidden.views[0].intent else {
			panic!("expected plot");
		};
		assert!(matches!(hidden.authored_settings.legend, Presence::Hide));
	}

	#[test]
	fn product_0050_legend_members_refuse_ambiguity_and_undefined_values_at_source() {
		for (source, rejected) in [
			(
				"workspace.table(\"t\").views([figure([legend_at(0.1, 0.2), hide_legend(), line(\"x\", \"y\")])])",
				"hide_legend()",
			),
			(
				"workspace.table(\"t\").views([figure([hide_legend(), legend_at(0.1, 0.2), line(\"x\", \"y\")])])",
				"legend_at(0.1, 0.2)",
			),
			(
				"workspace.table(\"t\").views([figure([legend_at(0.1, 0.2), legend_at(0.3, 0.4), line(\"x\", \"y\")])])",
				"legend_at(0.3, 0.4)",
			),
			(
				"workspace.table(\"t\").views([figure([hide_legend(), hide_legend(), line(\"x\", \"y\")])])",
				"hide_legend()",
			),
		] {
			let RuneEnvelopeError::InvalidView { span, message } =
				transform(source).expect_err("ambiguous legend property must refuse")
			else {
				panic!("expected attributed legend refusal");
			};
			assert_eq!(&source[span.start as usize..span.end as usize], rejected);
			assert!(message.contains("at most one legend placement or visibility"));
		}

		for (member, expected) in [
			("hide_legend(1)", "takes no arguments"),
			("legend_at(0.5)", "takes exactly two numeric literals"),
			("legend_at(1e999, 0.5)", "requires finite positions"),
			(
				"legend_at(value, 0.5)",
				"requires unsuffixed numeric literals",
			),
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
	fn product_0050_legend_spellings_are_ordinary_rune_outside_a_figure_member() {
		let source = r#"
legend_at(0.98, 0.02);
let hidden = hide_legend();
consume(legend_at(1.5, -0.5));
workspace.table("t").views([table()])
"#;
		let transformed = transform(source).expect("ordinary calls are retained");
		for retained in [
			"legend_at(0.98, 0.02);",
			"let hidden = hide_legend();",
			"consume(legend_at(1.5, -0.5));",
		] {
			assert!(transformed.source.contains(retained), "missing {retained}");
		}
		assert!(matches!(
			transformed.views[0].intent,
			RuneViewIntent::Table(_)
		));
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
			y_bounds(-1.0, 10.0),
			title("Bounded"),
			x_bounds(0.0, 20.0),
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
			"x_bounds(0.0, 20.0)"
		);
		assert_eq!(
			&source[plot.domain_spans.y.clone().unwrap().start as usize
				..plot.domain_spans.y.clone().unwrap().end as usize],
			"y_bounds(-1.0, 10.0)"
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
			"workspace.table(\"t\").views([figure([y_bounds(-1.0, 1.0), bar(\"cat\", \"val\")])])",
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
			("x_bounds(1e999, 2.0)", "requires finite bounds"),
			("x_bounds(10.0, 0.0)", "cannot reverse an axis"),
			("x_bounds(5.0, 5.0)", "cannot form a collapsed axis"),
			(
				"x_bounds(value, 2.0)",
				"requires unsuffixed numeric literals",
			),
			("x_bounds(0.0)", "takes exactly two numeric literals"),
		] {
			let source =
				format!("workspace.table(\"t\").views([figure([{bounds}, line(\"x\", \"y\")])])");
			let RuneEnvelopeError::InvalidView { span, message } =
				transform(&source).expect_err("undefined bounds must refuse")
			else {
				panic!("expected attributed bounds refusal");
			};
			assert_eq!(&source[span.start as usize..span.end as usize], bounds);
			assert!(message.contains(expected), "{message}");
		}

		let duplicate = "workspace.table(\"t\").views([figure([y_bounds(0.0, 1.0), y_bounds(2.0, 3.0), line(\"x\", \"y\")])])";
		let RuneEnvelopeError::InvalidView { span, message } =
			transform(duplicate).expect_err("duplicate bounds must refuse")
		else {
			panic!("expected duplicate refusal");
		};
		assert_eq!(
			&duplicate[span.start as usize..span.end as usize],
			"y_bounds(2.0, 3.0)"
		);
		assert!(message.contains("at most one y_bounds"));
	}

	#[test]
	fn product_0038_tick_steps_lower_once_and_keep_axes_independent() {
		for (literal, expected) in [("1", 1.0), ("1.0", 1.0)] {
			let source = format!(
				"workspace.table(\"t\").views([figure([y_bounds(0, 7), y_tick_step({literal}), bar(\"cat\", \"val\")])])"
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
				format!("y_tick_step({literal})")
			);
		}

		let no_bounds = transform(
			"workspace.table(\"t\").views([figure([y_tick_step(1), line(\"x\", \"y\")])])",
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
			("y_tick_step(0)", "step must be strictly positive"),
			("y_tick_step(-1)", "step must be strictly positive"),
			("y_tick_step(1e999)", "requires a finite step"),
			(
				"y_tick_step(value)",
				"requires an unsuffixed numeric literal",
			),
			("y_tick_step()", "takes exactly one numeric literal"),
		] {
			let source = format!(
				"workspace.table(\"t\").views([figure([{constructor}, line(\"x\", \"y\")])])"
			);
			let RuneEnvelopeError::InvalidView { span, message } =
				transform(&source).expect_err("invalid tick step")
			else {
				panic!("expected attributed tick-step refusal");
			};
			assert_eq!(&source[span.start as usize..span.end as usize], constructor);
			assert!(message.contains(expected), "{message}");
		}
		let duplicate = "workspace.table(\"t\").views([figure([y_tick_step(1), y_tick_step(2), line(\"x\", \"y\")])])";
		let RuneEnvelopeError::InvalidView { span, message } =
			transform(duplicate).expect_err("duplicate tick step")
		else {
			panic!("expected duplicate tick-step refusal");
		};
		assert_eq!(
			&duplicate[span.start as usize..span.end as usize],
			"y_tick_step(2)"
		);
		assert!(message.contains("at most one y_tick_step"));
	}

	#[test]
	fn product_0039_minor_ticks_lower_as_independent_counts() {
		for (literal, expected) in [("0", 0), ("1", 1), ("1.0", 1), ("4294967295", u32::MAX)] {
			let source = format!(
				"workspace.table(\"t\").views([figure([x_minor_ticks({literal}), y_minor_ticks({literal}), bar(\"cat\", \"val\")])])"
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

		let source =
			"workspace.table(\"t\").views([figure([y_minor_ticks(1), bar(\"cat\", \"val\")])])";
		let transformed = transform(source).expect("single minor axis");
		let RuneViewIntent::Plot(plot) = &transformed.views[0].intent else {
			panic!("expected plot");
		};
		assert!(plot.minor_tick_spans.x.is_none());
		assert_eq!(
			&source[plot.minor_tick_spans.y.clone().unwrap().start as usize
				..plot.minor_tick_spans.y.clone().unwrap().end as usize],
			"y_minor_ticks(1)"
		);
	}

	#[test]
	fn product_0039_minor_tick_value_refusals_are_attributed() {
		for (constructor, expected) in [
			("y_minor_ticks(-1)", "count must be a non-negative integer"),
			("y_minor_ticks(2.5)", "count must be a non-negative integer"),
			("y_minor_ticks(1e999)", "requires a finite count"),
			(
				"y_minor_ticks(value)",
				"requires an unsuffixed numeric literal",
			),
			("y_minor_ticks()", "takes exactly one numeric literal"),
		] {
			let source = format!(
				"workspace.table(\"t\").views([figure([{constructor}, line(\"x\", \"y\")])])"
			);
			let RuneEnvelopeError::InvalidView { span, message } =
				transform(&source).expect_err("invalid minor count must refuse")
			else {
				panic!("expected invalid-view refusal");
			};
			assert_eq!(&source[span.start as usize..span.end as usize], constructor);
			assert!(message.contains(expected), "{message}");
		}

		let duplicate = "workspace.table(\"t\").views([figure([y_minor_ticks(1), y_minor_ticks(2), line(\"x\", \"y\")])])";
		let RuneEnvelopeError::InvalidView { span, message } =
			transform(duplicate).expect_err("duplicate minor count must refuse")
		else {
			panic!("expected duplicate refusal");
		};
		assert_eq!(
			&duplicate[span.start as usize..span.end as usize],
			"y_minor_ticks(2)"
		);
		assert!(message.contains("at most one y_minor_ticks"));
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
    y_bounds(minimum, maximum),
    y_tick_step(major_step),
    y_minor_ticks(minor_count),
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
				"binding \"title_text\" does not resolve to a literal",
			),
			(
				"let step = -1; workspace.table(\"t\").views([figure([y_tick_step(step), line(\"x\", \"y\")])])",
				"step must be strictly positive",
			),
			(
				"let maximum = 0; workspace.table(\"t\").views([figure([y_bounds(1, maximum), line(\"x\", \"y\")])])",
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
				"let parts = [line(\"x\", \"y\")]; workspace.table(\"t\").views([figure(parts)])",
				"parts",
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
				"view(\"inner\", line(\"x\", \"y\"))",
			),
			(
				"workspace.table(\"t\").views([figure([layers([title(\"wrong level\"), line(\"x\", \"y\")])])])",
				"title(\"wrong level\")",
			),
			(
				"let heading = title_text(); workspace.table(\"t\").views([figure([title(heading), line(\"x\", \"y\")])])",
				"heading",
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
		let retained = transform(ordinary).expect("figure calls outside views stay ordinary Rune");
		assert!(retained.source.contains("figure([title(\"ordinary\")"));
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
				"p",
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
		let retained =
			transform(ordinary).expect("palette stays ordinary Rune outside a figure sink");
		assert!(retained.source.contains("palette(\"tab20c\");"));
		assert!(retained.source.contains("palette(\"ordinary\")"));
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
		assert_eq!(plot.authored_settings.chrome, Override::Inherit);

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
				"s",
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
			assert_eq!(&source[span.start as usize..span.end as usize], rejected);
		}

		let ordinary = r#"
			skin("nord");
			let held = skin("viridis");
			let quiet = skin("tab20c_s8").duotone();
			figure([skin("ordinary"), bar("cat", "val")]);
			workspace.table("t").views([table().skin("nord")])
		"#;
		let retained = transform(ordinary).expect("skin remains contextual");
		assert!(retained.source.contains("skin(\"nord\");"));
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
			"point_size(1.0 + 1.0)",
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
				format!("scatter(\"x\", \"y\").{rejected}")
			);
		}
	}

	#[test]
	fn product_0018_modifier_register_is_closed_duplicate_free_and_atomic() {
		for modified in [
			"scatter(\"x\", \"y\").opacity(0.5)",
			"scatter(\"x\", \"y\").title(\"wrong level\")",
			"scatter(\"x\", \"y\").point_size(2.0).point_size(3.0)",
			"line(\"x\", \"y\").point_size(2.0)",
		] {
			let source = format!("workspace.table(\"t\").views([table(), layers([{modified}])])");
			let RuneEnvelopeError::InvalidView { span, .. } =
				transform(&source).expect_err("invalid modifier must refuse the whole view set")
			else {
				panic!("expected an attributed modifier refusal")
			};
			assert_eq!(&source[span.start as usize..span.end as usize], modified);
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
		let transformed = transform(source).expect("ordinary method calls are retained");
		assert!(
			transformed
				.source
				.contains("scatter(\"x\", \"y\").point_size(2.0)")
		);
		assert!(
			transformed
				.source
				.contains("scatter(\"a\", \"b\").point_size(4.0)")
		);
		assert_eq!(
			transformed.views[0].intent,
			RuneViewIntent::Table(RuneTableSink::default())
		);
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

		for rejected in [
			"stroke_width()",
			"stroke_width(1.0, 2.0)",
			"stroke_width(width)",
			"stroke_width(1.0 + 1.0)",
			"stroke_width(-1.0)",
			"stroke_width(0.0)",
			"stroke_width(0.24)",
			"stroke_width(8.01)",
			"stroke_width(2.0).stroke_width(3.0)",
		] {
			let source = format!(
				"workspace.table(\"t\").views([table(), layers([line(\"x\", \"y\").{rejected}])])"
			);
			let RuneEnvelopeError::InvalidView { span, .. } =
				transform(&source).expect_err("invalid stroke width must refuse")
			else {
				panic!("expected attributed stroke-width refusal")
			};
			assert_eq!(
				&source[span.start as usize..span.end as usize],
				format!("line(\"x\", \"y\").{rejected}")
			);
		}
	}

	#[test]
	fn product_0020_modifier_compatibility_is_closed_and_order_independent() {
		for modified in [
			"scatter(\"x\", \"y\").stroke_width(2.0)",
			"line(\"x\", \"y\").point_size(2.0)",
			"line(\"x\", \"y\").stroke_width(2.0).point_size(2.0)",
			"line(\"x\", \"y\").point_size(2.0).stroke_width(2.0)",
		] {
			let source = format!("workspace.table(\"t\").views([layers([{modified}])])");
			let RuneEnvelopeError::InvalidView { span, .. } =
				transform(&source).expect_err("wrong receiver must refuse complete chain")
			else {
				panic!("expected attributed compatibility refusal")
			};
			assert_eq!(&source[span.start as usize..span.end as usize], modified);
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
		let transformed = transform(source).expect("ordinary line methods are retained");
		assert!(
			transformed
				.source
				.contains("line(\"x\", \"y\").stroke_width(2.0)")
		);
		assert!(
			transformed
				.source
				.contains("line(\"a\", \"b\").stroke_width(4.0)")
		);
		assert_eq!(
			transformed.views[0].intent,
			RuneViewIntent::Table(RuneTableSink::default())
		);
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
			"bar_width(0.25 + 0.25)",
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
		let transformed = transform(source).expect("unproven modifiers remain Rune");
		assert!(
			transformed
				.source
				.contains("bar(\"cat\", \"val\").bar_width(0.5)")
		);
		assert!(
			transformed
				.source
				.contains("bar(\"a\", \"b\").bar_width(0.7)")
		);
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
			"category_gap(0.1 + 0.1)",
			"category_gap(-0.1)",
			"category_gap(1.0)",
			"category_gap(1.01)",
			"category_gap(0.2).category_gap(0.3)",
		] {
			let source =
				format!("workspace.table(\"t\").views([bar(\"cat\", \"val\").{rejected}])");
			assert!(transform(&source).is_err(), "accepted {rejected}");
		}

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
		let transformed = transform(source).expect("unproven modifiers remain Rune");
		assert!(transformed.source.contains("category_gap(0.2)"));
		assert!(transformed.source.contains("category_gap(0.4)"));
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
				"let marks = [line(\"x\", \"y\")]; workspace.table(\"t\").views([layers(marks)])",
				"layers(marks)",
			),
			(
				"workspace.table(\"t\").views([layers([line(\"x\")])])",
				"line(\"x\")",
			),
			(
				"workspace.table(\"t\").views([layers([scatter(x, \"y\")])])",
				"scatter(x, \"y\")",
			),
			(
				"workspace.table(\"t\").views([layers([unknown(\"x\", \"y\")])])",
				"unknown(\"x\", \"y\")",
			),
			(
				"workspace.table(\"t\").views([layers([line(\"x\", \"y\"), unknown()])])",
				"unknown()",
			),
		] {
			let RuneEnvelopeError::InvalidView { span, .. } =
				transform(source).expect_err("malformed layers sink must refuse atomically")
			else {
				panic!("expected an attributed view refusal")
			};
			assert_eq!(&source[span.start as usize..span.end as usize], rejected);
		}
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
			"let s = scatter(\"c\", \"v\"); workspace.table(\"t\").views([s])",
			"let v = workspace.table(\"t\").views([table()]); consume(v)",
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
		let analysis = analyze("let r = 1; r").expect("view-free analysis");
		assert!(analysis.into_envelope().is_none());
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
		for sink in [
			"table().skin()",
			"table().skin(\"nord\", \"terrain\")",
			"table().skin(theme_name)",
			"table().skin(\"nord\").skin(\"terrain\")",
			"table().duotone()",
			"table().skin(\"nord\").duotone(1)",
			"table().skin(\"nord\").duotone().duotone()",
			"table().point_size(2.0)",
			"table().frobnicate(1)",
			"table(\"wrong\").skin(\"nord\")",
			"scatter(\"x\", \"y\").skin(\"nord\")",
		] {
			let source =
				format!("workspace.table(\"t\").views([table(), {sink}, line(\"x\", \"y\")])");
			let error = transform(&source).expect_err("invalid modifier must refuse the view list");
			let RuneEnvelopeError::InvalidView { span, .. } = error else {
				panic!("expected an attributed table modifier refusal");
			};
			let expected_start = source.find(sink).unwrap() as u32;
			assert_eq!(
				span,
				expected_start..expected_start + sink.len() as u32,
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
	fn product_0052_show_types_refusals_are_atomic_and_duotone_owns_its_dependency() {
		for (sink, message) in [
			("table().show_types(1)", "show_types() takes no arguments"),
			(
				"table().show_types().show_types()",
				"show_types() may appear at most once on a table sink",
			),
			(
				"table().duotone().show_types()",
				"duotone() requires skin(string_literal)",
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
			let expected_start = source.find(sink).unwrap() as u32;
			assert_eq!(span, expected_start..expected_start + sink.len() as u32);
			assert_eq!(actual, message);
		}

		let source = r#"
let held = table().show_types();
consume(table().show_types());
workspace.table("t").views([table()])
"#;
		let transformed = transform(source).expect("one proven default table view");
		assert!(transformed.source.contains("table().show_types()"));
		assert_eq!(
			transformed.views[0].intent,
			RuneViewIntent::Table(RuneTableSink::default())
		);
	}

	#[test]
	fn product_0027_table_projection_calls_are_ordinary_rune_outside_proven_positions() {
		let source = r#"
let styled = table().skin("terrain_rs10");
consume(table().skin("nord"));
consume(table().skin("yellows").duotone());
workspace.table("t").views([table()])
"#;
		let transformed = transform(source).expect("one proven default table view");
		assert!(
			transformed
				.source
				.contains("table().skin(\"terrain_rs10\")")
		);
		assert!(transformed.source.contains("table().skin(\"nord\")"));
		assert!(
			transformed
				.source
				.contains("table().skin(\"yellows\").duotone()")
		);
		assert_eq!(
			transformed.views[0].intent,
			RuneViewIntent::Table(RuneTableSink::default())
		);

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
	#[should_panic(expected = "Rune view completion requires an in-flight buffer")]
	fn scheduler_completion_without_in_flight_work_is_a_hard_invariant() {
		let mut scheduler = RuneViewScheduler::default();
		let _ = scheduler.complete(BufferId::new(99), "workspace.table(\"t\")");
	}
}
