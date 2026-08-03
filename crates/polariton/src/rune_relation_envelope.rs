//! PRODUCT-0003's deliberately tiny terminal-view envelope.
//!
//! This module recognizes presentation calls and rewrites their relation
//! argument into Toron's `main(workspace)` entrypoint. It owns no relation
//! verbs, typing, planning, or lowering; those belong exclusively to Toron's
//! native Rune frontend.

use crate::plot::composition::PlotComposition;
use crate::plot::core::PlotType;
use libaryon::core::BufferId;
use rune::ast::{self, Spanned};
use std::collections::{HashMap, HashSet};
use std::ops::Range;

const ENTRYPOINT_PREFIX: &str = "pub fn main(workspace) {\n";
const ENTRYPOINT_SUFFIX: &str = "\n}\n";

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum RuneViewIntent {
	Table,
	Plot(DisposableRunePlotSink),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum RuneViewForm {
	Table,
	Bar,
	Scatter,
}

impl RuneViewForm {
	pub(crate) const fn label(self) -> &'static str {
		match self {
			Self::Table => "table",
			Self::Bar => "plot.bar",
			Self::Scatter => "plot.scatter",
		}
	}
}

impl RuneViewIntent {
	pub(crate) fn form(&self) -> RuneViewForm {
		match self {
			Self::Table => RuneViewForm::Table,
			Self::Plot(plot) => match plot.composition.primary_plot_type() {
				PlotType::Bar => RuneViewForm::Bar,
				PlotType::Scatter => RuneViewForm::Scatter,
				other => unreachable!("first-contact sink admitted unexpected plot type {other:?}"),
			},
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SpannedRuneViewIntent {
	pub(crate) intent: RuneViewIntent,
	pub(crate) span: Range<u32>,
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TransformedRuneView {
	pub(crate) source: String,
	pub(crate) views: Vec<SpannedRuneViewIntent>,
	pub(crate) source_map: RuneEnvelopeSourceMap,
}

#[derive(Clone, Debug)]
pub(crate) struct PendingRuneView {
	pub(crate) source_buffer: BufferId,
	pub(crate) revision: u64,
	pub(crate) view: TransformedRuneView,
}

#[derive(Default)]
pub(crate) struct RuneViewScheduler {
	in_flight: HashSet<BufferId>,
	pending: HashMap<BufferId, PendingRuneView>,
	pub(crate) submitted: u64,
	pub(crate) executed: u64,
	pub(crate) superseded_before_execution: u64,
}

impl RuneViewScheduler {
	pub(crate) fn submit(&mut self, work: PendingRuneView) -> Option<PendingRuneView> {
		self.submitted = self.submitted.saturating_add(1);
		if self.in_flight.insert(work.source_buffer) {
			self.executed = self.executed.saturating_add(1);
			return Some(work);
		}
		if self.pending.insert(work.source_buffer, work).is_some() {
			self.superseded_before_execution = self.superseded_before_execution.saturating_add(1);
		}
		None
	}

	pub(crate) fn complete(&mut self, source_buffer: BufferId) -> Option<PendingRuneView> {
		assert!(
			self.in_flight.remove(&source_buffer),
			"Rune view completion requires an in-flight buffer"
		);
		let pending = self.pending.remove(&source_buffer)?;
		self.in_flight.insert(source_buffer);
		self.executed = self.executed.saturating_add(1);
		Some(pending)
	}

	pub(crate) fn cancel(&mut self, source_buffer: BufferId) {
		self.in_flight.remove(&source_buffer);
		self.pending.remove(&source_buffer);
	}

	#[cfg(test)]
	fn is_in_flight(&self, source_buffer: BufferId) -> bool {
		self.in_flight.contains(&source_buffer)
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
	MixedRelations {
		first: Range<u32>,
		differing: Range<u32>,
	},
	DuplicateForm {
		form: RuneViewForm,
		first: Range<u32>,
		duplicate: Range<u32>,
	},
}

impl std::fmt::Display for RuneEnvelopeError {
	fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::NoTerminalView => formatter.write_str(
				"Rune relation views use top-level table(relation), plot.bar(relation, \"x\", \"y\"), or plot.scatter(relation, \"x\", \"y\")",
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
			Self::MixedRelations { first, differing } => write!(
				formatter,
				"one view set must share one relation; first relation at bytes {}..{}, differing relation at bytes {}..{}",
				first.start, first.end, differing.start, differing.end
			),
			Self::DuplicateForm {
				form,
				first,
				duplicate,
			} => write!(
				formatter,
				"{} appears twice at bytes {}..{} and {}..{}; multiple views of one form wait for named views",
				form.label(), first.start, first.end, duplicate.start, duplicate.end
			),
		}
	}
}

impl std::error::Error for RuneEnvelopeError {}

pub(crate) fn transform(source: &str) -> Result<TransformedRuneView, RuneEnvelopeError> {
	let analysis = analyze(source)?;
	let recognized_presentation = analysis.recognized_presentation;
	let envelope = analysis.into_envelope().ok_or(if recognized_presentation {
		RuneEnvelopeError::RecognizedNoOutput
	} else {
		RuneEnvelopeError::NoTerminalView
	})?;
	render(source, &envelope)
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct OutputRootAnalysis {
	retained: Vec<Range<usize>>,
	relation_origin: Option<String>,
	relation: Option<Range<usize>>,
	sinks: Vec<ParsedSink>,
	recognized_presentation: bool,
	terminate_retained_tail: bool,
}

impl OutputRootAnalysis {
	fn into_envelope(self) -> Option<ExecutionEnvelope> {
		let mut sinks = self.sinks.into_iter();
		let first = sinks.next()?;
		Some(ExecutionEnvelope {
			retained: self.retained,
			relation: self.relation.expect("a nonempty analysis has a relation"),
			relation_origin: self
				.relation_origin
				.expect("a nonempty analysis has a relation origin"),
			sinks: NonEmptySinks {
				first,
				rest: sinks.collect(),
			},
			terminate_retained_tail: self.terminate_retained_tail,
		})
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ExecutionEnvelope {
	retained: Vec<Range<usize>>,
	relation: Range<usize>,
	relation_origin: String,
	sinks: NonEmptySinks,
	terminate_retained_tail: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
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
#[derive(Clone, Debug, PartialEq, Eq)]
struct EnvelopeSemantics {
	relation_origin: String,
	sinks: Vec<RuneViewIntent>,
	execution_count: u8,
}

impl ExecutionEnvelope {
	#[cfg(test)]
	fn semantics(&self) -> EnvelopeSemantics {
		EnvelopeSemantics {
			relation_origin: self.relation_origin.clone(),
			sinks: self.sinks.iter().map(|sink| sink.intent.clone()).collect(),
			execution_count: 1,
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ParsedSink {
	intent: RuneViewIntent,
	span: Range<usize>,
}

#[derive(Clone, Debug)]
struct ParsedOutputRoot {
	emission: Range<usize>,
	relation: Range<usize>,
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

#[derive(Clone, Copy, Debug, Default)]
struct ViewShadows {
	table: bool,
	plot: bool,
	workspace: bool,
}

impl ViewShadows {
	fn insert(&mut self, name: &str) {
		match name {
			"table" => self.table = true,
			"plot" => self.plot = true,
			"workspace" => self.workspace = true,
			_ => {}
		}
	}
}

#[derive(Clone, Debug)]
enum ExpressionProvenance {
	RelationCandidate {
		span: Range<usize>,
		origin: String,
	},
	ViewSet {
		relation: Range<usize>,
		origin: String,
		sinks: Vec<ParsedSink>,
	},
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
			let relation_origin = resolve_relation_origin(source, &root, &bindings)?;
			peeled.push(root.statement.clone());
			roots.push(ParsedOutputRoot {
				emission: root.statement.clone(),
				relation: root.relation,
				relation_origin,
				sinks: vec![ParsedSink {
					intent: root.intent,
					span: root.statement,
				}],
			});
			continue;
		}

		if let ast::Stmt::Local(local) = statement {
			let statement_span = byte_range(statement.span(), source.len())?;
			let provenance = expression_provenance(&local.expr, source, &kinds, shadows.workspace)?;
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
				origin,
				sinks,
			}) = expression_provenance(expression, source, &kinds, shadows.workspace)?
		{
			let emission = byte_range(statement.span(), source.len())?;
			recognized_presentation = true;
			peeled.push(emission.clone());
			roots.push(ParsedOutputRoot {
				emission,
				relation,
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

	let Some(first) = roots.first() else {
		return Ok(OutputRootAnalysis {
			retained: retained_regions(source.len(), &peeled)?,
			relation_origin: None,
			relation: None,
			sinks: Vec::new(),
			recognized_presentation,
			terminate_retained_tail: last_retained_is_expression,
		});
	};
	let relation_origin = first.relation_origin.clone();
	let relation = first.relation.clone();
	let first_relation = u32_range(&relation, "relation")?;
	let mut forms = HashMap::<RuneViewForm, Range<u32>>::new();
	for root in &roots {
		if root.relation_origin != relation_origin {
			return Err(RuneEnvelopeError::MixedRelations {
				first: first_relation.clone(),
				differing: u32_range(&root.relation, "relation")?,
			});
		}
		for sink in &root.sinks {
			let form = sink.intent.form();
			let span = u32_range(&sink.span, "view")?;
			if let Some(first) = forms.insert(form, span.clone()) {
				return Err(RuneEnvelopeError::DuplicateForm {
					form,
					first,
					duplicate: span,
				});
			}
		}
	}

	let retained = retained_regions(source.len(), &peeled)?;
	Ok(OutputRootAnalysis {
		retained,
		relation_origin: Some(relation_origin),
		relation: Some(relation),
		sinks: roots.into_iter().flat_map(|root| root.sinks).collect(),
		recognized_presentation,
		terminate_retained_tail: last_retained_is_expression,
	})
}

fn expression_provenance(
	expression: &ast::Expr,
	source: &str,
	bindings: &HashMap<String, ExpressionProvenance>,
	workspace_shadowed: bool,
) -> Result<Option<ExpressionProvenance>, RuneEnvelopeError> {
	match expression {
		ast::Expr::Path(path) => {
			let raw = source_slice(source, path.span())?.trim();
			if raw == "workspace" && !workspace_shadowed {
				return Ok(Some(ExpressionProvenance::RelationCandidate {
					span: byte_range(path.span(), source.len())?,
					origin: raw.to_owned(),
				}));
			}
			Ok(bindings.get(raw).cloned())
		}
		ast::Expr::Group(group) => {
			expression_provenance(&group.expr, source, bindings, workspace_shadowed)
		}
		ast::Expr::Call(call) => {
			let args = call.args.iter().map(|(arg, _)| arg).collect::<Vec<_>>();
			if let ast::Expr::FieldAccess(field) = call.expr.as_ref() {
				let method = source_slice(source, field.expr_field.span())?.trim();
				let receiver =
					expression_provenance(&field.expr, source, bindings, workspace_shadowed)?;
				return match receiver {
					Some(ExpressionProvenance::RelationCandidate { span, origin }) => {
						provenance_method_on_relation(
							call,
							method,
							&field.expr,
							span,
							origin,
							&args,
							source,
						)
					}
					Some(ExpressionProvenance::ViewSet {
						relation,
						origin,
						sinks,
					}) => provenance_method_on_view_set(
						call, method, relation, origin, sinks, &args, source,
					),
					None => Ok(None),
				};
			}

			if matches!(
				expression_provenance(&call.expr, source, bindings, workspace_shadowed)?,
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
					expression_provenance(arg, source, bindings, workspace_shadowed)?,
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
					expression_provenance(item, source, bindings, workspace_shadowed)?,
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
	mut origin: String,
	args: &[&ast::Expr],
	source: &str,
) -> Result<Option<ExpressionProvenance>, RuneEnvelopeError> {
	match method {
		"views" => {
			let sinks = parse_view_list(call, args, source)?;
			Ok(Some(ExpressionProvenance::ViewSet {
				relation,
				origin,
				sinks,
			}))
		}
		"scatter" if matches!(args.len(), 2 | 3) => Ok(Some(ExpressionProvenance::ViewSet {
			relation,
			origin,
			sinks: vec![parse_sugar_sink(PlotType::Scatter, call, args, source)?],
		})),
		"bar" if args.len() == 2 => Ok(Some(ExpressionProvenance::ViewSet {
			relation,
			origin,
			sinks: vec![parse_sugar_sink(PlotType::Bar, call, args, source)?],
		})),
		"table" if args.is_empty() => Ok(Some(ExpressionProvenance::ViewSet {
			relation,
			origin,
			sinks: vec![ParsedSink {
				intent: RuneViewIntent::Table,
				span: byte_range(call.span(), source.len())?,
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
				origin,
			}))
		}
	}
}

fn provenance_method_on_view_set(
	call: &ast::ExprCall,
	method: &str,
	relation: Range<usize>,
	origin: String,
	mut sinks: Vec<ParsedSink>,
	args: &[&ast::Expr],
	source: &str,
) -> Result<Option<ExpressionProvenance>, RuneEnvelopeError> {
	if method != "views" {
		return invalid_view(
			call.span(),
			source.len(),
			"a ViewSet may only be emitted or extended with .views([...])",
		);
	}
	sinks.extend(parse_view_list(call, args, source)?);
	Ok(Some(ExpressionProvenance::ViewSet {
		relation,
		origin,
		sinks,
	}))
}

fn parse_view_list(
	call: &ast::ExprCall,
	args: &[&ast::Expr],
	source: &str,
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
		.map(|(item, _)| parse_contextual_sink(item, source))
		.collect()
}

fn parse_contextual_sink(
	expression: &ast::Expr,
	source: &str,
) -> Result<ParsedSink, RuneEnvelopeError> {
	let ast::Expr::Call(call) = expression else {
		return invalid_view(
			expression.span(),
			source.len(),
			"a views list element must be scatter(...), bar(...), or table()",
		);
	};
	let callee = source_slice(source, call.expr.span())?.trim();
	let args = call.args.iter().map(|(arg, _)| arg).collect::<Vec<_>>();
	let intent = match callee {
		"table" if args.is_empty() => RuneViewIntent::Table,
		"bar" if args.len() == 2 => {
			let Some(sink) = plot_sink_fields(PlotType::Bar, source, &args, 0)? else {
				return invalid_view(
					expression.span(),
					source.len(),
					"bar fields must be strings",
				);
			};
			RuneViewIntent::Plot(sink)
		}
		"scatter" if matches!(args.len(), 2 | 3) => {
			let Some(sink) = plot_sink_fields(PlotType::Scatter, source, &args, 0)? else {
				return invalid_view(
					expression.span(),
					source.len(),
					"scatter fields must be strings",
				);
			};
			RuneViewIntent::Plot(sink)
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
				"bar(...) takes x and y string fields",
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
				"unknown view sink; expected scatter(...), bar(...), or table()",
			);
		}
	};
	Ok(ParsedSink {
		intent,
		span: byte_range(expression.span(), source.len())?,
	})
}

fn parse_sugar_sink(
	plot_type: PlotType,
	call: &ast::ExprCall,
	args: &[&ast::Expr],
	source: &str,
) -> Result<ParsedSink, RuneEnvelopeError> {
	let valid_arity = match plot_type {
		PlotType::Bar => args.len() == 2,
		PlotType::Scatter => matches!(args.len(), 2 | 3),
		_ => false,
	};
	if !valid_arity {
		return invalid_view(
			call.span(),
			source.len(),
			"plot view sugar requires x and y strings with an optional scatter category",
		);
	}
	let Some(sink) = plot_sink_fields(plot_type, source, args, 0)? else {
		return invalid_view(
			call.span(),
			source.len(),
			"plot view fields must be string literals",
		);
	};
	Ok(ParsedSink {
		intent: RuneViewIntent::Plot(sink),
		span: byte_range(call.span(), source.len())?,
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
) -> Result<TransformedRuneView, RuneEnvelopeError> {
	let retained_len = envelope.retained.iter().map(Range::len).sum::<usize>();
	let mut transformed = String::with_capacity(
		ENTRYPOINT_PREFIX.len()
			+ retained_len
			+ envelope.relation.len()
			+ ENTRYPOINT_SUFFIX.len()
			+ 3,
	);
	transformed.push_str(ENTRYPOINT_PREFIX);
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

	Ok(TransformedRuneView {
		source: transformed,
		views: envelope
			.sinks
			.iter()
			.map(|sink| {
				Ok(SpannedRuneViewIntent {
					intent: sink.intent.clone(),
					span: u32_range(&sink.span, "view")?,
				})
			})
			.collect::<Result<_, RuneEnvelopeError>>()?,
		source_map: RuneEnvelopeSourceMap {
			segments,
			diagnostic_fallback: u32_range(&envelope.relation, "relation")?,
		},
	})
}

#[derive(Clone, Debug, PartialEq, Eq)]
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
		"table" if !shadows.table && args.len() == 1 => (RuneViewIntent::Table, args[0]),
		"plot.bar" if !shadows.plot && args.len() == 3 => (
			RuneViewIntent::Plot(match plot_sink(PlotType::Bar, source, &args)? {
				Some(sink) => sink,
				None => return Ok(None),
			}),
			args[0],
		),
		"plot.scatter" if !shadows.plot && matches!(args.len(), 3 | 4) => (
			RuneViewIntent::Plot(match plot_sink(PlotType::Scatter, source, &args)? {
				Some(sink) => sink,
				None => return Ok(None),
			}),
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
) -> Result<String, RuneEnvelopeError> {
	let relation = source
		.get(root.relation.clone())
		.ok_or_else(|| RuneEnvelopeError::Invalid("relation span is not UTF-8".into()))?
		.trim();
	if is_simple_name(relation)
		&& let Some(binding) = bindings.iter().rev().find(|binding| {
			binding.name == relation && binding.statement_start < root.statement.start
		}) {
		return Ok(source
			.get(binding.initializer.clone())
			.ok_or_else(|| RuneEnvelopeError::Invalid("binding initializer is not UTF-8".into()))?
			.to_owned());
	}
	Ok(relation.to_owned())
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
		assert_eq!(transformed.views[0].intent, RuneViewIntent::Table);
		assert!(transformed.source.starts_with(ENTRYPOINT_PREFIX));
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
		let RuneViewIntent::Plot(bar) = &bar.views[0].intent else {
			panic!("expected plot");
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
		assert_eq!(scatter.composition.primary_plot_type(), PlotType::Scatter);
		assert_eq!(scatter.category.as_deref(), Some("cat"));
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
	fn origin_resolved_identity_keeps_mixed_duplicate_and_inline_behavior() {
		let mixed_source = "let a = workspace.table(\"a\"); let b = workspace.table(\"b\"); table(a); let marker = 1; plot.bar(b, \"x\", \"y\")";
		let mixed = transform(mixed_source).expect_err("mixed relations");
		let RuneEnvelopeError::MixedRelations { first, differing } = mixed else {
			panic!("expected mixed-relation refusal: {mixed:?}");
		};
		assert_eq!(&mixed_source[first.start as usize..first.end as usize], "a");
		assert_eq!(
			&mixed_source[differing.start as usize..differing.end as usize],
			"b"
		);

		let duplicate =
			transform("let r = workspace.table(\"t\"); table(r); let marker = 1; table(r)")
				.expect_err("duplicate table form");
		assert!(duplicate.to_string().contains("named views"));

		let repeated_inline = transform(
			"table(workspace.table(\"t\")); plot.bar(workspace.table(\"t\"), \"x\", \"y\")",
		)
		.expect("identical inline origins at different spans");
		assert_eq!(repeated_inline.views.len(), 2);
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
					"let r = workspace.table(\"test2\"); table(r); plot.bar(r, \"cat\", \"val\"); plot.scatter(r, \"cat\", \"val\")",
				)
				.expect("complete view set"),
			}
		}
		let buffer = BufferId::new(7);
		let mut scheduler = RuneViewScheduler::default();
		assert_eq!(
			scheduler.submit(work(buffer, 1)).map(|work| work.revision),
			Some(1)
		);
		assert!(scheduler.submit(work(buffer, 2)).is_none());
		assert!(scheduler.submit(work(buffer, 3)).is_none());
		assert_eq!(
			scheduler.complete(buffer).map(|work| work.revision),
			Some(3)
		);
		assert!(scheduler.complete(buffer).is_none());
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
		let _ = scheduler.complete(BufferId::new(99));
	}
}
