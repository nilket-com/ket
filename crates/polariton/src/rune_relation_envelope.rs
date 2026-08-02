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
	view_block: Range<u32>,
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

	pub(crate) fn view_block(&self) -> Range<u32> {
		self.view_block.clone()
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
	Parse(String),
	Invalid(String),
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
				"Rune relation views end with table(relation), plot.bar(relation, \"x\", \"y\"), or plot.scatter(relation, \"x\", \"y\")",
			),
			Self::Parse(message) => write!(formatter, "Rune parse: {message}"),
			Self::Invalid(message) => write!(formatter, "Rune view syntax: {message}"),
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
	let block = rune::parse::parse_all::<ast::EmptyBlock>(source, rune::SourceId::EMPTY, false)
		.map_err(|error| RuneEnvelopeError::Parse(error.to_string()))?;
	let mut parsed = Vec::new();
	for statement in block.statements.iter().rev() {
		match parse_view_statement(statement, source)? {
			Some(view) => parsed.push(view),
			None if parsed.is_empty() => return Err(RuneEnvelopeError::NoTerminalView),
			None => break,
		}
	}
	parsed.reverse();
	let first = parsed.first().ok_or(RuneEnvelopeError::NoTerminalView)?;
	let relation_source = source
		.get(first.relation.clone())
		.ok_or_else(|| RuneEnvelopeError::Invalid("relation span is not UTF-8".into()))?;
	let first_relation = u32_range(&first.relation, "relation")?;
	let mut forms = HashMap::<RuneViewForm, Range<u32>>::new();
	for view in &parsed {
		let candidate = source
			.get(view.relation.clone())
			.ok_or_else(|| RuneEnvelopeError::Invalid("relation span is not UTF-8".into()))?;
		if candidate != relation_source {
			return Err(RuneEnvelopeError::MixedRelations {
				first: first_relation.clone(),
				differing: u32_range(&view.relation, "relation")?,
			});
		}
		let form = view.intent.form();
		let span = u32_range(&view.statement, "view")?;
		if let Some(first) = forms.insert(form, span.clone()) {
			return Err(RuneEnvelopeError::DuplicateForm {
				form,
				first,
				duplicate: span,
			});
		}
	}
	let view_block = parsed.first().expect("nonempty view block").statement.start
		..parsed.last().expect("nonempty view block").statement.end;
	let relation = first.relation.clone();

	let mut transformed = String::with_capacity(
		ENTRYPOINT_PREFIX.len() + source.len() - view_block.len()
			+ relation.len()
			+ ENTRYPOINT_SUFFIX.len(),
	);
	transformed.push_str(ENTRYPOINT_PREFIX);
	let prefix_start = transformed.len();
	transformed.push_str(&source[..view_block.start]);
	let prefix_end = transformed.len();
	let relation_start = transformed.len();
	transformed.push_str(&source[relation.clone()]);
	let relation_end = transformed.len();
	let suffix_start = transformed.len();
	transformed.push_str(&source[view_block.end..]);
	let suffix_end = transformed.len();
	transformed.push_str(ENTRYPOINT_SUFFIX);

	let mut segments = Vec::new();
	push_segment(&mut segments, prefix_start..prefix_end, 0)?;
	push_segment(
		&mut segments,
		relation_start..relation_end,
		u32::try_from(relation.start)
			.map_err(|_| RuneEnvelopeError::Invalid("source exceeds u32".into()))?,
	)?;
	push_segment(
		&mut segments,
		suffix_start..suffix_end,
		u32::try_from(view_block.end)
			.map_err(|_| RuneEnvelopeError::Invalid("source exceeds u32".into()))?,
	)?;

	Ok(TransformedRuneView {
		source: transformed,
		views: parsed
			.into_iter()
			.map(|view| {
				Ok(SpannedRuneViewIntent {
					intent: view.intent,
					span: u32_range(&view.statement, "view")?,
				})
			})
			.collect::<Result<_, RuneEnvelopeError>>()?,
		source_map: RuneEnvelopeSourceMap {
			segments,
			view_block: u32_range(&view_block, "view block")?,
		},
	})
}

struct ParsedView {
	intent: RuneViewIntent,
	statement: Range<usize>,
	relation: Range<usize>,
}

fn parse_view_statement(
	statement: &ast::Stmt,
	source: &str,
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
		"table" if args.len() == 1 => (RuneViewIntent::Table, args[0]),
		"plot.bar" if args.len() == 3 => (
			RuneViewIntent::Plot(plot_sink(PlotType::Bar, source, &args)?),
			args[0],
		),
		"plot.scatter" if matches!(args.len(), 3 | 4) => (
			RuneViewIntent::Plot(plot_sink(PlotType::Scatter, source, &args)?),
			args[0],
		),
		"table" | "plot.bar" | "plot.scatter" => {
			return Err(RuneEnvelopeError::Invalid(format!(
				"{callee} received {} arguments",
				args.len()
			)));
		}
		_ => return Ok(None),
	};
	Ok(Some(ParsedView {
		intent,
		statement: byte_range(statement_span, source.len())?,
		relation: byte_range(relation.span(), source.len())?,
	}))
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
) -> Result<DisposableRunePlotSink, RuneEnvelopeError> {
	let field = |index: usize| {
		let raw = source_slice(source, args[index].span())?;
		decode_string_literal(raw).ok_or_else(|| {
			RuneEnvelopeError::Invalid(format!(
				"plot field argument {} must be a string literal",
				index + 1
			))
		})
	};
	Ok(DisposableRunePlotSink {
		composition: PlotComposition::single(plot_type),
		x: field(1)?,
		y: field(2)?,
		category: (args.len() == 4).then(|| field(3)).transpose()?,
	})
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

	#[test]
	fn terminal_table_is_substituted_and_mapped() {
		let source = "let r = workspace.table(\"test2\").sort([col(\"cat\").asc()]);\ntable(r)";
		let transformed = transform(source).expect("table envelope");
		assert_eq!(transformed.views.len(), 1);
		assert_eq!(transformed.views[0].intent, RuneViewIntent::Table);
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
			"wrapper-only diagnostics must fall back to the terminal view"
		);
		let terminal = transformed.source_map.view_block();
		assert_eq!(
			&source[terminal.start as usize..terminal.end as usize],
			"table(r)"
		);
	}

	#[test]
	fn semicolon_terminated_view_still_returns_the_relation() {
		let transformed = transform("let r = workspace.table(\"test2\");\ntable(r);")
			.expect("semicolon-terminated table envelope");
		assert!(transformed.source.contains("\nr\n}"));
		assert!(!transformed.source.contains("\nr;\n}"));
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
	fn mixed_relations_and_duplicate_forms_refuse_with_both_spans() {
		let mixed = transform("let a = workspace.table(\"a\"); let b = workspace.table(\"b\"); table(a); plot.bar(b, \"x\", \"y\")")
			.expect_err("mixed relations");
		let RuneEnvelopeError::MixedRelations { first, differing } = mixed else {
			panic!("expected mixed-relation refusal: {mixed:?}");
		};
		assert_eq!(
			&"let a = workspace.table(\"a\"); let b = workspace.table(\"b\"); table(a); plot.bar(b, \"x\", \"y\")"
				[first.start as usize..first.end as usize],
			"a"
		);
		assert_eq!(
			&"let a = workspace.table(\"a\"); let b = workspace.table(\"b\"); table(a); plot.bar(b, \"x\", \"y\")"
				[differing.start as usize..differing.end as usize],
			"b"
		);

		let duplicate = transform("let r = workspace.table(\"t\"); table(r); table(r)")
			.expect_err("duplicate table form");
		assert!(duplicate.to_string().contains("named views"));
	}

	#[test]
	fn a_view_before_a_non_view_statement_is_not_in_the_trailing_block() {
		let transformed = transform(
			"let r = workspace.table(\"test2\"); table(r); let marker = 1; plot.bar(r, \"cat\", \"val\")",
		)
		.expect("terminal plot only");
		assert_eq!(transformed.views.len(), 1);
		assert_eq!(transformed.views[0].intent.form(), RuneViewForm::Bar);
		assert!(transformed.source.contains("table(r)"));
		assert!(transformed.source.contains("let marker = 1"));
	}

	#[test]
	fn no_view_stays_outside_the_envelope_and_invalid_view_refuses() {
		assert_eq!(
			transform("let r = 1; r"),
			Err(RuneEnvelopeError::NoTerminalView)
		);
		let error = transform("plot.bar(workspace.table(\"t\"), \"x\")").expect_err("wrong arity");
		assert!(error.to_string().contains("received 2 arguments"));
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
