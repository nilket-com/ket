//! PRODUCT-0076's static view shell, source partition, and generated programs.
//!
//! This module may recognize where relation and presentation syntax meet. It
//! does not interpret presentation values; those execute in `rune_presentation`.

use crate::presentation_registry::{
	self, CALLABLES, CallPosition, ContainerKind, ExpressionShape, HandlerId,
	PresentationSourceModel, PresentationSyntaxBoundary, PresentationViewSlotBoundary,
	SemanticReceiver, SettingOccurrence, setting_for_call,
};
use crate::rune_presentation::{
	PresentationLimits, PresentationMapKind, PresentationMapSegment, PresentationSourceMap,
	execute_generated,
};
use crate::rune_relation_envelope::{
	LocalViewIdentity, RelationLineage, RelationLineageLiteral, RelationRootIdentity,
	RuneEnvelopeError, RuneEnvelopeSourceMap, SourceMapSegment, SpannedRuneViewIntent,
	TransformedRuneRelation, TransformedRuneView,
};
use rune::ast::{self, Spanned};
use std::collections::{HashMap, HashSet};
use std::ops::Range;

const WORKSPACE_ENTRYPOINT_PREFIX: &str = "pub fn main(workspace) {\n";
const KET_ENTRYPOINT_PREFIX: &str = "pub fn main(ket) {\n";
const ENTRYPOINT_SUFFIX: &str = "\n}\n";
const PRESENTATION_PREFIX: &str = "polariton_presentation";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RelationRoot {
	Workspace,
	Ket,
}

#[derive(Clone, Debug)]
struct Definition {
	name: String,
	name_span: Range<usize>,
	statement: Range<usize>,
	value: Range<usize>,
	literal: bool,
}

#[derive(Clone, Debug)]
enum StaticPresentationExpr {
	Copied(Range<usize>),
	Synthetic {
		constructor: &'static str,
		arguments: Vec<Range<usize>>,
		origin: Range<usize>,
	},
}

impl StaticPresentationExpr {
	fn dependency_ranges(&self) -> impl Iterator<Item = &Range<usize>> {
		match self {
			Self::Copied(span) => std::slice::from_ref(span).iter(),
			Self::Synthetic { arguments, .. } => arguments.iter(),
		}
	}
}

#[derive(Clone, Debug)]
struct StaticSlot {
	expression: StaticPresentationExpr,
	span: Range<usize>,
	name: Option<(String, Range<usize>)>,
	output_order: usize,
}

#[derive(Clone, Debug)]
enum StaticProvenance {
	Relation {
		span: Range<usize>,
		root: RelationRoot,
		origin: String,
		lineage: Option<RelationLineage>,
	},
	ViewSet {
		relation: Range<usize>,
		root: RelationRoot,
		origin: String,
		lineage: Option<RelationLineage>,
		slots: Vec<StaticSlot>,
	},
}

#[derive(Clone, Debug)]
struct StaticRoot {
	emission: Range<usize>,
	relation: Range<usize>,
	root: RelationRoot,
	origin: String,
	lineage: Option<RelationLineage>,
	slots: Vec<StaticSlot>,
}

#[derive(Clone, Debug)]
struct StaticExecution {
	relation: Range<usize>,
	root: RelationRoot,
	origin: String,
	lineage: Option<RelationLineage>,
	slots: Vec<StaticSlot>,
}

#[derive(Clone, Debug)]
struct StaticAnalysis {
	executions: Vec<StaticExecution>,
	definitions: Vec<Definition>,
	statements: Vec<Range<usize>>,
	output_statements: Vec<Range<usize>>,
	last_retained_is_expression: bool,
	recognized_presentation: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum LexKind {
	Ident(String),
	Punct(char),
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct LexToken {
	kind: LexKind,
	span: Range<usize>,
}

#[derive(Clone, Copy, Debug, Default)]
struct ShellShadows {
	table: bool,
	plot: bool,
	workspace: bool,
	ket: bool,
}

impl ShellShadows {
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

pub(crate) fn compile(source: &str) -> Result<TransformedRuneView, RuneEnvelopeError> {
	let analysis = analyze_static(source)?;
	if analysis.executions.is_empty() {
		return Err(if analysis.recognized_presentation {
			RuneEnvelopeError::RecognizedNoOutput
		} else {
			RuneEnvelopeError::NoTerminalView
		});
	}

	let partition = partition(source, &analysis)?;
	let ordered_slots = ordered_slots(&analysis);
	let presentation_source_model =
		build_presentation_source_model(source, &ordered_slots, &analysis)?;
	let presentation =
		render_presentation(source, &ordered_slots, &analysis, &partition.presentation)?;
	let slot_spans = ordered_slots
		.iter()
		.map(|slot| u32_range(&slot.span, "view slot"))
		.collect::<Result<Vec<_>, _>>()?;
	let intents = execute_generated(
		&presentation.source,
		&slot_spans,
		&presentation.source_map,
		PresentationLimits::default(),
	)
	.map_err(|failure| RuneEnvelopeError::InvalidView {
		span: failure.span,
		message: failure.message,
	})?;
	let intents = intents.into_iter().enumerate().collect::<HashMap<_, _>>();

	let mut executions = Vec::with_capacity(analysis.executions.len());
	for execution in &analysis.executions {
		let rendered = render_relation(
			source,
			execution,
			&analysis,
			&partition.presentation,
			&partition.relation,
		)?;
		let views = execution
			.slots
			.iter()
			.map(|slot| {
				let intent = intents
					.get(&slot.output_order)
					.cloned()
					.expect("one evaluated intent per static slot");
				Ok(SpannedRuneViewIntent {
					intent,
					span: u32_range(&slot.span, "view")?,
					identity: match &slot.name {
						Some((name, _)) => LocalViewIdentity::Named(name.clone()),
						None => LocalViewIdentity::Anonymous(slot.output_order),
					},
					output_order: slot.output_order,
				})
			})
			.collect::<Result<Vec<_>, RuneEnvelopeError>>()?;
		executions.push(TransformedRuneRelation {
			origin: execution.origin.clone(),
			root: match execution.root {
				RelationRoot::Workspace => RelationRootIdentity::Workspace,
				RelationRoot::Ket => RelationRootIdentity::Ket,
			},
			lineage: execution.lineage.clone(),
			source: rendered.source,
			views,
			source_map: rendered.source_map,
		});
	}
	let presentation_order = ordered_slots
		.into_iter()
		.map(|slot| match &slot.name {
			Some((name, _)) => LocalViewIdentity::Named(name.clone()),
			None => LocalViewIdentity::Anonymous(slot.output_order),
		})
		.collect();
	Ok(TransformedRuneView {
		executions,
		presentation_order,
		presentation_source_model,
	})
}

fn slot_identity(slot: &StaticSlot) -> LocalViewIdentity {
	match &slot.name {
		Some((name, _)) => LocalViewIdentity::Named(name.clone()),
		None => LocalViewIdentity::Anonymous(slot.output_order),
	}
}

fn build_presentation_source_model(
	source: &str,
	slots: &[&StaticSlot],
	analysis: &StaticAnalysis,
) -> Result<PresentationSourceModel, RuneEnvelopeError> {
	let mut model = PresentationSourceModel::default();
	for slot in slots {
		let owner = slot_identity(slot);
		model.view_slots.push(PresentationViewSlotBoundary {
			expression_span: u32_range(&slot.span, "view slot expression")?,
			owner: owner.clone(),
		});
		let mut fragments = HashSet::new();
		for range in slot.expression.dependency_ranges() {
			collect_dependencies(source, range, &analysis.definitions, &mut fragments)?;
		}
		if let StaticPresentationExpr::Copied(span) = &slot.expression {
			fragments.insert(span.clone());
		}
		let mut fragments = fragments.into_iter().collect::<Vec<_>>();
		fragments.sort_by_key(|span| span.start);
		for fragment in fragments {
			scan_source_fragment(source, fragment, &owner, &mut model)?;
		}
	}
	model
		.occurrences
		.sort_by_key(|occurrence| occurrence.call_span.start);
	model
		.syntax
		.sort_by_key(|boundary| boundary.call_span.start);
	model
		.view_slots
		.sort_by_key(|boundary| boundary.expression_span.start);
	Ok(model)
}

fn scan_source_fragment(
	source: &str,
	fragment_span: Range<usize>,
	owner: &LocalViewIdentity,
	model: &mut PresentationSourceModel,
) -> Result<(), RuneEnvelopeError> {
	let fragment = source.get(fragment_span.clone()).ok_or_else(|| {
		RuneEnvelopeError::Invalid("presentation source-model fragment is not UTF-8".to_owned())
	})?;
	let tokens = lex(fragment, fragment_span.start);
	let mut receiver_by_close = HashMap::<usize, (SemanticReceiver, usize)>::new();
	for (index, token) in tokens.iter().enumerate() {
		let LexKind::Ident(name) = &token.kind else {
			continue;
		};
		if !matches!(
			tokens.get(index + 1).map(|token| &token.kind),
			Some(LexKind::Punct('('))
		) {
			continue;
		}
		let method_chain = index >= 2
			&& tokens[index - 1].kind == LexKind::Punct('.')
			&& tokens[index - 2].kind == LexKind::Punct(')');
		let position = if method_chain {
			CallPosition::Method
		} else {
			CallPosition::Free
		};
		if !presentation_registry::is_presentation_callable(name, position) {
			continue;
		}
		let open_index = index + 1;
		let Some(close_index) = matching_paren(&tokens, open_index) else {
			return Err(RuneEnvelopeError::Invalid(
				"presentation source-model call has no matching close parenthesis".to_owned(),
			));
		};
		let inherited = method_chain
			.then(|| receiver_by_close.get(&(index - 2)).copied())
			.flatten();
		let receiver = inherited.map(|(receiver, _)| receiver);
		let callable = CALLABLES
			.iter()
			.filter(|callable| callable.name == name && callable.position == position)
			.find(|callable| {
				receiver
					.map(|receiver| callable.accepted_receivers.contains(&receiver))
					.unwrap_or(true)
			})
			.or_else(|| {
				CALLABLES
					.iter()
					.find(|callable| callable.name == name && callable.position == position)
			})
			.expect("recognition comes from CALLABLES");
		let produced_receiver = receiver.or_else(|| produced_receiver(callable.handler));
		let call_start = if method_chain {
			tokens[index - 1].span.start
		} else {
			token.span.start
		};
		let chain_start = inherited
			.map(|(_, chain_start)| chain_start)
			.unwrap_or(call_start);
		if let Some(receiver) = produced_receiver {
			receiver_by_close.insert(close_index, (receiver, chain_start));
		}
		let arguments = call_argument_ranges(
			fragment,
			fragment_span.start,
			&tokens,
			open_index,
			close_index,
		)?;
		let call_span = u32_range(&(call_start..tokens[close_index].span.end), "call")?;
		let argument_spans = arguments
			.iter()
			.map(|argument| u32_range(argument, "call argument"))
			.collect::<Result<Vec<_>, _>>()?;
		let argument_text = arguments
			.iter()
			.map(|argument| source.get(argument.clone()).unwrap_or_default().to_owned())
			.collect::<Vec<_>>();
		let expression_shape = if arguments
			.iter()
			.all(|argument| is_direct_literal(source.get(argument.clone()).unwrap_or_default()))
		{
			ExpressionShape::DirectLiteral
		} else {
			ExpressionShape::ExpressionOrBinding
		};
		if let Some(setting) = setting_for_call(callable.handler, receiver) {
			if let Some(existing) = model.occurrences.iter_mut().find(|occurrence| {
				occurrence.key == setting.key && occurrence.call_span == call_span
			}) {
				existing.owners.insert(owner.clone());
			} else {
				model.occurrences.push(SettingOccurrence {
					key: setting.key,
					target: setting.target,
					call_span: call_span.clone(),
					argument_spans: argument_spans.clone(),
					argument_text: argument_text.clone(),
					expression_shape,
					owners: HashSet::from([owner.clone()]),
				});
			}
		}
		let mut separator_ranges = top_level_separators(&tokens, open_index, close_index);
		if matches!(callable.handler, HandlerId::Figure | HandlerId::Layers)
			&& let Some(child_list) = arguments.first()
		{
			separator_ranges.extend(child_list_separators(&tokens, child_list));
		}
		let separators = separator_ranges
			.into_iter()
			.map(|span| u32_range(&span, "call separator"))
			.collect::<Result<Vec<_>, _>>()?;
		let container = match callable.handler {
			HandlerId::Figure => arguments
				.first()
				.map(|span| (ContainerKind::Figure, span.clone())),
			HandlerId::Layers => arguments
				.first()
				.map(|span| (ContainerKind::Layers, span.clone())),
			_ => None,
		}
		.map(|(kind, span)| Ok((kind, u32_range(&span, "container child list")?)))
		.transpose()?;
		if let Some(existing) = model.syntax.iter_mut().find(|boundary| {
			boundary.handler == callable.handler && boundary.call_span == call_span
		}) {
			existing.owners.insert(owner.clone());
		} else {
			model.syntax.push(PresentationSyntaxBoundary {
				handler: callable.handler,
				call_span,
				chain_span: u32_range(&(chain_start..tokens[close_index].span.end), "call chain")?,
				argument_list_span: u32_range(
					&(tokens[open_index].span.end..tokens[close_index].span.start),
					"argument list",
				)?,
				separators,
				container,
				owners: HashSet::from([owner.clone()]),
			});
		}
	}
	Ok(())
}

fn produced_receiver(handler: HandlerId) -> Option<SemanticReceiver> {
	match handler {
		HandlerId::Title => Some(SemanticReceiver::Title),
		HandlerId::Subtitle => Some(SemanticReceiver::Subtitle),
		HandlerId::XAxis => Some(SemanticReceiver::XAxis),
		HandlerId::YAxis => Some(SemanticReceiver::YAxis),
		HandlerId::Facet => Some(SemanticReceiver::Facet),
		HandlerId::Legend => Some(SemanticReceiver::Legend),
		HandlerId::Padding => Some(SemanticReceiver::Padding),
		HandlerId::Skin => Some(SemanticReceiver::Skin),
		HandlerId::Table => Some(SemanticReceiver::Table),
		HandlerId::Bar
		| HandlerId::Line
		| HandlerId::Scatter
		| HandlerId::Area
		| HandlerId::Histogram
		| HandlerId::Box
		| HandlerId::Bubble
		| HandlerId::Heatmap
		| HandlerId::Hexbin
		| HandlerId::FillBetween
		| HandlerId::Candlestick
		| HandlerId::Funnel
		| HandlerId::RadialDial
		| HandlerId::Pie
		| HandlerId::Violin
		| HandlerId::Radar
		| HandlerId::Parallel => Some(SemanticReceiver::Mark),
		_ => None,
	}
}

fn is_direct_literal(source: &str) -> bool {
	let source = source.trim();
	is_complete_quoted_literal(source)
		|| matches!(source, "true" | "false")
		|| is_numeric_literal(source)
}

fn is_complete_quoted_literal(source: &str) -> bool {
	let bytes = source.as_bytes();
	let Some(&quote @ (b'"' | b'\'')) = bytes.first() else {
		return false;
	};
	let mut index = 1;
	while index < bytes.len() {
		if bytes[index] == b'\\' {
			index = index.saturating_add(2);
		} else if bytes[index] == quote {
			return index + 1 == bytes.len();
		} else {
			index += 1;
		}
	}
	false
}

fn is_numeric_literal(source: &str) -> bool {
	let unsigned = source.strip_prefix('-').unwrap_or(source);
	if unsigned.is_empty() || !unsigned.as_bytes()[0].is_ascii_digit() {
		return false;
	}
	let normalized = source.replace('_', "");
	if normalized.parse::<f64>().is_ok() {
		return true;
	}
	let radix = unsigned
		.strip_prefix("0x")
		.or_else(|| unsigned.strip_prefix("0X"))
		.map(|digits| (16, digits))
		.or_else(|| {
			unsigned
				.strip_prefix("0o")
				.or_else(|| unsigned.strip_prefix("0O"))
				.map(|digits| (8, digits))
		})
		.or_else(|| {
			unsigned
				.strip_prefix("0b")
				.or_else(|| unsigned.strip_prefix("0B"))
				.map(|digits| (2, digits))
		});
	radix.is_some_and(|(radix, digits)| {
		!digits.is_empty()
			&& digits
				.chars()
				.all(|character| character == '_' || character.is_digit(radix))
	})
}

fn top_level_separators(tokens: &[LexToken], open: usize, close: usize) -> Vec<Range<usize>> {
	let mut parens = 0usize;
	let mut brackets = 0usize;
	let mut braces = 0usize;
	let mut separators = Vec::new();
	for token in &tokens[open + 1..close] {
		match token.kind {
			LexKind::Punct('(') => parens += 1,
			LexKind::Punct(')') => parens = parens.saturating_sub(1),
			LexKind::Punct('[') => brackets += 1,
			LexKind::Punct(']') => brackets = brackets.saturating_sub(1),
			LexKind::Punct('{') => braces += 1,
			LexKind::Punct('}') => braces = braces.saturating_sub(1),
			LexKind::Punct(',') if parens == 0 && brackets == 0 && braces == 0 => {
				separators.push(token.span.clone());
			}
			_ => {}
		}
	}
	separators
}

fn child_list_separators(tokens: &[LexToken], list: &Range<usize>) -> Vec<Range<usize>> {
	let mut parens = 0usize;
	let mut brackets = 0usize;
	let mut braces = 0usize;
	let mut separators = Vec::new();
	for token in tokens
		.iter()
		.filter(|token| token.span.start >= list.start && token.span.end <= list.end)
	{
		match token.kind {
			LexKind::Punct('(') => parens += 1,
			LexKind::Punct(')') => parens = parens.saturating_sub(1),
			LexKind::Punct('[') => brackets += 1,
			LexKind::Punct(']') => brackets = brackets.saturating_sub(1),
			LexKind::Punct('{') => braces += 1,
			LexKind::Punct('}') => braces = braces.saturating_sub(1),
			LexKind::Punct(',') if parens == 0 && brackets == 1 && braces == 0 => {
				separators.push(token.span.clone());
			}
			_ => {}
		}
	}
	separators
}

fn ordered_slots(analysis: &StaticAnalysis) -> Vec<&StaticSlot> {
	let mut slots = analysis
		.executions
		.iter()
		.flat_map(|execution| execution.slots.iter())
		.collect::<Vec<_>>();
	slots.sort_by_key(|slot| slot.output_order);
	slots
}

fn analyze_static(source: &str) -> Result<StaticAnalysis, RuneEnvelopeError> {
	let block = rune::parse::parse_all::<ast::EmptyBlock>(source, rune::SourceId::EMPTY, false)
		.map_err(|error| RuneEnvelopeError::Parse(error.to_string()))?;
	let mut shadows = ShellShadows::default();
	let mut definitions = Vec::new();
	for statement in &block.statements {
		collect_definition(statement, source, &mut shadows, &mut definitions)?;
	}

	let mut provenances = HashMap::<String, StaticProvenance>::new();
	let mut roots = Vec::new();
	let mut outputs = Vec::new();
	let mut recognized_presentation = false;
	let mut last_retained_is_expression = false;
	for statement in &block.statements {
		if let Some(root) = recognize_legacy_root(statement, source, shadows, &definitions)? {
			outputs.push(root.emission.clone());
			roots.push(root);
			last_retained_is_expression = false;
			continue;
		}
		if let ast::Stmt::Local(local) = statement {
			let statement_span = byte_range(statement.span(), source.len())?;
			if let Some(definition) = definitions
				.iter()
				.find(|definition| definition.statement == statement_span)
				&& let Some(provenance) =
					static_provenance(&local.expr, source, &provenances, shadows)?
			{
				if matches!(provenance, StaticProvenance::ViewSet { .. }) {
					recognized_presentation = true;
					outputs.push(statement_span);
				}
				provenances.insert(definition.name.clone(), provenance);
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
			&& let Some(StaticProvenance::ViewSet {
				relation,
				root,
				origin,
				lineage,
				slots,
			}) = static_provenance(expression, source, &provenances, shadows)?
		{
			let emission = byte_range(statement.span(), source.len())?;
			recognized_presentation = true;
			outputs.push(emission.clone());
			roots.push(StaticRoot {
				emission,
				relation,
				root,
				origin,
				lineage,
				slots,
			});
			last_retained_is_expression = false;
			continue;
		}
		// PRODUCT-0137: a terminal `ket` relation with no view root is a
		// complete program — it asks the engine to do something and presents
		// nothing. It becomes an execution carrying zero presentation slots.
		// The decision is structural: it reads the provenance's root and
		// never inspects a method or verb name, so no relation verb is known
		// here (PRODUCT-0003). `workspace` roots are deliberately excluded —
		// `workspace.result(...)` and `workspace.cell(...)` are local-host
		// programs and keep falling through to that host.
		if let Some(expression) = expression
			&& let Some(StaticProvenance::Relation {
				span,
				root: RelationRoot::Ket,
				origin,
				lineage,
			}) = static_provenance(expression, source, &provenances, shadows)?
		{
			let emission = byte_range(statement.span(), source.len())?;
			outputs.push(emission.clone());
			roots.push(StaticRoot {
				emission,
				relation: span,
				root: RelationRoot::Ket,
				origin,
				lineage,
				slots: Vec::new(),
			});
			last_retained_is_expression = false;
			continue;
		}
		last_retained_is_expression = matches!(statement, ast::Stmt::Expr(_));
	}

	roots.sort_by_key(|root| root.emission.start);
	outputs.sort_by_key(|span| span.start);
	let mut names = HashMap::<String, Range<u32>>::new();
	let mut output_order = 0;
	for root in &mut roots {
		for slot in &mut root.slots {
			slot.output_order = output_order;
			output_order += 1;
			if let Some((name, name_span)) = &slot.name {
				let span = u32_range(name_span, "view name")?;
				if let Some(first) = names.insert(name.clone(), span.clone()) {
					return Err(RuneEnvelopeError::DuplicateName {
						name: name.clone(),
						first,
						duplicate: span,
					});
				}
			}
		}
	}
	let mut grouped = Vec::<StaticRoot>::new();
	let mut by_origin = HashMap::<String, usize>::new();
	for root in roots {
		if let Some(index) = by_origin.get(&root.origin).copied() {
			grouped[index].slots.extend(root.slots);
		} else {
			by_origin.insert(root.origin.clone(), grouped.len());
			grouped.push(root);
		}
	}
	Ok(StaticAnalysis {
		executions: grouped
			.into_iter()
			.map(|root| StaticExecution {
				relation: root.relation,
				root: root.root,
				origin: root.origin,
				lineage: root.lineage,
				slots: root.slots,
			})
			.collect(),
		definitions,
		statements: block
			.statements
			.iter()
			.map(|statement| byte_range(statement.span(), source.len()))
			.collect::<Result<Vec<_>, _>>()?,
		output_statements: outputs,
		last_retained_is_expression,
		recognized_presentation,
	})
}

fn static_provenance(
	expression: &ast::Expr,
	source: &str,
	bindings: &HashMap<String, StaticProvenance>,
	shadows: ShellShadows,
) -> Result<Option<StaticProvenance>, RuneEnvelopeError> {
	match expression {
		ast::Expr::Path(path) => {
			let raw = source_slice(source, path.span())?.trim();
			let root = match raw {
				"workspace" if !shadows.workspace => Some(RelationRoot::Workspace),
				"ket" if !shadows.ket => Some(RelationRoot::Ket),
				_ => None,
			};
			if let Some(root) = root {
				return Ok(Some(StaticProvenance::Relation {
					span: byte_range(path.span(), source.len())?,
					root,
					origin: "workspace".to_owned(),
					lineage: None,
				}));
			}
			Ok(match bindings.get(raw).cloned() {
				Some(StaticProvenance::Relation {
					root,
					origin,
					lineage,
					..
				}) => {
					// Return the bound name from the rendered Rune program. Replaying
					// its initializer would execute a linear relation operation twice.
					Some(StaticProvenance::Relation {
						span: byte_range(path.span(), source.len())?,
						root,
						origin,
						lineage,
					})
				}
				other => other,
			})
		}
		ast::Expr::Group(group) => static_provenance(&group.expr, source, bindings, shadows),
		ast::Expr::Call(call) => {
			let args = call.args.iter().map(|(arg, _)| arg).collect::<Vec<_>>();
			if let ast::Expr::FieldAccess(field) = call.expr.as_ref() {
				let method = source_slice(source, field.expr_field.span())?.trim();
				let receiver = static_provenance(&field.expr, source, bindings, shadows)?;
				return match receiver {
					Some(StaticProvenance::Relation {
						span,
						root,
						origin,
						lineage,
					}) => static_method_on_relation(
						call,
						method,
						&field.expr,
						span,
						root,
						origin,
						lineage,
						&args,
						source,
					),
					Some(StaticProvenance::ViewSet {
						relation,
						root,
						origin,
						lineage,
						mut slots,
					}) => {
						if method != "views" {
							return invalid_view(
								call.span(),
								source.len(),
								"a ViewSet may only be emitted or extended with .views([...])",
							);
						}
						slots.extend(parse_static_view_list(call, &args, source)?);
						Ok(Some(StaticProvenance::ViewSet {
							relation,
							root,
							origin,
							lineage,
							slots,
						}))
					}
					None => Ok(None),
				};
			}
			Ok(None)
		}
		_ => Ok(None),
	}
}

#[allow(clippy::too_many_arguments)]
fn static_method_on_relation(
	call: &ast::ExprCall,
	method: &str,
	receiver: &ast::Expr,
	relation: Range<usize>,
	root: RelationRoot,
	mut origin: String,
	mut lineage: Option<RelationLineage>,
	args: &[&ast::Expr],
	source: &str,
) -> Result<Option<StaticProvenance>, RuneEnvelopeError> {
	if lineage.is_none() && origin == "workspace" {
		lineage = relation_head_lineage(method, args, source);
	}
	if method == "views" {
		return Ok(Some(StaticProvenance::ViewSet {
			relation,
			root,
			origin,
			lineage,
			slots: parse_static_view_list(call, args, source)?,
		}));
	}
	if let Some(constructor) = mark_constructor(method)
		&& ((method == "violin" && args.len() == 4)
			|| (method != "violin" && matches!(args.len(), 2 | 3)))
	{
		return Ok(Some(StaticProvenance::ViewSet {
			relation,
			root,
			origin,
			lineage,
			slots: vec![synthetic_slot(constructor, args, call.span(), source)?],
		}));
	}
	if matches!(method, "line" | "bar" | "scatter" | "area" | "histogram") {
		return invalid_view(
			call.span(),
			source.len(),
			if method == "histogram" {
				"histogram view sugar requires bin_start, bin_end, and count strings".to_owned()
			} else {
				format!("{method} view sugar requires x and y strings with an optional series")
			},
		);
	}
	if matches!(
		method,
		"box"
			| "bubble"
			| "funnel"
			| "heatmap"
			| "hexbin"
			| "fill_between"
			| "candlestick"
			| "radial_dial"
			| "pie" | "violin"
			| "radar" | "parallel"
	) {
		return invalid_view(
			call.span(),
			source.len(),
			format!(
				"{method} is a sink inside .views([...]); relation.{method}(...) sugar is not available"
			),
		);
	}
	if method == "table" && args.is_empty() {
		return Ok(Some(StaticProvenance::ViewSet {
			relation,
			root,
			origin,
			lineage,
			slots: vec![synthetic_slot("table", args, call.span(), source)?],
		}));
	}
	let receiver = byte_range(receiver.span(), source.len())?;
	let call_span = byte_range(call.span(), source.len())?;
	origin.push_str(
		source.get(receiver.end..call_span.end).ok_or_else(|| {
			RuneEnvelopeError::Invalid("relation method span is not UTF-8".into())
		})?,
	);
	Ok(Some(StaticProvenance::Relation {
		span: call_span,
		root,
		origin,
		lineage,
	}))
}

fn parse_static_view_list(
	call: &ast::ExprCall,
	args: &[&ast::Expr],
	source: &str,
) -> Result<Vec<StaticSlot>, RuneEnvelopeError> {
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
		.map(|(item, _)| parse_static_slot(item, source, true))
		.collect()
}

fn parse_static_slot(
	expression: &ast::Expr,
	source: &str,
	allow_view: bool,
) -> Result<StaticSlot, RuneEnvelopeError> {
	if let ast::Expr::Call(call) = expression {
		let callee = source_slice(source, call.expr.span())?.trim();
		if callee == "view" {
			if !allow_view {
				return invalid_view(
					expression.span(),
					source.len(),
					"view(...) wrappers cannot be nested",
				);
			}
			let args = call.args.iter().map(|(arg, _)| arg).collect::<Vec<_>>();
			let [name, body] = args.as_slice() else {
				return invalid_view(
					expression.span(),
					source.len(),
					"view(...) takes a name string and one sink",
				);
			};
			let name_span = byte_range(name.span(), source.len())?;
			let Some(name) =
				decode_string_literal(source.get(name_span.clone()).ok_or_else(|| {
					RuneEnvelopeError::Invalid("view name span is not UTF-8".into())
				})?)
			else {
				return invalid_view(
					name.span(),
					source.len(),
					"view name must be a string literal",
				);
			};
			let body_span = byte_range(body.span(), source.len())?;
			return Ok(StaticSlot {
				expression: StaticPresentationExpr::Copied(body_span.clone()),
				span: body_span,
				name: Some((name, name_span)),
				output_order: 0,
			});
		}
	}
	let span = byte_range(expression.span(), source.len())?;
	Ok(StaticSlot {
		expression: StaticPresentationExpr::Copied(span.clone()),
		span,
		name: None,
		output_order: 0,
	})
}

fn recognize_legacy_root(
	statement: &ast::Stmt,
	source: &str,
	shadows: ShellShadows,
	definitions: &[Definition],
) -> Result<Option<StaticRoot>, RuneEnvelopeError> {
	let (expression, statement_span) = match statement {
		ast::Stmt::Expr(expression) => (expression, expression.span()),
		ast::Stmt::Semi(statement) => (&statement.expr, statement.span()),
		_ => return Ok(None),
	};
	let ast::Expr::Call(call) = expression else {
		return Ok(None);
	};
	let callee = source_slice(source, call.expr.span())?.trim();
	let args = call.args.iter().map(|(arg, _)| arg).collect::<Vec<_>>();
	if !shadows.plot && matches!(callee, "plot.box" | "plot.bubble") {
		let mark = callee.trim_start_matches("plot.");
		return invalid_view(
			call.span(),
			source.len(),
			format!(
				"legacy plot.{mark}(relation, ...) is not available; use relation.views([{mark}(...)])"
			),
		);
	}
	if !shadows.plot
		&& matches!(
			callee,
			"plot.bar" | "plot.line" | "plot.scatter" | "plot.area"
		) && !matches!(args.len(), 3 | 4)
	{
		return invalid_view(
			call.span(),
			source.len(),
			format!("{callee}(...) requires a relation, x and y strings, and an optional series"),
		);
	}
	let (relation_expression, presentation) = match callee {
		"table" if !shadows.table && args.len() == 1 => (
			args[0],
			StaticPresentationExpr::Synthetic {
				constructor: "table",
				arguments: Vec::new(),
				origin: byte_range(call.span(), source.len())?,
			},
		),
		"plot.bar" if !shadows.plot && matches!(args.len(), 3 | 4) => (
			args[0],
			synthetic_expression("bar", &args[1..], call.span(), source)?,
		),
		"plot.line" if !shadows.plot && matches!(args.len(), 3 | 4) => (
			args[0],
			synthetic_expression("line", &args[1..], call.span(), source)?,
		),
		"plot.scatter" if !shadows.plot && matches!(args.len(), 3 | 4) => (
			args[0],
			synthetic_expression("scatter", &args[1..], call.span(), source)?,
		),
		"plot.area" if !shadows.plot && matches!(args.len(), 3 | 4) => (
			args[0],
			synthetic_expression("area", &args[1..], call.span(), source)?,
		),
		_ => return Ok(None),
	};
	let relation_span = byte_range(relation_expression.span(), source.len())?;
	let (origin, root) = resolve_relation(
		source,
		relation_span.clone(),
		statement_span.start.into_usize(),
		definitions,
		shadows,
	)?;
	let emission = byte_range(statement_span, source.len())?;
	Ok(Some(StaticRoot {
		emission: emission.clone(),
		relation: relation_span,
		root,
		lineage: legacy_relation_lineage(&origin),
		origin,
		slots: vec![StaticSlot {
			expression: presentation,
			span: emission,
			name: None,
			output_order: 0,
		}],
	}))
}

fn synthetic_slot(
	constructor: &'static str,
	args: &[&ast::Expr],
	span: ast::Span,
	source: &str,
) -> Result<StaticSlot, RuneEnvelopeError> {
	let origin = byte_range(span, source.len())?;
	Ok(StaticSlot {
		expression: synthetic_expression(constructor, args, span, source)?,
		span: origin,
		name: None,
		output_order: 0,
	})
}

fn synthetic_expression(
	constructor: &'static str,
	args: &[&ast::Expr],
	span: ast::Span,
	source: &str,
) -> Result<StaticPresentationExpr, RuneEnvelopeError> {
	Ok(StaticPresentationExpr::Synthetic {
		constructor,
		arguments: args
			.iter()
			.map(|argument| byte_range(argument.span(), source.len()))
			.collect::<Result<Vec<_>, _>>()?,
		origin: byte_range(span, source.len())?,
	})
}

fn mark_constructor(method: &str) -> Option<&'static str> {
	match method {
		"bar" => Some("bar"),
		"line" => Some("line"),
		"scatter" => Some("scatter"),
		"area" => Some("area"),
		"histogram" => Some("histogram"),
		"violin" => Some("violin"),
		_ => None,
	}
}

fn resolve_relation(
	source: &str,
	mut span: Range<usize>,
	use_start: usize,
	definitions: &[Definition],
	shadows: ShellShadows,
) -> Result<(String, RelationRoot), RuneEnvelopeError> {
	let mut visited = HashSet::new();
	loop {
		let raw = source
			.get(span.clone())
			.ok_or_else(|| RuneEnvelopeError::Invalid("relation span is not UTF-8".into()))?
			.trim();
		if raw.starts_with("workspace") && !shadows.workspace {
			return Ok((
				canonical_relation_origin(raw, "workspace"),
				RelationRoot::Workspace,
			));
		}
		if raw.starts_with("ket") && !shadows.ket {
			return Ok((canonical_relation_origin(raw, "ket"), RelationRoot::Ket));
		}
		if !is_simple_name(raw) || !visited.insert(raw.to_owned()) {
			return Err(RuneEnvelopeError::Invalid(format!(
				"relation origin {raw:?} does not resolve to workspace or ket"
			)));
		}
		let Some(definition) = resolve_definition(definitions, raw, use_start) else {
			return Err(RuneEnvelopeError::Invalid(format!(
				"relation binding {raw:?} has no preceding initializer"
			)));
		};
		span = definition.value.clone();
	}
}

fn canonical_relation_origin(raw: &str, root: &str) -> String {
	if root == "ket" {
		format!("workspace{}", &raw[3..])
	} else {
		raw.to_owned()
	}
}

/// Record PRODUCT-0151's structural head directly from the Rune AST. No
/// relation-verb list is consulted: the first method on the normalized root is
/// the head, and every direct argument must be a physical literal.
fn relation_head_lineage(
	method: &str,
	args: &[&ast::Expr],
	source: &str,
) -> Option<RelationLineage> {
	let arguments = args
		.iter()
		.map(|argument| {
			source_slice(source, argument.span())
				.ok()
				.and_then(parse_lineage_literal)
		})
		.collect::<Option<Vec<_>>>()?;
	Some(RelationLineage {
		method: method.to_owned(),
		arguments,
	})
}

/// Legacy `plot.*(relation, ...)` roots still arrive through the old span
/// resolver. Keep their compatibility lineage isolated here; contemporary
/// `.views(...)` programs never key identity from this rendered text.
fn legacy_relation_lineage(origin: &str) -> Option<RelationLineage> {
	let source = origin.trim();
	let mut cursor = "workspace".len();
	if !source.starts_with("workspace") {
		return None;
	}
	cursor += source[cursor..].find('.')? + 1;
	while source
		.as_bytes()
		.get(cursor)
		.is_some_and(u8::is_ascii_whitespace)
	{
		cursor += 1;
	}
	let method_start = cursor;
	while source
		.as_bytes()
		.get(cursor)
		.is_some_and(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
	{
		cursor += 1;
	}
	let method = source.get(method_start..cursor)?.trim();
	if method.is_empty() {
		return None;
	}
	while source
		.as_bytes()
		.get(cursor)
		.is_some_and(u8::is_ascii_whitespace)
	{
		cursor += 1;
	}
	if source.as_bytes().get(cursor) != Some(&b'(') {
		return None;
	}
	let args_start = cursor + 1;
	let mut depth = 1_u32;
	let mut quote = None;
	let mut escaped = false;
	let mut end = None;
	for (offset, byte) in source.as_bytes()[args_start..].iter().copied().enumerate() {
		if let Some(active) = quote {
			if escaped {
				escaped = false;
			} else if byte == b'\\' {
				escaped = true;
			} else if byte == active {
				quote = None;
			}
			continue;
		}
		match byte {
			b'\'' | b'"' => quote = Some(byte),
			b'(' => depth += 1,
			b')' => {
				depth -= 1;
				if depth == 0 {
					end = Some(args_start + offset);
					break;
				}
			}
			_ => {}
		}
	}
	let arguments = split_lineage_arguments(source.get(args_start..end?)?)?
		.into_iter()
		.map(parse_lineage_literal)
		.collect::<Option<Vec<_>>>()?;
	Some(RelationLineage {
		method: method.to_owned(),
		arguments,
	})
}

fn split_lineage_arguments(source: &str) -> Option<Vec<&str>> {
	if source.trim().is_empty() {
		return Some(Vec::new());
	}
	let mut quote = None;
	let mut escaped = false;
	let mut start = 0;
	let mut arguments = Vec::new();
	for (index, byte) in source.as_bytes().iter().copied().enumerate() {
		if let Some(active) = quote {
			if escaped {
				escaped = false;
			} else if byte == b'\\' {
				escaped = true;
			} else if byte == active {
				quote = None;
			}
			continue;
		}
		match byte {
			b'\'' | b'"' => quote = Some(byte),
			b',' => {
				arguments.push(source.get(start..index)?.trim());
				start = index + 1;
			}
			b'(' | b')' | b'[' | b']' | b'{' | b'}' => return None,
			_ => {}
		}
	}
	if quote.is_some() {
		return None;
	}
	arguments.push(source.get(start..)?.trim());
	Some(arguments)
}

fn parse_lineage_literal(source: &str) -> Option<RelationLineageLiteral> {
	let source = source.trim();
	if let Some(value) = decode_string_literal(source) {
		return Some(RelationLineageLiteral::String(value));
	}
	match source {
		"true" => return Some(RelationLineageLiteral::Bool(true)),
		"false" => return Some(RelationLineageLiteral::Bool(false)),
		_ => {}
	}
	let normalized = source.replace('_', "");
	if normalized.starts_with('-') {
		if let Ok(value) = normalized.parse::<i128>() {
			return Some(RelationLineageLiteral::Integer(value));
		}
	} else if let Ok(value) = normalized.parse::<i128>() {
		return Some(RelationLineageLiteral::Integer(value));
	} else if let Ok(value) = normalized.parse::<u128>() {
		return Some(RelationLineageLiteral::Unsigned(value));
	}
	let value = normalized.parse::<f64>().ok()?;
	value
		.is_finite()
		.then_some(RelationLineageLiteral::Float(if value == 0.0 {
			0.0_f64.to_bits()
		} else {
			value.to_bits()
		}))
}

#[derive(Default)]
struct Partition {
	presentation: HashSet<Range<usize>>,
	relation: HashSet<Range<usize>>,
}

fn partition(source: &str, analysis: &StaticAnalysis) -> Result<Partition, RuneEnvelopeError> {
	let mut presentation = HashSet::new();
	let mut relation = HashSet::new();
	for execution in &analysis.executions {
		collect_dependencies(
			source,
			&execution.relation,
			&analysis.definitions,
			&mut relation,
		)?;
		for slot in &execution.slots {
			for range in slot.expression.dependency_ranges() {
				collect_dependencies(source, range, &analysis.definitions, &mut presentation)?;
			}
		}
	}
	// A top-level occurrence which calls presentation vocabulary is claimed by
	// the local program even when no output reads its value. Retaining it in the
	// Toron program would leak a native Toron does not own. Ordinary unclaimed
	// statements remain relation-side for compatibility and possible effects.
	for statement in &analysis.statements {
		if analysis.output_statements.contains(statement) {
			continue;
		}
		let text = source
			.get(statement.clone())
			.ok_or_else(|| RuneEnvelopeError::Invalid("statement span is not UTF-8".into()))?;
		let (has_presentation, has_relation) =
			vocabulary_usage(&lex(text, statement.start), &analysis.definitions);
		if has_presentation && has_relation {
			return Err(RuneEnvelopeError::InvalidView {
				span: u32_range(statement, "mixed top-level occurrence")?,
				message: "a helper cannot depend on both relation and presentation vocabulary"
					.to_owned(),
			});
		}
		if has_presentation {
			presentation.insert(statement.clone());
		}
	}
	for statement in presentation.intersection(&relation) {
		let definition = analysis
			.definitions
			.iter()
			.find(|definition| &definition.statement == statement)
			.expect("partition sets contain definitions");
		if !definition.literal {
			return Err(RuneEnvelopeError::InvalidView {
				span: u32_range(&definition.statement, "dual-use binding")?,
				message: format!(
					"computed binding {:?} is used by both relation and presentation; only static literal and alias closures may cross both VMs",
					definition.name
				),
			});
		}
	}
	for statement in &presentation {
		let text = source
			.get(statement.clone())
			.ok_or_else(|| RuneEnvelopeError::Invalid("definition span is not UTF-8".into()))?;
		let tokens = lex(text, statement.start);
		let (has_presentation, has_relation) = vocabulary_usage(&tokens, &analysis.definitions);
		if has_presentation && has_relation {
			return Err(RuneEnvelopeError::InvalidView {
				span: u32_range(statement, "mixed helper")?,
				message: "a helper cannot depend on both relation and presentation vocabulary"
					.to_owned(),
			});
		}
	}
	Ok(Partition {
		presentation,
		relation,
	})
}

fn vocabulary_usage(tokens: &[LexToken], definitions: &[Definition]) -> (bool, bool) {
	let mut presentation = false;
	let mut relation = false;
	for (index, token) in tokens.iter().enumerate() {
		let LexKind::Ident(name) = &token.kind else {
			continue;
		};
		let previous = index.checked_sub(1).and_then(|index| tokens.get(index));
		let next = tokens.get(index + 1);
		let called = matches!(next.map(|token| &token.kind), Some(LexKind::Punct('(')));
		let declaration = matches!(previous.map(|token| &token.kind), Some(LexKind::Ident(keyword)) if keyword == "fn");
		let method = matches!(previous.map(|token| &token.kind), Some(LexKind::Punct('.')));
		let presentation_call = called
			&& !declaration
			&& if method {
				presentation_registry::is_presentation_callable(name, CallPosition::Method)
			} else {
				presentation_registry::is_presentation_callable(name, CallPosition::Free)
					&& !definitions
						.iter()
						.any(|definition| definition.name == *name)
			};
		presentation |= presentation_call;
		relation |= matches!(name.as_str(), "workspace" | "ket") || (called && name == "col");
	}
	(presentation, relation)
}

fn collect_dependencies(
	source: &str,
	range: &Range<usize>,
	definitions: &[Definition],
	out: &mut HashSet<Range<usize>>,
) -> Result<(), RuneEnvelopeError> {
	let text = source
		.get(range.clone())
		.ok_or_else(|| RuneEnvelopeError::Invalid("dependency span is not UTF-8".into()))?;
	let tokens = lex(text, range.start);
	let locals = local_names(&tokens);
	for (index, token) in tokens.iter().enumerate() {
		let LexKind::Ident(name) = &token.kind else {
			continue;
		};
		if locals.contains(name)
			|| is_keyword(name)
			|| matches!(
				tokens.get(index.wrapping_sub(1)).map(|token| &token.kind),
				Some(LexKind::Punct('.'))
			) {
			continue;
		}
		let Some(definition) = resolve_definition(definitions, name, token.span.start) else {
			continue;
		};
		if definition.name_span.contains(&token.span.start)
			|| !out.insert(definition.statement.clone())
		{
			continue;
		}
		collect_dependencies(source, &definition.value, definitions, out)?;
	}
	Ok(())
}

fn resolve_definition<'a>(
	definitions: &'a [Definition],
	name: &str,
	use_start: usize,
) -> Option<&'a Definition> {
	definitions
		.iter()
		.filter(|definition| definition.name == name && definition.statement.start < use_start)
		.max_by_key(|definition| definition.statement.start)
}

fn local_names(tokens: &[LexToken]) -> HashSet<String> {
	let mut names = HashSet::new();
	for window in tokens.windows(2) {
		if matches!(&window[0].kind, LexKind::Ident(keyword) if keyword == "let")
			&& let LexKind::Ident(name) = &window[1].kind
		{
			names.insert(name.clone());
		}
	}
	if matches!(tokens.first().map(|token| &token.kind), Some(LexKind::Ident(name)) if name == "fn")
		&& let Some(open) = tokens
			.iter()
			.position(|token| token.kind == LexKind::Punct('('))
		&& let Some(close) = matching_paren(tokens, open)
	{
		for token in &tokens[open + 1..close] {
			if let LexKind::Ident(name) = &token.kind
				&& !is_keyword(name)
			{
				names.insert(name.clone());
			}
		}
	}
	names
}

struct RenderedRelation {
	source: String,
	source_map: RuneEnvelopeSourceMap,
}

fn render_relation(
	source: &str,
	execution: &StaticExecution,
	analysis: &StaticAnalysis,
	presentation: &HashSet<Range<usize>>,
	relation: &HashSet<Range<usize>>,
) -> Result<RenderedRelation, RuneEnvelopeError> {
	let mut peeled = analysis.output_statements.clone();
	// Executions are independent relation branches. A sibling definition may
	// consume a shared ancestor, so retain only this execution's dependencies.
	let mut execution_relation = HashSet::new();
	collect_dependencies(
		source,
		&execution.relation,
		&analysis.definitions,
		&mut execution_relation,
	)?;
	for statement in relation {
		if !execution_relation.contains(statement) {
			peeled.push(statement.clone());
		}
	}
	for statement in presentation {
		if !relation.contains(statement) {
			peeled.push(statement.clone());
		}
	}
	peeled.sort_by_key(|span| span.start);
	peeled.dedup();
	let retained = retained_regions(source.len(), &peeled)?;
	let prefix = match execution.root {
		RelationRoot::Workspace => WORKSPACE_ENTRYPOINT_PREFIX,
		RelationRoot::Ket => KET_ENTRYPOINT_PREFIX,
	};
	let mut transformed = String::new();
	transformed.push_str(prefix);
	let mut segments = Vec::new();
	for retained in retained {
		let start = transformed.len();
		transformed.push_str(source.get(retained.clone()).ok_or_else(|| {
			RuneEnvelopeError::Invalid("retained source span is not UTF-8".into())
		})?);
		let end = transformed.len();
		push_segment(&mut segments, start..end, retained.start)?;
	}
	if analysis.last_retained_is_expression {
		transformed.push_str("\n;");
	}
	transformed.push('\n');
	let relation_start = transformed.len();
	transformed.push_str(
		source
			.get(execution.relation.clone())
			.ok_or_else(|| RuneEnvelopeError::Invalid("relation span is not UTF-8".into()))?,
	);
	let relation_end = transformed.len();
	transformed.push_str(ENTRYPOINT_SUFFIX);
	push_segment(
		&mut segments,
		relation_start..relation_end,
		execution.relation.start,
	)?;
	Ok(RenderedRelation {
		source: transformed,
		source_map: RuneEnvelopeSourceMap {
			segments,
			diagnostic_fallback: u32_range(&execution.relation, "relation")?,
		},
	})
}

struct RenderedPresentation {
	source: String,
	source_map: PresentationSourceMap,
}

#[derive(Default)]
struct PresentationBuilder {
	source: String,
	segments: Vec<PresentationMapSegment>,
}

impl PresentationBuilder {
	fn plain(&mut self, text: &str) {
		self.source.push_str(text);
	}

	fn copied(&mut self, text: &str, original_start: usize) -> Result<(), RuneEnvelopeError> {
		let start = self.source.len();
		self.source.push_str(text);
		let end = self.source.len();
		self.segment(
			start..end,
			PresentationMapKind::Copied {
				original_start: u32::try_from(original_start)
					.map_err(|_| RuneEnvelopeError::Invalid("source exceeds u32".into()))?,
			},
		)
	}

	fn synthetic(&mut self, text: &str, owner: &Range<usize>) -> Result<(), RuneEnvelopeError> {
		let start = self.source.len();
		self.source.push_str(text);
		let end = self.source.len();
		self.segment(
			start..end,
			PresentationMapKind::Synthetic {
				owner: u32_range(owner, "synthetic presentation owner")?,
			},
		)
	}

	fn rewritten(&mut self, fragment: RewrittenFragment) -> Result<(), RuneEnvelopeError> {
		let offset = self.source.len();
		self.source.push_str(&fragment.source);
		for segment in fragment.segments {
			let start = usize::try_from(segment.generated.start)
				.map_err(|_| RuneEnvelopeError::Invalid("generated span exceeds usize".into()))?;
			let end = usize::try_from(segment.generated.end)
				.map_err(|_| RuneEnvelopeError::Invalid("generated span exceeds usize".into()))?;
			self.segment(offset + start..offset + end, segment.kind)?;
		}
		Ok(())
	}

	fn segment(
		&mut self,
		generated: Range<usize>,
		kind: PresentationMapKind,
	) -> Result<(), RuneEnvelopeError> {
		if generated.is_empty() {
			return Ok(());
		}
		self.segments.push(PresentationMapSegment {
			generated: u32_range(&generated, "generated presentation")?,
			kind,
		});
		Ok(())
	}
}

fn render_presentation(
	source: &str,
	slots: &[&StaticSlot],
	analysis: &StaticAnalysis,
	presentation_statements: &HashSet<Range<usize>>,
) -> Result<RenderedPresentation, RuneEnvelopeError> {
	if source.contains(PRESENTATION_PREFIX) {
		return Err(RuneEnvelopeError::Invalid(
			"source uses Polariton's reserved generated presentation namespace".to_owned(),
		));
	}
	let fallback = slots
		.first()
		.map(|slot| u32_range(&slot.span, "presentation fallback"))
		.transpose()?
		.unwrap_or(0..0);
	let mut generated = PresentationBuilder::default();
	generated.plain("pub fn main() {\n");
	let mut slot_dependencies = HashSet::new();
	for slot in slots {
		for range in slot.expression.dependency_ranges() {
			collect_dependencies(source, range, &analysis.definitions, &mut slot_dependencies)?;
		}
	}
	let mut unclaimed = presentation_statements
		.iter()
		.filter(|statement| !slot_dependencies.contains(*statement))
		.cloned()
		.collect::<Vec<_>>();
	unclaimed.sort_by_key(|statement| statement.start);
	for statement in unclaimed {
		generated.synthetic("{\n", &statement)?;
		generated.rewritten(rewrite_calls_mapped(
			source.get(statement.clone()).ok_or_else(|| {
				RuneEnvelopeError::Invalid("presentation statement is not UTF-8".into())
			})?,
			statement.start,
		)?)?;
		generated.synthetic("\n};\n", &statement)?;
	}
	generated.plain("[\n");
	for slot in slots {
		// Each static slot executes in its own lexical block. This preserves the
		// most-recent-preceding binding seen at that slot instead of hoisting all
		// presentation definitions above every slot and letting a later shadow
		// rewrite an earlier view's meaning.
		generated.synthetic("{\n", &slot.span)?;
		let mut definitions = HashSet::new();
		for range in slot.expression.dependency_ranges() {
			collect_dependencies(source, range, &analysis.definitions, &mut definitions)?;
		}
		let mut definitions = definitions.into_iter().collect::<Vec<_>>();
		definitions.sort_by_key(|span| span.start);
		for statement in definitions {
			let rewritten = rewrite_calls_mapped(
				source.get(statement.clone()).ok_or_else(|| {
					RuneEnvelopeError::Invalid("presentation definition is not UTF-8".into())
				})?,
				statement.start,
			)?;
			generated.rewritten(rewritten)?;
			generated.plain("\n");
		}
		match &slot.expression {
			StaticPresentationExpr::Copied(span) => {
				generated.rewritten(rewrite_calls_mapped(
					source.get(span.clone()).ok_or_else(|| {
						RuneEnvelopeError::Invalid("presentation slot is not UTF-8".into())
					})?,
					span.start,
				)?)?;
			}
			StaticPresentationExpr::Synthetic {
				constructor,
				arguments,
				origin,
			} => {
				generated.synthetic(&format!("{PRESENTATION_PREFIX}::__{constructor}("), origin)?;
				for (index, argument) in arguments.iter().enumerate() {
					if index > 0 {
						generated.plain(", ");
					}
					generated.copied(
						source.get(argument.clone()).ok_or_else(|| {
							RuneEnvelopeError::Invalid("synthetic argument is not UTF-8".into())
						})?,
						argument.start,
					)?;
				}
				if !arguments.is_empty() {
					generated.plain(", ");
				}
				generated.synthetic(&format!("{}, {})", origin.start, origin.end), origin)?;
			}
		}
		generated.synthetic("\n},\n", &slot.span)?;
	}
	generated.plain("]\n}\n");
	Ok(RenderedPresentation {
		source: generated.source,
		source_map: PresentationSourceMap {
			segments: generated.segments,
			fallback,
		},
	})
}

#[derive(Clone, Debug)]
struct Edit {
	span: Range<usize>,
	replacement: String,
	owner: Range<u32>,
}

#[cfg(test)]
fn rewrite_calls(fragment: &str, original_start: usize) -> Result<String, RuneEnvelopeError> {
	Ok(rewrite_calls_mapped(fragment, original_start)?.source)
}

struct RewrittenFragment {
	source: String,
	segments: Vec<PresentationMapSegment>,
}

fn rewrite_calls_mapped(
	fragment: &str,
	original_start: usize,
) -> Result<RewrittenFragment, RuneEnvelopeError> {
	let tokens = lex(fragment, original_start);
	let mut edits = Vec::new();
	for (index, token) in tokens.iter().enumerate() {
		let LexKind::Ident(name) = &token.kind else {
			continue;
		};
		let Some(open) = tokens
			.get(index + 1)
			.filter(|token| token.kind == LexKind::Punct('('))
		else {
			continue;
		};
		let is_method = matches!(
			tokens.get(index.wrapping_sub(1)).map(|token| &token.kind),
			Some(LexKind::Punct('.'))
		);
		let recognized = if is_method {
			presentation_registry::is_presentation_callable(name, CallPosition::Method)
		} else {
			presentation_registry::is_presentation_callable(name, CallPosition::Free)
				&& !matches!(tokens.get(index.wrapping_sub(1)).map(|token| &token.kind), Some(LexKind::Ident(keyword)) if keyword == "fn")
		};
		if !recognized {
			continue;
		}
		let open_index = index + 1;
		let Some(close_index) = matching_paren(&tokens, open_index) else {
			return Err(RuneEnvelopeError::Invalid(
				"presentation call has no matching close parenthesis".to_owned(),
			));
		};
		let close = &tokens[close_index];
		let origin_start = if is_method {
			tokens[index - 1].span.start
		} else {
			token.span.start
		};
		let origin_end = close.span.end;
		let replacement = if is_method {
			format!("__{name}")
		} else {
			format!("{PRESENTATION_PREFIX}::__{name}")
		};
		edits.push(Edit {
			span: (token.span.start - original_start)..(token.span.end - original_start),
			replacement,
			owner: u32_range(&(origin_start..origin_end), "presentation call origin")?,
		});
		let origin_arities: Option<&[usize]> = match name.as_str() {
			"box" => Some(&[7]),
			"bubble" => Some(&[4, 5]),
			"heatmap" => Some(&[3]),
			"hexbin" => Some(&[2]),
			"fill_between" => Some(&[3, 4]),
			"candlestick" => Some(&[5]),
			"funnel" => Some(&[2]),
			"radial_dial" => Some(&[2, 3]),
			"pie" => Some(&[2]),
			"violin" => Some(&[4]),
			_ => None,
		};
		let variadic_origins = !is_method && matches!(name.as_str(), "radar" | "parallel");
		let injected_origins = if !is_method && (origin_arities.is_some() || variadic_origins) {
			let arguments =
				call_argument_ranges(fragment, original_start, &tokens, open_index, close_index)?;
			if !variadic_origins
				&& !origin_arities
					.expect("fixed origin arities are present")
					.contains(&arguments.len())
			{
				String::new()
			} else {
				arguments
					.into_iter()
					.flat_map(|span| [span.start, span.end])
					.map(|value| value.to_string())
					.collect::<Vec<_>>()
					.join(", ")
			}
		} else {
			String::new()
		};
		let mut injected = injected_origins;
		if !injected.is_empty() {
			injected.push_str(", ");
		}
		injected.push_str(&format!("{origin_start}, {origin_end}"));
		let insertion = if open.span.end == close.span.start {
			injected
		} else {
			format!(", {injected}")
		};
		edits.push(Edit {
			span: (close.span.start - original_start)..(close.span.start - original_start),
			replacement: insertion,
			owner: u32_range(&(origin_start..origin_end), "presentation call origin")?,
		});
	}
	edits.sort_by(|left, right| {
		left.span
			.start
			.cmp(&right.span.start)
			.then_with(|| right.span.end.cmp(&left.span.end))
	});
	let mut builder = PresentationBuilder::default();
	let mut cursor = 0;
	for edit in edits {
		if cursor < edit.span.start {
			builder.copied(&fragment[cursor..edit.span.start], original_start + cursor)?;
		}
		builder.synthetic(
			&edit.replacement,
			&(usize::try_from(edit.owner.start).unwrap_or(0)
				..usize::try_from(edit.owner.end).unwrap_or(0)),
		)?;
		cursor = cursor.max(edit.span.end);
	}
	if cursor < fragment.len() {
		builder.copied(&fragment[cursor..], original_start + cursor)?;
	}
	Ok(RewrittenFragment {
		source: builder.source,
		segments: builder.segments,
	})
}

fn call_argument_ranges(
	fragment: &str,
	original_start: usize,
	tokens: &[LexToken],
	open: usize,
	close: usize,
) -> Result<Vec<Range<usize>>, RuneEnvelopeError> {
	let mut ranges = Vec::new();
	let mut start = tokens[open].span.end;
	let mut parens = 0usize;
	let mut brackets = 0usize;
	let mut braces = 0usize;
	for token in &tokens[open + 1..close] {
		match token.kind {
			LexKind::Punct('(') => parens += 1,
			LexKind::Punct(')') => parens = parens.saturating_sub(1),
			LexKind::Punct('[') => brackets += 1,
			LexKind::Punct(']') => brackets = brackets.saturating_sub(1),
			LexKind::Punct('{') => braces += 1,
			LexKind::Punct('}') => braces = braces.saturating_sub(1),
			LexKind::Punct(',') if parens == 0 && brackets == 0 && braces == 0 => {
				ranges.push(trim_original_range(
					fragment,
					original_start,
					start..token.span.start,
				)?);
				start = token.span.end;
			}
			_ => {}
		}
	}
	if start < tokens[close].span.start {
		ranges.push(trim_original_range(
			fragment,
			original_start,
			start..tokens[close].span.start,
		)?);
	}
	Ok(ranges)
}

fn trim_original_range(
	fragment: &str,
	original_start: usize,
	span: Range<usize>,
) -> Result<Range<usize>, RuneEnvelopeError> {
	let local = span.start.saturating_sub(original_start)..span.end.saturating_sub(original_start);
	let value = fragment
		.get(local.clone())
		.ok_or_else(|| RuneEnvelopeError::Invalid("box argument span is not UTF-8".to_owned()))?;
	let leading = value.len() - value.trim_start().len();
	let trailing = value.len() - value.trim_end().len();
	Ok(span.start + leading..span.end - trailing)
}

fn matching_paren(tokens: &[LexToken], open: usize) -> Option<usize> {
	let mut depth = 0usize;
	for (index, token) in tokens.iter().enumerate().skip(open) {
		match token.kind {
			LexKind::Punct('(') => depth += 1,
			LexKind::Punct(')') => {
				depth = depth.checked_sub(1)?;
				if depth == 0 {
					return Some(index);
				}
			}
			_ => {}
		}
	}
	None
}

fn lex(source: &str, offset: usize) -> Vec<LexToken> {
	let bytes = source.as_bytes();
	let mut tokens = Vec::new();
	let mut index = 0;
	while index < bytes.len() {
		match bytes[index] {
			b'/' if bytes.get(index + 1) == Some(&b'/') => {
				index += 2;
				while index < bytes.len() && bytes[index] != b'\n' {
					index += 1;
				}
			}
			b'/' if bytes.get(index + 1) == Some(&b'*') => {
				index += 2;
				let mut depth = 1usize;
				while index < bytes.len() && depth > 0 {
					if bytes.get(index..index + 2) == Some(b"/*") {
						depth += 1;
						index += 2;
					} else if bytes.get(index..index + 2) == Some(b"*/") {
						depth -= 1;
						index += 2;
					} else {
						index += 1;
					}
				}
			}
			b'"' | b'\'' => {
				let quote = bytes[index];
				index += 1;
				while index < bytes.len() {
					if bytes[index] == b'\\' {
						index = (index + 2).min(bytes.len());
					} else if bytes[index] == quote {
						index += 1;
						break;
					} else {
						index += 1;
					}
				}
			}
			byte if byte == b'_' || byte.is_ascii_alphabetic() => {
				let start = index;
				index += 1;
				while index < bytes.len()
					&& (bytes[index] == b'_'
						|| bytes[index].is_ascii_alphanumeric()
						|| bytes[index] >= 0x80)
				{
					index += 1;
				}
				tokens.push(LexToken {
					kind: LexKind::Ident(source[start..index].to_owned()),
					span: offset + start..offset + index,
				});
			}
			byte if b"().,:[]{}".contains(&byte) => {
				tokens.push(LexToken {
					kind: LexKind::Punct(byte as char),
					span: offset + index..offset + index + 1,
				});
				index += 1;
			}
			_ => index += 1,
		}
	}
	tokens
}

fn collect_definition(
	statement: &ast::Stmt,
	source: &str,
	shadows: &mut ShellShadows,
	definitions: &mut Vec<Definition>,
) -> Result<(), RuneEnvelopeError> {
	match statement {
		ast::Stmt::Local(local) => {
			let statement_span = byte_range(statement.span(), source.len())?;
			let ast::Pat::Path(path) = &local.pat else {
				return Ok(());
			};
			let name_span = byte_range(path.path.span(), source.len())?;
			let name = source
				.get(name_span.clone())
				.ok_or_else(|| RuneEnvelopeError::Invalid("binding name is not UTF-8".into()))?
				.trim()
				.to_owned();
			if !is_simple_name(&name) {
				return Ok(());
			}
			shadows.insert(&name);
			let value = byte_range(local.expr.span(), source.len())?;
			definitions.push(Definition {
				name,
				name_span,
				statement: statement_span,
				literal: is_static_literal_expression(&local.expr, source, definitions),
				value,
			});
		}
		ast::Stmt::Item(ast::Item::Fn(function), _) => {
			let name_span = byte_range(function.name.span(), source.len())?;
			let name = source
				.get(name_span.clone())
				.ok_or_else(|| RuneEnvelopeError::Invalid("function name is not UTF-8".into()))?
				.to_owned();
			shadows.insert(&name);
			definitions.push(Definition {
				name,
				name_span,
				statement: byte_range(statement.span(), source.len())?,
				value: byte_range(statement.span(), source.len())?,
				literal: false,
			});
		}
		_ => {}
	}
	Ok(())
}

fn is_static_literal_expression(
	expression: &ast::Expr,
	source: &str,
	definitions: &[Definition],
) -> bool {
	match expression {
		ast::Expr::Lit(literal) => matches!(literal.lit, ast::Lit::Str(_) | ast::Lit::Number(_)),
		ast::Expr::Unary(unary) => {
			let text = source_slice(source, unary.span())
				.unwrap_or_default()
				.trim();
			text.starts_with('-')
				&& matches!(unary.expr.as_ref(), ast::Expr::Lit(literal) if matches!(literal.lit, ast::Lit::Number(_)))
		}
		ast::Expr::Path(path) => {
			let Ok(name) = source_slice(source, path.span()) else {
				return false;
			};
			resolve_definition(definitions, name.trim(), path.span().start.into_usize())
				.is_some_and(|definition| definition.literal)
		}
		_ => false,
	}
}

fn retained_regions(
	source_len: usize,
	peeled: &[Range<usize>],
) -> Result<Vec<Range<usize>>, RuneEnvelopeError> {
	let mut retained = Vec::new();
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

fn push_segment(
	segments: &mut Vec<SourceMapSegment>,
	transformed: Range<usize>,
	original_start: usize,
) -> Result<(), RuneEnvelopeError> {
	if transformed.is_empty() {
		return Ok(());
	}
	segments.push(SourceMapSegment {
		transformed: u32_range(&transformed, "transformed source")?,
		original_start: u32::try_from(original_start)
			.map_err(|_| RuneEnvelopeError::Invalid("source exceeds u32".into()))?,
	});
	Ok(())
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

fn u32_range(span: &Range<usize>, what: &str) -> Result<Range<u32>, RuneEnvelopeError> {
	Ok(u32::try_from(span.start)
		.map_err(|_| RuneEnvelopeError::Invalid(format!("{what} exceeds u32")))?
		..u32::try_from(span.end)
			.map_err(|_| RuneEnvelopeError::Invalid(format!("{what} exceeds u32")))?)
}

fn decode_string_literal(raw: &str) -> Option<String> {
	let raw = raw.trim();
	if raw.len() < 2 || !raw.starts_with('"') || !raw.ends_with('"') {
		return None;
	}
	let inner = &raw[1..raw.len() - 1];
	if inner.contains('"') || inner.contains('\\') {
		return None;
	}
	Some(inner.to_owned())
}

fn is_simple_name(raw: &str) -> bool {
	let mut chars = raw.chars();
	let Some(first) = chars.next() else {
		return false;
	};
	(first == '_' || first.is_alphabetic())
		&& chars.all(|character| character == '_' || character.is_alphanumeric())
}

fn is_keyword(name: &str) -> bool {
	matches!(
		name,
		"as" | "async"
			| "await" | "break"
			| "const" | "continue"
			| "else" | "enum"
			| "false" | "fn"
			| "for" | "if"
			| "impl" | "in"
			| "let" | "loop"
			| "match" | "mod"
			| "move" | "pub"
			| "return"
			| "self" | "struct"
			| "true" | "use"
			| "while"
	)
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::plot::colors::ColorTheme;
	use crate::plot::continuous_color::{ContinuousAnchor, ContinuousScaleIntent};
	use crate::plot::spec::ColorJob;
	use crate::plot::spec::{BubbleNumericField, BubblePreparationChannel, Override, Presence};
	use crate::plot::spec::{FacetLabelTextSize, LegendTextSize};
	use crate::rune_relation_envelope::{RunePlotBody, RuneViewIntent, SpannedMarkFields};
	use std::sync::Arc;

	#[test]
	fn product_0151_relation_lineage_is_structural_and_root_normalized() {
		let lineage = |source: &str| {
			compile(source)
				.expect("view compiles")
				.executions
				.into_iter()
				.next()
				.expect("execution")
				.lineage
		};
		let direct = lineage("workspace.table( \"a\" ).views([scatter(\"x\", \"y\")])");
		let sorted = lineage(
			"let t = workspace.table(\"a\"); t.sort([asc(col(\"x\"))]).views([scatter(\"x\", \"y\")])",
		);
		let alias = lineage("ket.table(\"a\").views([scatter(\"x\", \"y\")])");
		let other = lineage("workspace.table(\"b\").views([scatter(\"x\", \"y\")])");
		let reformatted = lineage(
			r#"
				workspace
					.table(
						"a",
					)
					.views([
						scatter("x", "y"),
					])
			"#,
		);
		assert_eq!(direct, sorted);
		assert_eq!(direct, alias);
		assert_eq!(direct, reformatted);
		assert_ne!(direct, other);
		assert_eq!(
			direct,
			Some(RelationLineage {
				method: "table".to_owned(),
				arguments: vec![RelationLineageLiteral::String("a".to_owned())],
			})
		);
		assert_eq!(
			lineage("workspace.table(name).views([scatter(\"x\", \"y\")])"),
			None,
			"a non-literal head is deliberately unselectable"
		);

		let two_heads = compile(
			r#"
				let a = workspace.table("a");
				a.views([scatter("x", "y")]);
				let b = workspace.table("b");
				b.views([scatter("x", "y")]);
			"#,
		)
		.expect("two head relations compile in one buffer");
		assert_eq!(two_heads.executions.len(), 2);
		assert_ne!(
			two_heads.executions[0].lineage,
			two_heads.executions[1].lineage
		);
	}

	/// PRODUCT-0137: a terminal `ket` relation with no view root compiles to
	/// an execution carrying zero presentation slots, so it reaches Toron
	/// instead of falling through to `NoTerminalView`. Deliberately spelled
	/// with a generic relation word — the route is structural, and a fixture
	/// naming a relation verb would defeat the guard below.
	#[test]
	fn product_0137_terminal_ket_relation_compiles_with_no_views() {
		let compiled = compile("ket.table(\"fixture\")").expect("terminal ket relation compiles");
		assert_eq!(compiled.executions.len(), 1);
		assert!(
			compiled.executions[0].views.is_empty(),
			"a terminal relation carries zero presentation slots"
		);
		assert!(compiled.presentation_order.is_empty());
	}

	/// PRODUCT-0137: `workspace` roots keep today's behavior. A terminal
	/// `workspace` relation is a local-host program and must still refuse
	/// here so the fallback at `state.rs` sends it to that host.
	#[test]
	fn product_0137_terminal_workspace_relation_is_not_stolen_from_the_local_host() {
		assert_eq!(
			compile("workspace.result(\"sales\")"),
			Err(RuneEnvelopeError::NoTerminalView)
		);
		assert_eq!(
			compile("workspace.cell(\"trips\")"),
			Err(RuneEnvelopeError::NoTerminalView)
		);
		assert_eq!(
			compile("workspace.table(\"t\")"),
			Err(RuneEnvelopeError::NoTerminalView)
		);
	}

	/// PRODUCT-0137/PRODUCT-0003: the terminal route is structural. It reads
	/// the provenance's root and never a method or verb name, and no relation
	/// verb is spelled in the compiler. Asserted against the implementation,
	/// not this file — a textual guard whose own fixtures contain the
	/// excluded spelling fails on itself.
	#[test]
	fn product_0137_terminal_route_is_structural_and_names_no_relation_verb() {
		let source = std::fs::read_to_string(
			std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/rune_view_compiler.rs"),
		)
		.expect("read the compiler");
		let production = source
			.split("\n#[cfg(test)]\nmod tests {")
			.next()
			.expect("bounded production source");
		assert!(
			production.contains("root: RelationRoot::Ket,"),
			"the terminal branch selects on the provenance root"
		);
		assert!(
			!production.contains("materialize"),
			"PRODUCT-0003 fences relation verbs out of the host compiler"
		);
	}

	/// Offline proof that the evidence exhibit's exact materialize source
	/// routes through the terminal `ket` branch. Spelled as the engine spells
	/// it — `project_named([... .named("x")])` — so a spelling error surfaces
	/// here rather than against a live daemon.
	#[test]
	fn product_0137_evidence_exhibit_source_routes_as_a_terminal_relation() {
		let source = concat!(
			"ket.range(50000).project_named([",
			"seeded_random(lit_i64(20260821), col(\"ordinal\")).named(\"x\"), ",
			"seeded_random(lit_i64(20260822), col(\"ordinal\")).named(\"y\")",
			"]).materialize(\"evidence_scatter_50000\")"
		);
		let compiled = compile(source).expect("evidence materialize source compiles");
		assert_eq!(compiled.executions.len(), 1);
		assert!(compiled.executions[0].views.is_empty());
		assert!(
			compiled.executions[0].source.contains("materialize"),
			"the relation program reaches Toron verbatim"
		);
	}

	#[test]
	fn product_0076_rewriter_injects_nested_constructor_and_modifier_origins() {
		let source = "bar(\"x\", \"y\").stacked().percent()";
		let rewritten = rewrite_calls(source, 100).expect("rewrite presentation calls");
		assert_eq!(
			rewritten,
			"polariton_presentation::__bar(\"x\", \"y\", 100, 113).__stacked(113, 123).__percent(123, 133)"
		);
	}

	#[test]
	fn product_0082_box_rewriter_retains_seven_argument_origins() {
		let source = "box(\"cat\", \"lo\", \"p25\", \"mid\", \"p75\", \"hi\", \"out\")";
		let rewritten = rewrite_calls(source, 100).expect("rewrite box call");
		assert_eq!(
			rewritten,
			"polariton_presentation::__box(\"cat\", \"lo\", \"p25\", \"mid\", \"p75\", \"hi\", \"out\", 104, 109, 111, 115, 117, 122, 124, 129, 131, 136, 138, 142, 144, 149, 100, 150)"
		);
	}

	#[test]
	fn product_0092_heatmap_rewriter_retains_three_argument_origins() {
		let source = "heatmap(\"region\", \"quarter\", \"margin\")";
		let rewritten = rewrite_calls(source, 100).expect("rewrite heatmap call");
		assert_eq!(
			rewritten,
			"polariton_presentation::__heatmap(\"region\", \"quarter\", \"margin\", 108, 116, 118, 127, 129, 137, 100, 138)"
		);
	}

	#[test]
	fn product_0144_hexbin_rewriter_retains_two_argument_origins() {
		let source = "hexbin(\"x\", \"y\")";
		let rewritten = rewrite_calls(source, 100).expect("rewrite hexbin call");
		assert_eq!(
			rewritten,
			"polariton_presentation::__hexbin(\"x\", \"y\", 107, 110, 112, 115, 100, 116)"
		);
	}

	#[test]
	fn product_0146_fill_between_rewriter_retains_three_or_four_argument_origins() {
		for (source, expected) in [
			(
				"fill_between(\"x\", \"low\", \"high\")",
				"polariton_presentation::__fill_between(\"x\", \"low\", \"high\", 113, 116, 118, 123, 125, 131, 100, 132)",
			),
			(
				"fill_between(\"x\", \"low\", \"high\", \"mid\")",
				"polariton_presentation::__fill_between(\"x\", \"low\", \"high\", \"mid\", 113, 116, 118, 123, 125, 131, 133, 138, 100, 139)",
			),
		] {
			assert_eq!(
				rewrite_calls(source, 100).expect("rewrite fill-between call"),
				expected
			);
		}
	}

	#[test]
	fn product_0148_candlestick_rewriter_retains_five_argument_origins() {
		let source = "candlestick(\"x\", \"o\", \"h\", \"l\", \"c\")";
		let rewritten = rewrite_calls(source, 100).expect("rewrite candlestick call");
		assert_eq!(
			rewritten,
			"polariton_presentation::__candlestick(\"x\", \"o\", \"h\", \"l\", \"c\", 112, 115, 117, 120, 122, 125, 127, 130, 132, 135, 100, 136)"
		);
	}

	#[test]
	fn product_0105_variadic_multivariate_words_retain_origins_and_single_routing() {
		let rewritten =
			rewrite_calls("radar(\"a\", \"b\", \"c\")", 100).expect("rewrite variadic radar call");
		assert_eq!(
			rewritten,
			"polariton_presentation::__radar(\"a\", \"b\", \"c\", 106, 109, 111, 114, 116, 119, 100, 120)"
		);

		for (word, dimensions) in [("radar", 3_usize), ("parallel", 2_usize)] {
			let arguments = if word == "radar" {
				"\"third\", \"first\", \"second\""
			} else {
				"\"third\", \"first\""
			};
			let source = format!(
				"let r = workspace.table(\"multivariate_probe\"); r.views([{word}({arguments}).category(\"group\")]);"
			);
			let compiled = compile(&source).unwrap_or_else(|error| panic!("{word}: {error}"));
			let RuneViewIntent::Plot(plot) = &compiled.executions[0].views[0].intent else {
				panic!("multivariate word produced a table")
			};
			let RunePlotBody::Single(sink) = &plot.body else {
				panic!("category(...) incorrectly routed {word} through LayerSettings")
			};
			let (plot_type, fields, origins) = sink
				.mark_fields
				.as_ref()
				.and_then(|fields| fields.multivariate())
				.expect("multivariate fields and origins");
			assert!(plot_type.to_string().to_lowercase().contains(word));
			assert_eq!(fields.dimensions().len(), dimensions);
			for (field, span) in fields.dimensions().iter().zip(&origins.dimensions) {
				assert_eq!(
					&source[span.start as usize..span.end as usize],
					format!("\"{field}\"")
				);
			}
			let category = origins.category.as_ref().expect("category modifier origin");
			assert_eq!(
				&source[category.start as usize..category.end as usize],
				".category(\"group\")"
			);
		}

		let duplicate = "let r = workspace.table(\"multivariate_probe\"); r.views([radar(\"a\", \"a\", \"b\")]);";
		let error = compile(duplicate).expect_err("duplicate dimension must refuse");
		let RuneEnvelopeError::InvalidView { span, message } = error else {
			panic!("duplicate dimension returned wrong error: {error}")
		};
		assert_eq!(&duplicate[span.start as usize..span.end as usize], "\"a\"");
		assert!(message.contains("dimension `a` is duplicated"), "{message}");

		for (source, expected_span, expected_message) in [
			(
				"let r = workspace.table(\"multivariate_probe\"); r.views([radar(\"a\", \"b\", \"c\").category(\"group\").category(\"other\")]);",
				".category(\"other\")",
				"category(...) may appear at most once",
			),
			(
				"let r = workspace.table(\"multivariate_probe\"); r.views([pie(\"group\", \"a\").category(\"other\")]);",
				".category(\"other\")",
				"category(...) is available on radar and parallel marks",
			),
		] {
			let error = compile(source).expect_err("invalid category modifier must refuse");
			let RuneEnvelopeError::InvalidView { span, message } = error else {
				panic!("category modifier returned wrong error: {error}")
			};
			assert_eq!(
				&source[span.start as usize..span.end as usize],
				expected_span
			);
			assert!(message.contains(expected_message), "{message}");
		}
	}

	#[test]
	fn product_0095_bubble_rewriter_retains_four_or_five_argument_origins() {
		for (source, expected) in [
			(
				"bubble(\"x\", \"y\", \"size\", \"color\")",
				"polariton_presentation::__bubble(\"x\", \"y\", \"size\", \"color\", 107, 110, 112, 115, 117, 123, 125, 132, 100, 133)",
			),
			(
				"bubble(\"x\", \"y\", \"size\", \"color\", \"label\")",
				"polariton_presentation::__bubble(\"x\", \"y\", \"size\", \"color\", \"label\", 107, 110, 112, 115, 117, 123, 125, 132, 134, 141, 100, 142)",
			),
		] {
			assert_eq!(
				rewrite_calls(source, 100).expect("rewrite bubble call"),
				expected
			);
		}
	}

	#[test]
	fn product_0098_funnel_rewriter_retains_two_argument_origins() {
		let source = "funnel(\"stage\", \"value\")";
		let rewritten = rewrite_calls(source, 100).expect("rewrite funnel call");
		assert_eq!(
			rewritten,
			"polariton_presentation::__funnel(\"stage\", \"value\", 107, 114, 116, 123, 100, 124)"
		);
	}

	#[test]
	fn product_0091_continuous_words_rewrite_and_retain_modifier_intent_origin() {
		let source = r#"let relation = workspace.table("heat_probe");
relation.views([figure([
    sequential_palette("magma"), diverging_palette("green_purples"),
    heatmap("region", "quarter", "margin").diverging_at(25.0)
])]);"#;
		let compiled = compile(source).expect("continuous colour surface compiles");
		let RuneViewIntent::Plot(plot) = &compiled.executions[0].views[0].intent else {
			panic!("continuous colour figure produced a table")
		};
		assert_eq!(
			plot.authored_settings.sequential_ramp,
			Override::Set(ColorTheme::Magma)
		);
		assert_eq!(
			plot.authored_settings.diverging_ramp,
			Override::Set(ColorTheme::GreenPurples)
		);
		assert_eq!(
			plot.continuous_intent,
			Some(ContinuousScaleIntent {
				job: ColorJob::Diverging,
				anchor: ContinuousAnchor::Authored(25.0),
			})
		);
		let span = plot
			.continuous_intent_span
			.clone()
			.expect("modifier origin");
		assert_eq!(
			&source[span.start as usize..span.end as usize],
			".diverging_at(25.0)"
		);
	}

	#[test]
	fn product_0091_modifier_provenance_does_not_change_figure_identity() {
		let compact = r#"workspace.table("t").views([heatmap("x", "y", "v").diverging_at(0.0)])"#;
		let spaced = r#"workspace.table("t").views([
			heatmap("x", "y", "v")
				.diverging_at(0.0)
		])"#;
		let left = compile(compact).expect("compact figure compiles");
		let right = compile(spaced).expect("spaced figure compiles");
		assert_eq!(
			left.executions[0].views[0].intent, right.executions[0].views[0].intent,
			"modifier provenance is diagnostic metadata, not figure identity"
		);
		let RuneViewIntent::Plot(left) = &left.executions[0].views[0].intent else {
			panic!("compact fixture produced a table")
		};
		let RuneViewIntent::Plot(right) = &right.executions[0].views[0].intent else {
			panic!("spaced fixture produced a table")
		};
		assert_ne!(left.continuous_intent_span, right.continuous_intent_span);
	}

	#[test]
	fn product_0091_continuous_surface_refuses_unknowns_and_second_spellings_precisely() {
		for (source, expected, message) in [
			(
				r#"workspace.table("heat_probe").views([figure([diverging_palette("viridis"), diverging_palette("magma"), heatmap("x", "y", "v").diverging()])])"#,
				"diverging_palette(\"magma\")",
				"at most one diverging_palette",
			),
			(
				r#"workspace.table("heat_probe").views([figure([sequential_palette("magma"), sequential_palette("inferno"), heatmap("x", "y", "v")])])"#,
				"sequential_palette(\"inferno\")",
				"at most one sequential_palette",
			),
			(
				r#"workspace.table("heat_probe").views([heatmap("x", "y", "v").diverging().diverging_at(0.0)])"#,
				".diverging_at(0.0)",
				"one modifier family",
			),
			(
				r#"workspace.table("t").views([bar("x", "y").diverging()])"#,
				".diverging()",
				"available on heatmap and bubble",
			),
			(
				r#"workspace.table("heat_probe").views([figure([sequential_palette("not_a_ramp"), heatmap("x", "y", "v")])])"#,
				"sequential_palette(\"not_a_ramp\")",
				"unknown ramp token `not_a_ramp`",
			),
		] {
			let error = compile(source).expect_err("invalid continuous spelling refuses");
			let RuneEnvelopeError::InvalidView {
				span,
				message: actual,
			} = error
			else {
				panic!("continuous refusal retains its authored origin")
			};
			assert!(actual.contains(message), "{actual}");
			assert_eq!(&source[span.start as usize..span.end as usize], expected);
		}
		let coexist = r#"workspace.table("heat_probe").views([figure([
			palette("tab20c"), sequential_palette("blue_white_red"),
			diverging_palette("viridis"), heatmap("x", "y", "v").diverging()
		])])"#;
		let compiled = compile(coexist).expect("three independent palette concepts coexist");
		let RuneViewIntent::Plot(plot) = &compiled.executions[0].views[0].intent else {
			panic!("coexistence fixture produced a table")
		};
		assert_eq!(
			plot.authored_settings.sequential_ramp,
			Override::Set(ColorTheme::BlueWhiteRed)
		);
		assert_eq!(
			plot.authored_settings.diverging_ramp,
			Override::Set(ColorTheme::Viridis)
		);
		assert_eq!(
			plot.continuous_intent.map(|intent| intent.job),
			Some(ColorJob::Diverging),
			"the token does not infer the job"
		);
	}

	#[test]
	fn product_0095_bubble_surface_positions_arities_and_origins_are_closed() {
		for fields in [
			"\"x\", \"y\", \"size\", \"color\"",
			"\"x\", \"y\", \"size\", \"color\", \"label\"",
		] {
			for slot in [
				format!("bubble({fields})"),
				format!("view(\"named\", bubble({fields}))"),
				format!("view(\"named\", figure([bubble({fields})]))"),
				format!("figure([bubble({fields})])"),
			] {
				let source = format!(
					"let relation = workspace.table(\"bubble_probe\");\nrelation.views([{slot}]);"
				);
				let compiled = compile(&source).unwrap_or_else(|error| panic!("{slot}: {error}"));
				let RuneViewIntent::Plot(plot) = &compiled.executions[0].views[0].intent else {
					panic!("bubble surface produced a table")
				};
				let sink = match &plot.body {
					RunePlotBody::Single(sink) => sink,
					RunePlotBody::StyledSingle(styled) => &styled.sink,
					RunePlotBody::Layered(_) => panic!("bubble surface produced layers"),
				};
				assert_eq!(
					sink.composition.primary_plot_type(),
					crate::plot::core::PlotType::Bubble
				);
				let (fields, origins) = sink
					.mark_fields
					.as_ref()
					.and_then(|fields| fields.bubble())
					.expect("bubble fields and origins");
				assert_eq!(fields.numeric.get(BubbleNumericField::Color), "color");
				assert_eq!(
					fields.label.as_deref(),
					fields.label.as_ref().map(|_| "label")
				);
				for role in [
					BubblePreparationChannel::X,
					BubblePreparationChannel::Y,
					BubblePreparationChannel::Size,
					BubblePreparationChannel::Color,
				] {
					let origin = origins.for_role(role).expect("required bubble origin");
					assert_eq!(
						&source[origin.start as usize..origin.end as usize],
						format!("\"{}\"", role.as_str())
					);
				}
				assert_eq!(
					origins
						.for_role(BubblePreparationChannel::Label)
						.map(|origin| &source[origin.start as usize..origin.end as usize]),
					fields.label.as_ref().map(|_| "\"label\"")
				);
			}
		}

		let fields = "\"x\", \"y\", \"size\", \"color\"";
		for source in [
			format!(
				"let r = workspace.table(\"bubble_probe\"); r.views([layers([bubble({fields})])]);"
			),
			format!("plot.bubble(workspace.table(\"bubble_probe\"), {fields});"),
			format!("workspace.table(\"bubble_probe\").bubble({fields});"),
		] {
			assert!(
				compile(&source).is_err(),
				"accepted forbidden surface: {source}"
			);
		}
		for source in [
			"let r = workspace.table(\"bubble_probe\"); r.views([bubble(\"x\", \"y\", \"size\")]);",
			"let r = workspace.table(\"bubble_probe\"); r.views([bubble(\"x\", \"y\", \"size\", \"color\", \"label\", \"extra\")]);",
		] {
			let error = compile(source).expect_err("invalid bubble arity must refuse");
			assert!(
				error.to_string().contains(
					"bubble(...) takes x, y, size, and color strings, with an optional label string"
				),
				"wrong invalid-arity diagnostic for {source}: {error}"
			);
		}
	}

	#[test]
	fn product_0098_funnel_surface_positions_arity_and_origins_are_closed() {
		let fields = "\"stage\", \"value\"";
		for slot in [
			format!("funnel({fields})"),
			format!("view(\"named\", funnel({fields}))"),
			format!("view(\"named\", figure([funnel({fields})]))"),
			format!("figure([funnel({fields})])"),
		] {
			let source = format!(
				"let relation = workspace.table(\"funnel_probe\");\nrelation.views([{slot}]);"
			);
			let compiled = compile(&source).unwrap_or_else(|error| panic!("{slot}: {error}"));
			let RuneViewIntent::Plot(plot) = &compiled.executions[0].views[0].intent else {
				panic!("funnel surface produced a table")
			};
			let sink = match &plot.body {
				RunePlotBody::Single(sink) => sink,
				RunePlotBody::StyledSingle(styled) => &styled.sink,
				RunePlotBody::Layered(_) => panic!("funnel surface produced layers"),
			};
			assert_eq!(
				sink.composition.primary_plot_type(),
				crate::plot::core::PlotType::Funnel
			);
			let (fields, origins) = sink
				.mark_fields
				.as_ref()
				.and_then(|fields| fields.funnel())
				.expect("funnel fields and origins");
			assert_eq!(
				(fields.stage.as_str(), fields.value.as_str()),
				("stage", "value")
			);
			assert_eq!(
				&source[origins.stage.start as usize..origins.stage.end as usize],
				"\"stage\""
			);
			assert_eq!(
				&source[origins.value.start as usize..origins.value.end as usize],
				"\"value\""
			);
		}

		for source in [
			format!(
				"let r = workspace.table(\"funnel_probe\"); r.views([layers([funnel({fields})])]);"
			),
			format!("plot.funnel(workspace.table(\"funnel_probe\"), {fields});"),
			format!("workspace.table(\"funnel_probe\").funnel({fields});"),
		] {
			assert!(
				compile(&source).is_err(),
				"accepted forbidden surface: {source}"
			);
		}

		let error =
			compile("let r = workspace.table(\"funnel_probe\"); r.views([funnel(\"value\")]);")
				.expect_err("one-argument funnel must refuse");
		assert!(
			error
				.to_string()
				.contains("funnel(...) takes stage and value strings"),
			"{error}"
		);
	}

	#[test]
	fn product_0100_radial_dial_surface_positions_arities_and_origins_are_closed() {
		for (fields, expected_label) in [
			("\"value\", \"maximum\"", None),
			("\"value\", \"maximum\", \"label\"", Some("label")),
		] {
			for slot in [
				format!("radial_dial({fields})"),
				format!("view(\"named\", radial_dial({fields}))"),
				format!("view(\"named\", figure([radial_dial({fields})]))"),
				format!("figure([radial_dial({fields})])"),
			] {
				let source = format!(
					"let relation = workspace.table(\"radial_dial_probe\");\nrelation.views([{slot}]);"
				);
				let compiled = compile(&source).unwrap_or_else(|error| panic!("{slot}: {error}"));
				let RuneViewIntent::Plot(plot) = &compiled.executions[0].views[0].intent else {
					panic!("radial-dial surface produced a table")
				};
				let sink = match &plot.body {
					RunePlotBody::Single(sink) => sink,
					RunePlotBody::StyledSingle(styled) => &styled.sink,
					RunePlotBody::Layered(_) => panic!("radial-dial surface produced layers"),
				};
				assert_eq!(
					sink.composition.primary_plot_type(),
					crate::plot::core::PlotType::RadialDial
				);
				let (actual, origins) = sink
					.mark_fields
					.as_ref()
					.and_then(|fields| fields.radial_dial())
					.expect("radial-dial fields and origins");
				assert_eq!(
					(
						actual.value.as_str(),
						actual.maximum.as_str(),
						actual.label.as_deref()
					),
					("value", "maximum", expected_label)
				);
				assert_eq!(
					&source[origins.value.start as usize..origins.value.end as usize],
					"\"value\""
				);
				assert_eq!(
					&source[origins.maximum.start as usize..origins.maximum.end as usize],
					"\"maximum\""
				);
				assert_eq!(
					origins
						.label
						.as_ref()
						.map(|span| &source[span.start as usize..span.end as usize]),
					expected_label.map(|_| "\"label\"")
				);
			}
		}

		let fields = "\"value\", \"maximum\"";
		for source in [
			format!(
				"let r = workspace.table(\"radial_dial_probe\"); r.views([layers([radial_dial({fields})])]);"
			),
			format!("plot.radial_dial(workspace.table(\"radial_dial_probe\"), {fields});"),
			format!("workspace.table(\"radial_dial_probe\").radial_dial({fields});"),
		] {
			assert!(
				compile(&source).is_err(),
				"accepted forbidden surface: {source}"
			);
		}
		for source in [
			"let r = workspace.table(\"radial_dial_probe\"); r.views([radial_dial(\"value\")]);",
			"let r = workspace.table(\"radial_dial_probe\"); r.views([radial_dial(\"value\", \"maximum\", \"label\", \"extra\")]);",
		] {
			let error = compile(source).expect_err("invalid radial-dial arity must refuse");
			assert!(
				error.to_string().contains(
					"radial_dial(...) takes value and maximum strings with an optional trailing label string"
				),
				"{error}"
			);
		}
	}

	#[test]
	fn product_0092_heatmap_surface_positions_are_closed_and_originated() {
		let fields = "\"region\", \"quarter\", \"margin\"";
		for slot in [
			format!("heatmap({fields})"),
			format!("view(\"named\", heatmap({fields}))"),
			format!("view(\"named\", figure([heatmap({fields})]))"),
			format!("figure([heatmap({fields})])"),
		] {
			let source = format!(
				"let relation = workspace.table(\"heat_probe\");\nrelation.views([{slot}]);"
			);
			let compiled = compile(&source).unwrap_or_else(|error| panic!("{slot}: {error}"));
			let RuneViewIntent::Plot(plot) = &compiled.executions[0].views[0].intent else {
				panic!("heatmap surface produced a table")
			};
			let sink = match &plot.body {
				RunePlotBody::Single(sink) => sink,
				RunePlotBody::StyledSingle(styled) => &styled.sink,
				RunePlotBody::Layered(_) => panic!("heatmap surface produced layers"),
			};
			assert_eq!(
				sink.composition.primary_plot_type(),
				crate::plot::core::PlotType::Heatmap
			);
			let (fields, origins) = sink
				.mark_fields
				.as_ref()
				.and_then(|fields| fields.heatmap())
				.expect("heatmap fields and origins");
			assert_eq!(fields.value, "margin");
			assert_eq!(
				&source[origins.value.start as usize..origins.value.end as usize],
				"\"margin\""
			);
		}

		for source in [
			format!(
				"let r = workspace.table(\"heat_probe\"); r.views([layers([heatmap({fields})])]);"
			),
			format!("plot.heatmap(workspace.table(\"heat_probe\"), {fields});"),
			format!("workspace.table(\"heat_probe\").heatmap({fields});"),
		] {
			assert!(
				compile(&source).is_err(),
				"accepted forbidden surface: {source}"
			);
		}
	}

	#[test]
	fn product_0144_hexbin_surface_is_closed_originated_and_honestly_labelled() {
		let fields = "\"x\", \"y\"";
		for slot in [
			format!("hexbin({fields})"),
			format!("view(\"named\", hexbin({fields}))"),
			format!("view(\"named\", figure([hexbin({fields})]))"),
			format!("figure([hexbin({fields})])"),
		] {
			let source = format!(
				"let relation = workspace.table(\"hexbin_probe\");\nrelation.views([{slot}]);"
			);
			let compiled = compile(&source).unwrap_or_else(|error| panic!("{slot}: {error}"));
			let RuneViewIntent::Plot(plot) = &compiled.executions[0].views[0].intent else {
				panic!("hexbin surface produced a table")
			};
			let RunePlotBody::Single(sink) = &plot.body else {
				panic!("an unmodified hexbin must use the live Single path")
			};
			assert_eq!(
				sink.composition.primary_plot_type(),
				crate::plot::core::PlotType::Hexbin
			);
			let (fields, origins) = sink
				.mark_fields
				.as_ref()
				.and_then(|fields| fields.hexbin())
				.expect("hexbin fields and origins");
			assert_eq!((fields.x.as_str(), fields.y.as_str()), ("x", "y"));
			assert_eq!(
				&source[origins.x.start as usize..origins.x.end as usize],
				"\"x\""
			);
			assert_eq!(
				&source[origins.y.start as usize..origins.y.end as usize],
				"\"y\""
			);
			let presentation = compiled.executions[0].views[0].intent.presentation();
			assert_eq!(presentation.display_label, "plot.hexbin");
			assert_eq!(sink.composition.title(), "Hexbin Plot");
		}

		let layered = format!(
			"let r = workspace.table(\"hexbin_probe\"); r.views([layers([hexbin({fields}), line(\"x\", \"y\")])]);"
		);
		let error = compile(&layered).expect_err("hexbin must remain outside LayerMark");
		assert!(
			error
				.to_string()
				.contains("hexbin(...) is a view sink, not a shared-Cartesian layer"),
			"{error}"
		);

		let sugar = format!("workspace.table(\"hexbin_probe\").hexbin({fields})");
		let error = compile(&sugar).expect_err("hexbin relation sugar is closed");
		assert!(
			error
				.to_string()
				.contains("hexbin is a sink inside .views([...]"),
			"{error}"
		);

		for source in [
			"workspace.table(\"hexbin_probe\").views([hexbin(\"x\")])",
			"workspace.table(\"hexbin_probe\").views([hexbin(\"x\", \"y\", \"z\")])",
			"workspace.table(\"hexbin_probe\").views([hexbin(1, \"y\")])",
		] {
			let error = compile(source).expect_err("malformed hexbin must refuse");
			let RuneEnvelopeError::InvalidView { span, .. } = &error else {
				panic!("malformed hexbin must retain an authored call span: {error}")
			};
			assert!(
				source[span.start as usize..span.end as usize].starts_with("hexbin("),
				"refusal did not land on the hexbin call: {error}"
			);
			assert!(
				error
					.to_string()
					.contains("hexbin(...) takes x and y strings")
					|| error
						.to_string()
						.contains("field arguments must evaluate to strings"),
				"{error}"
			);
		}

		let diverging =
			"workspace.table(\"hexbin_probe\").views([hexbin(\"x\", \"y\").diverging()])";
		let error = compile(diverging).expect_err("hexbin has no colour job word");
		assert!(
			error
				.to_string()
				.contains("diverging colour is available on heatmap and bubble marks"),
			"{error}"
		);

		let bounded = compile(
			"workspace.table(\"hexbin_probe\").views([figure([x_axis().bounds(0, 1), y_axis().bounds(0, 1), hexbin(\"x\", \"y\")])])",
		)
		.expect("hexbin accepts both quantitative axis bounds");
		let RuneViewIntent::Plot(plot) = &bounded.executions[0].views[0].intent else {
			panic!("bounded hexbin produced a table")
		};
		assert!(matches!(
			&plot.authored_settings.domains,
			Override::Set(domain)
				if domain.x_min == Some(0.0)
					&& domain.x_max == Some(1.0)
					&& domain.y_min == Some(0.0)
					&& domain.y_max == Some(1.0)
		));
	}

	#[test]
	fn product_0146_fill_between_surface_is_variable_originated_and_closed() {
		for fields in [
			"\"x\", \"low\", \"high\"",
			"\"x\", \"low\", \"high\", \"mid\"",
		] {
			let source = format!(
				"let relation = workspace.table(\"fill_between_probe\"); relation.views([fill_between({fields})]);"
			);
			let compiled = compile(&source).unwrap_or_else(|error| panic!("{source}: {error}"));
			let RuneViewIntent::Plot(plot) = &compiled.executions[0].views[0].intent else {
				panic!("fill-between surface produced a table")
			};
			let RunePlotBody::Single(sink) = &plot.body else {
				panic!("unmodified fill-between must use the live Single path")
			};
			assert_eq!(
				sink.composition.primary_plot_type(),
				crate::plot::core::PlotType::FillBetween
			);
			let (fields, origins) = sink
				.mark_fields
				.as_ref()
				.and_then(SpannedMarkFields::fill_between)
				.expect("fill-between fields and origins");
			assert_eq!(fields.x, "x");
			assert_eq!(fields.lower, "low");
			assert_eq!(fields.upper, "high");
			assert_eq!(fields.middle.is_some(), origins.middle.is_some());
			for (field, origin) in [
				("\"x\"", &origins.x),
				("\"low\"", &origins.lower),
				("\"high\"", &origins.upper),
			] {
				assert_eq!(&source[origin.start as usize..origin.end as usize], field);
			}
			if let Some(origin) = &origins.middle {
				assert_eq!(
					&source[origin.start as usize..origin.end as usize],
					"\"mid\""
				);
			}
			let presentation = compiled.executions[0].views[0].intent.presentation();
			assert_eq!(presentation.display_label, "plot.fill_between");
			assert_eq!(sink.composition.title(), "Fill Between Plot");
		}

		let layered = "let r = workspace.table(\"fill_between_probe\"); r.views([layers([fill_between(\"x\", \"low\", \"high\"), line(\"x\", \"low\")])]);";
		let error = compile(layered).expect_err("fill-between remains outside LayerMark");
		assert!(
			error
				.to_string()
				.contains("fill_between(...) is a view sink, not a shared-Cartesian layer"),
			"{error}"
		);

		let sugar =
			"workspace.table(\"fill_between_probe\").fill_between(\"x\", \"low\", \"high\")";
		let error = compile(sugar).expect_err("fill-between relation sugar is closed");
		assert!(
			error
				.to_string()
				.contains("fill_between is a sink inside .views([...]"),
			"{error}"
		);

		for source in [
			"workspace.table(\"t\").views([fill_between(\"x\", \"low\")])",
			"workspace.table(\"t\").views([fill_between(\"x\", \"low\", \"high\", \"mid\", \"extra\")])",
			"workspace.table(\"t\").views([fill_between(1, \"low\", \"high\")])",
		] {
			let error = compile(source).expect_err("malformed fill-between must refuse");
			assert!(
				error.to_string().contains(
					"fill_between(...) takes x, lower, upper, and optional middle strings"
				) || error
					.to_string()
					.contains("field arguments must evaluate to strings"),
				"{error}"
			);
		}

		let diverging =
			"workspace.table(\"t\").views([fill_between(\"x\", \"low\", \"high\").diverging()])";
		let error = compile(diverging).expect_err("fill-between has no colour job word");
		assert!(
			error
				.to_string()
				.contains("available on heatmap and bubble"),
			"{error}"
		);
	}

	#[test]
	fn product_0148_candlestick_surface_is_fixed_originated_and_closed() {
		let source = "let relation = workspace.table(\"ohlc\"); relation.views([candlestick(\"stamp\", \"open\", \"high\", \"low\", \"close\")]);";
		let compiled = compile(source).expect("candlestick surface compiles");
		let RuneViewIntent::Plot(plot) = &compiled.executions[0].views[0].intent else {
			panic!("candlestick surface produced a table")
		};
		let RunePlotBody::Single(sink) = &plot.body else {
			panic!("unmodified candlestick must use the live Single path")
		};
		assert_eq!(
			sink.composition.primary_plot_type(),
			crate::plot::core::PlotType::Candlestick
		);
		let (fields, origins) = sink
			.mark_fields
			.as_ref()
			.and_then(SpannedMarkFields::candlestick)
			.expect("candlestick fields and origins");
		for (field, expected, origin) in [
			(&fields.x, "stamp", &origins.x),
			(&fields.open, "open", &origins.open),
			(&fields.high, "high", &origins.high),
			(&fields.low, "low", &origins.low),
			(&fields.close, "close", &origins.close),
		] {
			assert_eq!(field, expected);
			assert_eq!(
				&source[origin.start as usize..origin.end as usize],
				format!("\"{expected}\"")
			);
		}
		assert_eq!(
			compiled.executions[0].views[0]
				.intent
				.presentation()
				.display_label,
			"plot.candlestick"
		);

		let layered = "let r = workspace.table(\"ohlc\"); r.views([layers([candlestick(\"stamp\", \"open\", \"high\", \"low\", \"close\"), line(\"stamp\", \"close\")])]);";
		let error = compile(layered).expect_err("candlestick remains outside LayerMark");
		assert!(
			error
				.to_string()
				.contains("candlestick(...) is a view sink, not a shared-Cartesian layer"),
			"{error}"
		);
		let sugar = "workspace.table(\"ohlc\").candlestick(\"stamp\", \"open\", \"high\", \"low\", \"close\")";
		let error = compile(sugar).expect_err("candlestick relation sugar is closed");
		assert!(
			error
				.to_string()
				.contains("candlestick is a sink inside .views([...]"),
			"{error}"
		);
		for malformed in [
			"workspace.table(\"t\").views([candlestick(\"x\", \"o\", \"h\", \"l\")])",
			"workspace.table(\"t\").views([candlestick(\"x\", \"o\", \"h\", \"l\", \"c\", \"v\")])",
			"workspace.table(\"t\").views([candlestick(1, \"o\", \"h\", \"l\", \"c\")])",
		] {
			let error = compile(malformed).expect_err("malformed candlestick refuses");
			assert!(
				error
					.to_string()
					.contains("candlestick(...) takes x, open, high, low, and close strings")
					|| error
						.to_string()
						.contains("field arguments must evaluate to strings"),
				"{error}"
			);
		}
		let diverging = "workspace.table(\"t\").views([candlestick(\"x\", \"o\", \"h\", \"l\", \"c\").diverging()])";
		let error = compile(diverging).expect_err("candlestick has no colour job word");
		assert!(
			error
				.to_string()
				.contains("diverging colour is available on heatmap and bubble marks"),
			"{error}"
		);
	}

	#[test]
	fn product_0082_box_surface_positions_are_closed_and_originated() {
		let fields = "\"cat\", \"lo\", \"p25\", \"mid\", \"p75\", \"hi\", \"out\"";
		for slot in [
			format!("box({fields})"),
			format!("view(\"named\", box({fields}))"),
			format!("view(\"named\", figure([box({fields})]))"),
			format!("figure([box({fields})])"),
		] {
			let source = format!(
				"let relation = workspace.table(\"box_probe\");\nrelation.views([{slot}]);"
			);
			let compiled = compile(&source).unwrap_or_else(|error| panic!("{slot}: {error}"));
			let RuneViewIntent::Plot(plot) = &compiled.executions[0].views[0].intent else {
				panic!("box surface produced a table")
			};
			let sink = match &plot.body {
				RunePlotBody::Single(sink) => sink,
				RunePlotBody::StyledSingle(styled) => &styled.sink,
				RunePlotBody::Layered(_) => panic!("box surface produced layers"),
			};
			assert_eq!(
				sink.composition.primary_plot_type(),
				crate::plot::core::PlotType::BoxPlot
			);
			let (fields, origins) = sink
				.mark_fields
				.as_ref()
				.and_then(|fields| fields.box_plot())
				.expect("box fields and origins");
			assert_eq!(fields.q1, "p25");
			assert_eq!(
				&source[origins.q1.start as usize..origins.q1.end as usize],
				"\"p25\""
			);
		}

		for source in [
			format!("let r = workspace.table(\"box_probe\"); r.views([layers([box({fields})])]);"),
			format!("plot.box(workspace.table(\"box_probe\"), {fields});"),
			format!("workspace.table(\"box_probe\").box({fields});"),
			format!(
				"let r = workspace.table(\"box_probe\"); r.views([box({fields}).bar_width(0.8)]);"
			),
			format!(
				"let r = workspace.table(\"box_probe\"); r.views([box({fields}).category_gap(0.1)]);"
			),
		] {
			assert!(
				compile(&source).is_err(),
				"accepted forbidden surface: {source}"
			);
		}
	}

	#[test]
	fn product_0085_0082_0083_test26_is_the_real_box_and_axis_title_exhibit() {
		let source = include_str!("../../../uat/test26.rn");
		let compiled = compile(source).expect("the checked-in box exhibit compiles");
		assert_eq!(compiled.executions.len(), 2);
		assert_eq!(compiled.presentation_order.len(), 3);
		assert_eq!(compiled.executions[0].views.len(), 2);
		for view in &compiled.executions[0].views {
			let RuneViewIntent::Plot(plot) = &view.intent else {
				panic!("box exhibit produced a table")
			};
			let RunePlotBody::Single(sink) = &plot.body else {
				panic!("box exhibit did not retain one standalone mark")
			};
			assert_eq!(
				sink.composition.primary_plot_type(),
				crate::plot::core::PlotType::BoxPlot
			);
			assert_eq!(
				plot.authored_settings.y_label,
				Presence::Show(Arc::new("Observed value".to_owned()))
			);
		}
		let RuneViewIntent::Plot(percent) = &compiled.executions[1].views[0].intent else {
			panic!("percent title exhibit produced a table")
		};
		assert_eq!(percent.authored_settings.y_label, Presence::Inherit);
		let RunePlotBody::StyledSingle(percent) = &percent.body else {
			panic!("percent title exhibit did not retain its styled bar")
		};
		assert!(percent.stacked && percent.percent);
		assert!(source.contains(".bounds(0, 40)"));
		assert!(source.contains(".bounds(-75, 225)"));
		for case in ["twin", "lowhigh", "none", "flat", "single"] {
			assert!(source.contains(case), "exhibit omits {case}");
		}
		assert!(source.contains("y_axis().label(\"Observed value\")"));
		assert!(source.contains("Axis title · derived percent-of-total"));
	}

	#[test]
	fn product_0092_test27_is_the_sparse_and_duplicate_heatmap_exhibit() {
		let source = include_str!("../../../uat/test27.rn");
		let compiled = compile(source).expect("the checked-in heatmap exhibit compiles");
		assert_eq!(compiled.executions.len(), 2);
		assert_eq!(compiled.presentation_order.len(), 2);
		for execution in &compiled.executions {
			let RuneViewIntent::Plot(plot) = &execution.views[0].intent else {
				panic!("heatmap exhibit produced a table")
			};
			let RunePlotBody::Single(sink) = &plot.body else {
				panic!("heatmap exhibit did not retain one standalone mark")
			};
			assert_eq!(
				sink.composition.primary_plot_type(),
				crate::plot::core::PlotType::Heatmap
			);
			let (fields, _) = sink
				.mark_fields
				.as_ref()
				.and_then(|fields| fields.heatmap())
				.expect("heatmap fields and origins");
			assert_eq!(fields.value, "margin");
		}
		assert!(source.contains("South/Q2 is absent"));
		assert!(source.contains("aggregate in the relation"));
	}

	#[test]
	fn product_0095_test28_is_the_labelled_and_unlabelled_bubble_exhibit() {
		let source = include_str!("../../../uat/test28.rn");
		let compiled = compile(source).expect("the checked-in bubble exhibit compiles");
		assert_eq!(compiled.executions.len(), 1);
		assert_eq!(compiled.presentation_order.len(), 2);
		assert_eq!(compiled.executions[0].views.len(), 2);
		for (index, view) in compiled.executions[0].views.iter().enumerate() {
			let RuneViewIntent::Plot(plot) = &view.intent else {
				panic!("bubble exhibit produced a table")
			};
			let RunePlotBody::Single(sink) = &plot.body else {
				panic!("bubble exhibit did not retain one standalone mark")
			};
			assert_eq!(
				sink.composition.primary_plot_type(),
				crate::plot::core::PlotType::Bubble
			);
			let (fields, _) = sink
				.mark_fields
				.as_ref()
				.and_then(|fields| fields.bubble())
				.expect("bubble fields and origins");
			assert_eq!(fields.label.as_deref(), (index == 0).then_some("label"));
		}
		assert!(source.contains("Signed colour −30…40"));
		assert!(source.contains("null label stays absent in hover"));
		assert!(source.contains("Point 0, Point 1, …"));
	}

	#[test]
	fn product_0098_test30_is_the_valid_and_refusing_funnel_exhibit() {
		let source = include_str!("../../../uat/test30.rn");
		let compiled = compile(source).expect("the checked-in funnel exhibit compiles");
		assert_eq!(compiled.executions.len(), 2);
		assert_eq!(compiled.presentation_order.len(), 3);
		for execution in &compiled.executions {
			for view in &execution.views {
				let RuneViewIntent::Plot(plot) = &view.intent else {
					panic!("funnel exhibit produced a table")
				};
				let RunePlotBody::Single(sink) = &plot.body else {
					panic!("funnel exhibit did not retain one standalone mark")
				};
				assert_eq!(
					sink.composition.primary_plot_type(),
					crate::plot::core::PlotType::Funnel
				);
				assert!(
					sink.mark_fields
						.as_ref()
						.and_then(|fields| fields.funnel())
						.is_some()
				);
			}
		}
		assert!(source.contains("equal-valued plateau"));
		assert!(source.contains("must refuse"));
	}

	#[test]
	fn product_0100_test31_is_the_grouped_and_ungrouped_radial_dial_exhibit() {
		let source = include_str!("../../../uat/test31.rn");
		let compiled = compile(source).expect("the checked-in radial-dial exhibit compiles");
		assert_eq!(compiled.executions.len(), 2);
		assert_eq!(compiled.presentation_order.len(), 2);
		let labels = compiled
			.executions
			.iter()
			.flat_map(|execution| execution.views.iter())
			.map(|view| {
				let RuneViewIntent::Plot(plot) = &view.intent else {
					panic!("radial-dial exhibit produced a table")
				};
				let RunePlotBody::Single(sink) = &plot.body else {
					panic!("radial-dial exhibit did not retain one standalone mark")
				};
				let (fields, _) = sink
					.mark_fields
					.as_ref()
					.and_then(|fields| fields.radial_dial())
					.expect("radial-dial exhibit fields");
				assert_eq!(
					sink.composition.primary_plot_type(),
					crate::plot::core::PlotType::RadialDial
				);
				fields.label.as_deref()
			})
			.collect::<Vec<_>>();
		assert_eq!(labels, [Some("label"), None]);
		assert!(source.contains("rings render lexically"));
		assert!(source.contains("presentation names it All Data"));
	}

	#[test]
	fn product_0091_test29_separates_ramp_slots_jobs_and_anchors() {
		let source = include_str!("../../../uat/test29.rn");
		let compiled = compile(source).expect("the continuous colour exhibit compiles");
		assert_eq!(compiled.executions.len(), 2);
		assert_eq!(compiled.presentation_order.len(), 3);
		let intents = compiled
			.executions
			.iter()
			.flat_map(|execution| execution.views.iter())
			.map(|view| match &view.intent {
				RuneViewIntent::Plot(plot) => plot,
				RuneViewIntent::Table(_) => panic!("continuous exhibit produced a table"),
			})
			.collect::<Vec<_>>();
		assert_eq!(intents[0].continuous_intent, None);
		for intent in &intents {
			assert!(
				matches!(intent.body, RunePlotBody::Single(_)),
				"continuous colour intent is mark semantics, not standalone Layer settings"
			);
		}
		assert_eq!(
			intents[0].authored_settings.sequential_ramp,
			Override::Set(ColorTheme::Magma)
		);
		assert_eq!(
			intents[1].continuous_intent,
			Some(ContinuousScaleIntent {
				job: ColorJob::Diverging,
				anchor: ContinuousAnchor::DefaultZero,
			})
		);
		assert_eq!(
			intents[2].continuous_intent,
			Some(ContinuousScaleIntent {
				job: ColorJob::Diverging,
				anchor: ContinuousAnchor::Authored(15.0),
			})
		);
		assert!(!source.contains("status_palette"));
	}

	#[test]
	fn product_0085_axis_labels_execute_helpers_preserve_text_and_refuse_precisely() {
		let helper = r#"
fn axis() { y_axis().label("Observed value") }
workspace.table("t").views([figure([
	x_axis().label(" Sales region "), axis(), box("category", "lo", "q1", "mid", "q3", "hi", "outlier"),
])])
"#;
		let compiled = compile(helper).expect("helper-produced axis titles compile");
		let RuneViewIntent::Plot(plot) = &compiled.executions[0].views[0].intent else {
			panic!("axis-title fixture produced a table")
		};
		assert_eq!(
			plot.authored_settings.x_label,
			Presence::Show(Arc::new(" Sales region ".to_owned()))
		);
		assert_eq!(
			plot.authored_settings.y_label,
			Presence::Show(Arc::new("Observed value".to_owned()))
		);

		for authored in ["", "   "] {
			let source = format!(
				"workspace.table(\"t\").views([figure([y_axis().label(\"{authored}\"), bar(\"x\", \"y\")])])"
			);
			let error = compile(&source).expect_err("blank axis title refuses");
			let RuneEnvelopeError::InvalidView { span, message } = error else {
				panic!("blank label refusal retains its origin")
			};
			assert_eq!(
				&source[span.start as usize..span.end as usize],
				format!(".label(\"{authored}\")")
			);
			assert!(message.contains("nonempty string"), "{message}");
		}

		let duplicate = "workspace.table(\"t\").views([figure([y_axis().label(\"first\"), y_axis().label(\"second\"), bar(\"x\", \"y\")])])";
		let error = compile(duplicate).expect_err("duplicate y labels refuse");
		let RuneEnvelopeError::InvalidView { span, message } = error else {
			panic!("duplicate refusal retains its origin")
		};
		assert_eq!(
			&duplicate[span.start as usize..span.end as usize],
			".label(\"second\")"
		);
		assert!(
			message.contains("y_axis().label(...) may appear at most once"),
			"{message}"
		);
	}

	#[test]
	fn product_0112_hover_size_is_a_generated_text_word_with_precise_refusals() {
		let source = r#"workspace.table("t").views([figure([hover_size(20), bar("x", "y")])])"#;
		for compiled in [
			compile(source).expect("hover_size figure compiles"),
			compile(source).expect("re-executing the source reproduces hover_size"),
		] {
			let RuneViewIntent::Plot(plot) = &compiled.executions[0].views[0].intent else {
				panic!("hover fixture produced a table")
			};
			assert_eq!(
				plot.authored_settings.hover_size,
				crate::plot::spec::Override::Set(
					crate::plot::spec::HoverTextSize::new(20.0).unwrap()
				)
			);
		}

		for accepted in ["6", "96"] {
			let source = format!(
				"workspace.table(\"t\").views([figure([hover_size({accepted}), bar(\"x\", \"y\")])])"
			);
			compile(&source).expect("inclusive hover-size endpoint compiles");
		}

		for rejected in ["5.9", "96.1", "\"large\""] {
			let source = format!(
				"workspace.table(\"t\").views([figure([hover_size({rejected}), bar(\"x\", \"y\")])])"
			);
			let error = compile(&source).expect_err("invalid hover size refuses");
			let RuneEnvelopeError::InvalidView { span, message } = error else {
				panic!("hover-size refusal retains its origin")
			};
			assert_eq!(
				&source[span.start as usize..span.end as usize],
				format!("hover_size({rejected})")
			);
			assert!(message.contains("hover size") || message.contains("requires a number"));
		}
	}

	#[test]
	fn product_0115_x_tick_rotation_is_a_typed_generated_text_word() {
		let source = r#"workspace.table("t").views([figure([
x_axis().tick_rotation(-45),
bar("x", "y"),
])])"#;
		let compiled = compile(source).expect("x-tick rotation compiles");
		let RuneViewIntent::Plot(plot) = &compiled.executions[0].views[0].intent else {
			panic!("rotation fixture produced a table")
		};
		assert_eq!(
			plot.authored_settings.x_tick_rotation,
			crate::plot::spec::Override::Set(crate::plot::spec::TickRotation::new(-45.0).unwrap())
		);

		for accepted in ["-90", "0", "90"] {
			compile(&format!(
				"workspace.table(\"t\").views([figure([x_axis().tick_rotation({accepted}), bar(\"x\", \"y\")])])"
			))
			.expect("inclusive degree endpoint compiles");
		}
		for rejected in ["-90.1", "90.1", "\"diagonal\""] {
			let source = format!(
				"workspace.table(\"t\").views([figure([x_axis().tick_rotation({rejected}), bar(\"x\", \"y\")])])"
			);
			let error = compile(&source).expect_err("invalid rotation refuses");
			let RuneEnvelopeError::InvalidView { span, message } = error else {
				panic!("rotation refusal retains its word origin")
			};
			assert_eq!(
				&source[span.start as usize..span.end as usize],
				format!(".tick_rotation({rejected})")
			);
			assert!(message.contains("x-tick rotation") || message.contains("requires a number"));
		}
		let legacy = compile(
			r#"workspace.table("t").views([figure([x_label_rotation(45), bar("x", "y")])])"#,
		)
		.expect_err("historical internal name is not public Rune vocabulary");
		let RuneEnvelopeError::InvalidView { message, .. } = legacy else {
			panic!("unknown legacy spelling remains an invalid view")
		};
		assert!(
			message.contains("Missing item x_label_rotation"),
			"{message}"
		);
	}

	#[test]
	fn product_0120_padding_edges_are_typed_partial_members_with_precise_refusals() {
		let source = r#"workspace.table("t").views([figure([
padding().right(120).bottom(24),
bar("x", "y"),
])])"#;
		let compiled = compile(source).expect("padding edges compile");
		let RuneViewIntent::Plot(plot) = &compiled.executions[0].views[0].intent else {
			panic!("padding fixture produced a table")
		};
		let pixels = |value| crate::plot::spec::PlotPaddingPixels::new(value).unwrap();
		assert_eq!(
			plot.authored_settings.plot_padding_right,
			crate::plot::spec::Override::Set(pixels(120.0))
		);
		assert_eq!(
			plot.authored_settings.plot_padding_bottom,
			crate::plot::spec::Override::Set(pixels(24.0))
		);
		assert!(matches!(
			plot.authored_settings.plot_padding_top,
			crate::plot::spec::Override::Inherit
		));
		assert!(matches!(
			plot.authored_settings.plot_padding_left,
			crate::plot::spec::Override::Inherit
		));

		for accepted in ["0", "100000"] {
			compile(&format!(
				"workspace.table(\"t\").views([figure([padding().right({accepted}), bar(\"x\", \"y\")])])"
			))
			.expect("finite non-negative padding accepts");
		}
		for rejected in ["-1", "\"wide\""] {
			let source = format!(
				"workspace.table(\"t\").views([figure([padding().right({rejected}), bar(\"x\", \"y\")])])"
			);
			let error = compile(&source).expect_err("invalid padding refuses");
			let RuneEnvelopeError::InvalidView { span, .. } = error else {
				panic!("padding refusal retains its modifier origin")
			};
			assert_eq!(
				&source[span.start as usize..span.end as usize],
				format!(".right({rejected})")
			);
		}
		assert!(
			crate::plot::spec::PlotPaddingPixels::new(f32::NAN).is_err()
				&& crate::plot::spec::PlotPaddingPixels::new(f32::INFINITY).is_err(),
			"the typed carrier rejects non-finite values before they reach geometry"
		);

		for source in [
			r#"workspace.table("t").views([figure([padding().right(1).right(2), bar("x", "y")])])"#,
			r#"workspace.table("t").views([figure([title("T").right(2), bar("x", "y")])])"#,
			r#"workspace.table("t").views([figure([skin("dark").right(2), bar("x", "y")])])"#,
		] {
			let error = compile(source).expect_err("padding-only modifiers refuse elsewhere");
			let RuneEnvelopeError::InvalidView { span, .. } = error else {
				panic!("padding modifier preserves its own span")
			};
			let refused = &source[span.start as usize..span.end as usize];
			assert!(
				refused == ".right(2)" || refused == ".right(1)",
				"the refusal points at the modifier, not its receiver: {refused}"
			);
		}
	}

	#[test]
	fn product_0113_title_and_subtitle_font_size_are_member_modifiers() {
		let source = r#"workspace.table("t").views([figure([
title("T").font_size(48),
subtitle("S").font_size(36),
bar("x", "y"),
])])"#;
		let compiled = compile(source).expect("title and subtitle font sizes compile");
		let RuneViewIntent::Plot(plot) = &compiled.executions[0].views[0].intent else {
			panic!("font-size fixture produced a table")
		};
		assert_eq!(
			plot.authored_settings.title_size,
			crate::plot::spec::Override::Set(crate::plot::spec::TitleTextSize::new(48.0).unwrap())
		);
		assert_eq!(
			plot.authored_settings.subtitle_size,
			crate::plot::spec::Override::Set(
				crate::plot::spec::SubtitleTextSize::new(36.0).unwrap()
			)
		);

		for accepted in ["6", "96"] {
			let source = format!(
				"workspace.table(\"t\").views([figure([title(\"T\").font_size({accepted}), bar(\"x\", \"y\")])])"
			);
			compile(&source).expect("inclusive title font-size endpoint compiles");
		}
		for rejected in ["5.9", "96.1", "\"large\""] {
			let source = format!(
				"workspace.table(\"t\").views([figure([subtitle(\"S\").font_size({rejected}), bar(\"x\", \"y\")])])"
			);
			let error = compile(&source).expect_err("invalid subtitle font size refuses");
			let RuneEnvelopeError::InvalidView { span, .. } = error else {
				panic!("font-size refusal retains its modifier origin")
			};
			assert_eq!(
				&source[span.start as usize..span.end as usize],
				format!(".font_size({rejected})")
			);
		}

		for (source, expected) in [
			(
				r#"workspace.table("t").views([figure([skin("greys_rs2").font_size(20), bar("x", "y")])])"#,
				".font_size(20)",
			),
			(
				r#"workspace.table("t").views([figure([title("T").font_size(20).font_size(24), bar("x", "y")])])"#,
				".font_size(24)",
			),
		] {
			let error = compile(source).expect_err("invalid font-size form refuses");
			let RuneEnvelopeError::InvalidView { span, .. } = error else {
				panic!("font-size refusal retains a presentation span")
			};
			assert_eq!(&source[span.start as usize..span.end as usize], expected);
		}
	}

	#[test]
	fn product_0123_axis_members_preserve_axis_authority_and_compose() {
		let source = r#"workspace.table("t").views([figure([
x_axis().label_size(22),
y_axis().label_size(20),
title().font_size(24),
subtitle().font_size(16),
x_axis().tick_size(14),
y_axis().tick_size(16),
bar("x", "y"),
])])"#;
		let compiled = compile(source).expect("axis and tick typography compiles");
		let RuneViewIntent::Plot(plot) = &compiled.executions[0].views[0].intent else {
			panic!("axis typography fixture produced a table")
		};
		use crate::plot::spec::{
			Override, Presence, SubtitleTextSize, TitleTextSize, XLabelTextSize, XTickTextSize,
			YLabelTextSize, YTickTextSize,
		};
		assert!(matches!(plot.authored_settings.title, Presence::Inherit));
		assert!(matches!(plot.authored_settings.subtitle, Presence::Inherit));
		assert!(matches!(plot.authored_settings.x_label, Presence::Inherit));
		assert!(matches!(plot.authored_settings.y_label, Presence::Inherit));
		assert_eq!(
			plot.authored_settings.title_size,
			Override::Set(TitleTextSize::new(24.0).unwrap())
		);
		assert_eq!(
			plot.authored_settings.subtitle_size,
			Override::Set(SubtitleTextSize::new(16.0).unwrap())
		);
		assert_eq!(
			plot.authored_settings.x_label_size,
			Override::Set(XLabelTextSize::new(22.0).unwrap())
		);
		assert_eq!(
			plot.authored_settings.y_label_size,
			Override::Set(YLabelTextSize::new(20.0).unwrap())
		);
		assert_eq!(
			plot.authored_settings.x_tick_size,
			Override::Set(XTickTextSize::new(14.0).unwrap())
		);
		assert_eq!(
			plot.authored_settings.y_tick_size,
			Override::Set(YTickTextSize::new(16.0).unwrap())
		);

		let authored = compile(
			r#"workspace.table("t").views([figure([y_axis().label("Revenue").label_size(20), bar("x", "y")])])"#,
		)
		.expect("authored label and size compile");
		let RuneViewIntent::Plot(authored) = &authored.executions[0].views[0].intent else {
			panic!("authored label fixture produced a table")
		};
		assert!(
			matches!(&authored.authored_settings.y_label, Presence::Show(value) if value.as_ref() == "Revenue")
		);
		assert_eq!(
			authored.authored_settings.y_label_size,
			Override::Set(YLabelTextSize::new(20.0).unwrap())
		);

		for spelling in ["x_axis().tick_size", "y_axis().tick_size"] {
			for accepted in ["6", "96"] {
				compile(&format!(
					"workspace.table(\"t\").views([figure([{spelling}({accepted}), bar(\"x\", \"y\")])])"
				))
				.expect("inclusive endpoint compiles");
			}
			for rejected in ["5.9", "96.1", "\"large\""] {
				let source = format!(
					"workspace.table(\"t\").views([figure([{spelling}({rejected}), bar(\"x\", \"y\")])])"
				);
				let RuneEnvelopeError::InvalidView { span, .. } =
					compile(&source).expect_err("invalid tick size refuses")
				else {
					panic!("invalid tick size produces a presentation error")
				};
				assert_eq!(
					&source[span.start as usize..span.end as usize],
					format!(".tick_size({rejected})")
				);
			}
		}

		for source in [
			r#"workspace.table("t").views([figure([x_axis(), bar("x", "y")])])"#,
			r#"workspace.table("t").views([figure([y_axis(), bar("x", "y")])])"#,
		] {
			let RuneEnvelopeError::InvalidView { span, .. } =
				compile(source).expect_err("bare no-effect member refuses")
			else {
				panic!("bare member produces a presentation error")
			};
			let refused = &source[span.start as usize..span.end as usize];
			assert!(
				refused.ends_with("()"),
				"the member call owns the refusal: {refused}"
			);
		}

		let separate = compile(
			r#"workspace.table("t").views([figure([x_axis().bounds(0, 10), x_axis().tick_step(2), line("x", "y")])])"#,
		)
		.expect("disjoint axis members compose");
		let chained = compile(
			r#"workspace.table("t").views([figure([x_axis().bounds(0, 10).tick_step(2), line("x", "y")])])"#,
		)
		.expect("chained axis members compile");
		assert_eq!(
			separate.executions[0].views[0].intent,
			chained.executions[0].views[0].intent
		);

		let duplicate = r#"workspace.table("t").views([figure([x_axis().bounds(0, 10), x_axis().bounds(2, 8), line("x", "y")])])"#;
		let RuneEnvelopeError::InvalidView { span, .. } =
			compile(duplicate).expect_err("duplicate axis property refuses")
		else {
			panic!("duplicate axis property retains a presentation span")
		};
		assert_eq!(
			&duplicate[span.start as usize..span.end as usize],
			".bounds(2, 8)"
		);

		for retired in [
			"x_label",
			"y_label",
			"x_bounds",
			"y_bounds",
			"x_tick_step",
			"y_tick_step",
			"x_minor_ticks",
			"y_minor_ticks",
			"x_tick_rotation",
			"x_tick_size",
			"y_tick_size",
		] {
			let source = format!(
				"workspace.table(\"t\").views([figure([{retired}(1), bar(\"x\", \"y\")])])"
			);
			let error = compile(&source).expect_err("retired axis word is unknown");
			assert!(error.to_string().contains(retired), "{error}");
		}

		for source in [
			r#"workspace.table("t").views([figure([skin("greys_rs2").bounds(0, 1), bar("x", "y")])])"#,
			r#"workspace.table("t").views([figure([x_axis().gutter(4), bar("x", "y")])])"#,
			r#"workspace.table("t").views([figure([y_axis().tick_rotation(45), bar("x", "y")])])"#,
		] {
			let RuneEnvelopeError::InvalidView { span, .. } =
				compile(source).expect_err("wrong axis modifier receiver refuses")
			else {
				panic!("wrong axis modifier retains a presentation span")
			};
			assert!(source[span.start as usize..span.end as usize].starts_with('.'));
		}
	}

	#[test]
	fn product_0076_lexer_does_not_rewrite_strings_or_comments() {
		let source = "// bar(\"x\", \"y\")\nlet text = \"y_axis().bounds(0, 1)\"; title(text)";
		let rewritten = rewrite_calls(source, 0).expect("rewrite live constructor only");
		assert!(rewritten.contains("// bar(\"x\", \"y\")"));
		assert!(rewritten.contains("\"y_axis().bounds(0, 1)\""));
		assert!(rewritten.contains("polariton_presentation::__title(text"));
	}

	#[test]
	fn product_0076_compiler_executes_computed_presentation_with_static_topology() {
		let source = r#"
let yb = y_axis().bounds(0, 160);
let category = "region";
let value = "revenue";
let standard = || figure([
	title("Computed"),
	yb,
	bar(category, value).bar_width(0.88),
]);
ket.table("demo_sales").views([
	view("computed", standard()),
]);
"#;
		let compiled = compile(source).expect("computed presentation compiles");
		assert_eq!(compiled.executions.len(), 1);
		assert_eq!(
			compiled.presentation_order,
			vec![LocalViewIdentity::Named("computed".to_owned())]
		);
		let relation = &compiled.executions[0];
		assert!(relation.source.contains("ket.table(\"demo_sales\")"));
		assert!(!relation.source.contains("y_bounds"));
		assert!(!relation.source.contains("standard"));
		let RuneViewIntent::Plot(plot) = &relation.views[0].intent else {
			panic!("computed figure is a plot");
		};
		assert_eq!(
			plot.authored_settings.title,
			Presence::Show(Arc::new("Computed".to_owned()))
		);
		assert!(matches!(
			&plot.authored_settings.domains,
			Override::Set(domain)
				if domain.y_min == Some(0.0) && domain.y_max == Some(160.0)
		));
	}

	#[test]
	fn product_0076_test21_computed_bounds_compile_as_twelve_static_views() {
		let source = include_str!("../../../uat/test21.rn");
		let compiled = compile(source).expect("test21 executable presentation compiles");
		assert_eq!(compiled.presentation_order.len(), 12);
		assert_eq!(compiled.executions.len(), 1);
		assert_eq!(compiled.executions[0].views.len(), 12);
		assert!(compiled.executions[0].source.contains(".sort(["));
		assert!(!compiled.executions[0].source.contains("y_bounds"));
	}

	#[test]
	fn product_0079_histogram_is_one_static_three_field_mark_with_empty_register() {
		let source = r#"
workspace.table("bins").views([
    histogram("bin_start", "bin_end", "count"),
])
"#;
		let compiled = compile(source).expect("histogram presentation compiles");
		assert_eq!(compiled.executions.len(), 1);
		let RuneViewIntent::Plot(plot) = &compiled.executions[0].views[0].intent else {
			panic!("histogram is a plot");
		};
		let RunePlotBody::Single(sink) = &plot.body else {
			panic!("unmodified histogram is a single mark");
		};
		assert_eq!(
			sink.composition.primary_plot_type(),
			crate::plot::core::PlotType::Histogram
		);
		assert_eq!(sink.x, "bin_start");
		assert_eq!(sink.interval_end.as_deref(), Some("bin_end"));
		assert_eq!(sink.y, "count");
		assert_eq!(sink.category, None);

		for modifier in ["bar_width(0.8)", "category_gap(0.1)"] {
			let invalid = format!(
				"workspace.table(\"bins\").views([histogram(\"bin_start\", \"bin_end\", \"count\").{modifier}])"
			);
			let error = compile(&invalid).expect_err("histogram register is empty");
			let RuneEnvelopeError::InvalidView { span, message } = error else {
				panic!("modifier refusal is attributed");
			};
			assert_eq!(
				&invalid[span.start as usize..span.end as usize],
				format!(".{modifier}")
			);
			assert!(message.contains("available on bar marks"), "{message}");
		}

		let exhibit = compile(include_str!("../../../uat/test25.rn"))
			.expect("the versioned histogram exhibit compiles through the production path");
		assert_eq!(exhibit.executions.len(), 2);
		assert_eq!(exhibit.presentation_order.len(), 2);
		assert!(
			exhibit
				.executions
				.iter()
				.all(|execution| execution.source.contains("FLOOR(revenue / 10.0)"))
		);
	}

	#[test]
	fn product_0076_helper_failures_keep_the_helper_body_origin() {
		let source = r#"
fn bad() { y_axis().bounds(9, 2) }
fn middle() { bad() }
fn outer() { middle() }
workspace.table("t").views([figure([outer(), line("x", "y")])])
"#;
		let error = compile(source).expect_err("reversed helper bounds refuse");
		let RuneEnvelopeError::InvalidView { span, message } = error else {
			panic!("expected attributed helper refusal");
		};
		assert_eq!(
			&source[span.start as usize..span.end as usize],
			".bounds(9, 2)"
		);
		assert!(message.contains("cannot reverse an axis"), "{message}");
	}

	#[test]
	fn product_0076_partition_separates_vocabularies_and_keeps_literal_dual_use() {
		let source = r#"
let category = "region";
let yb = y_axis().bounds(0, 10);
let r = workspace.table("t").sort([col(category).asc()]);
r.views([figure([yb, bar(category, "revenue")])])
"#;
		let analysis = analyze_static(source).expect("static shell");
		let partition = partition(source, &analysis).expect("partition");
		let execution = &analysis.executions[0];
		let relation = render_relation(
			source,
			execution,
			&analysis,
			&partition.presentation,
			&partition.relation,
		)
		.expect("relation program");
		let presentation = render_presentation(
			source,
			&ordered_slots(&analysis),
			&analysis,
			&partition.presentation,
		)
		.expect("presentation program");
		assert!(relation.source.contains("col(category)"));
		assert!(!relation.source.contains("y_bounds"));
		assert!(!relation.source.contains("bar(category"));
		assert!(presentation.source.contains("__y_axis"));
		assert!(presentation.source.contains(".__bounds"));
		assert!(presentation.source.contains("__bar(category"));
		assert!(!presentation.source.contains("workspace.table"));
		assert!(
			relation.source.contains("let category = \"region\"")
				&& presentation.source.contains("let category = \"region\"")
		);
	}

	#[test]
	fn product_0076_computed_dual_use_and_mixed_helpers_refuse_before_execution() {
		let dual = r#"
fn category() { "region" }
let r = workspace.table("t").sort([col(category()).asc()]);
r.views([bar(category(), "revenue")])
"#;
		let error = compile(dual).expect_err("computed dual use refuses");
		assert!(
			error
				.to_string()
				.contains("only static literal and alias closures")
		);

		let mixed = r#"
fn impossible() { [col("region"), y_axis().bounds(0, 10)] }
workspace.table("t").views([figure([impossible(), bar("region", "revenue")])])
"#;
		let error = compile(mixed).expect_err("mixed helper refuses");
		assert!(
			error
				.to_string()
				.contains("both relation and presentation vocabulary")
		);
	}

	#[test]
	fn product_0076_unclaimed_effects_stay_relation_side_but_presentation_does_not_leak() {
		let source = r#"
observe("kept");
let unused = y_axis().bounds(0, 10);
workspace.table("t").views([bar("region", "revenue")])
"#;
		let analysis = analyze_static(source).expect("static shell");
		let partition = partition(source, &analysis).expect("partition");
		let relation = render_relation(
			source,
			&analysis.executions[0],
			&analysis,
			&partition.presentation,
			&partition.relation,
		)
		.expect("relation program");
		assert!(relation.source.contains("observe(\"kept\")"));
		assert!(!relation.source.contains("y_bounds"));
	}

	#[test]
	fn product_0076_topology_remains_literal_and_names_remain_static() {
		for source in [
			"fn build() { [bar(\"x\", \"y\")] } workspace.table(\"t\").views(build())",
			"let name = \"dynamic\"; workspace.table(\"t\").views([view(name, bar(\"x\", \"y\"))])",
			"workspace.table(\"t\").views(if true { [bar(\"x\", \"y\")] } else { [] })",
			"workspace.table(\"t\").views({ let out = []; for n in [1] { out.push(bar(\"x\", \"y\")); } out })",
			"workspace.table(\"t\").views([view(if true { \"a\" } else { \"b\" }, bar(\"x\", \"y\"))])",
		] {
			assert!(
				matches!(compile(source), Err(RuneEnvelopeError::InvalidView { .. })),
				"runtime topology must refuse: {source}"
			);
		}
	}

	#[test]
	fn product_0076_helpers_cover_the_complete_presentation_vocabulary() {
		let helper = r#"
fn pass(value) { value }
fn twice(value) { pass(value) }
fn furniture() { [
	twice(title("Title")), twice(subtitle("Subtitle")),
	twice(x_axis().label("Horizontal")), twice(y_axis().label("Vertical")), twice(facet("region")),
	twice(palette("viridis")), twice(skin("nord").duotone()),
	twice(legend().at(0.2, 0.8)), twice(x_axis().bounds(0, 10)), twice(y_axis().bounds(0, 20)),
	twice(x_axis().tick_step(2)), twice(y_axis().tick_step(4)),
	twice(x_axis().minor_ticks(2)), twice(y_axis().minor_ticks(3)),
	twice(bar("region", "revenue", "product")
		.bar_width(0.8).category_gap(0.1).stacked().percent()),
] }
let r = workspace.table("t");
r.views([
	figure(furniture()),
	twice(table().skin("nord").duotone().show_types()),
	twice(layers([
		twice(line("x", "y").stroke_width(2)),
		twice(scatter("x", "y").point_size(1.5)),
	])),
	twice(area("x", "y", "product")),
])
"#;
		let inline = r#"
let r = workspace.table("t");
r.views([
	figure([
		title("Title"), subtitle("Subtitle"), x_axis().label("Horizontal"), y_axis().label("Vertical"),
		facet("region"), palette("viridis"),
		skin("nord").duotone(), legend().at(0.2, 0.8), x_axis().bounds(0, 10), y_axis().bounds(0, 20),
		x_axis().tick_step(2), y_axis().tick_step(4), x_axis().minor_ticks(2), y_axis().minor_ticks(3),
		bar("region", "revenue", "product").bar_width(0.8).category_gap(0.1).stacked().percent(),
	]),
	table().skin("nord").duotone().show_types(),
	layers([line("x", "y").stroke_width(2), scatter("x", "y").point_size(1.5)]),
	area("x", "y", "product"),
])
"#;
		let helper = compile(helper).expect("two-deep helpers execute every presentation member");
		let inline = compile(inline).expect("inline control compiles");
		let helper_intents = helper.executions[0]
			.views
			.iter()
			.map(|view| &view.intent)
			.collect::<Vec<_>>();
		let inline_intents = inline.executions[0]
			.views
			.iter()
			.map(|view| &view.intent)
			.collect::<Vec<_>>();
		assert_eq!(helper_intents, inline_intents);
	}

	#[test]
	fn product_0141_source_model_retains_arguments_shape_and_container_facts() {
		let source = r#"let angle = 45;
workspace.table("t").views([figure([
    x_axis().label("Revenue").tick_rotation(angle),
    bar("region", "revenue")
])])"#;
		let compiled = compile(source).expect("presentation source model compiles");
		let label = compiled
			.presentation_source_model
			.occurrences
			.iter()
			.find(|occurrence| {
				occurrence.key == crate::presentation_registry::SettingKey::XAxisLabel
			})
			.expect("x label occurrence");
		assert_eq!(
			&source[label.argument_spans[0].start as usize..label.argument_spans[0].end as usize],
			"\"Revenue\""
		);
		assert_eq!(
			label.expression_shape,
			crate::presentation_registry::ExpressionShape::DirectLiteral
		);
		let rotation = compiled
			.presentation_source_model
			.occurrences
			.iter()
			.find(|occurrence| {
				occurrence.key == crate::presentation_registry::SettingKey::XAxisTickRotation
			})
			.expect("x rotation occurrence");
		assert_eq!(
			rotation.expression_shape,
			crate::presentation_registry::ExpressionShape::ExpressionOrBinding
		);
		let figure = compiled
			.presentation_source_model
			.syntax
			.iter()
			.find(|boundary| boundary.handler == HandlerId::Figure)
			.expect("figure boundary");
		assert!(matches!(figure.container, Some((ContainerKind::Figure, _))));
		assert_eq!(figure.separators.len(), 1, "figure child-list separator");
		let slot = &compiled.presentation_source_model.view_slots[0];
		assert_eq!(slot.owner, LocalViewIdentity::Anonymous(0));
		let slot_text =
			&source[slot.expression_span.start as usize..slot.expression_span.end as usize];
		assert!(slot_text.starts_with("figure(["));
		assert!(slot_text.ends_with("])"));
	}

	#[test]
	fn product_0141_expression_shape_requires_one_complete_literal() {
		assert!(is_direct_literal(r#""Revenue""#));
		assert!(is_direct_literal("-45.0"));
		assert!(is_direct_literal("0xff"));
		assert!(!is_direct_literal(r#""Rev" + "enue""#));
		assert!(!is_direct_literal("NaN"));
		assert!(!is_direct_literal("angle"));
	}

	#[test]
	fn product_0141_shared_helper_occurrence_has_plural_owners() {
		let source = r#"let shared_axis = || x_axis().label("Shared axis");
let r = workspace.table("t");
r.views([
    view("first", figure([shared_axis(), bar("x", "y")])),
    view("second", figure([shared_axis(), line("x", "y")]))
])"#;
		let compiled = compile(source).expect("shared helper compiles");
		let occurrence = compiled
			.presentation_source_model
			.occurrences
			.iter()
			.find(|occurrence| {
				occurrence.key == crate::presentation_registry::SettingKey::XAxisLabel
			})
			.expect("shared x label occurrence");
		assert_eq!(occurrence.owners.len(), 2);
		assert!(
			occurrence
				.owners
				.contains(&LocalViewIdentity::Named("first".to_owned()))
		);
		assert!(
			occurrence
				.owners
				.contains(&LocalViewIdentity::Named("second".to_owned()))
		);
	}

	#[test]
	fn product_0076_partition_tracks_shadowed_occurrences_by_position() {
		let source = r#"
let value = "first";
let before = y_axis().bounds(0, 1);
let value = "region";
let r = workspace.table("t").sort([col(value).asc()]);
let value = "revenue";
r.views([figure([before, bar("region", value)])])
"#;
		let analysis = analyze_static(source).expect("static shell");
		let partition = partition(source, &analysis).expect("position-keyed partition");
		let relation = render_relation(
			source,
			&analysis.executions[0],
			&analysis,
			&partition.presentation,
			&partition.relation,
		)
		.expect("relation program");
		let presentation = render_presentation(
			source,
			&ordered_slots(&analysis),
			&analysis,
			&partition.presentation,
		)
		.expect("presentation program");
		assert!(relation.source.contains("let value = \"region\""));
		assert!(!relation.source.contains("let value = \"revenue\""));
		assert!(presentation.source.contains("let value = \"revenue\""));
		assert!(!presentation.source.contains("let value = \"region\""));
		assert!(
			relation.source.contains("let value = \"first\""),
			"an unclaimed legacy statement remains relation-side"
		);
		assert!(!presentation.source.contains("let value = \"first\""));
	}

	#[test]
	fn product_0076_context_divergence_executes_locally_but_never_dual_replays() {
		let local = r#"
fn category() { let probe = std::char::from_i64(114); "region" }
workspace.table("t").views([bar(category(), "revenue")])
"#;
		compile(local).expect("the presentation context supplies std::char");

		let dual = r#"
fn category() { let probe = std::char::from_i64(114); "region" }
let r = workspace.table("t").sort([col(category()).asc()]);
r.views([bar(category(), "revenue")])
"#;
		let error = compile(dual).expect_err("a computed helper cannot replay in both VMs");
		assert!(
			error
				.to_string()
				.contains("only static literal and alias closures")
		);
	}

	#[test]
	fn product_0076_compile_diagnostics_map_copied_text_to_the_exact_argument() {
		let source = "workspace.table(\"t\").views([bar(missing, \"y\")])";
		let RuneEnvelopeError::InvalidView { span, message } =
			compile(source).expect_err("undefined field binding refuses")
		else {
			panic!("expected attributed compile refusal");
		};
		assert_eq!(&source[span.start as usize..span.end as usize], "missing");
		assert!(message.contains("No local variable `missing`"), "{message}");
	}

	#[test]
	fn product_0103_pie_surface_positions_arity_and_origins_are_closed() {
		let fields = "\"category\", \"value\"";
		for slot in [
			format!("pie({fields})"),
			format!("view(\"named\", pie({fields}))"),
			format!("view(\"named\", figure([pie({fields})]))"),
			format!("figure([pie({fields})])"),
		] {
			let source = format!(
				"let relation = workspace.table(\"pie_probe\");\nrelation.views([{slot}]);"
			);
			let compiled = compile(&source).unwrap_or_else(|error| panic!("{slot}: {error}"));
			let RuneViewIntent::Plot(plot) = &compiled.executions[0].views[0].intent else {
				panic!("pie surface produced a table")
			};
			let sink = match &plot.body {
				RunePlotBody::Single(sink) => sink,
				RunePlotBody::StyledSingle(styled) => &styled.sink,
				RunePlotBody::Layered(_) => panic!("pie surface produced layers"),
			};
			let (actual, origins) = sink
				.mark_fields
				.as_ref()
				.and_then(|fields| fields.pie())
				.expect("pie fields and origins");
			assert_eq!(
				(actual.category.as_str(), actual.value.as_str()),
				("category", "value")
			);
			assert_eq!(
				&source[origins.category.start as usize..origins.category.end as usize],
				"\"category\""
			);
			assert_eq!(
				&source[origins.value.start as usize..origins.value.end as usize],
				"\"value\""
			);
		}

		for source in [
			format!("let r = workspace.table(\"pie_probe\"); r.views([layers([pie({fields})])]);"),
			format!("plot.pie(workspace.table(\"pie_probe\"), {fields});"),
			format!("workspace.table(\"pie_probe\").pie({fields});"),
		] {
			assert!(
				compile(&source).is_err(),
				"accepted forbidden surface: {source}"
			);
		}
		for source in [
			"let r = workspace.table(\"pie_probe\"); r.views([pie(\"value\")]);",
			"let r = workspace.table(\"pie_probe\"); r.views([pie(\"category\", \"value\", \"extra\")]);",
		] {
			let error = compile(source).expect_err("invalid pie arity must refuse");
			assert!(
				error
					.to_string()
					.contains("pie(...) takes category and value strings"),
				"{error}"
			);
		}
	}

	#[test]
	fn product_0103_test32_is_the_aggregated_and_refusing_pie_exhibit() {
		let source = include_str!("../../../uat/test32.rn");
		let compiled = compile(source).expect("the checked-in pie exhibit compiles");
		assert_eq!(compiled.executions.len(), 2);
		assert_eq!(compiled.presentation_order.len(), 2);
		for execution in &compiled.executions {
			let RuneViewIntent::Plot(plot) = &execution.views[0].intent else {
				panic!("pie exhibit produced a table")
			};
			let RunePlotBody::Single(sink) = &plot.body else {
				panic!("pie exhibit did not retain one standalone mark")
			};
			assert!(
				sink.mark_fields
					.as_ref()
					.and_then(|fields| fields.pie())
					.is_some()
			);
		}
		assert!(source.contains(".aggregate([col(\"category\")], [sum(col(\"value\"))])"));
		assert!(source.contains(".project_named(["));
		assert!(source.contains(".sort([col(\"value\").desc()])"));
		assert!(source.contains("must refuse") || source.contains("refusal"));
	}

	/// PRODUCT-0107 §2's only provenance gate, and it speaks about authored
	/// source rather than delivered rows.
	///
	/// Nothing at runtime can establish that the median and the density came
	/// from one execution — two independent executions over unchanged input
	/// would deliver coherent rows and be accepted. The contract is therefore
	/// locked here, in the checked-in spelling, which is why this test asserts
	/// on the source text and not on a compiled result.
	/// Strip line comments before counting calls.
	///
	/// An exhibit's prose necessarily names the calls it explains, so a gate that
	/// counted raw text would be measuring the comments — it flagged this very
	/// file twice while being written. String literals containing `//` would fool
	/// this, and none of the gated exhibits carry one.
	fn code_only(source: &str) -> String {
		source
			.lines()
			.map(|line| match line.find("//") {
				Some(index) => &line[..index],
				None => line,
			})
			.collect::<Vec<_>>()
			.join("\n")
	}

	#[test]
	fn product_0107_test34_composes_density_and_median_in_one_invocation() {
		let raw = include_str!("../../../uat/test34.rn");
		let compiled = compile(raw).expect("the checked-in violin exhibit compiles");
		let source = code_only(raw);
		let source = source.as_str();
		assert_eq!(
			compiled.executions.len(),
			1,
			"one execution is the contract: a second would be a second quantile draw"
		);

		assert_eq!(
			source.matches(".kernel_density(").count(),
			1,
			"exactly one density invocation"
		);
		assert_eq!(
			source.matches("approx_quantile(").count(),
			1,
			"exactly one median aggregate"
		);
		assert!(
			source.contains(".join("),
			"the median is joined to the density inside the same plan"
		);
		// `approx_quantile` names its output `quantile`; joining leaves that
		// unambiguous output available directly to the mark, so a cosmetic
		// projection must not become the reference composition pattern.
		assert!(
			source.contains("violin(\"group\", \"evaluation_value\", \"density\", \"quantile\")"),
			"the violin consumes the aggregate's real output name directly"
		);
		assert!(!source.contains(".project_named("));
		assert!(
			!source.contains("workspace.sql("),
			"ADR 552 has no SQL surface, so this composition must stay in the Rune DSL"
		);
	}

	#[test]
	fn product_0108_violin_surface_has_four_fields_and_refuses_raw_arity() {
		let source = include_str!("../../../uat/test34.rn");
		let compiled = compile(source).expect("the checked-in violin exhibit compiles");
		let RuneViewIntent::Plot(plot) = &compiled.executions[0].views[1].intent else {
			panic!("the second violin exhibit view is a plot")
		};
		let sink = match &plot.body {
			RunePlotBody::Single(sink) => sink,
			RunePlotBody::StyledSingle(styled) => &styled.sink,
			RunePlotBody::Layered(_) => panic!("violin surface produced layers"),
		};
		let (fields, origins) = sink
			.mark_fields
			.as_ref()
			.and_then(|fields| fields.violin())
			.expect("violin fields and origins");
		assert_eq!(
			(
				fields.group.as_str(),
				fields.evaluation_value.as_str(),
				fields.density.as_str(),
				fields.median.as_str(),
			),
			("group", "evaluation_value", "density", "quantile")
		);
		for (origin, expected) in [
			(&origins.group, "\"group\""),
			(&origins.evaluation_value, "\"evaluation_value\""),
			(&origins.density, "\"density\""),
			(&origins.median, "\"quantile\""),
		] {
			assert_eq!(
				&source[origin.start as usize..origin.end as usize],
				expected
			);
		}

		let raw =
			"let r = workspace.table(\"violin_probe\"); r.views([violin(\"group\", \"value\")]);";
		let error = compile(raw).expect_err("raw violin arity must refuse");
		assert!(
			error
				.to_string()
				.contains("violin(...) takes group, evaluation_value, density, and median strings"),
			"{error}"
		);
	}

	#[test]
	fn product_0103_each_exhibit_execution_contains_only_its_relation_branch() {
		let source = include_str!("../../../uat/test32.rn");
		let compiled = compile(source).expect("the checked-in pie exhibit compiles");
		let aggregated = compiled
			.executions
			.iter()
			.find(|execution| execution.origin.contains(".aggregate("))
			.expect("aggregated execution");
		let raw = compiled
			.executions
			.iter()
			.find(|execution| !execution.origin.contains(".aggregate("))
			.expect("raw execution");
		assert_eq!(raw.source.matches(".aggregate(").count(), 0);
		assert_eq!(aggregated.source.matches(".aggregate(").count(), 1);
	}

	#[test]
	fn product_0105_test33_is_the_variadic_radar_and_parallel_exhibit() {
		let source = include_str!("../../../uat/test33.rn");
		let compiled = compile(source).expect("the checked-in multivariate exhibit compiles");
		assert_eq!(compiled.executions.len(), 2);
		assert_eq!(compiled.presentation_order.len(), 3);
		let mut saw_radar = false;
		let mut saw_parallel = 0;
		for execution in &compiled.executions {
			for view in &execution.views {
				let RuneViewIntent::Plot(plot) = &view.intent else {
					panic!("multivariate exhibit produced a table")
				};
				let sink = match &plot.body {
					RunePlotBody::Single(sink) => sink,
					RunePlotBody::StyledSingle(styled) => &styled.sink,
					RunePlotBody::Layered(_) => panic!("multivariate exhibit produced layers"),
				};
				let (plot_type, fields, origins) = sink
					.mark_fields
					.as_ref()
					.and_then(|fields| fields.multivariate())
					.expect("multivariate fields and origins");
				match plot_type {
					crate::plot::core::PlotType::Radar => saw_radar = true,
					crate::plot::core::PlotType::Parallel => saw_parallel += 1,
					other => panic!("unexpected exhibit mark: {other:?}"),
				}
				assert_eq!(origins.dimensions.len(), fields.dimensions().len());
			}
		}
		assert!(saw_radar);
		assert_eq!(saw_parallel, 2);
		assert!(
			source.contains(
				"parallel(\"d8\", \"d1\", \"d6\", \"d3\", \"d7\", \"d2\", \"d5\", \"d4\")"
			)
		);
		assert!(source.contains("facet(\"cohort\")"));
	}

	#[test]
	fn product_0121_test36_is_the_authored_facet_gutter_member_exhibit() {
		let source = include_str!("../../../uat/test36.rn");
		let compiled = compile(source).expect("the checked-in gutter exhibit compiles");
		assert_eq!(compiled.executions.len(), 1);
		let RuneViewIntent::Plot(plot) = &compiled.executions[0].views[0].intent else {
			panic!("gutter exhibit produced a table");
		};
		assert_eq!(plot.facet.as_deref(), Some("cohort"));
		assert_eq!(plot.facet_gutter, Some(24.0));
		assert!(matches!(plot.body, RunePlotBody::Single(_)));

		let legacy =
			compile(r#"workspace.table("t").views([figure([facet_gutter(24), bar("x", "y")])])"#)
				.expect_err("the retired free word is not an alias");
		let RuneEnvelopeError::InvalidView { message, .. } = legacy else {
			panic!("retired gutter spelling remains an invalid view")
		};
		assert!(message.contains("Missing item facet_gutter"), "{message}");
	}

	#[test]
	fn product_0123_test38_is_the_axis_member_reference_exhibit() {
		let source = include_str!("../../../uat/test38.rn");
		let compiled = compile(source).expect("the checked-in axis-member exhibit compiles");
		assert_eq!(compiled.executions.len(), 1);
		assert_eq!(compiled.executions[0].views.len(), 2);
		assert!(source.contains("x_axis()\n\t\t\t.label(\"Units sold\")"));
		assert!(source.contains("y_axis().label(\"Revenue · $k\").label_size(18)"));
		assert!(
			source.contains("y_axis().bounds(0, 50).tick_step(10).minor_ticks(1).tick_size(12)")
		);
	}

	#[test]
	fn product_0124_test39_is_the_facet_label_typography_reference_exhibit() {
		let source = include_str!("../../../uat/test39.rn");
		let compiled = compile(source).expect("the checked-in facet label exhibit compiles");
		assert_eq!(compiled.executions.len(), 1);
		let RuneViewIntent::Plot(plot) = &compiled.executions[0].views[0].intent else {
			panic!("facet label exhibit produced a table");
		};
		assert_eq!(plot.facet.as_deref(), Some("region"));
		assert_eq!(plot.facet_gutter, Some(18.0));
		assert_eq!(
			plot.authored_settings.facet_label_size,
			Override::Set(FacetLabelTextSize::new(24.0).unwrap())
		);
	}

	#[test]
	fn product_0125_test40_is_the_legend_member_reference_exhibit() {
		let source = include_str!("../../../uat/test40.rn");
		let compiled = compile(source).expect("the checked-in legend exhibit compiles");
		assert_eq!(compiled.executions.len(), 1);
		assert_eq!(compiled.executions[0].views.len(), 2);
		assert!(source.contains("legend().at(0.62, 0.08).font_size(18)"));
		assert!(source.contains("legend().at(0.62, 0.08),\n\t\tlegend().font_size(18)"));
		for view in &compiled.executions[0].views {
			let RuneViewIntent::Plot(plot) = &view.intent else {
				panic!("legend exhibit produced a table");
			};
			assert!(matches!(plot.authored_settings.legend, Presence::Show(_)));
			assert_eq!(
				plot.authored_settings.legend_size,
				Override::Set(LegendTextSize::new(18.0).unwrap())
			);
		}
	}

	#[test]
	fn product_0144_test41_is_the_authored_hexbin_and_temporal_refusal_exhibit() {
		let source = include_str!("../../../uat/test41.rn");
		let compiled = compile(source).expect("the checked-in hexbin exhibit compiles");
		assert_eq!(compiled.executions.len(), 2);
		assert_eq!(compiled.presentation_order.len(), 2);
		assert!(source.contains("hexbin(\"x\", \"y\")"));
		assert!(source.contains("hexbin(\"stamp\", \"y\")"));
		for execution in &compiled.executions {
			let RuneViewIntent::Plot(plot) = &execution.views[0].intent else {
				panic!("hexbin exhibit produced a table")
			};
			let RunePlotBody::Single(sink) = &plot.body else {
				panic!("hexbin exhibit produced a styled or layered sink")
			};
			assert_eq!(
				sink.composition.primary_plot_type(),
				crate::plot::core::PlotType::Hexbin
			);
		}
	}

	#[test]
	fn product_0146_test42_is_the_temporal_fill_between_and_crossing_exhibit() {
		let source = include_str!("../../../uat/test42.rn");
		let compiled = compile(source).expect("the checked-in fill-between exhibit compiles");
		assert_eq!(compiled.executions.len(), 2);
		assert_eq!(compiled.presentation_order.len(), 3);
		assert!(source.contains(".sort([asc(col(\"stamp\"))])"));
		assert!(source.contains(".sort([asc(col(\"x\"))])"));
		assert!(source.contains("fill_between(\"stamp\", \"low\", \"high\")"));
		assert!(source.contains("fill_between(\"stamp\", \"low\", \"high\", \"expected\")"));
		assert!(source.contains("fill_between(\"x\", \"low\", \"high\")"));
		for execution in &compiled.executions {
			for view in &execution.views {
				let RuneViewIntent::Plot(plot) = &view.intent else {
					panic!("fill-between exhibit produced a table")
				};
				let RunePlotBody::Single(sink) = &plot.body else {
					panic!("fill-between exhibit produced a styled or layered sink")
				};
				assert_eq!(
					sink.composition.primary_plot_type(),
					crate::plot::core::PlotType::FillBetween
				);
			}
		}
	}

	#[test]
	fn product_0148_test43_is_the_sorted_temporal_candlestick_and_containment_exhibit() {
		let source = include_str!("../../../uat/test43.rn");
		let compiled = compile(source).expect("the checked-in candlestick exhibit compiles");
		assert_eq!(compiled.executions.len(), 2);
		assert_eq!(compiled.presentation_order.len(), 2);
		assert_eq!(source.matches(".sort([asc(col(\"stamp\"))])").count(), 2);
		assert_eq!(
			source
				.matches("candlestick(\"stamp\", \"open\", \"high\", \"low\", \"close\")")
				.count(),
			2
		);
		for execution in &compiled.executions {
			let RuneViewIntent::Plot(plot) = &execution.views[0].intent else {
				panic!("candlestick exhibit produced a table")
			};
			let RunePlotBody::Single(sink) = &plot.body else {
				panic!("candlestick exhibit produced a styled or layered sink")
			};
			assert_eq!(
				sink.composition.primary_plot_type(),
				crate::plot::core::PlotType::Candlestick
			);
		}
	}

	#[test]
	fn semantic_trends_showcase_compiles_all_four_cartesian_marks() {
		let source = include_str!("../../../uat/plot_trends_showcase.rn");
		let compiled = compile(source).expect("the Trends showcase compiles");
		assert_eq!(compiled.executions.len(), 1);
		assert_eq!(compiled.executions[0].views.len(), 4);
		for mark in ["line", "area", "scatter", "bubble"] {
			assert!(
				source.contains(&format!("{mark}(\"period\"")),
				"missing {mark}"
			);
		}
	}

	#[test]
	fn semantic_distribution_showcase_compiles_relation_native_mark_inputs() {
		let source = include_str!("../../../uat/plot_distribution_showcase.rn");
		let compiled = compile(source).expect("the Distribution showcase compiles");
		assert_eq!(compiled.executions.len(), 4);
		assert!(source.contains("kernel_density(col(\"cohort\"), col(\"score\"), 128)"));
		assert!(source.contains("approx_quantile(col(\"score\"), 0.25)"));
		for mark in ["bar", "histogram", "box", "violin"] {
			assert!(source.contains(&format!("{mark}(\"")), "missing {mark}");
		}
	}

	#[test]
	fn semantic_composition_showcase_compiles_all_three_radial_marks() {
		let source = include_str!("../../../uat/plot_composition_showcase.rn");
		let compiled = compile(source).expect("the Composition showcase compiles");
		assert_eq!(compiled.executions.len(), 2);
		assert_eq!(compiled.executions[0].views.len(), 2);
		assert_eq!(compiled.executions[1].views.len(), 1);
		for mark in ["pie", "funnel", "radial_dial"] {
			assert!(source.contains(&format!("{mark}(\"")), "missing {mark}");
		}
	}

	#[test]
	fn semantic_multivariate_showcase_compiles_long_and_wide_mark_inputs() {
		let source = include_str!("../../../uat/plot_multivariate_showcase.rn");
		let compiled = compile(source).expect("the Multivariate showcase compiles");
		assert_eq!(compiled.executions.len(), 2);
		assert_eq!(compiled.executions[0].views.len(), 1);
		assert_eq!(compiled.executions[1].views.len(), 2);
		for mark in ["heatmap", "radar", "parallel"] {
			assert!(source.contains(&format!("{mark}(\"")), "missing {mark}");
		}
		assert!(source.contains("padding().left(96)"));
	}

	#[test]
	fn product_0130_temporal_tick_interval_exhibit_compiles_calendar_surface() {
		let source = include_str!("../../../uat/temporal_tick_intervals.rn");
		let compiled = compile(source).expect("the temporal tick exhibit compiles");
		assert_eq!(compiled.executions.len(), 1);
		assert_eq!(compiled.executions[0].views.len(), 4);
		assert!(
			source.contains("x_axis().label(\"Month\").label_size(16).tick_every(\"1 month\")")
		);

		for (source, expected_span, expected_message) in [
			(
				"workspace.table(\"t\").views([figure([x_axis().tick_every(\"0 day\"), line(\"x\", \"y\")])])",
				".tick_every(\"0 day\")",
				"strictly positive",
			),
			(
				"workspace.table(\"t\").views([figure([x_axis().tick_every(\"1 day\").tick_step(1), line(\"x\", \"y\")])])",
				".tick_step(1)",
				"cannot combine",
			),
			(
				"workspace.table(\"t\").views([figure([x_axis().tick_every(\"1 day\").minor_ticks(1), line(\"x\", \"y\")])])",
				".minor_ticks(1)",
				"cannot combine",
			),
		] {
			let error = compile(source).expect_err("invalid temporal placement refuses");
			let RuneEnvelopeError::InvalidView { span, message } = error else {
				panic!("temporal placement refusal retains its origin")
			};
			assert_eq!(
				&source[span.start as usize..span.end as usize],
				expected_span
			);
			assert!(message.contains(expected_message), "{message}");
		}
	}
}
