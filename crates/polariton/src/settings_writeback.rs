//! PRODUCT-0142 source-edit planning.
//!
//! This module is deliberately text-facing. A GUI gesture becomes one edit
//! plan here; preview applies that plan to a shadow string and commit sends the
//! same plan to libaryon. No plot-spec patch representation exists.

use crate::presentation_registry::{
	CALLABLES, CallPosition, ContainerKind, ExpressionShape, HandlerId, LogicalTarget,
	NumericDomain, PlotSourceAttachment, PresentationSourceModel, SettingKey, SettingOccurrence,
	SettingValue, ValueShape, setting_spec,
};
use crate::rune_relation_envelope::{
	ArtifactReuseWitness, RelationPlanKey, RunePlotBody, TransformedRuneView,
};
use libaryon::engine::DocumentTextEdit;

fn doc_byte(value: u32) -> libaryon::core::DocByte {
	libaryon::core::DocByte::new(u64::from(value))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum WritebackCapability {
	Editable,
	DisplayOnly,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct WritebackSpec {
	pub(crate) key: SettingKey,
	pub(crate) capability: WritebackCapability,
}

pub(crate) const WRITEBACK_SPECS: &[WritebackSpec] = &[
	WritebackSpec {
		key: SettingKey::Title,
		capability: WritebackCapability::Editable,
	},
	WritebackSpec {
		key: SettingKey::Subtitle,
		capability: WritebackCapability::Editable,
	},
	WritebackSpec {
		key: SettingKey::TitleSize,
		capability: WritebackCapability::Editable,
	},
	WritebackSpec {
		key: SettingKey::SubtitleSize,
		capability: WritebackCapability::Editable,
	},
	WritebackSpec {
		key: SettingKey::CategoricalPalette,
		capability: WritebackCapability::Editable,
	},
	WritebackSpec {
		key: SettingKey::SequentialPalette,
		capability: WritebackCapability::Editable,
	},
	WritebackSpec {
		key: SettingKey::DivergingPalette,
		capability: WritebackCapability::Editable,
	},
	WritebackSpec {
		key: SettingKey::PlotSkin,
		capability: WritebackCapability::Editable,
	},
	WritebackSpec {
		key: SettingKey::SkinDuotone,
		capability: WritebackCapability::DisplayOnly,
	},
	WritebackSpec {
		key: SettingKey::HoverSize,
		capability: WritebackCapability::Editable,
	},
	WritebackSpec {
		key: SettingKey::FacetGutter,
		capability: WritebackCapability::DisplayOnly,
	},
	WritebackSpec {
		key: SettingKey::FacetLabelSize,
		capability: WritebackCapability::Editable,
	},
	WritebackSpec {
		key: SettingKey::PaddingTop,
		capability: WritebackCapability::Editable,
	},
	WritebackSpec {
		key: SettingKey::PaddingRight,
		capability: WritebackCapability::Editable,
	},
	WritebackSpec {
		key: SettingKey::PaddingBottom,
		capability: WritebackCapability::Editable,
	},
	WritebackSpec {
		key: SettingKey::PaddingLeft,
		capability: WritebackCapability::Editable,
	},
	WritebackSpec {
		key: SettingKey::XAxisLabel,
		capability: WritebackCapability::Editable,
	},
	WritebackSpec {
		key: SettingKey::YAxisLabel,
		capability: WritebackCapability::Editable,
	},
	WritebackSpec {
		key: SettingKey::XAxisLabelSize,
		capability: WritebackCapability::Editable,
	},
	WritebackSpec {
		key: SettingKey::YAxisLabelSize,
		capability: WritebackCapability::Editable,
	},
	WritebackSpec {
		key: SettingKey::XAxisBounds,
		capability: WritebackCapability::Editable,
	},
	WritebackSpec {
		key: SettingKey::YAxisBounds,
		capability: WritebackCapability::Editable,
	},
	WritebackSpec {
		key: SettingKey::XAxisTickStep,
		capability: WritebackCapability::Editable,
	},
	WritebackSpec {
		key: SettingKey::YAxisTickStep,
		capability: WritebackCapability::Editable,
	},
	WritebackSpec {
		key: SettingKey::XAxisTickEvery,
		capability: WritebackCapability::Editable,
	},
	WritebackSpec {
		key: SettingKey::YAxisTickEvery,
		capability: WritebackCapability::Editable,
	},
	WritebackSpec {
		key: SettingKey::XAxisMinorTicks,
		capability: WritebackCapability::Editable,
	},
	WritebackSpec {
		key: SettingKey::YAxisMinorTicks,
		capability: WritebackCapability::Editable,
	},
	WritebackSpec {
		key: SettingKey::XAxisTickSize,
		capability: WritebackCapability::Editable,
	},
	WritebackSpec {
		key: SettingKey::YAxisTickSize,
		capability: WritebackCapability::Editable,
	},
	WritebackSpec {
		key: SettingKey::XAxisTickRotation,
		capability: WritebackCapability::Editable,
	},
	WritebackSpec {
		key: SettingKey::LegendPosition,
		capability: WritebackCapability::Editable,
	},
	WritebackSpec {
		key: SettingKey::LegendPresence,
		capability: WritebackCapability::DisplayOnly,
	},
	WritebackSpec {
		key: SettingKey::LegendSize,
		capability: WritebackCapability::Editable,
	},
	WritebackSpec {
		key: SettingKey::PointSize,
		capability: WritebackCapability::Editable,
	},
	WritebackSpec {
		key: SettingKey::StrokeWidth,
		capability: WritebackCapability::Editable,
	},
	WritebackSpec {
		key: SettingKey::BarWidth,
		capability: WritebackCapability::Editable,
	},
	WritebackSpec {
		key: SettingKey::CategoryGap,
		capability: WritebackCapability::Editable,
	},
	WritebackSpec {
		key: SettingKey::Stacked,
		capability: WritebackCapability::DisplayOnly,
	},
	WritebackSpec {
		key: SettingKey::Percent,
		capability: WritebackCapability::DisplayOnly,
	},
	WritebackSpec {
		key: SettingKey::Diverging,
		capability: WritebackCapability::DisplayOnly,
	},
	WritebackSpec {
		key: SettingKey::DivergingAt,
		capability: WritebackCapability::DisplayOnly,
	},
];

pub(crate) fn writeback_spec(key: SettingKey) -> &'static WritebackSpec {
	WRITEBACK_SPECS
		.iter()
		.find(|spec| spec.key == key)
		.expect("every SettingKey has a PRODUCT-0142 capability")
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum WritebackAction {
	Set,
	Reset,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum WritebackRefusal {
	DisplayOnly,
	Detached,
	LayeredPlot,
	SourceModelIncomplete,
	ExpressionAuthored,
	SharedOccurrence,
	AmbiguousInsertion,
	Conflict(SettingKey),
	InvalidValue,
	InvalidSourceBoundary,
}

impl std::fmt::Display for WritebackRefusal {
	fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::DisplayOnly => formatter.write_str("this setting is display-only"),
			Self::Detached => formatter.write_str("this plot has no editable producing source"),
			Self::LayeredPlot => formatter.write_str("per-layer settings editing is not supported"),
			Self::SourceModelIncomplete => {
				formatter.write_str("the authored setting could not be attributed safely")
			}
			Self::ExpressionAuthored => {
				formatter.write_str("the setting is authored by an expression")
			}
			Self::SharedOccurrence => {
				formatter.write_str("the source occurrence is shared by multiple views")
			}
			Self::AmbiguousInsertion => {
				formatter.write_str("the source has no unambiguous insertion point")
			}
			Self::Conflict(key) => write!(formatter, "the edit conflicts with {key:?}"),
			Self::InvalidValue => formatter.write_str("the value is invalid for this setting"),
			Self::InvalidSourceBoundary => {
				formatter.write_str("the compiler-reported source boundary is invalid")
			}
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CandidateEditPlan {
	pub(crate) edits: Vec<DocumentTextEdit>,
	pub(crate) candidate_source: String,
}

#[derive(Clone, Debug)]
pub(crate) struct CompiledCandidate {
	pub(crate) plan: CandidateEditPlan,
	pub(crate) transformed: TransformedRuneView,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum CandidatePreviewRefusal {
	Compile(String),
	RelationReuseRequired,
}

/// Run the real presentation compiler over the shadow source and join its
/// compiler-derived relation key to the separately retained runtime witness.
/// No relation dispatch path is reachable from this function.
pub(crate) fn compile_candidate(
	plan: CandidateEditPlan,
	witness: &ArtifactReuseWitness,
	authoritative_artifact_generation: u64,
) -> Result<CompiledCandidate, CandidatePreviewRefusal> {
	let transformed = crate::rune_view_compiler::compile(&plan.candidate_source)
		.map_err(|error| CandidatePreviewRefusal::Compile(error.to_string()))?;
	let candidate_key = RelationPlanKey::from_view(&transformed);
	if !witness.proves(&candidate_key, authoritative_artifact_generation) {
		return Err(CandidatePreviewRefusal::RelationReuseRequired);
	}
	Ok(CompiledCandidate { plan, transformed })
}

impl CandidateEditPlan {
	fn new(source: &str, edits: Vec<DocumentTextEdit>) -> Result<Self, WritebackRefusal> {
		let mut candidate_source = source.to_owned();
		let mut ordered = edits.clone();
		ordered.sort_unstable_by_key(|edit| std::cmp::Reverse((edit.start, edit.end)));
		for edit in &ordered {
			let (Ok(start), Ok(end)) = (
				usize::try_from(edit.start.get()),
				usize::try_from(edit.end.get()),
			) else {
				return Err(WritebackRefusal::InvalidSourceBoundary);
			};
			if start > end
				|| end > candidate_source.len()
				|| !candidate_source.is_char_boundary(start)
				|| !candidate_source.is_char_boundary(end)
			{
				return Err(WritebackRefusal::InvalidSourceBoundary);
			}
			candidate_source.replace_range(start..end, &edit.replacement);
		}
		Ok(Self {
			edits,
			candidate_source,
		})
	}
}

/// Plan one inspector gesture from compiler facts. `authored_without_occurrence`
/// is the conservative bridge for PRODUCT-0141's deliberately partial scanner:
/// it prevents an insertion from duplicating authored semantics the evaluator
/// saw but the source model could not attribute.
pub(crate) fn plan_writeback(
	source: &str,
	attachment: Option<&PlotSourceAttachment>,
	body: Option<&RunePlotBody>,
	key: SettingKey,
	action: WritebackAction,
	value: Option<&SettingValue>,
	authored_without_occurrence: bool,
) -> Result<CandidateEditPlan, WritebackRefusal> {
	if writeback_spec(key).capability != WritebackCapability::Editable {
		return Err(WritebackRefusal::DisplayOnly);
	}
	if matches!(body, Some(RunePlotBody::Layered(_))) {
		return Err(WritebackRefusal::LayeredPlot);
	}
	let attachment = attachment.ok_or(WritebackRefusal::Detached)?;
	let setting = setting_spec(key);
	let model = attachment.source.model.as_ref();
	let occurrence = occurrence_for(model, attachment, key, setting.target);
	if occurrence.is_none() && authored_without_occurrence {
		return Err(WritebackRefusal::SourceModelIncomplete);
	}
	for conflict in setting.conflicts {
		if occurrence_for(model, attachment, *conflict, setting_spec(*conflict).target).is_some() {
			return Err(WritebackRefusal::Conflict(*conflict));
		}
	}

	match (action, occurrence) {
		(WritebackAction::Set, Some(occurrence)) => {
			check_occurrence(occurrence)?;
			let arguments =
				encode_arguments(setting.shape, value.ok_or(WritebackRefusal::InvalidValue)?)?;
			if arguments.len() != occurrence.argument_spans.len() {
				return Err(WritebackRefusal::InvalidValue);
			}
			CandidateEditPlan::new(
				source,
				occurrence
					.argument_spans
					.iter()
					.cloned()
					.zip(arguments)
					.map(|(span, replacement)| DocumentTextEdit {
						start: doc_byte(span.start),
						end: doc_byte(span.end),
						replacement,
					})
					.collect(),
			)
		}
		(WritebackAction::Set, None) => {
			let arguments =
				encode_arguments(setting.shape, value.ok_or(WritebackRefusal::InvalidValue)?)?;
			plan_insertion(source, model, attachment, key, &arguments)
		}
		(WritebackAction::Reset, Some(occurrence)) => {
			check_occurrence(occurrence)?;
			plan_reset(source, model, attachment, occurrence)
		}
		(WritebackAction::Reset, None) => CandidateEditPlan::new(source, Vec::new()),
	}
}

fn occurrence_for<'a>(
	model: &'a PresentationSourceModel,
	attachment: &PlotSourceAttachment,
	key: SettingKey,
	target: LogicalTarget,
) -> Option<&'a SettingOccurrence> {
	model.occurrences.iter().find(|occurrence| {
		occurrence.key == key
			&& occurrence.target == target
			&& occurrence.owners.contains(&attachment.identity)
	})
}

fn check_occurrence(occurrence: &SettingOccurrence) -> Result<(), WritebackRefusal> {
	if occurrence.owners.len() != 1 {
		return Err(WritebackRefusal::SharedOccurrence);
	}
	if occurrence.expression_shape != ExpressionShape::DirectLiteral {
		return Err(WritebackRefusal::ExpressionAuthored);
	}
	Ok(())
}

fn encode_arguments(
	shape: ValueShape,
	value: &SettingValue,
) -> Result<Vec<String>, WritebackRefusal> {
	let quote = |value: &str| format!("{value:?}");
	match (shape, value) {
		(
			ValueShape::Text | ValueShape::Choice | ValueShape::TemporalInterval,
			SettingValue::Text(value),
		)
		| (
			ValueShape::Text | ValueShape::Choice | ValueShape::TemporalInterval,
			SettingValue::Choice(value),
		) => Ok(vec![quote(value)]),
		(ValueShape::Number(domain), SettingValue::Number(value)) if domain.accepts(*value) => {
			Ok(vec![format_number(*value)])
		}
		(ValueShape::Number(domain), SettingValue::Integer(value))
			if domain.accepts(f64::from(*value)) =>
		{
			Ok(vec![value.to_string()])
		}
		(ValueShape::NumberPair(domain), SettingValue::Pair(left, right))
			if domain.accepts(*left) && domain.accepts(*right) =>
		{
			Ok(vec![format_number(*left), format_number(*right)])
		}
		_ => Err(WritebackRefusal::InvalidValue),
	}
}

pub(crate) fn parse_editor_value(
	key: SettingKey,
	input: &str,
) -> Result<SettingValue, WritebackRefusal> {
	match setting_spec(key).shape {
		ValueShape::Text | ValueShape::TemporalInterval => Ok(SettingValue::Text(input.to_owned())),
		ValueShape::Choice => Ok(SettingValue::Choice(input.to_owned())),
		ValueShape::Number(domain) => {
			let value = input
				.trim()
				.parse::<f64>()
				.map_err(|_| WritebackRefusal::InvalidValue)?;
			if !domain.accepts(value) {
				return Err(WritebackRefusal::InvalidValue);
			}
			if domain == NumericDomain::NonNegativeInteger {
				return u32::try_from(value as u64)
					.map(SettingValue::Integer)
					.map_err(|_| WritebackRefusal::InvalidValue);
			}
			Ok(SettingValue::Number(value))
		}
		ValueShape::NumberPair(domain) => {
			let (left, right) = input
				.split_once(',')
				.ok_or(WritebackRefusal::InvalidValue)?;
			let left = left
				.trim()
				.parse::<f64>()
				.map_err(|_| WritebackRefusal::InvalidValue)?;
			let right = right
				.trim()
				.parse::<f64>()
				.map_err(|_| WritebackRefusal::InvalidValue)?;
			if !domain.accepts(left) || !domain.accepts(right) {
				return Err(WritebackRefusal::InvalidValue);
			}
			Ok(SettingValue::Pair(left, right))
		}
		ValueShape::Toggle => Err(WritebackRefusal::InvalidValue),
	}
}

fn format_number(value: f64) -> String {
	if value.fract() == 0.0 {
		format!("{value:.1}")
	} else {
		value.to_string()
	}
}

fn callable_name(handler: HandlerId, position: CallPosition) -> &'static str {
	CALLABLES
		.iter()
		.find(|callable| callable.handler == handler && callable.position == position)
		.map(|callable| callable.name)
		.expect("writeback setting references a registered callable")
}

fn receiver_handler(receiver: crate::presentation_registry::SemanticReceiver) -> Option<HandlerId> {
	use crate::presentation_registry::SemanticReceiver;
	match receiver {
		SemanticReceiver::Title => Some(HandlerId::Title),
		SemanticReceiver::Subtitle => Some(HandlerId::Subtitle),
		SemanticReceiver::XAxis => Some(HandlerId::XAxis),
		SemanticReceiver::YAxis => Some(HandlerId::YAxis),
		SemanticReceiver::Facet => Some(HandlerId::Facet),
		SemanticReceiver::Legend => Some(HandlerId::Legend),
		SemanticReceiver::Padding => Some(HandlerId::Padding),
		SemanticReceiver::Skin => Some(HandlerId::Skin),
		SemanticReceiver::Mark | SemanticReceiver::Table => None,
	}
}

fn plan_insertion(
	source: &str,
	model: &PresentationSourceModel,
	attachment: &PlotSourceAttachment,
	key: SettingKey,
	arguments: &[String],
) -> Result<CandidateEditPlan, WritebackRefusal> {
	let setting = setting_spec(key);
	let argument_text = arguments.join(", ");
	let method_name = callable_name(
		setting.callable,
		if setting.receiver.is_some() {
			CallPosition::Method
		} else {
			CallPosition::Free
		},
	);
	if setting.receiver.is_some() {
		let receiver = if setting.target == LogicalTarget::Mark {
			let mark_handlers = [
				HandlerId::Bar,
				HandlerId::Line,
				HandlerId::Scatter,
				HandlerId::Area,
				HandlerId::Histogram,
				HandlerId::Box,
				HandlerId::Bubble,
				HandlerId::Heatmap,
				HandlerId::Funnel,
				HandlerId::RadialDial,
				HandlerId::Pie,
				HandlerId::Violin,
				HandlerId::Radar,
				HandlerId::Parallel,
			];
			unique_boundary(model, attachment, |boundary| {
				mark_handlers.contains(&boundary.handler)
			})?
		} else {
			let handler = receiver_handler(setting.receiver.expect("method setting has receiver"))
				.ok_or(WritebackRefusal::AmbiguousInsertion)?;
			let candidates = model
				.syntax
				.iter()
				.filter(|boundary| {
					boundary.owners.contains(&attachment.identity) && boundary.handler == handler
				})
				.collect::<Vec<_>>();
			match candidates.as_slice() {
				[receiver] => *receiver,
				[] if matches!(
					setting.target,
					LogicalTarget::XAxis
						| LogicalTarget::YAxis
						| LogicalTarget::Padding
						| LogicalTarget::Legend
				) =>
				{
					let constructor = callable_name(handler, CallPosition::Free);
					return insert_figure_child(
						source,
						model,
						attachment,
						format!("{constructor}().{method_name}({argument_text})"),
					);
				}
				_ => return Err(WritebackRefusal::AmbiguousInsertion),
			}
		};
		if receiver.owners.len() != 1 {
			return Err(WritebackRefusal::SharedOccurrence);
		}
		let receiver_is_inline = model.syntax.iter().any(|boundary| {
			boundary.owners.contains(&attachment.identity)
				&& boundary.container.as_ref().is_some_and(|(kind, list)| {
					*kind == ContainerKind::Figure
						&& list.start <= receiver.chain_span.start
						&& list.end >= receiver.chain_span.end
				})
		}) || model.view_slots.iter().any(|slot| {
			slot.owner == attachment.identity
				&& slot.expression_span.start <= receiver.chain_span.start
				&& slot.expression_span.end >= receiver.chain_span.end
		});
		if !receiver_is_inline {
			return Err(WritebackRefusal::AmbiguousInsertion);
		}
		let insertion = receiver.chain_span.end;
		return CandidateEditPlan::new(
			source,
			vec![DocumentTextEdit {
				start: doc_byte(insertion),
				end: doc_byte(insertion),
				replacement: format!(".{method_name}({argument_text})"),
			}],
		);
	}

	let call = format!("{method_name}({argument_text})");
	insert_figure_child(source, model, attachment, call)
}

fn unique_boundary<'a>(
	model: &'a PresentationSourceModel,
	attachment: &PlotSourceAttachment,
	mut matches: impl FnMut(&crate::presentation_registry::PresentationSyntaxBoundary) -> bool,
) -> Result<&'a crate::presentation_registry::PresentationSyntaxBoundary, WritebackRefusal> {
	let mut candidates = model
		.syntax
		.iter()
		.filter(|boundary| boundary.owners.contains(&attachment.identity) && matches(boundary));
	let candidate = candidates
		.next()
		.ok_or(WritebackRefusal::AmbiguousInsertion)?;
	if candidates.next().is_some() {
		return Err(WritebackRefusal::AmbiguousInsertion);
	}
	Ok(candidate)
}

fn insert_figure_child(
	source: &str,
	model: &PresentationSourceModel,
	attachment: &PlotSourceAttachment,
	call: String,
) -> Result<CandidateEditPlan, WritebackRefusal> {
	let figure = unique_boundary(model, attachment, |boundary| {
		boundary.handler == HandlerId::Figure
			&& matches!(boundary.container, Some((ContainerKind::Figure, _)))
	})?;
	if figure.owners.len() != 1 {
		return Err(WritebackRefusal::SharedOccurrence);
	}
	let (_, list) = figure
		.container
		.as_ref()
		.expect("figure boundary has child list");
	let list_start = list.start as usize;
	let list_end = list.end as usize;
	let list_text = source
		.get(list_start..list_end)
		.ok_or(WritebackRefusal::InvalidSourceBoundary)?;
	let close = list_text
		.rfind(']')
		.ok_or(WritebackRefusal::InvalidSourceBoundary)?
		+ list_start;
	let inner = source
		.get(list_start + 1..close)
		.ok_or(WritebackRefusal::InvalidSourceBoundary)?;
	let replacement = if inner.trim().is_empty() {
		call
	} else {
		format!(", {call}")
	};
	CandidateEditPlan::new(
		source,
		vec![DocumentTextEdit {
			start: (close as u64).into(),
			end: (close as u64).into(),
			replacement,
		}],
	)
}

fn plan_reset(
	source: &str,
	model: &PresentationSourceModel,
	attachment: &PlotSourceAttachment,
	occurrence: &SettingOccurrence,
) -> Result<CandidateEditPlan, WritebackRefusal> {
	let boundary = model
		.syntax
		.iter()
		.find(|boundary| boundary.call_span == occurrence.call_span)
		.ok_or(WritebackRefusal::InvalidSourceBoundary)?;
	let chain_span = model
		.syntax
		.iter()
		.filter(|candidate| {
			candidate.owners.contains(&attachment.identity)
				&& candidate.chain_span.start == boundary.chain_span.start
		})
		.map(|candidate| candidate.chain_span.clone())
		.max_by_key(|span| span.end)
		.unwrap_or_else(|| boundary.chain_span.clone());
	let remove_container = matches!(
		occurrence.target,
		LogicalTarget::XAxis
			| LogicalTarget::YAxis
			| LogicalTarget::Padding
			| LogicalTarget::Legend
	) && !model.occurrences.iter().any(|other| {
		other.call_span != occurrence.call_span
			&& other.owners.contains(&attachment.identity)
			&& other.call_span.start >= chain_span.start
			&& other.call_span.end <= chain_span.end
	});
	let remove_inline_element = setting_spec(occurrence.key).receiver.is_none();
	if !remove_container && !remove_inline_element {
		return CandidateEditPlan::new(
			source,
			vec![DocumentTextEdit {
				start: doc_byte(occurrence.call_span.start),
				end: doc_byte(occurrence.call_span.end),
				replacement: String::new(),
			}],
		);
	}
	let span = chain_span;
	let container = model
		.syntax
		.iter()
		.find_map(|candidate| {
			let (_, list) = candidate.container.as_ref()?;
			(candidate.owners.contains(&attachment.identity)
				&& list.start <= span.start
				&& list.end >= span.end)
				.then_some((candidate, list))
		})
		.ok_or(WritebackRefusal::AmbiguousInsertion)?;
	if container.0.owners.len() != 1 {
		return Err(WritebackRefusal::SharedOccurrence);
	}
	let removal = list_element_removal(source, span, container.1, &container.0.separators)?;
	CandidateEditPlan::new(source, vec![removal])
}

fn list_element_removal(
	source: &str,
	span: std::ops::Range<u32>,
	list: &std::ops::Range<u32>,
	separators: &[std::ops::Range<u32>],
) -> Result<DocumentTextEdit, WritebackRefusal> {
	let mut start = span.start;
	let mut end = span.end;
	let previous = separators
		.iter()
		.rev()
		.find(|separator| separator.end <= span.start);
	let next = separators
		.iter()
		.find(|separator| separator.start >= span.end);
	if previous.is_none() && next.is_none() {
		start = list.start.saturating_add(1);
		end = list.end.saturating_sub(1);
	} else if let Some(next) = next {
		start = previous.map_or(list.start.saturating_add(1), |separator| separator.end);
		end = next.end;
		while source
			.as_bytes()
			.get(end as usize)
			.is_some_and(u8::is_ascii_whitespace)
		{
			end += 1;
		}
	} else if let Some(previous) = previous {
		start = previous.start;
		end = list.end.saturating_sub(1);
	}
	if start < list.start || end > list.end || start > end {
		return Err(WritebackRefusal::InvalidSourceBoundary);
	}
	Ok(DocumentTextEdit {
		start: doc_byte(start),
		end: doc_byte(end),
		replacement: String::new(),
	})
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::presentation_registry::StampedPresentationSourceModel;
	use libaryon::core::{BufferId, StateId};
	use std::sync::Arc;

	fn fixture(
		source: &str,
	) -> (
		crate::rune_relation_envelope::TransformedRuneView,
		PlotSourceAttachment,
	) {
		let compiled = crate::rune_view_compiler::compile(source).expect("fixture compiles");
		let identity = compiled.presentation_order[0].clone();
		let attachment = PlotSourceAttachment {
			identity,
			source: StampedPresentationSourceModel {
				buffer_id: BufferId::new(3),
				document_generation: StateId::new(7),
				model: Arc::new(compiled.presentation_source_model.clone()),
			},
			expression: None,
		};
		(compiled, attachment)
	}

	fn plot_body(
		compiled: &crate::rune_relation_envelope::TransformedRuneView,
	) -> Option<&RunePlotBody> {
		match &compiled.executions[0].views[0].intent {
			crate::rune_relation_envelope::RuneViewIntent::Plot(plot) => Some(&plot.body),
			crate::rune_relation_envelope::RuneViewIntent::Table(_) => None,
		}
	}

	#[test]
	fn product_0142_capability_set_covers_every_setting_exactly() {
		assert_eq!(
			WRITEBACK_SPECS.len(),
			crate::presentation_registry::SETTINGS.len()
		);
		let keys = WRITEBACK_SPECS
			.iter()
			.map(|spec| spec.key)
			.collect::<std::collections::HashSet<_>>();
		assert_eq!(keys.len(), WRITEBACK_SPECS.len());
		assert_eq!(
			writeback_spec(SettingKey::LegendPresence).capability,
			WritebackCapability::DisplayOnly
		);
		assert_eq!(
			writeback_spec(SettingKey::LegendPosition).capability,
			WritebackCapability::Editable
		);
	}

	#[test]
	fn product_0142_literal_replacement_changes_only_argument_bytes() {
		let source = "workspace.table(\"t\").views([figure([x_axis().bounds(0.0, 10.0), line(\"x\", \"y\")])])";
		let (compiled, attachment) = fixture(source);
		let plan = plan_writeback(
			source,
			Some(&attachment),
			plot_body(&compiled),
			SettingKey::XAxisBounds,
			WritebackAction::Set,
			Some(&SettingValue::Pair(-2.0, 12.0)),
			false,
		)
		.unwrap();
		assert_eq!(plan.edits.len(), 2);
		assert_eq!(
			plan.candidate_source,
			"workspace.table(\"t\").views([figure([x_axis().bounds(-2.0, 12.0), line(\"x\", \"y\")])])"
		);
	}

	#[test]
	fn product_0142_expression_and_shared_occurrences_refuse() {
		let expression = "let n = 3.0; workspace.table(\"t\").views([figure([scatter(\"x\", \"y\").point_size(n)])])";
		let (compiled, attachment) = fixture(expression);
		assert_eq!(
			plan_writeback(
				expression,
				Some(&attachment),
				plot_body(&compiled),
				SettingKey::PointSize,
				WritebackAction::Set,
				Some(&SettingValue::Number(4.0)),
				false
			)
			.unwrap_err(),
			WritebackRefusal::ExpressionAuthored
		);

		let shared = "let ax = x_axis().label(\"old\"); workspace.table(\"t\").views([figure([ax, line(\"x\", \"y\")]), figure([ax, line(\"x\", \"z\")])])";
		let (compiled, attachment) = fixture(shared);
		assert_eq!(
			plan_writeback(
				shared,
				Some(&attachment),
				plot_body(&compiled),
				SettingKey::XAxisLabel,
				WritebackAction::Set,
				Some(&SettingValue::Text("new".into())),
				false
			)
			.unwrap_err(),
			WritebackRefusal::SharedOccurrence
		);
	}

	#[test]
	fn product_0142_insertion_and_last_modifier_reset_use_syntax_boundaries() {
		let source = "workspace.table(\"t\").views([figure([line(\"x\", \"y\")])])";
		let (compiled, attachment) = fixture(source);
		let inserted = plan_writeback(
			source,
			Some(&attachment),
			plot_body(&compiled),
			SettingKey::XAxisLabel,
			WritebackAction::Set,
			Some(&SettingValue::Text("Revenue".into())),
			false,
		)
		.unwrap();
		assert_eq!(
			inserted.candidate_source,
			"workspace.table(\"t\").views([figure([line(\"x\", \"y\"), x_axis().label(\"Revenue\")])])"
		);
		let (candidate, candidate_attachment) = fixture(&inserted.candidate_source);
		let reset = plan_writeback(
			&inserted.candidate_source,
			Some(&candidate_attachment),
			plot_body(&candidate),
			SettingKey::XAxisLabel,
			WritebackAction::Reset,
			None,
			false,
		)
		.unwrap();
		assert_eq!(reset.candidate_source, source);
	}

	#[test]
	fn product_0142_candidate_source_compile_is_the_reuse_gate() {
		let source = "workspace.table(\"t\").views([figure([title(\"Old\"), line(\"x\", \"y\")])])";
		let (baseline, attachment) = fixture(source);
		let plan = plan_writeback(
			source,
			Some(&attachment),
			plot_body(&baseline),
			SettingKey::Title,
			WritebackAction::Set,
			Some(&SettingValue::Text("New".to_owned())),
			false,
		)
		.unwrap();
		let witness = ArtifactReuseWitness {
			baseline: RelationPlanKey::from_view(&baseline),
			artifact_generation: 7,
		};
		let compiled = compile_candidate(plan.clone(), &witness, 7).unwrap();
		assert_eq!(compiled.plan, plan);
		assert_eq!(
			compiled.transformed.presentation_source_model,
			crate::rune_view_compiler::compile(&plan.candidate_source)
				.unwrap()
				.presentation_source_model
		);

		let relation_edit = CandidateEditPlan::new(
			source,
			vec![DocumentTextEdit {
				start: libaryon::core::DocByte::new(17),
				end: libaryon::core::DocByte::new(18),
				replacement: "u".to_owned(),
			}],
		)
		.unwrap();
		assert_eq!(
			compile_candidate(relation_edit, &witness, 7).unwrap_err(),
			CandidatePreviewRefusal::RelationReuseRequired
		);
	}

	#[test]
	fn product_0142_absent_setting_refuses_helper_built_furniture() {
		let source = "let ax = x_axis().label(\"Existing\"); workspace.table(\"t\").views([figure([ax, line(\"x\", \"y\")])])";
		let (compiled, attachment) = fixture(source);
		assert_eq!(
			plan_writeback(
				source,
				Some(&attachment),
				plot_body(&compiled),
				SettingKey::XAxisTickSize,
				WritebackAction::Set,
				Some(&SettingValue::Number(8.0)),
				false,
			)
			.unwrap_err(),
			WritebackRefusal::AmbiguousInsertion
		);
	}

	#[test]
	fn product_0142_absent_mark_setting_appends_to_a_bare_inline_slot() {
		let source = "workspace.table(\"t\").views([scatter(\"x\", \"y\")])";
		let (compiled, attachment) = fixture(source);
		let plan = plan_writeback(
			source,
			Some(&attachment),
			plot_body(&compiled),
			SettingKey::PointSize,
			WritebackAction::Set,
			Some(&SettingValue::Number(3.0)),
			false,
		)
		.expect("the slot expression proves the bare mark is inline");
		assert_eq!(
			plan.candidate_source,
			"workspace.table(\"t\").views([scatter(\"x\", \"y\").point_size(3.0)])"
		);
		crate::rune_view_compiler::compile(&plan.candidate_source)
			.expect("the candidate bare-mark source compiles");
	}
}
