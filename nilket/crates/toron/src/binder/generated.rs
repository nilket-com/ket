//! ADR 572: the materialize verb's synchronous authoring bind.

use super::*;

/// Synchronously bind a catalog-free generated relation: a `Range`, optionally
/// under one `Project`. The materialize verb authors its certificate from the
/// bound nodes this produces; the admissible vocabulary is decided by the
/// canonical converter over those nodes, never by a plan-shape list here.
pub(crate) fn bind_generated_relation_for_authoring(
	plan: &LogicalPlan,
) -> Result<ExecutionPlan, BindingError> {
	let generated_only = || BindingError::UnsupportedExpression {
		message: "materialize admits only a generated range with at most one projection"
			.to_string(),
	};
	let bound_range = |count: i64| -> Result<(ExecutionPlan, RelationScope), BindingError> {
		if count < 0 {
			return Err(BindingError::UnsupportedExpression {
				message: format!("range cardinality must be non-negative, got {count}"),
			});
		}
		if count > crate::ir::RANGE_MAX_CARDINALITY {
			return Err(BindingError::UnsupportedExpression {
				message: format!(
					"range cardinality {count} exceeds the materializing source's cap of {}",
					crate::ir::RANGE_MAX_CARDINALITY
				),
			});
		}
		let schema = crate::ir::range_relation_schema();
		let scope = RelationScope::scan(TableRef::new("range"), &schema);
		Ok((ExecutionPlan::Range { count, schema }, scope))
	};
	match plan {
		LogicalPlan::Range { count } => Ok(bound_range(*count)?.0),
		LogicalPlan::Project {
			input,
			exprs,
			output_names,
		} => {
			let LogicalPlan::Range { count } = input.as_ref() else {
				return Err(generated_only());
			};
			if exprs.len() != output_names.len() {
				return Err(BindingError::UnsupportedExpression {
					message: "internal: Project exprs/output_names length mismatch".to_string(),
				});
			}
			let (input_plan, scope) = bound_range(*count)?;
			let input_schema = input_plan.schema();
			let config = PlannerConfig::default();
			let session_timezone = config.resolved_session_timezone()?;
			let logical_type_specs = HashMap::new();
			let logical_type_registry = LogicalTypeRegistry::new();
			let ctx = ExprBindContext {
				config: &config,
				session_timezone,
				statement_timestamp_nanos: 0,
				logical_type_specs: &logical_type_specs,
				logical_type_registry: &logical_type_registry,
				relation_scope: Some(&scope),
			};
			let exprs = bind_expr_program_with_null_policy(
				exprs,
				&input_schema,
				&ctx,
				NullRootPolicy::MaterializeResultNull,
			)?;
			let output_schema = schema_for_exprs_with_output_names(&exprs, output_names);
			Ok(ExecutionPlan::Project {
				input: Box::new(input_plan),
				exprs,
				output_schema,
			})
		}
		_ => Err(generated_only()),
	}
}

/// ADR 572: an attached certificate must be the canonical encoding of the
/// bound CTAS source, recomputed here and compared byte for byte. Raw bytes
/// that merely fit the size bound never become the durable attachment.
pub(super) fn require_certificate_describes_bound_source(
	source: &ExecutionPlan,
	certificate: &[u8],
) -> Result<(), BindingError> {
	let mismatch = |detail: String| BindingError::UnsupportedExpression {
		message: format!(
			"materialization certificate does not describe the bound CTAS source: {detail}"
		),
	};
	let canonical = crate::canonical_expression::try_canonical_generated_relation(source)
		.map_err(|error| mismatch(error.to_string()))?;
	let encoded = canonical
		.encode_v1()
		.map_err(|error| mismatch(error.to_string()))?;
	if encoded != certificate {
		return Err(mismatch(
			"the recomputed canonical encoding differs".to_string(),
		));
	}
	Ok(())
}
