use super::*;

pub(super) fn reject_storage_synthetic_returning_reference(
	expr: &Expr,
) -> Result<(), BindingError> {
	match expr {
		Expr::Column(column)
			if STORAGE_SYNTHETIC_COLUMN_NAMES.contains(&column.name.0.as_str()) =>
		{
			Err(BindingError::UnsupportedExpression {
				message: format!(
					"RETURNING column is reserved for storage metadata: {}",
					column.name.0
				),
			})
		}
		Expr::Column(_) | Expr::Literal(_) | Expr::UntypedNull => Ok(()),
		Expr::LogicalAscription { expr: inner, .. }
		| Expr::SqlLogicalCast { expr: inner, .. }
		| Expr::SqlConvert { expr: inner, .. }
		| Expr::Cast(inner, _)
		| Expr::TryCast(inner, _)
		| Expr::Not(inner)
		| Expr::IsNull(inner)
		| Expr::IsNotNull(inner)
		| Expr::Like { expr: inner, .. }
		| Expr::TimeBucket(_, inner)
		| Expr::AtTimeZone(inner, _)
		| Expr::Extract(_, inner)
		| Expr::DateTrunc(_, inner) => reject_storage_synthetic_returning_reference(inner),
		Expr::ScalarFunction { args, .. } => {
			for arg in args {
				reject_storage_synthetic_returning_reference(arg)?;
			}
			Ok(())
		}
		Expr::InList { expr, list, .. } => {
			reject_storage_synthetic_returning_reference(expr)?;
			for item in list {
				reject_storage_synthetic_returning_reference(item)?;
			}
			Ok(())
		}
		Expr::Case {
			when_then,
			else_expr,
		} => {
			for (condition, result) in when_then {
				reject_storage_synthetic_returning_reference(condition)?;
				reject_storage_synthetic_returning_reference(result)?;
			}
			if let Some(else_expr) = else_expr {
				reject_storage_synthetic_returning_reference(else_expr)?;
			}
			Ok(())
		}
		Expr::Add(left, right)
		| Expr::Sub(left, right)
		| Expr::Mul(left, right)
		| Expr::Div(left, right)
		| Expr::Eq(left, right)
		| Expr::Neq(left, right)
		| Expr::Lt(left, right)
		| Expr::Lte(left, right)
		| Expr::Gt(left, right)
		| Expr::Gte(left, right)
		| Expr::And(left, right)
		| Expr::Or(left, right) => {
			reject_storage_synthetic_returning_reference(left)?;
			reject_storage_synthetic_returning_reference(right)
		}
	}
}

pub(super) fn collect_logical_cast_names(
	expr: &Expr,
	names: &mut HashSet<String>,
) -> Result<(), BindingError> {
	match expr {
		Expr::Column(_) | Expr::Literal(_) | Expr::UntypedNull => Ok(()),
		Expr::SqlLogicalCast {
			expr,
			logical_name,
			try_cast: _,
		} => {
			names.insert(logical_name.clone());
			collect_logical_cast_names(expr, names)
		}
		Expr::SqlConvert {
			expr,
			target_logical_name,
		} => {
			names.insert(target_logical_name.clone());
			collect_logical_cast_names(expr, names)
		}
		Expr::LogicalAscription { expr, .. }
		| Expr::Cast(expr, _)
		| Expr::TryCast(expr, _)
		| Expr::Not(expr)
		| Expr::IsNull(expr)
		| Expr::IsNotNull(expr)
		| Expr::Like { expr, .. }
		| Expr::TimeBucket(_, expr)
		| Expr::AtTimeZone(expr, _)
		| Expr::Extract(_, expr)
		| Expr::DateTrunc(_, expr) => collect_logical_cast_names(expr, names),
		Expr::ScalarFunction { args, .. } => {
			for arg in args {
				collect_logical_cast_names(arg, names)?;
			}
			Ok(())
		}
		Expr::InList { expr, list, .. } => {
			collect_logical_cast_names(expr, names)?;
			for item in list {
				collect_logical_cast_names(item, names)?;
			}
			Ok(())
		}
		Expr::Case {
			when_then,
			else_expr,
		} => {
			for (condition, result) in when_then {
				collect_logical_cast_names(condition, names)?;
				collect_logical_cast_names(result, names)?;
			}
			if let Some(else_expr) = else_expr {
				collect_logical_cast_names(else_expr, names)?;
			}
			Ok(())
		}
		Expr::Add(left, right)
		| Expr::Sub(left, right)
		| Expr::Mul(left, right)
		| Expr::Div(left, right)
		| Expr::Eq(left, right)
		| Expr::Neq(left, right)
		| Expr::Lt(left, right)
		| Expr::Lte(left, right)
		| Expr::Gt(left, right)
		| Expr::Gte(left, right)
		| Expr::And(left, right)
		| Expr::Or(left, right) => {
			collect_logical_cast_names(left, names)?;
			collect_logical_cast_names(right, names)
		}
	}
}
