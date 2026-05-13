use crate::catalog::TableRef;
use crate::error::FrontendError;
use crate::ir::{AggregateFunction, Expr, JoinKind, JoinStrategy, LogicalPlan, ScalarValue};
use sqlparser::ast::{
	BinaryOperator, Expr as SqlExpr, FunctionArg, JoinConstraint, JoinOperator, ObjectName, Query,
	Select, SelectItem, SetExpr, Statement, TableAlias, TableFactor, Value,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

fn unsupported(message: impl Into<String>) -> FrontendError {
	FrontendError::Unsupported {
		message: message.into(),
	}
}

const DEFAULT_JOIN_PARTITION_COUNT: u32 = 4;

pub struct SqlPlanner;

impl SqlPlanner {
	pub fn new() -> Self {
		Self
	}

	pub fn plan(&self, sql: &str) -> Result<LogicalPlan, FrontendError> {
		let dialect = GenericDialect {};
		let ast = Parser::parse_sql(&dialect, sql).map_err(|e| FrontendError::SqlParse {
			message: e.to_string(),
		})?;

		if ast.is_empty() {
			return Err(unsupported("empty SQL statement"));
		}

		match &ast[0] {
			Statement::Query(query) => self.lower_query(query),
			_ => Err(unsupported(
				"unsupported SQL statement; only SELECT is supported",
			)),
		}
	}

	fn lower_query(&self, query: &Query) -> Result<LogicalPlan, FrontendError> {
		match &*query.body {
			SetExpr::Select(select) => self.lower_select(select),
			_ => Err(unsupported("unsupported query body")),
		}
	}

	fn lower_select(&self, select: &Select) -> Result<LogicalPlan, FrontendError> {
		// 1. Handle FROM (Scan)
		if select.from.is_empty() {
			return Err(unsupported("SELECT without FROM is not supported"));
		}

		if select.from.len() > 1 {
			return Err(unsupported("comma joins are not supported"));
		}

		let table_with_joins = &select.from[0];
		let mut plan = self.lower_table_factor(&table_with_joins.relation)?;
		for join in &table_with_joins.joins {
			let right = self.lower_table_factor(&join.relation)?;
			let on = match &join.join_operator {
				JoinOperator::Inner(JoinConstraint::On(expr)) => self.lower_join_on(expr)?,
				JoinOperator::Inner(_) => {
					return Err(unsupported("INNER JOIN requires ON equality predicates"));
				}
				_ => return Err(unsupported("only INNER JOIN ... ON is supported")),
			};
			let (left_keys, right_keys) = self.join_exchange_keys(&on, &plan, &right)?;
			plan = LogicalPlan::Join {
				left: Box::new(LogicalPlan::Exchange {
					input: Box::new(plan),
					partition_count: DEFAULT_JOIN_PARTITION_COUNT,
					key: left_keys,
				}),
				right: Box::new(LogicalPlan::Exchange {
					input: Box::new(right),
					partition_count: DEFAULT_JOIN_PARTITION_COUNT,
					key: right_keys,
				}),
				kind: JoinKind::Inner,
				on,
				strategy: JoinStrategy::HashPartitioned,
			};
		}

		// 2. Handle WHERE (Filter)
		if let Some(selection) = &select.selection {
			let predicate = self.lower_expr(selection)?;
			plan = LogicalPlan::Filter {
				input: Box::new(plan),
				predicate,
			};
		}

		// 3. Handle SELECT list (Project or Aggregate)
		let mut aggr_exprs = Vec::new();
		let mut is_aggregate = false;

		for item in &select.projection {
			if let SelectItem::UnnamedExpr(SqlExpr::Function(f)) = item {
				let name = f.name.to_string().to_uppercase();
				let args: &[FunctionArg] = match &f.args {
					sqlparser::ast::FunctionArguments::List(l) => &l.args,
					_ => &[],
				};

				match name.as_str() {
					"COUNT" => {
						if args.is_empty()
							|| matches!(
								&args[0],
								FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Wildcard)
							) {
							aggr_exprs
								.push((AggregateFunction::CountAll, vec![Expr::column("count")]));
						} else {
							let arg = self.lower_arg(&args[0])?;
							aggr_exprs.push((AggregateFunction::Count, vec![arg]));
						}
						is_aggregate = true;
					}
					"SUM" => {
						let arg = self.lower_arg(&args[0])?;
						aggr_exprs.push((AggregateFunction::Sum, vec![arg]));
						is_aggregate = true;
					}
					"LINEAR_REGRESS" => {
						let y = self.lower_arg(&args[0])?;
						let x = self.lower_arg(&args[1])?;
						aggr_exprs.push((AggregateFunction::LinearRegress, vec![y, x]));
						is_aggregate = true;
					}
					"APPROX_QUANTILE" => {
						let val = self.lower_arg(&args[0])?;
						let q = match &args[1] {
							FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(
								SqlExpr::Value(Value::Number(n, _)),
							)) => n.parse::<f64>().map_err(|e| unsupported(e.to_string()))?,
							_ => {
								return Err(unsupported(
									"APPROX_QUANTILE expects a numeric literal for quantile",
								));
							}
						};
						aggr_exprs.push((AggregateFunction::ApproxQuantile(q), vec![val]));
						is_aggregate = true;
					}
					_ => {}
				}
			}
		}

		if is_aggregate {
			return Ok(LogicalPlan::Aggregate {
				input: Box::new(plan),
				group_expr: vec![],
				aggr_expr: aggr_exprs,
			});
		}

		let mut exprs = Vec::new();
		for item in &select.projection {
			match item {
				SelectItem::UnnamedExpr(sql_expr) => {
					exprs.push(self.lower_expr(sql_expr)?);
				}
				SelectItem::Wildcard(_) => {
					return Ok(plan);
				}
				_ => return Err(unsupported("unsupported select item")),
			}
		}

		Ok(LogicalPlan::Project {
			input: Box::new(plan),
			exprs,
		})
	}

	fn lower_arg(&self, arg: &FunctionArg) -> Result<Expr, FrontendError> {
		match arg {
			FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(e)) => self.lower_expr(e),
			_ => Err(unsupported("unsupported function argument")),
		}
	}

	fn lower_table_factor(&self, factor: &TableFactor) -> Result<LogicalPlan, FrontendError> {
		match factor {
			TableFactor::Table { name, alias, .. } => {
				let table = table_ref_from_object_name(name);
				let plan = LogicalPlan::ScanNamed {
					table,
					projection: None,
				};
				Ok(alias_plan(plan, alias.as_ref()))
			}
			_ => Err(unsupported("unsupported table factor")),
		}
	}

	fn lower_join_on(&self, expr: &SqlExpr) -> Result<Vec<(Expr, Expr)>, FrontendError> {
		match expr {
			SqlExpr::BinaryOp { left, op, right } if *op == BinaryOperator::And => {
				let mut pairs = self.lower_join_on(left)?;
				pairs.extend(self.lower_join_on(right)?);
				Ok(pairs)
			}
			SqlExpr::BinaryOp { left, op, right } if *op == BinaryOperator::Eq => {
				Ok(vec![(self.lower_expr(left)?, self.lower_expr(right)?)])
			}
			_ => Err(unsupported("join ON only supports equality predicates")),
		}
	}

	fn lower_expr(&self, sql_expr: &SqlExpr) -> Result<Expr, FrontendError> {
		match sql_expr {
			SqlExpr::Identifier(ident) => Ok(Expr::column(ident.value.clone())),
			SqlExpr::CompoundIdentifier(idents) if idents.len() == 2 => Ok(Expr::qualified_column(
				TableRef::new(idents[0].value.clone()),
				idents[1].value.clone(),
			)),
			SqlExpr::Value(val) => match val {
				Value::Number(n, _) => {
					if let Ok(i) = n.parse::<i32>() {
						Ok(Expr::Literal(ScalarValue::Int32(Some(i))))
					} else if let Ok(f) = n.parse::<f64>() {
						Ok(Expr::Literal(ScalarValue::Float64(Some(f))))
					} else {
						Err(unsupported(format!("unsupported number literal: {n}")))
					}
				}
				Value::SingleQuotedString(s) => {
					Ok(Expr::Literal(ScalarValue::Utf8(Some(s.clone()))))
				}
				Value::Boolean(b) => Ok(Expr::Literal(ScalarValue::Boolean(Some(*b)))),
				Value::Null => Err(unsupported("explicit NULL literal not supported")),
				_ => Err(unsupported(format!("unsupported value: {val:?}"))),
			},
			SqlExpr::BinaryOp { left, op, right } => {
				let l = Box::new(self.lower_expr(left)?);
				let r = Box::new(self.lower_expr(right)?);
				match op {
					BinaryOperator::Eq => Ok(Expr::Eq(l, r)),
					BinaryOperator::NotEq => Ok(Expr::Neq(l, r)),
					BinaryOperator::Lt => Ok(Expr::Lt(l, r)),
					BinaryOperator::LtEq => Ok(Expr::Lte(l, r)),
					BinaryOperator::Gt => Ok(Expr::Gt(l, r)),
					BinaryOperator::GtEq => Ok(Expr::Gte(l, r)),
					BinaryOperator::And => Ok(Expr::And(l, r)),
					BinaryOperator::Or => Ok(Expr::Or(l, r)),
					_ => Err(unsupported(format!("unsupported operator: {op:?}"))),
				}
			}
			SqlExpr::IsNull(expr) => Ok(Expr::IsNull(Box::new(self.lower_expr(expr)?))),
			SqlExpr::IsNotNull(expr) => Ok(Expr::IsNotNull(Box::new(self.lower_expr(expr)?))),
			SqlExpr::Nested(expr) => self.lower_expr(expr),
			SqlExpr::Function(f) if f.name.to_string().to_uppercase() == "TIME_BUCKET" => {
				let args = match &f.args {
					sqlparser::ast::FunctionArguments::List(l) => &l.args,
					_ => return Err(unsupported("TIME_BUCKET expects arguments")),
				};
				let ts = self.lower_arg(&args[0])?;
				let interval = match &args[1] {
					FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(
						SqlExpr::Value(Value::SingleQuotedString(s)),
					)) => match s.as_str() {
						"1m" => std::time::Duration::from_secs(60),
						"1h" => std::time::Duration::from_secs(3600),
						"1d" => std::time::Duration::from_secs(86400),
						_ => {
							return Err(unsupported(format!(
								"unsupported time_bucket interval: {s}"
							)));
						}
					},
					_ => return Err(unsupported("time_bucket expects a string literal interval")),
				};
				Ok(Expr::TimeBucket(interval, Box::new(ts)))
			}
			_ => Err(unsupported(format!("unsupported expression: {sql_expr:?}"))),
		}
	}

	fn join_exchange_keys(
		&self,
		on: &[(Expr, Expr)],
		left: &LogicalPlan,
		right: &LogicalPlan,
	) -> Result<(Vec<Expr>, Vec<Expr>), FrontendError> {
		let left_qualifiers = plan_qualifiers(left);
		let right_qualifiers = plan_qualifiers(right);
		let mut left_keys = Vec::with_capacity(on.len());
		let mut right_keys = Vec::with_capacity(on.len());

		for (first, second) in on {
			let first_side = expr_side(first, &left_qualifiers, &right_qualifiers);
			let second_side = expr_side(second, &left_qualifiers, &right_qualifiers);
			match (first_side, second_side) {
				(Some(JoinSide::Left), Some(JoinSide::Right))
				| (Some(JoinSide::Left), None)
				| (None, Some(JoinSide::Right))
				| (None, None) => {
					left_keys.push(first.clone());
					right_keys.push(second.clone());
				}
				(Some(JoinSide::Right), Some(JoinSide::Left))
				| (Some(JoinSide::Right), None)
				| (None, Some(JoinSide::Left)) => {
					left_keys.push(second.clone());
					right_keys.push(first.clone());
				}
				_ => {
					return Err(unsupported(
						"join ON equality must reference one column from each side",
					));
				}
			}
		}

		Ok((left_keys, right_keys))
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JoinSide {
	Left,
	Right,
}

fn alias_plan(plan: LogicalPlan, alias: Option<&TableAlias>) -> LogicalPlan {
	match alias {
		Some(alias) => LogicalPlan::Alias {
			input: Box::new(plan),
			alias: TableRef::new(alias.name.value.clone()),
		},
		None => plan,
	}
}

fn plan_qualifiers(plan: &LogicalPlan) -> Vec<TableRef> {
	match plan {
		LogicalPlan::ScanNamed { table, .. } => vec![table.clone()],
		LogicalPlan::Alias { alias, .. } => vec![alias.clone()],
		LogicalPlan::Filter { input, .. }
		| LogicalPlan::Project { input, .. }
		| LogicalPlan::Aggregate { input, .. }
		| LogicalPlan::Exchange { input, .. } => plan_qualifiers(input),
		LogicalPlan::Join { left, right, .. } => {
			let mut qualifiers = plan_qualifiers(left);
			qualifiers.extend(plan_qualifiers(right));
			qualifiers
		}
		LogicalPlan::Scan { .. } => Vec::new(),
	}
}

fn expr_side(expr: &Expr, left: &[TableRef], right: &[TableRef]) -> Option<JoinSide> {
	let Expr::Column(column_ref) = expr else {
		return None;
	};
	let qualifier = column_ref.qualifier.as_ref()?;
	if left.iter().any(|left_ref| left_ref == qualifier) {
		Some(JoinSide::Left)
	} else if right.iter().any(|right_ref| right_ref == qualifier) {
		Some(JoinSide::Right)
	} else {
		None
	}
}

fn table_ref_from_object_name(name: &ObjectName) -> TableRef {
	let parts = object_name_parts(name);
	match parts.as_slice() {
		[schema, table] => TableRef::with_schema(schema.clone(), table.clone()),
		[table] => TableRef::new(table.clone()),
		_ => TableRef::new(name.to_string()),
	}
}

fn object_name_parts(name: &ObjectName) -> Vec<String> {
	name.to_string()
		.split('.')
		.map(|part| part.trim_matches('"').to_string())
		.collect()
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn sql_inner_join_emits_explicit_hash_exchanges() {
		let plan = SqlPlanner::new()
			.plan("SELECT * FROM orders INNER JOIN customers ON orders.customer_id = customers.id")
			.expect("plan join");

		match plan {
			LogicalPlan::Join {
				left,
				right,
				kind,
				strategy,
				on,
			} => {
				assert_eq!(kind, JoinKind::Inner);
				assert_eq!(strategy, JoinStrategy::HashPartitioned);
				assert_eq!(on.len(), 1);
				assert!(matches!(
					*left,
					LogicalPlan::Exchange {
						partition_count: DEFAULT_JOIN_PARTITION_COUNT,
						..
					}
				));
				assert!(matches!(
					*right,
					LogicalPlan::Exchange {
						partition_count: DEFAULT_JOIN_PARTITION_COUNT,
						..
					}
				));
			}
			other => panic!("expected join plan, got {other:?}"),
		}
	}
}
