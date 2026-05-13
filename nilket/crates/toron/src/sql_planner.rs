use crate::catalog::TableRef;
use crate::error::FrontendError;
use crate::ir::{AggregateFunction, Expr, LogicalPlan, ScalarValue};
use sqlparser::ast::{
	BinaryOperator, Expr as SqlExpr, FunctionArg, Query, Select, SelectItem, SetExpr, Statement,
	TableFactor, Value,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

fn unsupported(message: impl Into<String>) -> FrontendError {
	FrontendError::Unsupported {
		message: message.into(),
	}
}

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

		let table_with_joins = &select.from[0];
		let table_name = match &table_with_joins.relation {
			TableFactor::Table { name, .. } => name.to_string(),
			_ => return Err(unsupported("unsupported table factor")),
		};

		let mut plan = LogicalPlan::ScanNamed {
			table: TableRef::new(table_name),
			projection: None,
		};

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
							aggr_exprs.push((
								AggregateFunction::CountAll,
								vec![Expr::Column("count".to_string())],
							));
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

	fn lower_expr(&self, sql_expr: &SqlExpr) -> Result<Expr, FrontendError> {
		match sql_expr {
			SqlExpr::Identifier(ident) => Ok(Expr::Column(ident.value.clone())),
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
}
