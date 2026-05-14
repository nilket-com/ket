use crate::catalog::TableRef;
use crate::error::FrontendError;
use crate::ir::{AggregateFunction, Expr, JoinKind, JoinStrategy, LogicalPlan, ScalarValue};
use sqlparser::ast::{
	BinaryOperator, Expr as SqlExpr, FunctionArg, FunctionArgExpr, GroupByExpr, JoinConstraint,
	JoinOperator, ObjectName, Query, Select, SelectItem, SetExpr, Statement, TableAlias,
	TableFactor, Value,
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
			let (kind, on) = self.lower_join_operator(&join.join_operator)?;
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
				kind,
				on,
				strategy: JoinStrategy::HashPartitioned,
			};
		}

		// 2. Handle WHERE (Filter / correlated EXISTS decorrelation)
		if let Some(selection) = &select.selection {
			plan = self.lower_where(plan, selection)?;
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

	fn lower_join_operator(
		&self,
		operator: &JoinOperator,
	) -> Result<(JoinKind, Vec<(Expr, Expr)>), FrontendError> {
		let (kind, constraint) = match operator {
			JoinOperator::Inner(constraint) => (JoinKind::Inner, constraint),
			JoinOperator::LeftOuter(constraint) => (JoinKind::LeftOuter, constraint),
			JoinOperator::RightOuter(constraint) => (JoinKind::RightOuter, constraint),
			JoinOperator::FullOuter(constraint) => (JoinKind::FullOuter, constraint),
			JoinOperator::Semi(constraint) | JoinOperator::LeftSemi(constraint) => {
				(JoinKind::LeftSemi, constraint)
			}
			JoinOperator::RightSemi(constraint) => (JoinKind::RightSemi, constraint),
			JoinOperator::Anti(constraint) | JoinOperator::LeftAnti(constraint) => {
				(JoinKind::LeftAnti, constraint)
			}
			JoinOperator::RightAnti(constraint) => (JoinKind::RightAnti, constraint),
			_ => {
				return Err(unsupported(
					"only INNER/OUTER/SEMI/ANTI JOIN ... ON is supported",
				));
			}
		};

		match constraint {
			JoinConstraint::On(expr) => Ok((kind, self.lower_join_on(expr)?)),
			_ => Err(unsupported("JOIN requires ON equality predicates")),
		}
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

	fn lower_where(
		&self,
		mut plan: LogicalPlan,
		selection: &SqlExpr,
	) -> Result<LogicalPlan, FrontendError> {
		let mut residuals = Vec::new();
		for conjunct in split_top_level_and(selection) {
			match strip_nested_sql_expr(conjunct) {
				SqlExpr::Exists { subquery, negated } => {
					plan = self.lower_exists_join(plan, subquery, *negated)?;
				}
				_ => residuals.push(self.lower_expr(conjunct)?),
			}
		}

		if let Some(predicate) = combine_and_exprs(residuals) {
			plan = LogicalPlan::Filter {
				input: Box::new(plan),
				predicate,
			};
		}

		Ok(plan)
	}

	fn lower_exists_join(
		&self,
		outer_plan: LogicalPlan,
		subquery: &Query,
		negated: bool,
	) -> Result<LogicalPlan, FrontendError> {
		validate_exists_query_shape(subquery)?;
		let SetExpr::Select(select) = &*subquery.body else {
			return Err(unsupported("unsupported EXISTS subquery body"));
		};
		validate_exists_select_shape(select)?;
		validate_exists_projection(&select.projection)?;

		let table_with_joins = select
			.from
			.first()
			.expect("exists select shape has one FROM item");
		let mut inner_plan = self.lower_table_factor(&table_with_joins.relation)?;
		let outer_qualifiers = plan_qualifiers(&outer_plan);
		let inner_qualifiers = plan_qualifiers(&inner_plan);
		let mut on = Vec::new();
		let mut inner_residuals = Vec::new();

		if let Some(selection) = &select.selection {
			for conjunct in split_top_level_and(selection) {
				match self.classify_exists_conjunct(
					conjunct,
					&outer_qualifiers,
					&inner_qualifiers,
				)? {
					ExistsConjunct::CorrelatedPair { outer, inner } => on.push((outer, inner)),
					ExistsConjunct::InnerResidual(predicate) => inner_residuals.push(predicate),
				}
			}
		}

		if on.is_empty() {
			return Err(unsupported("uncorrelated EXISTS is not supported"));
		}

		if let Some(predicate) = combine_and_exprs(inner_residuals) {
			inner_plan = LogicalPlan::Filter {
				input: Box::new(inner_plan),
				predicate,
			};
		}

		let left_key = on.iter().map(|(outer, _)| outer.clone()).collect();
		let right_key = on.iter().map(|(_, inner)| inner.clone()).collect();
		Ok(LogicalPlan::Join {
			left: Box::new(LogicalPlan::Exchange {
				input: Box::new(outer_plan),
				partition_count: DEFAULT_JOIN_PARTITION_COUNT,
				key: left_key,
			}),
			right: Box::new(LogicalPlan::Exchange {
				input: Box::new(inner_plan),
				partition_count: DEFAULT_JOIN_PARTITION_COUNT,
				key: right_key,
			}),
			kind: if negated {
				JoinKind::LeftAnti
			} else {
				JoinKind::LeftSemi
			},
			on,
			strategy: JoinStrategy::HashPartitioned,
		})
	}

	fn classify_exists_conjunct(
		&self,
		conjunct: &SqlExpr,
		outer_qualifiers: &[TableRef],
		inner_qualifiers: &[TableRef],
	) -> Result<ExistsConjunct, FrontendError> {
		let conjunct = strip_nested_sql_expr(conjunct);
		if let SqlExpr::BinaryOp { left, op, right } = conjunct {
			if *op == BinaryOperator::Eq {
				let left_column = scoped_column_expr(left, outer_qualifiers, inner_qualifiers)?;
				let right_column = scoped_column_expr(right, outer_qualifiers, inner_qualifiers)?;
				match (left_column, right_column) {
					(Some((ColumnScope::Outer, outer)), Some((ColumnScope::Inner, inner)))
					| (Some((ColumnScope::Inner, inner)), Some((ColumnScope::Outer, outer))) => {
						return Ok(ExistsConjunct::CorrelatedPair { outer, inner });
					}
					_ => {}
				}
			}
		}

		let mut scope_refs = ScopeRefs::default();
		collect_scope_refs(
			conjunct,
			outer_qualifiers,
			inner_qualifiers,
			&mut scope_refs,
		)?;
		if scope_refs.outer || scope_refs.unsupported {
			return Err(unsupported("unsupported EXISTS subquery predicate"));
		}

		Ok(ExistsConjunct::InnerResidual(self.lower_expr(conjunct)?))
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

#[derive(Debug)]
enum ExistsConjunct {
	CorrelatedPair { outer: Expr, inner: Expr },
	InnerResidual(Expr),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ColumnScope {
	Outer,
	Inner,
}

#[derive(Default)]
struct ScopeRefs {
	outer: bool,
	unsupported: bool,
}

fn split_top_level_and(expr: &SqlExpr) -> Vec<&SqlExpr> {
	let expr = strip_nested_sql_expr(expr);
	match expr {
		SqlExpr::BinaryOp { left, op, right } if *op == BinaryOperator::And => {
			let mut conjuncts = split_top_level_and(left);
			conjuncts.extend(split_top_level_and(right));
			conjuncts
		}
		_ => vec![expr],
	}
}

fn combine_and_exprs(exprs: Vec<Expr>) -> Option<Expr> {
	let mut exprs = exprs.into_iter();
	let first = exprs.next()?;
	Some(exprs.fold(first, |left, right| {
		Expr::And(Box::new(left), Box::new(right))
	}))
}

fn validate_exists_query_shape(query: &Query) -> Result<(), FrontendError> {
	if query.with.is_some()
		|| query.order_by.is_some()
		|| query.limit.is_some()
		|| !query.limit_by.is_empty()
		|| query.offset.is_some()
		|| query.fetch.is_some()
		|| !query.locks.is_empty()
		|| query.for_clause.is_some()
		|| query.settings.is_some()
		|| query.format_clause.is_some()
	{
		return Err(unsupported("unsupported EXISTS subquery shape"));
	}
	Ok(())
}

fn validate_exists_select_shape(select: &Select) -> Result<(), FrontendError> {
	if select.distinct.is_some()
		|| select.top.is_some()
		|| select.into.is_some()
		|| select.from.len() != 1
		|| !select.from[0].joins.is_empty()
		|| !select.lateral_views.is_empty()
		|| select.prewhere.is_some()
		|| !group_by_is_empty(&select.group_by)
		|| !select.cluster_by.is_empty()
		|| !select.distribute_by.is_empty()
		|| !select.sort_by.is_empty()
		|| select.having.is_some()
		|| !select.named_window.is_empty()
		|| select.qualify.is_some()
		|| select.value_table_mode.is_some()
		|| select.connect_by.is_some()
	{
		return Err(unsupported("unsupported EXISTS subquery shape"));
	}
	Ok(())
}

fn validate_exists_projection(projection: &[SelectItem]) -> Result<(), FrontendError> {
	for item in projection {
		match item {
			SelectItem::Wildcard(_) => {}
			SelectItem::UnnamedExpr(expr) if exists_projection_expr_supported(expr) => {}
			_ => return Err(unsupported("unsupported EXISTS subquery projection")),
		}
	}
	Ok(())
}

fn exists_projection_expr_supported(expr: &SqlExpr) -> bool {
	match strip_nested_sql_expr(expr) {
		SqlExpr::Identifier(_)
		| SqlExpr::CompoundIdentifier(_)
		| SqlExpr::Value(Value::Number(_, _))
		| SqlExpr::Value(Value::SingleQuotedString(_))
		| SqlExpr::Value(Value::Boolean(_)) => true,
		_ => false,
	}
}

fn group_by_is_empty(group_by: &GroupByExpr) -> bool {
	match group_by {
		GroupByExpr::Expressions(exprs, modifiers) => exprs.is_empty() && modifiers.is_empty(),
		GroupByExpr::All(_) => false,
	}
}

fn strip_nested_sql_expr(expr: &SqlExpr) -> &SqlExpr {
	match expr {
		SqlExpr::Nested(inner) => strip_nested_sql_expr(inner),
		_ => expr,
	}
}

fn scoped_column_expr(
	expr: &SqlExpr,
	outer_qualifiers: &[TableRef],
	inner_qualifiers: &[TableRef],
) -> Result<Option<(ColumnScope, Expr)>, FrontendError> {
	let Some((qualifier, column)) = sql_column_ref(expr)? else {
		return Ok(None);
	};
	let scope = resolve_column_scope(&qualifier, outer_qualifiers, inner_qualifiers)?;
	Ok(Some((scope, Expr::qualified_column(qualifier, column))))
}

fn sql_column_ref(expr: &SqlExpr) -> Result<Option<(TableRef, String)>, FrontendError> {
	match strip_nested_sql_expr(expr) {
		SqlExpr::CompoundIdentifier(idents) if idents.len() == 2 => Ok(Some((
			TableRef::new(idents[0].value.clone()),
			idents[1].value.clone(),
		))),
		SqlExpr::Identifier(_) | SqlExpr::CompoundIdentifier(_) => {
			Err(unsupported("unsupported EXISTS column reference"))
		}
		_ => Ok(None),
	}
}

fn resolve_column_scope(
	qualifier: &TableRef,
	outer_qualifiers: &[TableRef],
	inner_qualifiers: &[TableRef],
) -> Result<ColumnScope, FrontendError> {
	let outer_matches = outer_qualifiers
		.iter()
		.filter(|candidate| *candidate == qualifier)
		.count();
	let inner_matches = inner_qualifiers
		.iter()
		.filter(|candidate| *candidate == qualifier)
		.count();
	match (outer_matches, inner_matches) {
		(1, 0) => Ok(ColumnScope::Outer),
		(0, 1) => Ok(ColumnScope::Inner),
		_ => Err(unsupported("unsupported EXISTS column scope")),
	}
}

fn collect_scope_refs(
	expr: &SqlExpr,
	outer_qualifiers: &[TableRef],
	inner_qualifiers: &[TableRef],
	refs: &mut ScopeRefs,
) -> Result<(), FrontendError> {
	match strip_nested_sql_expr(expr) {
		SqlExpr::Identifier(_) | SqlExpr::CompoundIdentifier(_) => {
			let Some((qualifier, _)) = sql_column_ref(expr)? else {
				return Ok(());
			};
			match resolve_column_scope(&qualifier, outer_qualifiers, inner_qualifiers) {
				Ok(ColumnScope::Outer) => refs.outer = true,
				Ok(ColumnScope::Inner) => {}
				Err(_) => refs.unsupported = true,
			}
		}
		SqlExpr::Exists { .. } | SqlExpr::Subquery(_) => refs.unsupported = true,
		SqlExpr::BinaryOp { left, right, .. } => {
			collect_scope_refs(left, outer_qualifiers, inner_qualifiers, refs)?;
			collect_scope_refs(right, outer_qualifiers, inner_qualifiers, refs)?;
		}
		SqlExpr::IsNull(inner) | SqlExpr::IsNotNull(inner) => {
			collect_scope_refs(inner, outer_qualifiers, inner_qualifiers, refs)?;
		}
		SqlExpr::Function(function) => match &function.args {
			sqlparser::ast::FunctionArguments::List(list) => {
				for arg in &list.args {
					match arg {
						FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))
						| FunctionArg::Named {
							arg: FunctionArgExpr::Expr(expr),
							..
						}
						| FunctionArg::ExprNamed {
							arg: FunctionArgExpr::Expr(expr),
							..
						} => {
							collect_scope_refs(expr, outer_qualifiers, inner_qualifiers, refs)?;
						}
						FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => {}
						_ => refs.unsupported = true,
					}
				}
			}
			sqlparser::ast::FunctionArguments::Subquery(_) => refs.unsupported = true,
			_ => {}
		},
		SqlExpr::Value(_) => {}
		_ => {}
	}
	Ok(())
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
		LogicalPlan::Join {
			left, right, kind, ..
		} => match kind {
			JoinKind::LeftSemi | JoinKind::LeftAnti => plan_qualifiers(left),
			JoinKind::RightSemi | JoinKind::RightAnti => plan_qualifiers(right),
			JoinKind::Inner | JoinKind::LeftOuter | JoinKind::RightOuter | JoinKind::FullOuter => {
				let mut qualifiers = plan_qualifiers(left);
				qualifiers.extend(plan_qualifiers(right));
				qualifiers
			}
		},
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
	fn classifier_recognizes_correlated_equi_pair() {
		let planner = SqlPlanner::new();
		let classified = planner
			.classify_exists_conjunct(
				&parse_selection("SELECT * FROM fact WHERE dim.k = fact.k"),
				&[TableRef::new("fact")],
				&[TableRef::new("dim")],
			)
			.expect("classify");

		match classified {
			ExistsConjunct::CorrelatedPair { outer, inner } => {
				assert_qualified_column(&outer, "fact", "k");
				assert_qualified_column(&inner, "dim", "k");
			}
			ExistsConjunct::InnerResidual(_) => panic!("expected correlated pair"),
		}
	}

	#[test]
	fn classifier_recognizes_inner_only_residual_including_literals() {
		let planner = SqlPlanner::new();
		for sql in [
			"SELECT * FROM fact WHERE dim.active = true",
			"SELECT * FROM fact WHERE dim.score > 10",
		] {
			let classified = planner
				.classify_exists_conjunct(
					&parse_selection(sql),
					&[TableRef::new("fact")],
					&[TableRef::new("dim")],
				)
				.expect("classify residual");

			assert!(
				matches!(classified, ExistsConjunct::InnerResidual(_)),
				"{sql}"
			);
		}
	}

	#[test]
	fn classifier_rejects_outer_only_subquery_predicate() {
		assert_classifier_rejects("SELECT * FROM fact WHERE fact.flag = true");
	}

	#[test]
	fn classifier_rejects_non_equi_and_computed_correlation() {
		assert_classifier_rejects("SELECT * FROM fact WHERE dim.k > fact.k");
		assert_classifier_rejects("SELECT * FROM fact WHERE dim.k + 1 = fact.k");
	}

	#[test]
	fn classifier_rejects_unqualified_ambiguous_correlation() {
		assert_classifier_rejects("SELECT * FROM fact WHERE k = fact.k");
		let planner = SqlPlanner::new();
		let err = planner
			.classify_exists_conjunct(
				&parse_selection("SELECT * FROM fact WHERE t.k = t.k"),
				&[TableRef::new("t")],
				&[TableRef::new("t")],
			)
			.expect_err("same-table unaliased self-correlation rejects");
		assert!(err.to_string().contains("unsupported"));

		let err = planner
			.classify_exists_conjunct(
				&parse_selection("SELECT * FROM fact WHERE dim.k = fact.k"),
				&[TableRef::new("fact"), TableRef::new("fact")],
				&[TableRef::new("dim")],
			)
			.expect_err("duplicate outer qualifier rejects");
		assert!(err.to_string().contains("unsupported"));
	}

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

	fn assert_classifier_rejects(sql: &str) {
		let planner = SqlPlanner::new();
		let err = planner
			.classify_exists_conjunct(
				&parse_selection(sql),
				&[TableRef::new("fact")],
				&[TableRef::new("dim")],
			)
			.expect_err("classifier should reject");
		assert!(err.to_string().contains("unsupported"), "{sql}: {err}");
	}

	fn parse_selection(sql: &str) -> SqlExpr {
		let dialect = GenericDialect {};
		let ast = Parser::parse_sql(&dialect, sql).expect("parse sql");
		let Statement::Query(query) = ast.into_iter().next().expect("statement") else {
			panic!("expected query");
		};
		let SetExpr::Select(select) = *query.body else {
			panic!("expected select");
		};
		select.selection.expect("selection")
	}

	fn assert_qualified_column(expr: &Expr, qualifier: &str, name: &str) {
		let Expr::Column(column) = expr else {
			panic!("expected column, got {expr:?}");
		};
		assert_eq!(column.qualifier.as_ref(), Some(&TableRef::new(qualifier)));
		assert_eq!(column.name.0, name);
	}
}
