use std::collections::HashMap;

use crate::catalog::TableRef;
use crate::error::FrontendError;
use crate::ir::{
	AggregateFunction, ColumnRef, Expr, JoinKind, JoinStrategy, LogicalPlan, ScalarValue, SortExpr,
};
use sqlparser::ast::{
	BinaryOperator, Expr as SqlExpr, Function, FunctionArg, FunctionArgExpr, FunctionArgumentList,
	FunctionArguments, GroupByExpr, Ident, JoinConstraint, JoinOperator, ObjectName, OrderBy,
	Query, Select, SelectItem, SetExpr, Statement, TableAlias, TableFactor, Value,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

fn unsupported(message: impl Into<String>) -> FrontendError {
	FrontendError::Unsupported {
		message: message.into(),
	}
}

const DEFAULT_JOIN_PARTITION_COUNT: u32 = 4;
// P0.3: grouped SQL reuses the join shuffle fanout until aggregate fanout has separate evidence.
const DEFAULT_AGGREGATE_PARTITION_COUNT: u32 = DEFAULT_JOIN_PARTITION_COUNT;

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
		let Query {
			with,
			body,
			order_by,
			limit,
			limit_by,
			offset,
			fetch,
			locks,
			for_clause,
			settings,
			format_clause,
		} = query;

		if with.is_some() {
			return Err(unsupported("WITH is not supported"));
		}
		if !limit_by.is_empty() {
			return Err(unsupported("LIMIT BY is not supported"));
		}
		if fetch.is_some() {
			return Err(unsupported("FETCH is not supported"));
		}
		if !locks.is_empty() {
			return Err(unsupported("locking clauses are not supported"));
		}
		if for_clause.is_some() {
			return Err(unsupported("FOR clause is not supported"));
		}
		if settings.is_some() {
			return Err(unsupported("query settings are not supported"));
		}
		if format_clause.is_some() {
			return Err(unsupported("FORMAT clause is not supported"));
		}
		if order_by.is_none() && (limit.is_some() || offset.is_some()) {
			return Err(unsupported(
				"LIMIT/OFFSET without ORDER BY is not supported",
			));
		}

		let LoweredSelect {
			plan,
			grouped_aggregate_scope,
		} = match &**body {
			SetExpr::Select(select) => self.lower_select(select)?,
			_ => return Err(unsupported("unsupported query body")),
		};

		if let Some(order_by) = order_by {
			let order_by = self.lower_order_by(order_by, grouped_aggregate_scope.as_ref())?;
			let limit = limit
				.as_ref()
				.map(parse_non_negative_usize_literal)
				.transpose()?;
			let offset = offset
				.as_ref()
				.map(|offset| parse_non_negative_usize_literal(&offset.value))
				.transpose()?
				.unwrap_or(0);
			Ok(LogicalPlan::Sort {
				input: Box::new(LogicalPlan::Exchange {
					input: Box::new(plan),
					partition_count: 1,
					key: Vec::new(),
				}),
				order_by,
				limit,
				offset,
			})
		} else {
			Ok(plan)
		}
	}

	fn lower_select(&self, select: &Select) -> Result<LoweredSelect, FrontendError> {
		let Select {
			select_token,
			distinct,
			top,
			top_before_distinct,
			projection,
			into,
			from,
			lateral_views,
			prewhere,
			selection,
			group_by,
			cluster_by,
			distribute_by,
			sort_by,
			having,
			named_window,
			qualify,
			window_before_qualify,
			value_table_mode,
			connect_by,
		} = select;
		let _parser_metadata_only = (select_token, top_before_distinct, window_before_qualify);

		if distinct.is_some() {
			return Err(unsupported("DISTINCT is not supported"));
		}
		if top.is_some() {
			return Err(unsupported("TOP is not supported"));
		}
		if into.is_some() {
			return Err(unsupported("SELECT INTO is not supported"));
		}
		if !lateral_views.is_empty() {
			return Err(unsupported("LATERAL VIEW is not supported"));
		}
		if prewhere.is_some() {
			return Err(unsupported("PREWHERE is not supported"));
		}
		if !cluster_by.is_empty() {
			return Err(unsupported("CLUSTER BY is not supported"));
		}
		if !distribute_by.is_empty() {
			return Err(unsupported("DISTRIBUTE BY is not supported"));
		}
		if !sort_by.is_empty() {
			return Err(unsupported("SORT BY is not supported"));
		}
		if !named_window.is_empty() {
			return Err(unsupported("WINDOW is not supported"));
		}
		if qualify.is_some() {
			return Err(unsupported("QUALIFY is not supported"));
		}
		if value_table_mode.is_some() {
			return Err(unsupported("SELECT AS VALUE/STRUCT is not supported"));
		}
		if connect_by.is_some() {
			return Err(unsupported("CONNECT BY is not supported"));
		}
		let group_keys = if group_by_is_empty(group_by) {
			None
		} else {
			Some(lower_group_by_keys(group_by)?)
		};
		if having.is_some()
			&& group_keys.is_none()
			&& !projection_contains_aggregate_call(projection)
		{
			return Err(unsupported(
				"HAVING requires GROUP BY or an aggregate projection",
			));
		}

		// 1. Handle FROM (Scan)
		if from.is_empty() {
			return Err(unsupported("SELECT without FROM is not supported"));
		}

		if from.len() > 1 {
			return Err(unsupported("comma joins are not supported"));
		}

		let table_with_joins = &from[0];
		if group_keys.is_some() && !table_with_joins.joins.is_empty() {
			return Err(unsupported("GROUP BY over joins is not supported"));
		}
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
		if let Some(selection) = selection {
			plan = self.lower_where(plan, selection)?;
		}
		if group_keys.is_some() && plan_contains_join(&plan) {
			return Err(unsupported("GROUP BY over joins is not supported"));
		}

		// 3. Handle SELECT list (Project or Aggregate)
		if let Some(group_keys) = group_keys {
			let group_expr = group_keys
				.iter()
				.map(|key| key.expr.clone())
				.collect::<Vec<_>>();
			let GroupedProjection {
				aggr_expr,
				grouped_scope,
			} = self.lower_grouped_projection(projection, &group_keys)?;
			let having_predicate = having
				.as_ref()
				.map(|having| self.lower_having_predicate(having, &grouped_scope))
				.transpose()?;
			if aggr_expr.is_empty() {
				return Err(unsupported("GROUP BY requires at least one aggregate call"));
			}
			let aggregate = LogicalPlan::Aggregate {
				input: Box::new(LogicalPlan::Exchange {
					input: Box::new(plan),
					partition_count: DEFAULT_AGGREGATE_PARTITION_COUNT,
					key: group_expr.clone(),
				}),
				group_expr,
				aggr_expr,
			};
			let plan = if let Some(predicate) = having_predicate {
				LogicalPlan::Filter {
					input: Box::new(aggregate),
					predicate,
				}
			} else {
				aggregate
			};
			return Ok(LoweredSelect {
				plan,
				grouped_aggregate_scope: Some(grouped_scope),
			});
		}

		if projection_contains_aggregate_call(projection) {
			// P0.5: accepted global aggregates keep the pre-ADR single-stage shape; no
			// distributed exchange is inserted for the accounting refactor.
			let aggr_expr = self.lower_global_aggregate_projection(projection)?;
			let scope = GroupedAggregateScope::from_projection(&[], &aggr_expr);
			let aggregate = LogicalPlan::Aggregate {
				input: Box::new(plan),
				group_expr: vec![],
				aggr_expr,
			};
			let plan = if let Some(having) = having.as_ref() {
				let predicate = self.lower_having_predicate(having, &scope)?;
				LogicalPlan::Filter {
					input: Box::new(aggregate),
					predicate,
				}
			} else {
				aggregate
			};
			return Ok(LoweredSelect {
				plan,
				grouped_aggregate_scope: Some(scope),
			});
		}

		if matches!(projection.as_slice(), [SelectItem::Wildcard(_)]) {
			return Ok(LoweredSelect {
				plan,
				grouped_aggregate_scope: None,
			});
		}

		let mut exprs = Vec::new();
		for item in projection {
			match item {
				SelectItem::UnnamedExpr(sql_expr) => {
					exprs.push(self.lower_expr(sql_expr)?);
				}
				SelectItem::Wildcard(_) => {
					return Err(unsupported("mixed wildcard projections are not supported"));
				}
				_ => return Err(unsupported("unsupported select item")),
			}
		}

		Ok(LoweredSelect {
			plan: LogicalPlan::Project {
				input: Box::new(plan),
				exprs,
			},
			grouped_aggregate_scope: None,
		})
	}

	fn lower_order_by(
		&self,
		order_by: &OrderBy,
		scope: Option<&GroupedAggregateScope>,
	) -> Result<Vec<SortExpr>, FrontendError> {
		if order_by.interpolate.is_some() {
			return Err(unsupported("ORDER BY INTERPOLATE is not supported"));
		}
		if order_by.exprs.is_empty() {
			return Err(unsupported("ORDER BY requires at least one expression"));
		}

		order_by
			.exprs
			.iter()
			.map(|expr| {
				if expr.with_fill.is_some() {
					return Err(unsupported("ORDER BY WITH FILL is not supported"));
				}
				let sql_expr = if let Some(scope) = scope {
					self.rewrite_grouped_aggregate_calls(&expr.expr, scope, "ORDER BY")?
				} else {
					expr.expr.clone()
				};
				Ok(SortExpr {
					expr: self.lower_expr(&sql_expr)?,
					descending: expr.asc == Some(false),
					nulls_first: expr.nulls_first.unwrap_or(false),
				})
			})
			.collect()
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

	fn lower_global_aggregate_projection(
		&self,
		projection: &[SelectItem],
	) -> Result<Vec<(AggregateFunction, Vec<Expr>)>, FrontendError> {
		projection
			.iter()
			.map(|item| match item {
				SelectItem::UnnamedExpr(SqlExpr::Function(function)) => {
					self.lower_aggregate_function(function, AggregateSqlMode::Global)
				}
				_ => Err(unsupported("unsupported aggregate projection item")),
			})
			.collect()
	}

	fn lower_grouped_projection(
		&self,
		projection: &[SelectItem],
		group_keys: &[GroupKey],
	) -> Result<GroupedProjection, FrontendError> {
		for (index, key) in group_keys.iter().enumerate() {
			let item = projection.get(index).ok_or_else(|| {
				unsupported("GROUP BY projection must list all keys in order before aggregates")
			})?;
			let projected = projection_group_key_name(item)?;
			if projected != key.name {
				return Err(unsupported(
					"GROUP BY projection must list all keys in order before aggregates",
				));
			}
		}

		let aggr_expr = projection
			.iter()
			.skip(group_keys.len())
			.map(|item| match item {
				SelectItem::UnnamedExpr(SqlExpr::Function(function)) => {
					self.lower_aggregate_function(function, AggregateSqlMode::Grouped)
				}
				_ => Err(unsupported(
					"GROUP BY projection must list all keys in order before aggregates",
				)),
			})
			.collect::<Result<Vec<_>, _>>()?;

		if aggr_expr.is_empty() {
			return Ok(GroupedProjection {
				aggr_expr,
				grouped_scope: GroupedAggregateScope::from_projection(group_keys, &[]),
			});
		}
		let grouped_scope = GroupedAggregateScope::from_projection(group_keys, &aggr_expr);
		Ok(GroupedProjection {
			aggr_expr,
			grouped_scope,
		})
	}

	fn lower_having_predicate(
		&self,
		having: &SqlExpr,
		scope: &GroupedAggregateScope,
	) -> Result<Expr, FrontendError> {
		reject_qualified_having_column_references(having)?;
		let rewritten = self.rewrite_grouped_aggregate_calls(having, scope, "HAVING")?;
		self.lower_expr(&rewritten)
	}

	fn rewrite_grouped_aggregate_calls(
		&self,
		expr: &SqlExpr,
		scope: &GroupedAggregateScope,
		clause: &'static str,
	) -> Result<SqlExpr, FrontendError> {
		match expr {
			SqlExpr::Function(function) if aggregate_function_name(&function.name).is_some() => {
				let output_name =
					self.resolve_grouped_aggregate_reference(function, scope, clause)?;
				Ok(SqlExpr::Identifier(Ident::new(output_name)))
			}
			SqlExpr::BinaryOp { left, op, right } => Ok(SqlExpr::BinaryOp {
				left: Box::new(self.rewrite_grouped_aggregate_calls(left, scope, clause)?),
				op: op.clone(),
				right: Box::new(self.rewrite_grouped_aggregate_calls(right, scope, clause)?),
			}),
			SqlExpr::IsNull(inner) => Ok(SqlExpr::IsNull(Box::new(
				self.rewrite_grouped_aggregate_calls(inner, scope, clause)?,
			))),
			SqlExpr::IsNotNull(inner) => Ok(SqlExpr::IsNotNull(Box::new(
				self.rewrite_grouped_aggregate_calls(inner, scope, clause)?,
			))),
			SqlExpr::Nested(inner) => Ok(SqlExpr::Nested(Box::new(
				self.rewrite_grouped_aggregate_calls(inner, scope, clause)?,
			))),
			_ => Ok(expr.clone()),
		}
	}

	fn resolve_grouped_aggregate_reference(
		&self,
		function: &Function,
		scope: &GroupedAggregateScope,
		clause: &'static str,
	) -> Result<String, FrontendError> {
		let (function, args) =
			self.lower_aggregate_function(function, AggregateSqlMode::Grouped)?;
		let signature = grouped_aggregate_signature(&function, &args)
			.ok_or_else(|| unsupported(aggregate_call_not_in_select_message(clause)))?;
		match scope.resolve(&signature) {
			ResolveOutcome::Found(output_name) => Ok(output_name),
			ResolveOutcome::NotInSelect => {
				Err(unsupported(aggregate_call_not_in_select_message(clause)))
			}
			ResolveOutcome::Ambiguous => Err(unsupported(ambiguous_aggregate_call_message(clause))),
		}
	}

	fn lower_aggregate_function(
		&self,
		function: &Function,
		mode: AggregateSqlMode,
	) -> Result<(AggregateFunction, Vec<Expr>), FrontendError> {
		let name = aggregate_function_name(&function.name)
			.ok_or_else(|| unsupported("unsupported aggregate function"))?;
		let args = aggregate_function_args(function)?;

		match name {
			"COUNT" => self.lower_count_aggregate(args),
			"SUM" => self.lower_sum_aggregate(args),
			"LINEAR_REGRESS" => {
				if matches!(mode, AggregateSqlMode::Grouped) {
					return Err(unsupported(
						"experimental aggregate functions are not supported with GROUP BY",
					));
				}
				self.lower_linear_regress_aggregate(args)
			}
			"APPROX_QUANTILE" => {
				if matches!(mode, AggregateSqlMode::Grouped) {
					return Err(unsupported(
						"experimental aggregate functions are not supported with GROUP BY",
					));
				}
				self.lower_approx_quantile_aggregate(args)
			}
			_ => Err(unsupported("unsupported aggregate function")),
		}
	}

	fn lower_count_aggregate(
		&self,
		args: &[FunctionArg],
	) -> Result<(AggregateFunction, Vec<Expr>), FrontendError> {
		if args.len() != 1 {
			return Err(unsupported("COUNT requires one argument"));
		}
		match &args[0] {
			FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => {
				Ok((AggregateFunction::CountAll, vec![Expr::column("count")]))
			}
			FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
				Ok((AggregateFunction::Count, vec![self.lower_expr(expr)?]))
			}
			_ => Err(unsupported("unsupported COUNT argument")),
		}
	}

	fn lower_sum_aggregate(
		&self,
		args: &[FunctionArg],
	) -> Result<(AggregateFunction, Vec<Expr>), FrontendError> {
		if args.len() != 1 {
			return Err(unsupported("SUM requires one argument"));
		}
		Ok((AggregateFunction::Sum, vec![self.lower_arg(&args[0])?]))
	}

	fn lower_linear_regress_aggregate(
		&self,
		args: &[FunctionArg],
	) -> Result<(AggregateFunction, Vec<Expr>), FrontendError> {
		if args.len() != 2 {
			return Err(unsupported("LINEAR_REGRESS requires two arguments"));
		}
		Ok((
			AggregateFunction::LinearRegress,
			vec![self.lower_arg(&args[0])?, self.lower_arg(&args[1])?],
		))
	}

	fn lower_approx_quantile_aggregate(
		&self,
		args: &[FunctionArg],
	) -> Result<(AggregateFunction, Vec<Expr>), FrontendError> {
		if args.len() != 2 {
			return Err(unsupported("APPROX_QUANTILE requires two arguments"));
		}
		let val = self.lower_arg(&args[0])?;
		let q = match &args[1] {
			FunctionArg::Unnamed(FunctionArgExpr::Expr(SqlExpr::Value(Value::Number(n, _)))) => {
				n.parse::<f64>().map_err(|e| unsupported(e.to_string()))?
			}
			_ => {
				return Err(unsupported(
					"APPROX_QUANTILE expects a numeric literal for quantile",
				));
			}
		};
		Ok((AggregateFunction::ApproxQuantile(q), vec![val]))
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
				if args.len() != 2 {
					return Err(unsupported("TIME_BUCKET expects exactly two arguments"));
				}
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

#[derive(Debug, Clone)]
struct GroupKey {
	name: String,
	expr: Expr,
}

#[derive(Debug)]
struct LoweredSelect {
	plan: LogicalPlan,
	grouped_aggregate_scope: Option<GroupedAggregateScope>,
}

#[derive(Debug)]
struct GroupedProjection {
	aggr_expr: Vec<(AggregateFunction, Vec<Expr>)>,
	grouped_scope: GroupedAggregateScope,
}

#[derive(Debug)]
struct GroupedAggregateScope {
	entries: Vec<GroupedAggregateEntry>,
	output_name_counts: HashMap<String, usize>,
}

impl GroupedAggregateScope {
	fn from_projection(
		group_keys: &[GroupKey],
		aggr_expr: &[(AggregateFunction, Vec<Expr>)],
	) -> Self {
		let mut output_name_counts = HashMap::new();
		for key in group_keys {
			*output_name_counts.entry(key.name.clone()).or_insert(0) += 1;
		}

		let mut entries = Vec::new();
		for (function, args) in aggr_expr {
			let output_name = aggregate_output_name(function).to_string();
			*output_name_counts.entry(output_name.clone()).or_insert(0) += 1;
			if let Some(signature) = grouped_aggregate_signature(function, args) {
				entries.push(GroupedAggregateEntry {
					signature,
					output_name,
				});
			}
		}

		Self {
			entries,
			output_name_counts,
		}
	}

	fn resolve(&self, signature: &GroupedAggregateSignature) -> ResolveOutcome {
		let matches = self
			.entries
			.iter()
			.filter(|entry| &entry.signature == signature)
			.collect::<Vec<_>>();
		let [entry] = matches.as_slice() else {
			return if matches.is_empty() {
				ResolveOutcome::NotInSelect
			} else {
				ResolveOutcome::Ambiguous
			};
		};
		if self
			.output_name_counts
			.get(&entry.output_name)
			.copied()
			.unwrap_or(0)
			> 1
		{
			return ResolveOutcome::Ambiguous;
		}
		ResolveOutcome::Found(entry.output_name.clone())
	}
}

#[derive(Debug)]
struct GroupedAggregateEntry {
	signature: GroupedAggregateSignature,
	output_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum GroupedAggregateSignature {
	CountAll,
	Count(ColumnRef),
	Sum(ColumnRef),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ResolveOutcome {
	Found(String),
	NotInSelect,
	Ambiguous,
}

#[derive(Debug, Clone, Copy)]
enum AggregateSqlMode {
	Global,
	Grouped,
}

fn lower_group_by_keys(group_by: &GroupByExpr) -> Result<Vec<GroupKey>, FrontendError> {
	// P0.1: sqlparser 0.54 stores plain keys in Expressions and ClickHouse-style modifiers
	// beside them; GROUP BY ALL and every modifier are outside ADR 011.
	match group_by {
		GroupByExpr::Expressions(exprs, modifiers) => {
			if !modifiers.is_empty() {
				return Err(unsupported("GROUP BY modifiers are not supported"));
			}
			if exprs.is_empty() {
				return Err(unsupported("GROUP BY requires at least one key"));
			}
			exprs.iter().map(lower_group_by_key).collect()
		}
		GroupByExpr::All(_) => Err(unsupported("GROUP BY ALL is not supported")),
	}
}

fn lower_group_by_key(expr: &SqlExpr) -> Result<GroupKey, FrontendError> {
	match expr {
		SqlExpr::Identifier(ident) => {
			let name = ident.value.clone();
			Ok(GroupKey {
				name: name.clone(),
				expr: Expr::column(name),
			})
		}
		_ => Err(unsupported(
			"GROUP BY keys must be simple unqualified columns",
		)),
	}
}

fn reject_qualified_having_column_references(expr: &SqlExpr) -> Result<(), FrontendError> {
	match strip_nested_sql_expr(expr) {
		SqlExpr::CompoundIdentifier(_) => Err(unsupported(
			"qualified HAVING column references are not supported",
		)),
		SqlExpr::BinaryOp { left, right, .. } => {
			reject_qualified_having_column_references(left)?;
			reject_qualified_having_column_references(right)
		}
		SqlExpr::IsNull(inner) | SqlExpr::IsNotNull(inner) | SqlExpr::Nested(inner) => {
			reject_qualified_having_column_references(inner)
		}
		_ => Ok(()),
	}
}

fn aggregate_call_not_in_select_message(clause: &str) -> &'static str {
	match clause {
		"HAVING" => "HAVING aggregate-call must appear in SELECT list",
		"ORDER BY" => "ORDER BY aggregate-call must appear in SELECT list",
		other => unreachable!("unsupported grouped aggregate clause: {other}"),
	}
}

fn ambiguous_aggregate_call_message(clause: &str) -> &'static str {
	match clause {
		"HAVING" => "ambiguous HAVING aggregate-call",
		"ORDER BY" => "ambiguous ORDER BY aggregate-call",
		other => unreachable!("unsupported grouped aggregate clause: {other}"),
	}
}

fn projection_group_key_name(item: &SelectItem) -> Result<&str, FrontendError> {
	// P0.2: the exact no-post-project shape is SelectItem::UnnamedExpr(Identifier)
	// for keys and SelectItem::UnnamedExpr(Function) for aggregate calls.
	match item {
		SelectItem::UnnamedExpr(SqlExpr::Identifier(ident)) => Ok(ident.value.as_str()),
		_ => Err(unsupported(
			"GROUP BY projection must list all keys in order before aggregates",
		)),
	}
}

fn projection_contains_aggregate_call(projection: &[SelectItem]) -> bool {
	projection.iter().any(select_item_contains_aggregate_call)
}

fn select_item_contains_aggregate_call(item: &SelectItem) -> bool {
	match item {
		SelectItem::UnnamedExpr(expr) => expr_contains_aggregate_call(expr),
		SelectItem::ExprWithAlias { expr, alias: _ } => expr_contains_aggregate_call(expr),
		SelectItem::QualifiedWildcard(_, _) | SelectItem::Wildcard(_) => false,
	}
}

fn expr_contains_aggregate_call(expr: &SqlExpr) -> bool {
	match strip_nested_sql_expr(expr) {
		SqlExpr::Function(function) => aggregate_function_name(&function.name).is_some(),
		_ => false,
	}
}

fn aggregate_function_name(name: &ObjectName) -> Option<&'static str> {
	let [ident] = name.0.as_slice() else {
		return None;
	};
	match ident.value.to_ascii_uppercase().as_str() {
		"COUNT" => Some("COUNT"),
		"SUM" => Some("SUM"),
		"LINEAR_REGRESS" => Some("LINEAR_REGRESS"),
		"APPROX_QUANTILE" => Some("APPROX_QUANTILE"),
		_ => None,
	}
}

fn aggregate_output_name(function: &AggregateFunction) -> &'static str {
	match function {
		AggregateFunction::CountAll | AggregateFunction::Count => "count",
		AggregateFunction::Sum => "sum",
		AggregateFunction::LinearRegress => "linreg",
		AggregateFunction::ApproxQuantile(_) => "quantile",
	}
}

fn grouped_aggregate_signature(
	function: &AggregateFunction,
	args: &[Expr],
) -> Option<GroupedAggregateSignature> {
	match function {
		AggregateFunction::CountAll => Some(GroupedAggregateSignature::CountAll),
		AggregateFunction::Count => match args {
			[Expr::Column(column)] => Some(GroupedAggregateSignature::Count(column.clone())),
			_ => None,
		},
		AggregateFunction::Sum => match args {
			[Expr::Column(column)] => Some(GroupedAggregateSignature::Sum(column.clone())),
			_ => None,
		},
		AggregateFunction::LinearRegress | AggregateFunction::ApproxQuantile(_) => None,
	}
}

fn aggregate_function_args(function: &Function) -> Result<&[FunctionArg], FrontendError> {
	// P0.4: global experimental aggregate calls still lower to the existing IR; the binder's
	// allow_experimental_kernels gate remains the authority for executing those kernels.
	if function.uses_odbc_syntax
		|| !matches!(function.parameters, FunctionArguments::None)
		|| function.filter.is_some()
		|| function.null_treatment.is_some()
		|| function.over.is_some()
		|| !function.within_group.is_empty()
	{
		return Err(unsupported(
			"aggregate function modifiers are not supported",
		));
	}

	let FunctionArguments::List(FunctionArgumentList {
		duplicate_treatment,
		args,
		clauses,
	}) = &function.args
	else {
		return Err(unsupported("aggregate function requires arguments"));
	};
	if duplicate_treatment.is_some() || !clauses.is_empty() {
		return Err(unsupported(
			"aggregate function modifiers are not supported",
		));
	}
	Ok(args)
}

fn plan_contains_join(plan: &LogicalPlan) -> bool {
	// P0.6: a recursive logical-plan walk is sufficient because ADR 009 decorrelation is
	// the only path that can add a Join under syntactically single-table grouped SQL.
	match plan {
		LogicalPlan::Scan {
			table: _,
			projection: _,
		}
		| LogicalPlan::ScanNamed {
			table: _,
			projection: _,
		} => false,
		LogicalPlan::Alias { input, alias: _ }
		| LogicalPlan::Filter {
			input,
			predicate: _,
		}
		| LogicalPlan::Project { input, exprs: _ }
		| LogicalPlan::Aggregate {
			input,
			group_expr: _,
			aggr_expr: _,
		}
		| LogicalPlan::Exchange {
			input,
			partition_count: _,
			key: _,
		}
		| LogicalPlan::Sort {
			input,
			order_by: _,
			limit: _,
			offset: _,
		} => plan_contains_join(input),
		LogicalPlan::Join {
			left: _,
			right: _,
			kind: _,
			on: _,
			strategy: _,
		} => true,
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

fn parse_non_negative_usize_literal(expr: &SqlExpr) -> Result<usize, FrontendError> {
	match strip_nested_sql_expr(expr) {
		SqlExpr::Value(Value::Number(number, _)) => {
			if number.starts_with('-')
				|| number.contains('.')
				|| number.contains('e')
				|| number.contains('E')
			{
				return Err(unsupported("LIMIT/OFFSET expects a non-negative integer"));
			}
			number
				.parse::<usize>()
				.map_err(|_| unsupported("LIMIT/OFFSET expects a non-negative integer"))
		}
		_ => Err(unsupported("LIMIT/OFFSET expects a non-negative integer")),
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
		| LogicalPlan::Exchange { input, .. }
		| LogicalPlan::Sort { input, .. } => plan_qualifiers(input),
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
