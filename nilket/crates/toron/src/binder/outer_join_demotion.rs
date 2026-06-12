use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct OuterJoinDemotionReceipt {
	pub(super) from_kind: JoinKind,
	pub(super) to_kind: JoinKind,
	pub(super) conjunct_text: String,
	pub(super) propagation_path: Vec<JoinSideStep>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum JoinSideStep {
	Left,
	Right,
}

impl JoinSideStep {
	pub(super) fn as_str(self) -> &'static str {
		match self {
			Self::Left => "left",
			Self::Right => "right",
		}
	}
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct NullRejectedSides {
	left: bool,
	right: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct ConjunctColumnSides {
	left: bool,
	right: bool,
	ambiguous: bool,
}

impl ConjunctColumnSides {
	fn side(side: LogicalJoinSide) -> Self {
		match side {
			LogicalJoinSide::Left => Self {
				left: true,
				right: false,
				ambiguous: false,
			},
			LogicalJoinSide::Right => Self {
				left: false,
				right: true,
				ambiguous: false,
			},
		}
	}

	fn ambiguous() -> Self {
		Self {
			left: false,
			right: false,
			ambiguous: true,
		}
	}

	fn merge(&mut self, other: Self) {
		self.left |= other.left;
		self.right |= other.right;
		self.ambiguous |= other.ambiguous;
	}
}

#[derive(Debug, Clone)]
struct RoutedConjuncts {
	input: LogicalPlan,
	residual: Vec<Expr>,
	receipts: Vec<OuterJoinDemotionReceipt>,
}

impl NullRejectedSides {
	fn side(side: LogicalJoinSide) -> Self {
		match side {
			LogicalJoinSide::Left => Self {
				left: true,
				right: false,
			},
			LogicalJoinSide::Right => Self {
				left: false,
				right: true,
			},
		}
	}

	fn union(self, other: Self) -> Self {
		Self {
			left: self.left || other.left,
			right: self.right || other.right,
		}
	}

	fn is_empty(self) -> bool {
		!self.left && !self.right
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LogicalJoinSide {
	Left,
	Right,
}

impl<'a> Binder<'a> {
	pub(super) fn demote_null_rejected_outer_joins<'b>(
		&'b self,
		plan: &'b LogicalPlan,
	) -> Pin<
		Box<
			dyn Future<Output = Result<(LogicalPlan, Vec<OuterJoinDemotionReceipt>), BindingError>>
				+ Send
				+ 'b,
		>,
	> {
		Box::pin(async move {
			let mut receipts = Vec::new();
			let plan = self
				.demote_null_rejected_outer_joins_inner(plan, &mut receipts)
				.await?;
			Ok((plan, receipts))
		})
	}

	fn demote_null_rejected_outer_joins_inner<'b>(
		&'b self,
		plan: &'b LogicalPlan,
		receipts: &'b mut Vec<OuterJoinDemotionReceipt>,
	) -> Pin<Box<dyn Future<Output = Result<LogicalPlan, BindingError>> + Send + 'b>> {
		Box::pin(async move {
			match plan {
				LogicalPlan::Filter { input, predicate } => {
					let input = self
						.demote_null_rejected_outer_joins_inner(input, receipts)
						.await?;
					let conjuncts = split_logical_and_conjuncts(predicate);
					let routed = self
						.route_filter_conjuncts(input, conjuncts, Vec::new())
						.await?;
					receipts.extend(routed.receipts);
					Ok(wrap_residual_filter(routed.input, routed.residual))
				}
				LogicalPlan::Alias {
					input,
					alias,
					columns,
				} => Ok(LogicalPlan::Alias {
					input: Box::new(
						self.demote_null_rejected_outer_joins_inner(input, receipts)
							.await?,
					),
					alias: alias.clone(),
					columns: columns.clone(),
				}),
				LogicalPlan::Project {
					input,
					exprs,
					output_names,
				} => Ok(LogicalPlan::Project {
					input: Box::new(
						self.demote_null_rejected_outer_joins_inner(input, receipts)
							.await?,
					),
					exprs: exprs.clone(),
					output_names: output_names.clone(),
				}),
				LogicalPlan::Aggregate {
					input,
					group_expr,
					aggr_expr,
				} => Ok(LogicalPlan::Aggregate {
					input: Box::new(
						self.demote_null_rejected_outer_joins_inner(input, receipts)
							.await?,
					),
					group_expr: group_expr.clone(),
					aggr_expr: aggr_expr.clone(),
				}),
				LogicalPlan::Exchange {
					input,
					partition_count,
					key,
				} => Ok(LogicalPlan::Exchange {
					input: Box::new(
						self.demote_null_rejected_outer_joins_inner(input, receipts)
							.await?,
					),
					partition_count: *partition_count,
					key: key.clone(),
				}),
				LogicalPlan::Sort {
					input,
					order_by,
					limit,
					offset,
				} => Ok(LogicalPlan::Sort {
					input: Box::new(
						self.demote_null_rejected_outer_joins_inner(input, receipts)
							.await?,
					),
					order_by: order_by.clone(),
					limit: *limit,
					offset: *offset,
				}),
				LogicalPlan::Take { input, limit } => Ok(LogicalPlan::Take {
					input: Box::new(
						self.demote_null_rejected_outer_joins_inner(input, receipts)
							.await?,
					),
					limit: *limit,
				}),
				LogicalPlan::Join {
					left,
					right,
					kind,
					on,
					strategy_intent,
				} => Ok(LogicalPlan::Join {
					left: Box::new(
						self.demote_null_rejected_outer_joins_inner(left, receipts)
							.await?,
					),
					right: Box::new(
						self.demote_null_rejected_outer_joins_inner(right, receipts)
							.await?,
					),
					kind: *kind,
					on: on.clone(),
					strategy_intent: *strategy_intent,
				}),
				LogicalPlan::CreateTableAsSelect {
					table,
					if_not_exists,
					or_replace,
					source,
				} => Ok(LogicalPlan::CreateTableAsSelect {
					table: table.clone(),
					if_not_exists: *if_not_exists,
					or_replace: *or_replace,
					source: Box::new(
						self.demote_null_rejected_outer_joins_inner(source, receipts)
							.await?,
					),
				}),
				LogicalPlan::InsertSelect {
					table,
					columns,
					source,
					returning,
				} => Ok(LogicalPlan::InsertSelect {
					table: table.clone(),
					columns: columns.clone(),
					source: Box::new(
						self.demote_null_rejected_outer_joins_inner(source, receipts)
							.await?,
					),
					returning: returning.clone(),
				}),
				LogicalPlan::Scan { .. }
				| LogicalPlan::ScanNamed { .. }
				| LogicalPlan::Explain { .. }
				| LogicalPlan::ExplainAnalyze { .. }
				| LogicalPlan::CreateTable { .. }
				| LogicalPlan::CreateLogicalType { .. }
				| LogicalPlan::DropTable { .. }
				| LogicalPlan::Delete { .. }
				| LogicalPlan::Update { .. }
				| LogicalPlan::Insert { .. } => Ok(plan.clone()),
			}
		})
	}

	fn logical_scope_for_outer_demotion<'b>(
		&'b self,
		plan: &'b LogicalPlan,
	) -> Pin<Box<dyn Future<Output = Result<Option<RelationScope>, BindingError>> + Send + 'b>> {
		Box::pin(async move {
			match plan {
				LogicalPlan::Scan { table, projection } => {
					let binding = self.catalog.resolve_table_id(*table).await?;
					let schema =
						projected_schema_for_outer_demotion(&binding.schema, projection.as_ref());
					Ok(Some(RelationScope::empty(schema)))
				}
				LogicalPlan::ScanNamed { table, projection } => {
					if is_information_schema_namespace(table) {
						return Ok(None);
					}
					let binding = self.catalog.resolve_table(table).await?;
					let schema =
						projected_schema_for_outer_demotion(&binding.schema, projection.as_ref());
					Ok(Some(RelationScope::scan(table.clone(), &schema)))
				}
				LogicalPlan::Alias {
					input,
					alias,
					columns,
				} => {
					let Some(input) = self.logical_scope_for_outer_demotion(input).await? else {
						return Ok(None);
					};
					let schema = alias_schema_for_outer_demotion(&input.schema, columns)?;
					Ok(Some(RelationScope::alias(alias.clone(), &schema)))
				}
				LogicalPlan::Filter { input, .. }
				| LogicalPlan::Exchange { input, .. }
				| LogicalPlan::Sort { input, .. }
				| LogicalPlan::Take { input, .. } => self.logical_scope_for_outer_demotion(input).await,
				LogicalPlan::Join {
					left, right, kind, ..
				} => {
					let Some(left_scope) = self.logical_scope_for_outer_demotion(left).await?
					else {
						return Ok(None);
					};
					let Some(right_scope) = self.logical_scope_for_outer_demotion(right).await?
					else {
						return Ok(None);
					};
					let schema = join_schema(*kind, &left_scope.schema, &right_scope.schema);
					Ok(Some(RelationScope::join(
						*kind,
						&left_scope,
						&right_scope,
						&schema,
					)))
				}
				LogicalPlan::Project { .. }
				| LogicalPlan::Aggregate { .. }
				| LogicalPlan::Explain { .. }
				| LogicalPlan::ExplainAnalyze { .. }
				| LogicalPlan::CreateTable { .. }
				| LogicalPlan::CreateLogicalType { .. }
				| LogicalPlan::CreateTableAsSelect { .. }
				| LogicalPlan::DropTable { .. }
				| LogicalPlan::Delete { .. }
				| LogicalPlan::Update { .. }
				| LogicalPlan::Insert { .. }
				| LogicalPlan::InsertSelect { .. } => Ok(None),
			}
		})
	}

	fn route_filter_conjuncts<'b>(
		&'b self,
		input: LogicalPlan,
		conjuncts: Vec<Expr>,
		path: Vec<JoinSideStep>,
	) -> Pin<Box<dyn Future<Output = Result<RoutedConjuncts, BindingError>> + Send + 'b>> {
		Box::pin(async move {
			if conjuncts.is_empty() {
				return Ok(RoutedConjuncts {
					input,
					residual: Vec::new(),
					receipts: Vec::new(),
				});
			}
			match input {
				LogicalPlan::Join {
					left,
					right,
					kind: JoinKind::Inner,
					on,
					strategy_intent,
				} => {
					let Some(left_scope) = self.logical_scope_for_outer_demotion(&left).await?
					else {
						return Ok(RoutedConjuncts {
							input: LogicalPlan::Join {
								left,
								right,
								kind: JoinKind::Inner,
								on,
								strategy_intent,
							},
							residual: conjuncts,
							receipts: Vec::new(),
						});
					};
					let Some(right_scope) = self.logical_scope_for_outer_demotion(&right).await?
					else {
						return Ok(RoutedConjuncts {
							input: LogicalPlan::Join {
								left,
								right,
								kind: JoinKind::Inner,
								on,
								strategy_intent,
							},
							residual: conjuncts,
							receipts: Vec::new(),
						});
					};
					let mut left_conjuncts = Vec::new();
					let mut right_conjuncts = Vec::new();
					let mut residual = Vec::new();
					for conjunct in conjuncts {
						let sides = conjunct_column_sides(&conjunct, &left_scope, &right_scope);
						if sides.left && !sides.right && !sides.ambiguous {
							left_conjuncts.push(conjunct);
						} else if sides.right && !sides.left && !sides.ambiguous {
							right_conjuncts.push(conjunct);
						} else {
							residual.push(conjunct);
						}
					}
					let mut left_path = path.clone();
					left_path.push(JoinSideStep::Left);
					let left = self
						.route_filter_conjuncts(*left, left_conjuncts, left_path)
						.await?;
					let mut right_path = path;
					right_path.push(JoinSideStep::Right);
					let right = self
						.route_filter_conjuncts(*right, right_conjuncts, right_path)
						.await?;
					residual.extend(left.residual);
					residual.extend(right.residual);
					let mut receipts = left.receipts;
					receipts.extend(right.receipts);
					Ok(RoutedConjuncts {
						input: LogicalPlan::Join {
							left: Box::new(left.input),
							right: Box::new(right.input),
							kind: JoinKind::Inner,
							on,
							strategy_intent,
						},
						residual,
						receipts,
					})
				}
				LogicalPlan::Join {
					left,
					right,
					kind,
					on,
					strategy_intent,
				} => {
					let Some(left_scope) = self.logical_scope_for_outer_demotion(&left).await?
					else {
						return Ok(RoutedConjuncts {
							input: LogicalPlan::Join {
								left,
								right,
								kind,
								on,
								strategy_intent,
							},
							residual: conjuncts,
							receipts: Vec::new(),
						});
					};
					let Some(right_scope) = self.logical_scope_for_outer_demotion(&right).await?
					else {
						return Ok(RoutedConjuncts {
							input: LogicalPlan::Join {
								left,
								right,
								kind,
								on,
								strategy_intent,
							},
							residual: conjuncts,
							receipts: Vec::new(),
						});
					};
					let mut rejected = NullRejectedSides::default();
					let mut conjunct_sides = Vec::with_capacity(conjuncts.len());
					for conjunct in &conjuncts {
						let sides = null_rejecting_sides(conjunct, &left_scope, &right_scope);
						rejected = rejected.union(sides);
						conjunct_sides.push(sides);
					}
					let Some(to_kind) = demoted_join_kind(kind, rejected) else {
						return Ok(RoutedConjuncts {
							input: LogicalPlan::Join {
								left,
								right,
								kind,
								on,
								strategy_intent,
							},
							residual: conjuncts,
							receipts: Vec::new(),
						});
					};
					let demotion_sides = demotion_owned_sides(kind, to_kind, rejected);
					let mut left_push = Vec::new();
					let mut right_push = Vec::new();
					let mut residual = Vec::new();
					let mut receipt_text = Vec::new();
					for (conjunct, sides) in conjuncts.into_iter().zip(conjunct_sides) {
						if demotion_sides.left && sides.left && !sides.right {
							receipt_text.push(format!("{conjunct:?}"));
							left_push.push(conjunct);
						} else if demotion_sides.right && sides.right && !sides.left {
							receipt_text.push(format!("{conjunct:?}"));
							right_push.push(conjunct);
						} else {
							residual.push(conjunct);
						}
					}
					let left = push_logical_filter_conjuncts(*left, left_push);
					let right = push_logical_filter_conjuncts(*right, right_push);
					Ok(RoutedConjuncts {
						input: LogicalPlan::Join {
							left: Box::new(left),
							right: Box::new(right),
							kind: to_kind,
							on,
							strategy_intent,
						},
						residual,
						receipts: vec![OuterJoinDemotionReceipt {
							from_kind: kind,
							to_kind,
							conjunct_text: receipt_text.join(" AND "),
							propagation_path: path,
						}],
					})
				}
				LogicalPlan::Exchange {
					input,
					partition_count,
					key,
				} => {
					let routed = self.route_filter_conjuncts(*input, conjuncts, path).await?;
					Ok(RoutedConjuncts {
						input: LogicalPlan::Exchange {
							input: Box::new(routed.input),
							partition_count,
							key,
						},
						residual: routed.residual,
						receipts: routed.receipts,
					})
				}
				LogicalPlan::Sort {
					input,
					order_by,
					limit: None,
					offset: 0,
				} => {
					let routed = self.route_filter_conjuncts(*input, conjuncts, path).await?;
					Ok(RoutedConjuncts {
						input: LogicalPlan::Sort {
							input: Box::new(routed.input),
							order_by,
							limit: None,
							offset: 0,
						},
						residual: routed.residual,
						receipts: routed.receipts,
					})
				}
				stopped @ (LogicalPlan::Scan { .. }
				| LogicalPlan::ScanNamed { .. }
				| LogicalPlan::Alias { .. }
				| LogicalPlan::Filter { .. }
				| LogicalPlan::Project { .. }
				| LogicalPlan::Aggregate { .. }
				| LogicalPlan::Sort { .. }
				| LogicalPlan::Take { .. }
				| LogicalPlan::Explain { .. }
				| LogicalPlan::ExplainAnalyze { .. }
				| LogicalPlan::CreateTable { .. }
				| LogicalPlan::CreateLogicalType { .. }
				| LogicalPlan::CreateTableAsSelect { .. }
				| LogicalPlan::DropTable { .. }
				| LogicalPlan::Delete { .. }
				| LogicalPlan::Update { .. }
				| LogicalPlan::Insert { .. }
				| LogicalPlan::InsertSelect { .. }) => Ok(RoutedConjuncts {
					input: stopped,
					residual: conjuncts,
					receipts: Vec::new(),
				}),
			}
		})
	}
}

fn split_logical_and_conjuncts(expr: &Expr) -> Vec<Expr> {
	match expr {
		Expr::And(left, right) => {
			let mut conjuncts = split_logical_and_conjuncts(left);
			conjuncts.extend(split_logical_and_conjuncts(right));
			conjuncts
		}
		_ => vec![expr.clone()],
	}
}

fn combine_logical_and_conjuncts(conjuncts: Vec<Expr>) -> Option<Expr> {
	conjuncts
		.into_iter()
		.reduce(|left, right| Expr::And(Box::new(left), Box::new(right)))
}

fn push_logical_filter_conjuncts(input: LogicalPlan, conjuncts: Vec<Expr>) -> LogicalPlan {
	match combine_logical_and_conjuncts(conjuncts) {
		Some(predicate) => LogicalPlan::Filter {
			input: Box::new(input),
			predicate,
		},
		None => input,
	}
}

fn wrap_residual_filter(input: LogicalPlan, residual: Vec<Expr>) -> LogicalPlan {
	push_logical_filter_conjuncts(input, residual)
}

fn demoted_join_kind(kind: JoinKind, rejected: NullRejectedSides) -> Option<JoinKind> {
	match kind {
		JoinKind::LeftOuter if rejected.right => Some(JoinKind::Inner),
		JoinKind::RightOuter if rejected.left => Some(JoinKind::Inner),
		JoinKind::FullOuter if rejected.left && rejected.right => Some(JoinKind::Inner),
		JoinKind::FullOuter if rejected.left => Some(JoinKind::LeftOuter),
		JoinKind::FullOuter if rejected.right => Some(JoinKind::RightOuter),
		JoinKind::Inner
		| JoinKind::LeftOuter
		| JoinKind::RightOuter
		| JoinKind::FullOuter
		| JoinKind::LeftSemi
		| JoinKind::RightSemi
		| JoinKind::LeftAnti
		| JoinKind::RightAnti => None,
	}
}

fn demotion_owned_sides(
	from_kind: JoinKind,
	to_kind: JoinKind,
	rejected: NullRejectedSides,
) -> NullRejectedSides {
	match (from_kind, to_kind) {
		(JoinKind::LeftOuter, JoinKind::Inner) => NullRejectedSides {
			left: false,
			right: true,
		},
		(JoinKind::RightOuter, JoinKind::Inner) => NullRejectedSides {
			left: true,
			right: false,
		},
		(JoinKind::FullOuter, _) => rejected,
		_ => NullRejectedSides::default(),
	}
}

fn null_rejecting_sides(
	expr: &Expr,
	left_scope: &RelationScope,
	right_scope: &RelationScope,
) -> NullRejectedSides {
	match expr {
		Expr::IsNotNull(inner) => null_rejecting_column_side(inner, left_scope, right_scope),
		Expr::Eq(left, right)
		| Expr::Neq(left, right)
		| Expr::Lt(left, right)
		| Expr::Lte(left, right)
		| Expr::Gt(left, right)
		| Expr::Gte(left, right) => comparison_null_rejecting_side(left, right, left_scope, right_scope),
		Expr::Like {
			expr,
			negated: false,
			..
		} => null_rejecting_column_side(expr, left_scope, right_scope),
		Expr::InList {
			expr,
			list,
			negated: false,
		} if !list.is_empty() && list.iter().all(non_null_literal) => {
			null_rejecting_column_side(expr, left_scope, right_scope)
		}
		Expr::And(left, right) => null_rejecting_sides(left, left_scope, right_scope)
			.union(null_rejecting_sides(right, left_scope, right_scope)),
		Expr::Column(_)
		| Expr::Literal(_)
		| Expr::UntypedNull
		| Expr::LogicalAscription { .. }
		| Expr::SqlLogicalCast { .. }
		| Expr::SqlConvert { .. }
		| Expr::Cast(_, _)
		| Expr::TryCast(_, _)
		| Expr::Add(_, _)
		| Expr::Sub(_, _)
		| Expr::Mul(_, _)
		| Expr::Div(_, _)
		| Expr::Or(_, _)
		| Expr::Not(_)
		| Expr::InList { .. }
		| Expr::Like { .. }
		| Expr::Case { .. }
		| Expr::IsNull(_)
		| Expr::TimeBucket(_, _)
		| Expr::AtTimeZone(_, _)
		| Expr::Extract(_, _)
		| Expr::DateTrunc(_, _) => NullRejectedSides::default(),
	}
}

fn comparison_null_rejecting_side(
	left: &Expr,
	right: &Expr,
	left_scope: &RelationScope,
	right_scope: &RelationScope,
) -> NullRejectedSides {
	if non_null_literal(right) {
		let sides = null_rejecting_column_side(left, left_scope, right_scope);
		if !sides.is_empty() {
			return sides;
		}
	}
	if non_null_literal(left) {
		return null_rejecting_column_side(right, left_scope, right_scope);
	}
	NullRejectedSides::default()
}

fn null_rejecting_column_side(
	expr: &Expr,
	left_scope: &RelationScope,
	right_scope: &RelationScope,
) -> NullRejectedSides {
	let Expr::Column(column_ref) = expr else {
		return NullRejectedSides::default();
	};
	match column_side(column_ref, left_scope, right_scope) {
		Some(side) => NullRejectedSides::side(side),
		None => NullRejectedSides::default(),
	}
}

fn column_side(
	column_ref: &ColumnRef,
	left_scope: &RelationScope,
	right_scope: &RelationScope,
) -> Option<LogicalJoinSide> {
	let left = left_scope
		.resolve_join_column(column_ref, "left")
		.ok()
		.flatten();
	let right = right_scope
		.resolve_join_column(column_ref, "right")
		.ok()
		.flatten();
	match (left, right) {
		(Some(_), None) => Some(LogicalJoinSide::Left),
		(None, Some(_)) => Some(LogicalJoinSide::Right),
		(Some(_), Some(_)) | (None, None) => None,
	}
}

fn conjunct_column_sides(
	expr: &Expr,
	left_scope: &RelationScope,
	right_scope: &RelationScope,
) -> ConjunctColumnSides {
	match expr {
		Expr::Column(column_ref) => match column_side(column_ref, left_scope, right_scope) {
			Some(side) => ConjunctColumnSides::side(side),
			None => ConjunctColumnSides::ambiguous(),
		},
		Expr::Literal(_) | Expr::UntypedNull => ConjunctColumnSides::default(),
		Expr::LogicalAscription { expr, .. }
		| Expr::SqlLogicalCast { expr, .. }
		| Expr::SqlConvert { expr, .. } => conjunct_column_sides(expr, left_scope, right_scope),
		Expr::Cast(inner, _)
		| Expr::TryCast(inner, _)
		| Expr::Not(inner)
		| Expr::IsNull(inner)
		| Expr::IsNotNull(inner)
		| Expr::TimeBucket(_, inner)
		| Expr::AtTimeZone(inner, _)
		| Expr::Extract(_, inner)
		| Expr::DateTrunc(_, inner) => conjunct_column_sides(inner, left_scope, right_scope),
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
			let mut sides = conjunct_column_sides(left, left_scope, right_scope);
			sides.merge(conjunct_column_sides(right, left_scope, right_scope));
			sides
		}
		Expr::InList { expr, list, .. } => {
			let mut sides = conjunct_column_sides(expr, left_scope, right_scope);
			for item in list {
				sides.merge(conjunct_column_sides(item, left_scope, right_scope));
			}
			sides
		}
		Expr::Like { expr, .. } => conjunct_column_sides(expr, left_scope, right_scope),
		Expr::Case {
			when_then,
			else_expr,
		} => {
			let mut sides = ConjunctColumnSides::default();
			for (when_expr, then_expr) in when_then {
				sides.merge(conjunct_column_sides(when_expr, left_scope, right_scope));
				sides.merge(conjunct_column_sides(then_expr, left_scope, right_scope));
			}
			if let Some(else_expr) = else_expr {
				sides.merge(conjunct_column_sides(else_expr, left_scope, right_scope));
			}
			sides
		}
	}
}

fn non_null_literal(expr: &Expr) -> bool {
	matches!(expr, Expr::Literal(value) if !scalar_value_is_null(value))
}

fn scalar_value_is_null(value: &ScalarValue) -> bool {
	match value {
		ScalarValue::Boolean(value) => value.is_none(),
		ScalarValue::Int32(value) => value.is_none(),
		ScalarValue::Int64(value) => value.is_none(),
		ScalarValue::Date32(value) => value.is_none(),
		ScalarValue::Timestamp { value, .. } => value.is_none(),
		ScalarValue::Interval(value) => value.is_none(),
		ScalarValue::CalendarMonthInterval(value) => value.is_none(),
		ScalarValue::Float32(value) => value.is_none(),
		ScalarValue::Float64(value) => value.is_none(),
		ScalarValue::Utf8(value) => value.is_none(),
	}
}

fn projected_schema_for_outer_demotion(
	schema: &SchemaRef,
	projection: Option<&Vec<String>>,
) -> SchemaRef {
	let Some(projection) = projection else {
		return schema.clone();
	};
	let fields = projection
		.iter()
		.filter_map(|name| schema.fields().iter().find(|field| field.name() == name))
		.map(|field| field.as_ref().clone())
		.collect::<Vec<_>>();
	Arc::new(Schema::new(fields))
}

fn alias_schema_for_outer_demotion(
	schema: &SchemaRef,
	columns: &[String],
) -> Result<SchemaRef, BindingError> {
	if columns.is_empty() {
		return Ok(schema.clone());
	}
	if columns.len() != schema.fields().len() {
		return Err(BindingError::UnsupportedExpression {
			message: format!(
				"alias column list arity mismatch: expected {}, got {}",
				schema.fields().len(),
				columns.len()
			),
		});
	}
	let fields = schema
		.fields()
		.iter()
		.zip(columns)
		.map(|(field, name)| {
			Field::new(name.clone(), field.data_type().clone(), field.is_nullable())
				.with_metadata(field.metadata().clone())
		})
		.collect::<Vec<_>>();
	Ok(Arc::new(Schema::new(fields)))
}
