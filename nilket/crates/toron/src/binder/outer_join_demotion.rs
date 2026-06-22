use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct OuterJoinDemotionReceipt {
	pub(super) from_kind: JoinKind,
	pub(super) to_kind: JoinKind,
	pub(super) conjunct_text: String,
	pub(super) propagation_path: Vec<JoinSideStep>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum JoinSideStep {
	Left,
	Right,
	Alias(TableRef),
	Project,
}

impl JoinSideStep {
	pub(super) fn label(&self) -> String {
		match self {
			Self::Left => "left".to_string(),
			Self::Right => "right".to_string(),
			Self::Alias(alias) => format!("alias({})", relation_key_label(alias)),
			Self::Project => "project".to_string(),
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

	fn is_left_only(&self) -> bool {
		self.left && !self.right && !self.ambiguous
	}

	fn is_right_only(&self) -> bool {
		self.right && !self.left && !self.ambiguous
	}
}

#[derive(Debug, Clone)]
struct RoutedConjuncts {
	input: LogicalPlan,
	residual: Vec<Expr>,
	receipts: Vec<OuterJoinDemotionReceipt>,
}

#[derive(Debug, Clone)]
struct RoutedExpr {
	original: Expr,
	routed: Expr,
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

	fn intersect(self, other: Self) -> Self {
		Self {
			left: self.left && other.left,
			right: self.right && other.right,
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
				LogicalPlan::Distinct { input } => Ok(LogicalPlan::Distinct {
					input: Box::new(
						self.demote_null_rejected_outer_joins_inner(input, receipts)
							.await?,
					),
				}),
				LogicalPlan::Union { left, right } => Ok(LogicalPlan::Union {
					left: Box::new(
						self.demote_null_rejected_outer_joins_inner(left, receipts)
							.await?,
					),
					right: Box::new(
						self.demote_null_rejected_outer_joins_inner(right, receipts)
							.await?,
					),
				}),
				LogicalPlan::SetOpAll { left, right, kind } => Ok(LogicalPlan::SetOpAll {
					left: Box::new(
						self.demote_null_rejected_outer_joins_inner(left, receipts)
							.await?,
					),
					right: Box::new(
						self.demote_null_rejected_outer_joins_inner(right, receipts)
							.await?,
					),
					kind: *kind,
				}),
				LogicalPlan::Repeat { input, count } => Ok(LogicalPlan::Repeat {
					input: Box::new(
						self.demote_null_rejected_outer_joins_inner(input, receipts)
							.await?,
					),
					count: count.clone(),
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
				LogicalPlan::Window {
					input,
					partition_by,
					order_by,
					functions,
				} => Ok(LogicalPlan::Window {
					input: Box::new(
						self.demote_null_rejected_outer_joins_inner(input, receipts)
							.await?,
					),
					partition_by: partition_by.clone(),
					order_by: order_by.clone(),
					functions: functions.clone(),
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
					on_filter,
					null_safe_keys,
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
					on_filter: on_filter.clone(),
					null_safe_keys: *null_safe_keys,
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
				// ADR 279: a pre-bind DeferredExists — recurse into both sides (the outer query's own joins
				// still need demotion) and rebuild; the binder resolves it to a real Join later.
				LogicalPlan::DeferredExists {
					outer,
					inner,
					predicates,
					negated,
				} => Ok(LogicalPlan::DeferredExists {
					outer: Box::new(
						self.demote_null_rejected_outer_joins_inner(outer, receipts)
							.await?,
					),
					inner: Box::new(
						self.demote_null_rejected_outer_joins_inner(inner, receipts)
							.await?,
					),
					predicates: predicates.clone(),
					negated: *negated,
				}),
				LogicalPlan::DeferredScalarSubquery {
					outer,
					inner,
					predicates,
					aggregate,
					alias,
				} => Ok(LogicalPlan::DeferredScalarSubquery {
					outer: Box::new(
						self.demote_null_rejected_outer_joins_inner(outer, receipts)
							.await?,
					),
					inner: Box::new(
						self.demote_null_rejected_outer_joins_inner(inner, receipts)
							.await?,
					),
					predicates: predicates.clone(),
					aggregate: aggregate.clone(),
					alias: alias.clone(),
				}),
				// ADR 255: opaque-local — a recursive CTE's body is bound separately; outer-join
				// demotion does not rewrite inside it, and its self-reference is a leaf.
				LogicalPlan::RecursiveCte { .. }
				| LogicalPlan::RecursiveRef { .. }
				| LogicalPlan::Scan { .. }
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

	/// ADR 270: comma-join equi-join recognition. Lifts cross-bridging equality conjuncts out of a
	/// `Filter` over a keyless `Cross` into the join's `on` keys (`Cross` → `Inner`), routing single-side
	/// conjuncts into a `Cross` child so a left-deep n-ary comma fold lifts every edge. Syntax-agnostic: any
	/// keyless `Cross` carrying an extractable bridging equi rewrites (comma join or explicit `CROSS JOIN`)
	/// — result-identical, and it hands the keyed shape to the existing inner-join strategy/fragmenter.
	/// Runs in the binder (post-resolution) because official TPC-H join columns are unqualified and the SQL
	/// planner has no catalog to resolve a column's side.
	pub(super) fn recognize_cross_equi_joins<'b>(
		&'b self,
		plan: &'b LogicalPlan,
	) -> Pin<Box<dyn Future<Output = Result<LogicalPlan, BindingError>> + Send + 'b>> {
		Box::pin(async move {
			match plan {
				LogicalPlan::Filter { input, predicate } => {
					let input = self.recognize_cross_equi_joins(input).await?;
					match input {
						LogicalPlan::Join {
							left,
							right,
							kind: JoinKind::Cross,
							strategy_intent,
							..
						} => {
							let conjuncts = split_logical_and_conjuncts(predicate);
							self.lift_cross_equi(*left, *right, strategy_intent, conjuncts)
								.await
						}
						// ADR 282: a deferred scalar subquery's recombine sits between the WHERE filter and the
						// comma-join Cross it filters; push the outer-only conjuncts down so the recognizer can
						// key the comma join. Kept in a separate future (like `lift_cross_equi`) so this
						// recursive dispatch's poll frame stays small (deep plans share its stack).
						input @ LogicalPlan::DeferredScalarSubquery { .. } => {
							self.push_deferred_scalar_outer_filter(input, predicate)
								.await
						}
						input @ LogicalPlan::Join {
							kind: JoinKind::LeftSemi | JoinKind::LeftAnti,
							..
						} => {
							self.push_left_semi_anti_output_filter(input, predicate)
								.await
						}
						other => Ok(LogicalPlan::Filter {
							input: Box::new(other),
							predicate: predicate.clone(),
						}),
					}
				}
				LogicalPlan::Alias {
					input,
					alias,
					columns,
				} => Ok(LogicalPlan::Alias {
					input: Box::new(self.recognize_cross_equi_joins(input).await?),
					alias: alias.clone(),
					columns: columns.clone(),
				}),
				LogicalPlan::Project {
					input,
					exprs,
					output_names,
				} => Ok(LogicalPlan::Project {
					input: Box::new(self.recognize_cross_equi_joins(input).await?),
					exprs: exprs.clone(),
					output_names: output_names.clone(),
				}),
				LogicalPlan::Distinct { input } => Ok(LogicalPlan::Distinct {
					input: Box::new(self.recognize_cross_equi_joins(input).await?),
				}),
				LogicalPlan::Union { left, right } => Ok(LogicalPlan::Union {
					left: Box::new(self.recognize_cross_equi_joins(left).await?),
					right: Box::new(self.recognize_cross_equi_joins(right).await?),
				}),
				LogicalPlan::SetOpAll { left, right, kind } => Ok(LogicalPlan::SetOpAll {
					left: Box::new(self.recognize_cross_equi_joins(left).await?),
					right: Box::new(self.recognize_cross_equi_joins(right).await?),
					kind: *kind,
				}),
				LogicalPlan::Repeat { input, count } => Ok(LogicalPlan::Repeat {
					input: Box::new(self.recognize_cross_equi_joins(input).await?),
					count: count.clone(),
				}),
				LogicalPlan::Aggregate {
					input,
					group_expr,
					aggr_expr,
				} => Ok(LogicalPlan::Aggregate {
					input: Box::new(self.recognize_cross_equi_joins(input).await?),
					group_expr: group_expr.clone(),
					aggr_expr: aggr_expr.clone(),
				}),
				LogicalPlan::Exchange {
					input,
					partition_count,
					key,
				} => Ok(LogicalPlan::Exchange {
					input: Box::new(self.recognize_cross_equi_joins(input).await?),
					partition_count: *partition_count,
					key: key.clone(),
				}),
				LogicalPlan::Sort {
					input,
					order_by,
					limit,
					offset,
				} => Ok(LogicalPlan::Sort {
					input: Box::new(self.recognize_cross_equi_joins(input).await?),
					order_by: order_by.clone(),
					limit: *limit,
					offset: *offset,
				}),
				LogicalPlan::Window {
					input,
					partition_by,
					order_by,
					functions,
				} => Ok(LogicalPlan::Window {
					input: Box::new(self.recognize_cross_equi_joins(input).await?),
					partition_by: partition_by.clone(),
					order_by: order_by.clone(),
					functions: functions.clone(),
				}),
				LogicalPlan::Take { input, limit } => Ok(LogicalPlan::Take {
					input: Box::new(self.recognize_cross_equi_joins(input).await?),
					limit: *limit,
				}),
				LogicalPlan::Join {
					left,
					right,
					kind,
					on,
					on_filter,
					null_safe_keys,
					strategy_intent,
				} => {
					let left = self.recognize_cross_equi_joins(left).await?;
					let right = self.recognize_cross_equi_joins(right).await?;
					self.push_on_clause_residual(
						left,
						right,
						*kind,
						on.clone(),
						on_filter.clone(),
						*null_safe_keys,
						*strategy_intent,
					)
					.await
				}
				LogicalPlan::CreateTableAsSelect {
					table,
					if_not_exists,
					or_replace,
					source,
				} => Ok(LogicalPlan::CreateTableAsSelect {
					table: table.clone(),
					if_not_exists: *if_not_exists,
					or_replace: *or_replace,
					source: Box::new(self.recognize_cross_equi_joins(source).await?),
				}),
				LogicalPlan::InsertSelect {
					table,
					columns,
					source,
					returning,
				} => Ok(LogicalPlan::InsertSelect {
					table: table.clone(),
					columns: columns.clone(),
					source: Box::new(self.recognize_cross_equi_joins(source).await?),
					returning: returning.clone(),
				}),
				// ADR 279: recurse both sides (the outer query may carry comma-joins to recognize), then
				// rebuild; the binder resolves it to a real Join afterward.
				LogicalPlan::DeferredExists {
					outer,
					inner,
					predicates,
					negated,
				} => Ok(LogicalPlan::DeferredExists {
					outer: Box::new(self.recognize_cross_equi_joins(outer).await?),
					inner: Box::new(self.recognize_cross_equi_joins(inner).await?),
					predicates: predicates.clone(),
					negated: *negated,
				}),
				LogicalPlan::DeferredScalarSubquery {
					outer,
					inner,
					predicates,
					aggregate,
					alias,
				} => Ok(LogicalPlan::DeferredScalarSubquery {
					outer: Box::new(self.recognize_cross_equi_joins(outer).await?),
					inner: Box::new(self.recognize_cross_equi_joins(inner).await?),
					predicates: predicates.clone(),
					aggregate: aggregate.clone(),
					alias: alias.clone(),
				}),
				LogicalPlan::RecursiveCte { .. }
				| LogicalPlan::RecursiveRef { .. }
				| LogicalPlan::Scan { .. }
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

	/// ADR 282 helper: filter pushdown through a deferred scalar subquery recombine. The recombine sits
	/// between a WHERE filter and the comma-join `Cross` it filters, so the bridging equality
	/// (`p_partkey = l_partkey`) is stranded above where [`recognize_cross_equi_joins`] can't key it — and the
	/// comma join would execute as a materialized cartesian. Push the conjuncts that do NOT reference the
	/// scalar value down into the outer (a valid pushdown through the left-preserving recombine; recurse so
	/// the recognizer keys the comma join), and keep the value-referencing conjuncts above. A separate future
	/// (like [`lift_cross_equi`]) so the deep-recursing dispatch's poll frame stays small.
	fn push_deferred_scalar_outer_filter<'b>(
		&'b self,
		recombine: LogicalPlan,
		predicate: &'b Expr,
	) -> Pin<Box<dyn Future<Output = Result<LogicalPlan, BindingError>> + Send + 'b>> {
		Box::pin(async move {
			let LogicalPlan::DeferredScalarSubquery {
				outer,
				inner,
				predicates,
				aggregate,
				alias,
			} = recombine
			else {
				unreachable!(
					"push_deferred_scalar_outer_filter is only called on a DeferredScalarSubquery"
				);
			};
			let alias_ref = TableRef::new(alias.clone());
			let (above, pushed): (Vec<Expr>, Vec<Expr>) = split_logical_and_conjuncts(predicate)
				.into_iter()
				.partition(|conjunct| {
					expr_any_column(conjunct, &|column_ref| {
						column_ref.qualifier.as_ref() == Some(&alias_ref)
					})
				});
			let outer = if pushed.is_empty() {
				outer
			} else {
				let pushed_outer = push_logical_filter_conjuncts(*outer, pushed);
				Box::new(self.recognize_cross_equi_joins(&pushed_outer).await?)
			};
			let recombine = LogicalPlan::DeferredScalarSubquery {
				outer,
				inner,
				predicates,
				aggregate,
				alias,
			};
			Ok(wrap_residual_filter(recombine, above))
		})
	}

	/// A `LeftSemi`/`LeftAnti` emits only its left rows, so left-output-only conjuncts above
	/// the join can move into the left child without changing semi/anti semantics. Ambiguous,
	/// right-only, and cross-side conjuncts stay above.
	fn push_left_semi_anti_output_filter<'b>(
		&'b self,
		join: LogicalPlan,
		predicate: &'b Expr,
	) -> Pin<Box<dyn Future<Output = Result<LogicalPlan, BindingError>> + Send + 'b>> {
		Box::pin(async move {
			let LogicalPlan::Join {
				left,
				right,
				kind,
				on,
				on_filter,
				null_safe_keys,
				strategy_intent,
			} = join
			else {
				unreachable!(
					"push_left_semi_anti_output_filter is only called on a semi/anti Join"
				);
			};
			debug_assert!(matches!(kind, JoinKind::LeftSemi | JoinKind::LeftAnti));

			let (Some(left_scope), Some(right_scope)) = (
				self.logical_scope_for_outer_demotion(&left).await?,
				self.logical_scope_for_outer_demotion(&right).await?,
			) else {
				let join = LogicalPlan::Join {
					left,
					right,
					kind,
					on,
					on_filter,
					null_safe_keys,
					strategy_intent,
				};
				return Ok(LogicalPlan::Filter {
					input: Box::new(join),
					predicate: predicate.clone(),
				});
			};

			let mut left_push = Vec::new();
			let mut residual = Vec::new();
			for conjunct in split_logical_and_conjuncts(predicate) {
				if conjunct_column_sides(&conjunct, &left_scope, &right_scope).is_left_only() {
					left_push.push(conjunct);
				} else {
					residual.push(conjunct);
				}
			}

			let left = if left_push.is_empty() {
				left
			} else {
				let pushed = push_left_output_filter_conjuncts(*left, left_push);
				Box::new(self.recognize_cross_equi_joins(&pushed).await?)
			};
			let join = LogicalPlan::Join {
				left,
				right,
				kind,
				on,
				on_filter,
				null_safe_keys,
				strategy_intent,
			};
			Ok(wrap_residual_filter(join, residual))
		})
	}

	/// ADR 308: classify a SQL ON residual after table schemas are available, then push the side-local cases
	/// that are equivalent to filtering one join input before the key-only join. Cross-side/preserved-side
	/// residuals need full theta-join match semantics, so this cut rejects them instead of smuggling a
	/// post-join WHERE.
	pub(super) fn push_on_clause_residual<'b>(
		&'b self,
		left: LogicalPlan,
		right: LogicalPlan,
		kind: JoinKind,
		on: Vec<(Expr, Expr)>,
		on_filter: Option<Expr>,
		null_safe_keys: bool,
		strategy_intent: JoinStrategyIntent,
	) -> Pin<Box<dyn Future<Output = Result<LogicalPlan, BindingError>> + Send + 'b>> {
		Box::pin(async move {
			let Some(on_filter) = on_filter else {
				return Ok(LogicalPlan::Join {
					left: Box::new(left),
					right: Box::new(right),
					kind,
					on,
					on_filter: None,
					null_safe_keys,
					strategy_intent,
				});
			};
			if on.is_empty() {
				return Err(on_clause_residual_unsupported());
			}
			let (Some(left_scope), Some(right_scope)) = (
				self.logical_scope_for_outer_demotion(&left).await?,
				self.logical_scope_for_outer_demotion(&right).await?,
			) else {
				return Err(on_clause_residual_unsupported());
			};

			let mut left_push = Vec::new();
			let mut right_push = Vec::new();
			for conjunct in split_logical_and_conjuncts(&on_filter) {
				let sides = conjunct_column_sides(&conjunct, &left_scope, &right_scope);
				match kind {
					JoinKind::Inner if sides.is_left_only() => left_push.push(conjunct),
					JoinKind::Inner if sides.is_right_only() => right_push.push(conjunct),
					JoinKind::LeftOuter if sides.is_right_only() => right_push.push(conjunct),
					JoinKind::RightOuter if sides.is_left_only() => left_push.push(conjunct),
					_ => return Err(on_clause_residual_unsupported()),
				}
			}

			let left = if left_push.is_empty() {
				left
			} else {
				let pushed = push_logical_filter_conjuncts(left, left_push);
				self.recognize_cross_equi_joins(&pushed).await?
			};
			let right = if right_push.is_empty() {
				right
			} else {
				let pushed = push_logical_filter_conjuncts(right, right_push);
				self.recognize_cross_equi_joins(&pushed).await?
			};
			Ok(LogicalPlan::Join {
				left: Box::new(left),
				right: Box::new(right),
				kind,
				on,
				on_filter: None,
				null_safe_keys,
				strategy_intent,
			})
		})
	}

	/// ADR 270 helper: given the two children of a keyless `Cross` and the conjuncts of the filter above it,
	/// lift cross-bridging equalities into `on` (demoting to `Inner`, preserving `strategy_intent`), route
	/// single-side conjuncts into a `Cross` child (so nested comma edges lift), and re-wrap the rest as the
	/// residual filter. If a child scope is opaque (e.g. aggregate/union), the cross is left untouched.
	fn lift_cross_equi<'b>(
		&'b self,
		left: LogicalPlan,
		right: LogicalPlan,
		strategy_intent: JoinStrategyIntent,
		conjuncts: Vec<Expr>,
	) -> Pin<Box<dyn Future<Output = Result<LogicalPlan, BindingError>> + Send + 'b>> {
		Box::pin(async move {
			let (Some(left_scope), Some(right_scope)) = (
				self.logical_scope_for_outer_demotion(&left).await?,
				self.logical_scope_for_outer_demotion(&right).await?,
			) else {
				let join = LogicalPlan::Join {
					left: Box::new(left),
					right: Box::new(right),
					kind: JoinKind::Cross,
					on: Vec::new(),
					on_filter: None,
					null_safe_keys: false,
					strategy_intent,
				};
				return Ok(wrap_residual_filter(join, conjuncts));
			};
			let mut keys: Vec<(Expr, Expr)> = Vec::new();
			let mut left_only: Vec<Expr> = Vec::new();
			let mut right_only: Vec<Expr> = Vec::new();
			let mut residual: Vec<Expr> = Vec::new();
			for conjunct in conjuncts {
				if let Some((lifted_keys, residual_or)) =
					extract_common_or_bridging_keys(&conjunct, &left_scope, &right_scope)
				{
					for pair in lifted_keys {
						push_join_key_if_absent(&mut keys, pair);
					}
					if let Some(residual_or) = residual_or {
						residual.push(residual_or);
					}
					continue;
				}
				let lifted = match &conjunct {
					Expr::Eq(l, r) => {
						orient_bridging_equi(l.as_ref(), r.as_ref(), &left_scope, &right_scope)
					}
					_ => None,
				};
				if let Some(pair) = lifted {
					push_join_key_if_absent(&mut keys, pair);
					continue;
				}
				let sides = conjunct_column_sides(&conjunct, &left_scope, &right_scope);
				if sides.left && !sides.right && !sides.ambiguous {
					left_only.push(conjunct);
				} else if sides.right && !sides.left && !sides.ambiguous {
					right_only.push(conjunct);
				} else {
					residual.push(conjunct);
				}
			}
			let left = self
				.push_into_cross_child(left, left_only, &mut residual)
				.await?;
			let right = self
				.push_into_cross_child(right, right_only, &mut residual)
				.await?;
			let kind = if keys.is_empty() {
				JoinKind::Cross
			} else {
				JoinKind::Inner
			};
			let join = LogicalPlan::Join {
				left: Box::new(left),
				right: Box::new(right),
				kind,
				on: keys,
				on_filter: None,
				null_safe_keys: false,
				strategy_intent,
			};
			Ok(wrap_residual_filter(join, residual))
		})
	}

	/// ADR 270 helper: route single-side conjuncts into a `Cross` child (wrap them as a filter and recurse,
	/// so a nested comma edge can lift there); for a non-`Cross` child keep them in the parent residual (the
	/// existing predicate-pushdown places them at the scan). Either way recurse the child for nested shapes.
	fn push_into_cross_child<'b>(
		&'b self,
		child: LogicalPlan,
		conjuncts: Vec<Expr>,
		residual: &'b mut Vec<Expr>,
	) -> Pin<Box<dyn Future<Output = Result<LogicalPlan, BindingError>> + Send + 'b>> {
		Box::pin(async move {
			if conjuncts.is_empty() {
				return self.recognize_cross_equi_joins(&child).await;
			}
			if matches!(
				child,
				LogicalPlan::Join {
					kind: JoinKind::Cross,
					..
				}
			) {
				let pushed = wrap_residual_filter(child, conjuncts);
				self.recognize_cross_equi_joins(&pushed).await
			} else {
				residual.extend(conjuncts);
				self.recognize_cross_equi_joins(&child).await
			}
		})
	}

	pub(super) fn logical_scope_for_outer_demotion<'b>(
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
				| LogicalPlan::Distinct { input }
				| LogicalPlan::Repeat { input, .. }
				| LogicalPlan::Exchange { input, .. }
				| LogicalPlan::Sort { input, .. }
				| LogicalPlan::Window { input, .. }
				| LogicalPlan::Take { input, .. } => self.logical_scope_for_outer_demotion(input).await,
				LogicalPlan::Project {
					input,
					exprs,
					output_names,
				} => {
					let Some(input) = self.logical_scope_for_outer_demotion(input).await? else {
						return Ok(None);
					};
					Ok(Some(input.logical_project_passthrough(exprs, output_names)))
				}
				LogicalPlan::Aggregate {
					input,
					group_expr,
					aggr_expr,
				} => {
					let Some(input) = self.logical_scope_for_outer_demotion(input).await? else {
						return Ok(None);
					};
					Ok(Some(input.logical_aggregate_group_keys(group_expr, aggr_expr)))
				}
				LogicalPlan::Union { .. } | LogicalPlan::SetOpAll { .. } => Ok(None),
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
				// ADR 255: opaque-local — a recursive CTE's internal schema is not resolved for
				// outer-demotion column routing (sealed local node); its self-reference is a leaf.
				LogicalPlan::RecursiveCte { .. }
				| LogicalPlan::RecursiveRef { .. }
				| LogicalPlan::Explain { .. }
				| LogicalPlan::ExplainAnalyze { .. }
				| LogicalPlan::CreateTable { .. }
				| LogicalPlan::CreateLogicalType { .. }
				| LogicalPlan::CreateTableAsSelect { .. }
				| LogicalPlan::DropTable { .. }
				| LogicalPlan::Delete { .. }
				| LogicalPlan::Update { .. }
				| LogicalPlan::Insert { .. }
				| LogicalPlan::InsertSelect { .. }
				// ADR 279: opaque for outer-demotion column routing — demotion does not cross a deferred
				// EXISTS (it is resolved to a real Join afterward).
				| LogicalPlan::DeferredExists { .. }
				| LogicalPlan::DeferredScalarSubquery { .. } => Ok(None),
			}
		})
	}

	fn route_filter_conjuncts<'b>(
		&'b self,
		input: LogicalPlan,
		conjuncts: Vec<Expr>,
		path: Vec<JoinSideStep>,
	) -> Pin<Box<dyn Future<Output = Result<RoutedConjuncts, BindingError>> + Send + 'b>> {
		let conjuncts = conjuncts
			.into_iter()
			.map(|expr| RoutedExpr {
				original: expr.clone(),
				routed: expr,
			})
			.collect();
		self.route_filter_routed_conjuncts(input, conjuncts, path)
	}

	fn route_filter_routed_conjuncts<'b>(
		&'b self,
		input: LogicalPlan,
		conjuncts: Vec<RoutedExpr>,
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
					on_filter,
					null_safe_keys,
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
								on_filter,
								null_safe_keys,
								strategy_intent,
							},
							residual: routed_originals(conjuncts),
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
								on_filter,
								null_safe_keys,
								strategy_intent,
							},
							residual: routed_originals(conjuncts),
							receipts: Vec::new(),
						});
					};
					let mut left_conjuncts = Vec::new();
					let mut right_conjuncts = Vec::new();
					let mut residual = Vec::new();
					for conjunct in conjuncts {
						let sides =
							conjunct_column_sides(&conjunct.routed, &left_scope, &right_scope);
						if sides.left && !sides.right && !sides.ambiguous {
							left_conjuncts.push(conjunct);
						} else if sides.right && !sides.left && !sides.ambiguous {
							right_conjuncts.push(conjunct);
						} else {
							residual.push(conjunct.original);
						}
					}
					let mut left_path = path.clone();
					left_path.push(JoinSideStep::Left);
					let left = self
						.route_filter_routed_conjuncts(*left, left_conjuncts, left_path)
						.await?;
					let mut right_path = path;
					right_path.push(JoinSideStep::Right);
					let right = self
						.route_filter_routed_conjuncts(*right, right_conjuncts, right_path)
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
							on_filter,
							null_safe_keys,
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
					on_filter,
					null_safe_keys,
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
								on_filter,
								null_safe_keys,
								strategy_intent,
							},
							residual: routed_originals(conjuncts),
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
								on_filter,
								null_safe_keys,
								strategy_intent,
							},
							residual: routed_originals(conjuncts),
							receipts: Vec::new(),
						});
					};
					let mut rejected = NullRejectedSides::default();
					let mut proof_sides = Vec::with_capacity(conjuncts.len());
					let mut ownership_sides = Vec::with_capacity(conjuncts.len());
					for conjunct in &conjuncts {
						let sides =
							null_rejecting_sides(&conjunct.routed, &left_scope, &right_scope);
						rejected = rejected.union(sides);
						proof_sides.push(sides);
						ownership_sides.push(conjunct_column_sides(
							&conjunct.routed,
							&left_scope,
							&right_scope,
						));
					}
					let Some(to_kind) = demoted_join_kind(kind, rejected) else {
						return Ok(RoutedConjuncts {
							input: LogicalPlan::Join {
								left,
								right,
								kind,
								on,
								on_filter,
								null_safe_keys,
								strategy_intent,
							},
							residual: routed_originals(conjuncts),
							receipts: Vec::new(),
						});
					};
					let demotion_sides = demotion_owned_sides(kind, to_kind, rejected);
					let mut left_push = Vec::new();
					let mut right_push = Vec::new();
					let mut residual = Vec::new();
					let mut receipt_text = Vec::new();
					for ((conjunct, proof), ownership) in
						conjuncts.into_iter().zip(proof_sides).zip(ownership_sides)
					{
						if !proof.intersect(demotion_sides).is_empty() {
							receipt_text.push(format!("{:?}", conjunct.original));
						}
						if demotion_sides.left
							&& proof.left && ownership.left
							&& !ownership.right && !ownership.ambiguous
						{
							left_push.push(conjunct.routed);
						} else if demotion_sides.right
							&& proof.right && ownership.right
							&& !ownership.left && !ownership.ambiguous
						{
							right_push.push(conjunct.routed);
						} else {
							residual.push(conjunct.original);
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
							on_filter,
							null_safe_keys,
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
				LogicalPlan::Alias {
					input,
					alias,
					columns,
				} => {
					let Some(input_scope) = self.logical_scope_for_outer_demotion(&input).await?
					else {
						return Ok(RoutedConjuncts {
							input: LogicalPlan::Alias {
								input,
								alias,
								columns,
							},
							residual: routed_originals(conjuncts),
							receipts: Vec::new(),
						});
					};
					let alias_schema =
						alias_schema_for_outer_demotion(&input_scope.schema, &columns)?;
					let alias_scope = RelationScope::alias(alias.clone(), &alias_schema);
					let mut routed_conjuncts = Vec::new();
					let mut residual = Vec::new();
					for conjunct in conjuncts {
						match remap_expr_through_alias(
							&conjunct.routed,
							&alias,
							&alias_scope,
							&input_scope,
						)? {
							Some(routed) => routed_conjuncts.push(RoutedExpr {
								original: conjunct.original,
								routed,
							}),
							None => residual.push(conjunct.original),
						}
					}
					let mut wrapper_path = path;
					wrapper_path.push(JoinSideStep::Alias(alias.clone()));
					let routed = self
						.route_filter_routed_conjuncts(*input, routed_conjuncts, wrapper_path)
						.await?;
					residual.extend(routed.residual);
					Ok(RoutedConjuncts {
						input: LogicalPlan::Alias {
							input: Box::new(routed.input),
							alias,
							columns,
						},
						residual,
						receipts: routed.receipts,
					})
				}
				LogicalPlan::Project {
					input,
					exprs,
					output_names,
				} => {
					let Some(input_scope) = self.logical_scope_for_outer_demotion(&input).await?
					else {
						return Ok(RoutedConjuncts {
							input: LogicalPlan::Project {
								input,
								exprs,
								output_names,
							},
							residual: routed_originals(conjuncts),
							receipts: Vec::new(),
						});
					};
					let map = project_passthrough_map(&exprs, &output_names, &input_scope)?;
					let mut routed_conjuncts = Vec::new();
					let mut residual = Vec::new();
					for conjunct in conjuncts {
						match remap_expr_through_project(&conjunct.routed, &map, &input_scope)? {
							Some(routed) => routed_conjuncts.push(RoutedExpr {
								original: conjunct.original,
								routed,
							}),
							None => residual.push(conjunct.original),
						}
					}
					let mut wrapper_path = path;
					wrapper_path.push(JoinSideStep::Project);
					let routed = self
						.route_filter_routed_conjuncts(*input, routed_conjuncts, wrapper_path)
						.await?;
					residual.extend(routed.residual);
					Ok(RoutedConjuncts {
						input: LogicalPlan::Project {
							input: Box::new(routed.input),
							exprs,
							output_names,
						},
						residual,
						receipts: routed.receipts,
					})
				}
				LogicalPlan::Distinct { input } => {
					let routed = self
						.route_filter_routed_conjuncts(*input, conjuncts, path)
						.await?;
					Ok(RoutedConjuncts {
						input: LogicalPlan::Distinct {
							input: Box::new(routed.input),
						},
						residual: routed.residual,
						receipts: routed.receipts,
					})
				}
				LogicalPlan::Exchange {
					input,
					partition_count,
					key,
				} => {
					let routed = self
						.route_filter_routed_conjuncts(*input, conjuncts, path)
						.await?;
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
					let routed = self
						.route_filter_routed_conjuncts(*input, conjuncts, path)
						.await?;
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
				// ADR 255: opaque-local — filter conjuncts are not pushed into a sealed recursive CTE
				// fixpoint, and its self-reference is a leaf; routing stops here as for Scan.
				stopped @ (LogicalPlan::RecursiveCte { .. }
				| LogicalPlan::RecursiveRef { .. }
				| LogicalPlan::Scan { .. }
				| LogicalPlan::ScanNamed { .. }
				| LogicalPlan::Filter { .. }
				| LogicalPlan::Union { .. }
				| LogicalPlan::SetOpAll { .. }
				| LogicalPlan::Repeat { .. }
				| LogicalPlan::Aggregate { .. }
				| LogicalPlan::Sort { .. }
				| LogicalPlan::Window { .. }
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
				| LogicalPlan::InsertSelect { .. }
					// ADR 279: conjuncts don't route through a deferred EXISTS; keep them as a residual above.
					| LogicalPlan::DeferredExists { .. }
					| LogicalPlan::DeferredScalarSubquery { .. }) => Ok(RoutedConjuncts {
					input: stopped,
					residual: routed_originals(conjuncts),
					receipts: Vec::new(),
				}),
			}
		})
	}
}

fn routed_originals(conjuncts: Vec<RoutedExpr>) -> Vec<Expr> {
	conjuncts
		.into_iter()
		.map(|conjunct| conjunct.original)
		.collect()
}

fn remap_expr_through_alias(
	expr: &Expr,
	alias: &TableRef,
	alias_scope: &RelationScope,
	input_scope: &RelationScope,
) -> Result<Option<Expr>, BindingError> {
	remap_expr_columns(expr, &mut |column_ref| {
		let Some(output_ordinal) = alias_output_ordinal(column_ref, alias, alias_scope)? else {
			return Ok(None);
		};
		let input_name = input_scope.schema.field(output_ordinal).name().clone();
		let remapped = ColumnRef::unqualified(input_name);
		if input_scope
			.resolve_join_column(&remapped, "alias input")
			.ok()
			.flatten()
			.is_none()
		{
			return Ok(None);
		}
		Ok(Some(remapped))
	})
}

fn alias_output_ordinal(
	column_ref: &ColumnRef,
	alias: &TableRef,
	alias_scope: &RelationScope,
) -> Result<Option<usize>, BindingError> {
	if let Some(qualifier) = &column_ref.qualifier {
		if qualifier != alias {
			return Ok(None);
		}
		return Ok(unique_schema_ordinal(
			&alias_scope.schema,
			&column_ref.name.0,
		));
	}
	let resolved = match alias_scope.resolve_join_column(column_ref, "alias output") {
		Ok(Some(resolved)) => resolved,
		Ok(None) | Err(_) => return Ok(None),
	};
	Ok(Some(resolved.column.ordinal.0 as usize))
}

fn project_passthrough_map(
	exprs: &[Expr],
	output_names: &[Option<String>],
	input_scope: &RelationScope,
) -> Result<HashMap<String, Option<ColumnRef>>, BindingError> {
	let mut map = HashMap::new();
	for (idx, output_name) in output_names.iter().enumerate() {
		let Some(output_name) = output_name else {
			continue;
		};
		let candidate = match exprs.get(idx) {
			Some(Expr::Column(column_ref))
				if input_scope
					.resolve_join_column(column_ref, "project input")
					.ok()
					.flatten()
					.is_some() =>
			{
				Some(column_ref.clone())
			}
			_ => None,
		};
		if map.insert(output_name.clone(), candidate).is_some() {
			map.insert(output_name.clone(), None);
		}
	}
	Ok(map)
}

fn remap_expr_through_project(
	expr: &Expr,
	map: &HashMap<String, Option<ColumnRef>>,
	input_scope: &RelationScope,
) -> Result<Option<Expr>, BindingError> {
	remap_expr_columns(expr, &mut |column_ref| {
		if column_ref.qualifier.is_some() {
			return Ok(None);
		}
		let Some(Some(remapped)) = map.get(&column_ref.name.0) else {
			return Ok(None);
		};
		if input_scope
			.resolve_join_column(remapped, "project input")
			.ok()
			.flatten()
			.is_none()
		{
			return Ok(None);
		}
		Ok(Some(remapped.clone()))
	})
}

fn unique_schema_ordinal(schema: &SchemaRef, name: &str) -> Option<usize> {
	let mut matches = schema
		.fields()
		.iter()
		.enumerate()
		.filter(|(_, field)| field.name() == name)
		.map(|(idx, _)| idx);
	let first = matches.next()?;
	if matches.next().is_some() {
		return None;
	}
	Some(first)
}

fn remap_expr_columns<F>(expr: &Expr, remap: &mut F) -> Result<Option<Expr>, BindingError>
where
	F: FnMut(&ColumnRef) -> Result<Option<ColumnRef>, BindingError>,
{
	macro_rules! child {
		($expr:expr) => {
			match remap_expr_columns($expr, remap)? {
				Some(expr) => expr,
				None => return Ok(None),
			}
		};
	}
	macro_rules! binary {
		($ctor:expr, $left:expr, $right:expr) => {
			$ctor(Box::new(child!($left)), Box::new(child!($right)))
		};
	}

	Ok(Some(match expr {
		Expr::Column(column_ref) => {
			let Some(column_ref) = remap(column_ref)? else {
				return Ok(None);
			};
			Expr::Column(column_ref)
		}
		Expr::Literal(value) => Expr::Literal(value.clone()),
		Expr::UntypedNull => Expr::UntypedNull,
		Expr::LogicalAscription { expr, logical_type } => Expr::LogicalAscription {
			expr: Box::new(child!(expr)),
			logical_type: *logical_type,
		},
		Expr::SqlLogicalCast {
			expr,
			logical_name,
			try_cast,
		} => Expr::SqlLogicalCast {
			expr: Box::new(child!(expr)),
			logical_name: logical_name.clone(),
			try_cast: *try_cast,
		},
		Expr::SqlConvert {
			expr,
			target_logical_name,
		} => Expr::SqlConvert {
			expr: Box::new(child!(expr)),
			target_logical_name: target_logical_name.clone(),
		},
		Expr::Cast(inner, data_type) => Expr::Cast(Box::new(child!(inner)), data_type.clone()),
		Expr::TryCast(inner, data_type) => {
			Expr::TryCast(Box::new(child!(inner)), data_type.clone())
		}
		Expr::Add(left, right) => binary!(Expr::Add, left, right),
		Expr::Sub(left, right) => binary!(Expr::Sub, left, right),
		Expr::Mul(left, right) => binary!(Expr::Mul, left, right),
		Expr::Div(left, right) => binary!(Expr::Div, left, right),
		Expr::Eq(left, right) => binary!(Expr::Eq, left, right),
		Expr::Neq(left, right) => binary!(Expr::Neq, left, right),
		Expr::Lt(left, right) => binary!(Expr::Lt, left, right),
		Expr::Lte(left, right) => binary!(Expr::Lte, left, right),
		Expr::Gt(left, right) => binary!(Expr::Gt, left, right),
		Expr::Gte(left, right) => binary!(Expr::Gte, left, right),
		Expr::And(left, right) => binary!(Expr::And, left, right),
		Expr::Or(left, right) => binary!(Expr::Or, left, right),
		Expr::Not(inner) => Expr::Not(Box::new(child!(inner))),
		Expr::InList {
			expr,
			list,
			negated,
		} => {
			let expr = Box::new(child!(expr));
			let mut remapped_list = Vec::with_capacity(list.len());
			for item in list {
				remapped_list.push(child!(item));
			}
			Expr::InList {
				expr,
				list: remapped_list,
				negated: *negated,
			}
		}
		Expr::Like {
			expr,
			pattern,
			negated,
			case_insensitive,
		} => Expr::Like {
			expr: Box::new(child!(expr)),
			pattern: pattern.clone(),
			negated: *negated,
			case_insensitive: *case_insensitive,
		},
		Expr::Substring {
			input,
			from,
			length,
		} => Expr::Substring {
			input: Box::new(child!(input)),
			from: *from,
			length: *length,
		},
		Expr::Case {
			when_then,
			else_expr,
		} => {
			let mut remapped = Vec::with_capacity(when_then.len());
			for (when_expr, then_expr) in when_then {
				remapped.push((Box::new(child!(when_expr)), Box::new(child!(then_expr))));
			}
			let else_expr = match else_expr {
				Some(expr) => Some(Box::new(child!(expr))),
				None => None,
			};
			Expr::Case {
				when_then: remapped,
				else_expr,
			}
		}
		Expr::IsNull(inner) => Expr::IsNull(Box::new(child!(inner))),
		Expr::IsNotNull(inner) => Expr::IsNotNull(Box::new(child!(inner))),
		Expr::TimeBucket(duration, inner) => Expr::TimeBucket(*duration, Box::new(child!(inner))),
		Expr::AtTimeZone(inner, timezone) => {
			Expr::AtTimeZone(Box::new(child!(inner)), timezone.clone())
		}
		Expr::Extract(field, inner) => Expr::Extract(*field, Box::new(child!(inner))),
		Expr::DateTrunc(field, inner) => Expr::DateTrunc(*field, Box::new(child!(inner))),
	}))
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

fn split_logical_or_disjuncts(expr: &Expr) -> Vec<Expr> {
	match expr {
		Expr::Or(left, right) => {
			let mut disjuncts = split_logical_or_disjuncts(left);
			disjuncts.extend(split_logical_or_disjuncts(right));
			disjuncts
		}
		_ => vec![expr.clone()],
	}
}

fn combine_logical_and_conjuncts(conjuncts: Vec<Expr>) -> Option<Expr> {
	conjuncts
		.into_iter()
		.reduce(|left, right| Expr::And(Box::new(left), Box::new(right)))
}

fn combine_logical_or_disjuncts(disjuncts: Vec<Expr>) -> Option<Expr> {
	disjuncts
		.into_iter()
		.reduce(|left, right| Expr::Or(Box::new(left), Box::new(right)))
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

fn push_left_output_filter_conjuncts(input: LogicalPlan, mut conjuncts: Vec<Expr>) -> LogicalPlan {
	if conjuncts.is_empty() {
		return input;
	}
	match input {
		LogicalPlan::Exchange {
			input,
			partition_count,
			key,
		} => LogicalPlan::Exchange {
			input: Box::new(push_left_output_filter_conjuncts(*input, conjuncts)),
			partition_count,
			key,
		},
		LogicalPlan::Filter { input, predicate } => {
			conjuncts.extend(split_logical_and_conjuncts(&predicate));
			push_left_output_filter_conjuncts(*input, conjuncts)
		}
		other => push_logical_filter_conjuncts(other, conjuncts),
	}
}

fn wrap_residual_filter(input: LogicalPlan, residual: Vec<Expr>) -> LogicalPlan {
	push_logical_filter_conjuncts(input, residual)
}

fn extract_common_or_bridging_keys(
	expr: &Expr,
	left_scope: &RelationScope,
	right_scope: &RelationScope,
) -> Option<(Vec<(Expr, Expr)>, Option<Expr>)> {
	let disjuncts = split_logical_or_disjuncts(expr);
	if disjuncts.len() < 2 {
		return None;
	}

	let mut branches = Vec::with_capacity(disjuncts.len());
	for disjunct in disjuncts {
		let conjuncts = split_logical_and_conjuncts(&disjunct);
		let keys = conjuncts
			.iter()
			.filter_map(|conjunct| oriented_key_for_conjunct(conjunct, left_scope, right_scope))
			.collect::<Vec<_>>();
		if keys.is_empty() {
			return None;
		}
		branches.push((conjuncts, keys));
	}

	let mut common_keys = Vec::new();
	for key in &branches[0].1 {
		if common_keys
			.iter()
			.any(|existing| join_key_pair_same(existing, key))
		{
			continue;
		}
		if branches[1..].iter().all(|(_, keys)| {
			keys.iter()
				.any(|candidate| join_key_pair_same(candidate, key))
		}) {
			common_keys.push(key.clone());
		}
	}
	if common_keys.is_empty() {
		return None;
	}

	let mut residuals = Vec::with_capacity(branches.len());
	for (mut conjuncts, _) in branches {
		for key in &common_keys {
			let index = conjuncts.iter().position(|conjunct| {
				oriented_key_for_conjunct(conjunct, left_scope, right_scope)
					.is_some_and(|candidate| join_key_pair_same(&candidate, key))
			})?;
			conjuncts.remove(index);
		}
		let Some(residual) = combine_logical_and_conjuncts(conjuncts) else {
			return Some((common_keys, None));
		};
		residuals.push(residual);
	}

	Some((common_keys, combine_logical_or_disjuncts(residuals)))
}

fn oriented_key_for_conjunct(
	conjunct: &Expr,
	left_scope: &RelationScope,
	right_scope: &RelationScope,
) -> Option<(Expr, Expr)> {
	let Expr::Eq(left, right) = conjunct else {
		return None;
	};
	orient_bridging_equi(left.as_ref(), right.as_ref(), left_scope, right_scope)
}

fn push_join_key_if_absent(keys: &mut Vec<(Expr, Expr)>, pair: (Expr, Expr)) {
	if !keys
		.iter()
		.any(|existing| join_key_pair_same(existing, &pair))
	{
		keys.push(pair);
	}
}

fn join_key_pair_same(left: &(Expr, Expr), right: &(Expr, Expr)) -> bool {
	expr_column_same(&left.0, &right.0) && expr_column_same(&left.1, &right.1)
}

fn expr_column_same(left: &Expr, right: &Expr) -> bool {
	matches!((left, right), (Expr::Column(left), Expr::Column(right)) if left == right)
}

fn on_clause_residual_unsupported() -> BindingError {
	BindingError::UnsupportedExpression {
		message:
			"unsupported ON-clause residual: requires side-local predicate on null-supplying input"
				.to_string(),
	}
}

/// ADR 282: does any column in `expr` satisfy `predicate`? A focused expression walker reused by the
/// deferred-scalar filter pushdown (qualifier match) and the deferred-scalar correlation classifier
/// (outer-ownership match). Mirrors [`conjunct_column_sides`]'s variant coverage and is exhaustive, so a
/// new `Expr` variant forces an update here too.
fn expr_any_column<F: Fn(&ColumnRef) -> bool>(expr: &Expr, predicate: &F) -> bool {
	match expr {
		Expr::Column(column_ref) => predicate(column_ref),
		Expr::Literal(_) | Expr::UntypedNull => false,
		Expr::LogicalAscription { expr, .. }
		| Expr::SqlLogicalCast { expr, .. }
		| Expr::SqlConvert { expr, .. } => expr_any_column(expr, predicate),
		Expr::Cast(inner, _)
		| Expr::TryCast(inner, _)
		| Expr::Not(inner)
		| Expr::IsNull(inner)
		| Expr::IsNotNull(inner)
		| Expr::TimeBucket(_, inner)
		| Expr::AtTimeZone(inner, _)
		| Expr::Extract(_, inner)
		| Expr::DateTrunc(_, inner)
		| Expr::Substring { input: inner, .. } => expr_any_column(inner, predicate),
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
		| Expr::Or(left, right) => expr_any_column(left, predicate) || expr_any_column(right, predicate),
		Expr::InList { expr, list, .. } => {
			expr_any_column(expr, predicate)
				|| list.iter().any(|item| expr_any_column(item, predicate))
		}
		Expr::Like { expr, .. } => expr_any_column(expr, predicate),
		Expr::Case {
			when_then,
			else_expr,
		} => {
			when_then.iter().any(|(when_expr, then_expr)| {
				expr_any_column(when_expr, predicate) || expr_any_column(then_expr, predicate)
			}) || else_expr
				.as_ref()
				.is_some_and(|else_expr| expr_any_column(else_expr, predicate))
		}
	}
}

fn demoted_join_kind(kind: JoinKind, rejected: NullRejectedSides) -> Option<JoinKind> {
	match kind {
		JoinKind::LeftOuter if rejected.right => Some(JoinKind::Inner),
		JoinKind::RightOuter if rejected.left => Some(JoinKind::Inner),
		JoinKind::FullOuter if rejected.left && rejected.right => Some(JoinKind::Inner),
		JoinKind::FullOuter if rejected.left => Some(JoinKind::LeftOuter),
		JoinKind::FullOuter if rejected.right => Some(JoinKind::RightOuter),
		JoinKind::Inner
		| JoinKind::Cross
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
		Expr::Or(left, right) => null_rejecting_sides(left, left_scope, right_scope)
			.intersect(null_rejecting_sides(right, left_scope, right_scope)),
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
		| Expr::Not(_)
		| Expr::InList { .. }
		| Expr::Like { .. }
		| Expr::Substring { .. }
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

/// ADR 270: orient a cross-side equality `l = r` of direct columns into a `(left_input, right_input)`
/// join-key pair, or `None` if it is not a clean bridge — a non-column operand, the same side on both, or an
/// ambiguous/unresolved column (`column_side` returns `None` for those, so an ambiguous unqualified equality
/// never silently picks a side).
fn orient_bridging_equi(
	left_expr: &Expr,
	right_expr: &Expr,
	left_scope: &RelationScope,
	right_scope: &RelationScope,
) -> Option<(Expr, Expr)> {
	let (Expr::Column(left_col), Expr::Column(right_col)) = (left_expr, right_expr) else {
		return None;
	};
	match (
		column_side_with_type(left_col, left_scope, right_scope),
		column_side_with_type(right_col, left_scope, right_scope),
	) {
		(Some((LogicalJoinSide::Left, left_type)), Some((LogicalJoinSide::Right, right_type)))
			if cross_equi_join_key_type_supported(&left_type, &right_type) =>
		{
			Some((left_expr.clone(), right_expr.clone()))
		}
		(Some((LogicalJoinSide::Right, right_type)), Some((LogicalJoinSide::Left, left_type)))
			if cross_equi_join_key_type_supported(&left_type, &right_type) =>
		{
			Some((right_expr.clone(), left_expr.clone()))
		}
		_ => None,
	}
}

fn column_side_with_type(
	column_ref: &ColumnRef,
	left_scope: &RelationScope,
	right_scope: &RelationScope,
) -> Option<(LogicalJoinSide, DataType)> {
	let left = left_scope.resolve_join_column(column_ref, "left").ok()?;
	let right = right_scope.resolve_join_column(column_ref, "right").ok()?;
	match (left, right) {
		(Some(left), None) => Some((LogicalJoinSide::Left, left.column.data_type)),
		(None, Some(right)) => Some((LogicalJoinSide::Right, right.column.data_type)),
		(Some(_), Some(_)) | (None, None) => None,
	}
}

fn cross_equi_join_key_type_supported(left: &DataType, right: &DataType) -> bool {
	if left != right {
		return false;
	}
	match left {
		DataType::Boolean
		| DataType::Int8
		| DataType::Int16
		| DataType::Int32
		| DataType::Int64
		| DataType::UInt8
		| DataType::UInt16
		| DataType::UInt32
		| DataType::UInt64
		| DataType::Date32
		| DataType::Utf8 => true,
		DataType::Timestamp(TimeUnit::Millisecond, Some(timezone))
		| DataType::Timestamp(TimeUnit::Microsecond, Some(timezone))
		| DataType::Timestamp(TimeUnit::Nanosecond, Some(timezone)) => timezone.as_ref() == "UTC",
		_ => false,
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
		| Expr::DateTrunc(_, inner)
		| Expr::Substring { input: inner, .. } => conjunct_column_sides(inner, left_scope, right_scope),
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

/// ADR 279: one classified conjunct of a deferred correlated EXISTS — either a correlated equality lifted to a
/// join key (oriented outer-first) or an inner-only/constant predicate kept as an inner residual.
pub(super) enum DeferredExistsConjunct {
	Correlated { outer_key: Expr, inner_key: Expr },
	InnerResidual(Expr),
}

/// ADR 279: classify a deferred correlated-EXISTS predicate against the bound outer (`left`) + inner (`right`)
/// scopes (catalog-aware via [`column_side`]) — the binder-side analogue of the frontend's
/// `classify_exists_conjunct`, but resolving *unqualified* columns by ownership. Ambiguous, outer-only, or a
/// non-column-equi correlation are rejected (the safe-resolution invariant — never guess).
pub(super) fn classify_deferred_exists_predicate(
	predicate: &Expr,
	outer_scope: &RelationScope,
	inner_scope: &RelationScope,
) -> Result<DeferredExistsConjunct, BindingError> {
	let sides = conjunct_column_sides(predicate, outer_scope, inner_scope);
	if sides.ambiguous {
		return Err(BindingError::UnsupportedExpression {
			message: "ambiguous correlated EXISTS column reference".to_string(),
		});
	}
	match (sides.left, sides.right) {
		// No outer reference → an inner-only (or constant) predicate; keep it on the inner side.
		(false, _) => Ok(DeferredExistsConjunct::InnerResidual(predicate.clone())),
		// References both sides → must be a simple correlated equality (one outer column = one inner column).
		(true, true) => {
			let Expr::Eq(left, right) = predicate else {
				return Err(BindingError::UnsupportedExpression {
					message: "correlated EXISTS predicate must be a column equality".to_string(),
				});
			};
			match (
				column_only_side(left, outer_scope, inner_scope),
				column_only_side(right, outer_scope, inner_scope),
			) {
				(Some(LogicalJoinSide::Left), Some(LogicalJoinSide::Right)) => {
					Ok(DeferredExistsConjunct::Correlated {
						outer_key: (**left).clone(),
						inner_key: (**right).clone(),
					})
				}
				(Some(LogicalJoinSide::Right), Some(LogicalJoinSide::Left)) => {
					Ok(DeferredExistsConjunct::Correlated {
						outer_key: (**right).clone(),
						inner_key: (**left).clone(),
					})
				}
				_ => Err(BindingError::UnsupportedExpression {
					message: "unsupported correlated EXISTS predicate".to_string(),
				}),
			}
		}
		// References only the outer query → cannot correlate to the inner subquery.
		(true, false) => Err(BindingError::UnsupportedExpression {
			message: "correlated EXISTS predicate references only the outer query".to_string(),
		}),
	}
}

fn column_only_side(
	expr: &Expr,
	outer_scope: &RelationScope,
	inner_scope: &RelationScope,
) -> Option<LogicalJoinSide> {
	match expr {
		Expr::Column(column_ref) => column_side(column_ref, outer_scope, inner_scope),
		_ => None,
	}
}

/// ADR 282: inner-first column ownership for a deferred scalar subquery's correlation. SQL subquery scoping
/// resolves an unqualified column in the INNER scope first (it shadows the outer), so a column resolvable in
/// `inner_scope` is inner even when the same relation also appears in `outer_scope` (q17's inner `lineitem`
/// shadows the outer `lineitem` — symmetric `column_side` would wrongly flag `l_partkey` ambiguous). A column
/// resolvable only in `outer_scope` is the correlated outer ref. `Left` = outer, `Right` = inner.
fn scalar_column_side(
	column_ref: &ColumnRef,
	outer_scope: &RelationScope,
	inner_scope: &RelationScope,
) -> Option<LogicalJoinSide> {
	if inner_scope
		.resolve_join_column(column_ref, "right")
		.ok()
		.flatten()
		.is_some()
	{
		Some(LogicalJoinSide::Right)
	} else if outer_scope
		.resolve_join_column(column_ref, "left")
		.ok()
		.flatten()
		.is_some()
	{
		Some(LogicalJoinSide::Left)
	} else {
		None
	}
}

/// ADR 282: classify a deferred correlated-scalar predicate by inner-first ownership — the binder-side
/// analogue of [`classify_deferred_exists_predicate`]. A predicate that references no outer-owned column
/// (inner-only or constant, e.g. `dim.active = true`) is kept as an inner residual exactly as the EXISTS
/// classifier does (review parity fix — the simple-column-equality requirement is only for predicates that
/// actually correlate). A predicate that references the outer must be a simple column equality with one
/// outer-owned side and one inner-owned side, which becomes a correlation key (outer-first); anything else
/// (outer-only, non-equi, non-column) rejects as out of scope.
pub(super) fn classify_deferred_scalar_predicate(
	predicate: &Expr,
	outer_scope: &RelationScope,
	inner_scope: &RelationScope,
) -> Result<DeferredExistsConjunct, BindingError> {
	let references_outer = expr_any_column(predicate, &|column_ref| {
		matches!(
			scalar_column_side(column_ref, outer_scope, inner_scope),
			Some(LogicalJoinSide::Left)
		)
	});
	if !references_outer {
		// No outer reference → an inner-only (or constant) predicate; keep it on the inner side.
		return Ok(DeferredExistsConjunct::InnerResidual(predicate.clone()));
	}
	// References the outer → must be a simple correlated equality (one outer column = one inner column).
	if let Expr::Eq(left, right) = predicate {
		let left_side = match left.as_ref() {
			Expr::Column(column_ref) => scalar_column_side(column_ref, outer_scope, inner_scope),
			_ => None,
		};
		let right_side = match right.as_ref() {
			Expr::Column(column_ref) => scalar_column_side(column_ref, outer_scope, inner_scope),
			_ => None,
		};
		match (left_side, right_side) {
			(Some(LogicalJoinSide::Left), Some(LogicalJoinSide::Right)) => {
				return Ok(DeferredExistsConjunct::Correlated {
					outer_key: (**left).clone(),
					inner_key: (**right).clone(),
				});
			}
			(Some(LogicalJoinSide::Right), Some(LogicalJoinSide::Left)) => {
				return Ok(DeferredExistsConjunct::Correlated {
					outer_key: (**right).clone(),
					inner_key: (**left).clone(),
				});
			}
			_ => {}
		}
	}
	Err(BindingError::UnsupportedExpression {
		message: "unsupported correlated scalar subquery predicate".to_string(),
	})
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
		ScalarValue::Decimal128 { value, .. } => value.is_none(),
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
